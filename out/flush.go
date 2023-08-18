package out

import "C"
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/fluent/fluent-bit-go/output"
	"k8s.io/klog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (this *ClickHouseClient) Flush(dec *output.FLBDecoder) int {
	client := Client.client

	// ping
	if err := client.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			klog.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			klog.Errorf("Failed to ping clickhouse: %v", err)
		}
		return output.FLB_ERROR
	}

	var ret int
	var timestampData interface{}
	var mapData map[interface{}]interface{}

	for {
		//// decode the msgpack data
		//err = dec.Decode(&m)
		//if err != nil {
		//	break
		//}

		ret, timestampData, mapData = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Get a slice and their two entries: timestamp and map
		//slice := reflect.ValueOf(m)
		//timestampData := slice.Index(0).Interface()
		//data := slice.Index(1)

		//timestamp, ok := timestampData.Interface().(uint64)
		//if !ok {
		//	klog.Errorf("Unable to convert timestamp: %+v", timestampData)
		//	return output.FLB_ERROR
		//}
		var timestamp time.Time
		switch t := timestampData.(type) {
		case output.FLBTime:
			timestamp = timestampData.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			//klog.Infof("msg", "timestamp isn't known format. Use current time.")
			timestamp = time.Now()
		}

		// Convert slice data to a real map and iterate
		//mapData := data.Interface().(map[interface{}]interface{})
		flattenData, err := Flatten(mapData, "", UnderscoreStyle)
		if err != nil {
			break
		}

		log := Log{}
		for k, v := range flattenData {
			value := ""
			switch t := v.(type) {
			case string:
				value = t
			case []byte:
				value = string(t)
			default:
				value = fmt.Sprintf("%v", v)
			}

			switch k {
			case "cluster":
				log.Cluster = value
			case "kubernetes_namespace_name":
				log.Namespace = value
			case "kubernetes_labels_app":
				log.App = value
			case "kubernetes_labels_k8s-app":
				log.App = value
			case "kubernetes_pod_name":
				log.Pod = value
			case "kubernetes_container_name":
				log.Container = value
			case "kubernetes_host":
				log.Host = value
			case "log":
				log.Log = ClearCriOFormat(value)
			}
		}

		if log.App == "" {
			break
		}

		//log.Ts = time.Unix(int64(timestamp), 0)
		log.Ts = timestamp

		// json parse
		if strings.HasPrefix(log.Log, "{") && strings.HasSuffix(log.Log, "}") {
			obj := &LogJson{}
			err = json.Unmarshal([]byte(log.Log), obj)
			if err == nil {
				log.Level = obj.Level
				log.Trace = obj.Trace
				log.Msg = obj.Msg
				log.Type = obj.Type
				log.Req = obj.Req
				log.Ip = obj.Ip
				log.Latency = obj.Latency
				log.UserId = strconv.FormatInt(obj.UserId, 10)
				log.Username = obj.Username

				switch log.Type {
				case "req":
					reqLog := ReqLog{}
					err = json.Unmarshal([]byte(log.Log), &reqLog)
					if err == nil {
						reqLog.BaseLog = log.BaseLog
					}
					this.reqBuffer = append(this.reqBuffer, reqLog)
					klog.Info("req log ", log.Log, " ", len(this.reqBuffer))
				case "sql":
					sqlLog := SqlLog{}
					sqlLog.BaseLog = log.BaseLog
					ParseSqlLog(log.Log, &sqlLog)
					this.sqlBuffer = append(this.sqlBuffer, sqlLog)
					klog.Info("sql log ", log.Log, " ", len(this.sqlBuffer))
				}
			}
			// 如果有错误就不处理
		}

		this.buffer = append(this.buffer, log)
	}

	// sink data
	if !this.CanFlush() {
		return output.FLB_OK
	}

	if !this.doFlush() {
		return output.FLB_ERROR
	}

	Client.Clean()

	return output.FLB_OK
}

func ClearCriOFormat(str string) (ret string) {
	regex := "^([^ ]+) (stdout|stderr) ([^ ]*)"
	r, err := regexp.Compile(regex)
	if err != nil {
		return str
	}

	return strings.TrimSpace(r.ReplaceAllString(str, ""))
}

func ParseSqlLog(str string, log *SqlLog) {
	index := strings.LastIndex(str, "]")
	if index < 0 {
		log.Sql = str
		return
	}
	log.Sql = strings.TrimSpace(str[index+1:])

	pattern := `(.*?):(\d+).*?\[([\d.]+)ms\].*?\[rows:(\d+)\].*`

	// 编译正则表达式
	re := regexp.MustCompile(pattern)

	// 查找匹配项
	matches := re.FindStringSubmatch(strings.ReplaceAll(str[:index+1], "\n", " "))

	if len(matches) == 5 {
		log.File = fmt.Sprintf("%s:%s", strings.TrimSpace(matches[1]), matches[2])
		log.Ms, _ = strconv.ParseFloat(matches[3], 64)
		log.Rows, _ = strconv.ParseInt(matches[4], 10, 64)
	}
}

func (this *ClickHouseClient) doFlush() (ret bool) {
	tx, err := this.client.Begin()
	if err != nil {
		klog.Errorf("begin transaction failure: %s", err.Error())
		return false
	}

	defer func() {
		// commit and record metrics
		if err = tx.Commit(); err != nil {
			klog.Errorf("commit failed failure: %s", err.Error())
			ret = false
		}
	}()

	// build statements
	return this.FlushAll(tx) && this.FlushReq(tx) && this.FlushSql(tx)
}

func (this *ClickHouseClient) FlushAll(tx *sql.Tx) bool {
	if len(this.buffer) == 0 {
		return true
	}

	s := this.GetSql()
	// build statements
	smt, err := tx.Prepare(s)
	if err != nil {
		klog.Errorf("FlushAll prepare statement failure: %s", err.Error())
		return false
	}
	for _, l := range this.buffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Log, l.Ts, l.Trace, l.Level, l.Type, l.Msg, l.Req, l.Ip,
			l.Latency, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("FlushAll statement exec failure: %s", err.Error())
			return false
		}
	}
	return true
}

func (this *ClickHouseClient) FlushReq(tx *sql.Tx) bool {
	if len(this.reqBuffer) == 0 {
		return true
	}

	s := this.GetReqSql()

	smt, err := tx.Prepare(s)
	if err != nil {
		klog.Errorf("FlushReq prepare statement failure: %s", err.Error())
		return false
	}
	for _, l := range this.reqBuffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Ts, l.Trace, l.Method, l.Path, l.Action, l.Query, l.UserAgent, l.Status, l.Form,
			l.Ip, l.Latency, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("FlushReq statement exec failure: %s", err.Error())
			return false
		}
	}
	return true
}

func (this *ClickHouseClient) FlushSql(tx *sql.Tx) bool {
	if len(this.sqlBuffer) == 0 {
		return true
	}
	s := this.GetSqlSql()

	smt, err := tx.Prepare(s)
	if err != nil {
		klog.Errorf("FlushSql prepare statement failure: %s", err.Error())
		return false
	}
	for _, l := range this.sqlBuffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Ts, l.Trace, l.Sql, l.File, l.Ms, l.Rows,
			l.Ip, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("FlushSql statement exec failure: %s", err.Error())
			return false
		}
	}
	return true
}

// CanFlush 是否要把数据写入数据库,当数据量大于batchSize或者距离上次写入时间超过flushTime时,返回true
func (this *ClickHouseClient) CanFlush() bool {
	deltaSeconds := int(time.Now().Sub(this.lastFlushTime))
	return len(this.buffer) >= this.batchSize || deltaSeconds >= this.flushTime
}

func (this *ClickHouseClient) GetSql() (sql string) {
	return fmt.Sprintf(insertSQL, this.database, this.table)
}

func (this *ClickHouseClient) GetReqSql() (sql string) {
	return fmt.Sprintf(insertSQLForReq, this.database, "req_log")
}

func (this *ClickHouseClient) GetSqlSql() (sql string) {
	return fmt.Sprintf(insertSQLForSql, this.database, "sql_log")
}
