package main

import (
	"C"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	//"reflect"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	//"github.com/ugorji/go/codec"
	"github.com/ClickHouse/clickhouse-go"
	"k8s.io/klog"
)

var (
	client *sql.DB

	database      string
	table         string
	batchSize     int
	flushTime     int
	lastFlushTime time.Time = time.Now()

	insertSQL       = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, log, ts, trace, level, type, msg, req, ip, latency, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertSQLForReq = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, ts, trace, method, path, action, query, user_agent, status, form, ip, latency, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertSQLForSql = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, ts, trace, sql, file, ms, rows, ip, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	rw        sync.RWMutex
	buffer    = make([]Log, 0)
	reqBuffer = make([]ReqLog, 0)
	sqlBuffer = make([]SqlLog, 0)
)

const (
	DefaultWriteTimeout string = "20"
	DefaultReadTimeout  string = "10"

	DefaultBatchSize int = 1024
	DefaultFlushTime int = 10
)

type BaseLog struct {
	Cluster   string
	Namespace string
	App       string
	Pod       string
	Container string
	Trace     string
	Ip        string `json:"ip"`
	Host      string
	Latency   float64 `json:"latency"`
	UserId    string  `json:"userId"`
	Username  string  `json:"username"`
	Ts        time.Time
}

type Log struct {
	BaseLog
	Log   string
	Ts    time.Time
	Trace string
	Level string
	Type  string
	Msg   string
	Req   string
	Ip    string
}

type LogJson struct {
	Ts       string  `json:"ts"`
	Trace    string  `json:"trace"`
	Level    string  `json:"level"`
	Type     string  `json:"type"`
	Msg      string  `json:"_msg"`
	Req      string  `json:"path"`
	Ip       string  `json:"ip"`
	Latency  float64 `json:"latency"`
	UserId   int64   `json:"user_id"`
	Username string  `json:"username"`
}

type ReqLog struct {
	BaseLog
	Method    string `json:"method"`
	Path      string `json:"path"`
	Action    string `json:"action"`
	Query     string `json:"query"`
	UserAgent string `json:"user_agent"`
	Status    string `json:"status"`
	Form      string `json:"form"`
}

type SqlLog struct {
	BaseLog
	Sql  string  `json:"sql"`
	File string  `json:"file"`
	Ms   float64 `json:"ms"`
	Rows int64   `json:"rows"`
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "clickhouse", "Clickhouse Output Plugin.!")
}

// ctx (context) pointer to fluentbit context (state/ c code)
//
//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	// init log
	//klog.InitFlags(nil)
	//flag.Set("stderrthreshold", "3")
	//flag.Parse()
	//
	//defer klog.Flush()

	// get config
	var host string
	if v := os.Getenv("CLICKHOUSE_HOST"); v != "" {
		host = v
	} else {
		klog.Error("you must set host of clickhouse!")
		return output.FLB_ERROR
	}

	var user string
	if v := os.Getenv("CLICKHOUSE_USER"); v != "" {
		user = v
	} else {
		klog.Error("you must set user of clickhouse!")
		return output.FLB_ERROR
	}

	var password string
	if v := os.Getenv("CLICKHOUSE_PASSWORD"); v != "" {
		password = v
	} else {
		klog.Error("you must set password of clickhouse!")
		return output.FLB_ERROR
	}

	if v := os.Getenv("CLICKHOUSE_DATABASE"); v != "" {
		database = v
	} else {
		klog.Error("you must set database of clickhouse!")
		return output.FLB_ERROR
	}

	if v := os.Getenv("CLICKHOUSE_TABLE"); v != "" {
		table = v
	} else {
		klog.Error("you must set table of clickhouse!")
		return output.FLB_ERROR
	}

	if v := os.Getenv("CLICKHOUSE_BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			klog.Infof("you set the default batch_size: %d", DefaultBatchSize)
			batchSize = DefaultBatchSize
		}
		batchSize = size
	} else {
		klog.Infof("you set the default batch_size: %d", DefaultBatchSize)
		batchSize = DefaultBatchSize
	}

	if v := os.Getenv("CLICKHOUSE_FLUSH_TIME"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			klog.Infof("you set the default flush_time: %d", DefaultFlushTime)
			batchSize = DefaultBatchSize
		}
		flushTime = size
	} else {
		klog.Infof("you set the default flush_time: %d", DefaultFlushTime)
		flushTime = DefaultBatchSize
	}

	var writeTimeout string
	if v := os.Getenv("CLICKHOUSE_WRITE_TIMEOUT"); v != "" {
		writeTimeout = v
	} else {
		klog.Infof("you set the default write_timeout: %s", DefaultWriteTimeout)
		writeTimeout = DefaultWriteTimeout
	}

	var readTimeout string
	if v := os.Getenv("CLICKHOUSE_READ_TIMEOUT"); v != "" {
		readTimeout = v
	} else {
		klog.Infof("you set the default read_timeout: %s", DefaultReadTimeout)
		readTimeout = DefaultReadTimeout
	}

	dsn := "tcp://" + host + "?username=" + user + "&password=" + password + "&database=" + database + "&write_timeout=" + writeTimeout + "&read_timeout=" + readTimeout

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		klog.Errorf("connecting to clickhouse: %v", err)
		return output.FLB_ERROR
	}

	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			klog.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			klog.Errorf("Failed to ping clickhouse: %v", err)
		}
		return output.FLB_ERROR
	}
	// ==
	client = db

	return output.FLB_OK
}

// FLBPluginFlush is called from fluent-bit when data need to be sent. is called from fluent-bit when data need to be sent.
//
//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	rw.Lock()
	defer rw.Unlock()

	// ping
	if err := client.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			klog.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			klog.Errorf("Failed to ping clickhouse: %v", err)
		}
		return output.FLB_ERROR
	}

	// prepare data
	//var h codec.Handle = new(codec.MsgpackHandle)

	//var b []byte
	//var m interface{}
	//var err error

	//b = C.GoBytes(data, length)
	//dec := codec.NewDecoderBytes(b, h)

	var ret int
	var timestampData interface{}
	var mapData map[interface{}]interface{}

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

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

			klog.Info("value ", k, " ", value)

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
				case "sql":
					sqlLog := SqlLog{}
					sqlLog.BaseLog = log.BaseLog

				}
			}
			// 如果有错误就不处理
		}

		buffer = append(buffer, log)
	}

	// sink data
	deltaSeconds := int(time.Now().Sub(lastFlushTime))
	if len(buffer) < batchSize && deltaSeconds < flushTime {
		return output.FLB_OK
	}

	allSql := fmt.Sprintf(insertSQL, database, table)
	reqSql := fmt.Sprintf(insertSQLForReq, database, "req_log")
	sqlSql := fmt.Sprintf(insertSQLForSql, database, "sql_log")

	//start := time.Now()
	// post them to db all at once
	tx, err := client.Begin()
	if err != nil {
		klog.Errorf("begin transaction failure: %s", err.Error())
		return output.FLB_ERROR
	}

	// build statements
	smt, err := tx.Prepare(allSql)
	if err != nil {
		klog.Errorf("prepare statement failure: %s", err.Error())
		return output.FLB_ERROR
	}
	for _, l := range buffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Log, l.Ts, l.Trace, l.Level, l.Type, l.Msg, l.Req, l.Ip,
			l.Latency, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("statement exec failure: %s", err.Error())
			return output.FLB_ERROR
		}
	}

	smt, err = tx.Prepare(reqSql)
	if err != nil {
		klog.Errorf("prepare statement failure: %s", err.Error())
		return output.FLB_ERROR
	}
	for _, l := range reqBuffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Ts, l.Trace, l.Method, l.Path, l.Action, l.Query, l.UserAgent, l.Status, l.Form,
			l.Ip, l.Latency, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("statement exec failure: %s", err.Error())
			return output.FLB_ERROR
		}
	}

	smt, err = tx.Prepare(sqlSql)
	if err != nil {
		klog.Errorf("prepare statement failure: %s", err.Error())
		return output.FLB_ERROR
	}
	for _, l := range sqlBuffer {
		// ensure tags are inserted in the same order each time
		// possibly/probably impacts indexing?
		_, err = smt.Exec(
			l.Ts, l.Cluster, l.Namespace, l.App, l.Pod, l.Container, l.Host,
			l.Ts, l.Trace, l.Sql, l.File, l.Ms, l.Rows,
			l.Ip, l.UserId, l.Username,
		)

		if err != nil {
			klog.Errorf("statement exec failure: %s", err.Error())
			return output.FLB_ERROR
		}
	}

	// commit and record metrics
	if err = tx.Commit(); err != nil {
		klog.Errorf("commit failed failure: %s", err.Error())
		return output.FLB_ERROR
	}

	//end := time.Now()
	//klog.Infof("Exported %d log to clickhouse in %s", len(buffer), end.Sub(start))

	buffer = make([]Log, 0)
	sqlBuffer = make([]SqlLog, 0)
	reqBuffer = make([]ReqLog, 0)
	lastFlushTime = time.Now()

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
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

func main() {
}
