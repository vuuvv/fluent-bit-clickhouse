package out

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/fluent/fluent-bit-go/output"
	"k8s.io/klog"
	"os"
	"strconv"
)

// Connect 初始化clickhouse的连接参数,并进行连接
func (this *ClickHouseClient) Connect() int {
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
		this.database = v
	} else {
		klog.Error("you must set database of clickhouse!")
		return output.FLB_ERROR
	}

	if v := os.Getenv("CLICKHOUSE_TABLE"); v != "" {
		this.table = v
	} else {
		klog.Error("you must set table of clickhouse!")
		return output.FLB_ERROR
	}

	if v := os.Getenv("CLICKHOUSE_BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			klog.Infof("you set the default batch_size: %d", DefaultBatchSize)
			this.batchSize = DefaultBatchSize
		}
		this.batchSize = size
	} else {
		klog.Infof("you set the default batch_size: %d", DefaultBatchSize)
		this.batchSize = DefaultBatchSize
	}

	if v := os.Getenv("CLICKHOUSE_FLUSH_TIME"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			klog.Infof("you set the default flush_time: %d", DefaultFlushTime)
			this.batchSize = DefaultBatchSize
		}
		this.flushTime = size
	} else {
		klog.Infof("you set the default flush_time: %d", DefaultFlushTime)
		this.flushTime = DefaultBatchSize
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

	//dsn := "tcp://" + host + "?username=" + user + "&password=" + password + "&database=" + client.database + "&write_timeout=" + writeTimeout + "&read_timeout=" + readTimeout
	dsn := fmt.Sprintf("tcp://%s?username=%s&password=%s&database=%s&write_timeout=%s&read_timeout=%s",
		host, user, password, this.database, writeTimeout, readTimeout,
	)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		klog.Errorf("connecting to clickhouse: %v", err)
		return output.FLB_ERROR
	}
	klog.Infof("connecting to clickhouse: %s", dsn)

	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			klog.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			klog.Errorf("Failed to ping clickhouse: %v", err)
		}
		return output.FLB_ERROR
	}
	// ==
	this.client = db

	return output.FLB_OK
}
