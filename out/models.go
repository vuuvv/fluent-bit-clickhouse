package out

import (
	"database/sql"
	"sync"
	"time"
)

type BaseLog struct {
	Cluster   string
	Namespace string
	App       string
	Pod       string
	Container string
	Trace     string
	Ip        string    `json:"ip"`
	Host      string    `json:"host"`
	Latency   float64   `json:"latency"`
	UserId    string    `json:"userId"`
	Username  string    `json:"username"`
	Ts        time.Time `json:"-"`
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

type ClickHouseClient struct {
	client        *sql.DB
	database      string
	table         string
	batchSize     int
	flushTime     int
	lastFlushTime time.Time

	rw        sync.RWMutex
	buffer    []Log
	reqBuffer []ReqLog
	sqlBuffer []SqlLog
}

func NewClickHouseClient() *ClickHouseClient {
	return &ClickHouseClient{
		lastFlushTime: time.Now(),
	}
}

func (this *ClickHouseClient) Clean() {
	Client.buffer = make([]Log, 0)
	Client.sqlBuffer = make([]SqlLog, 0)
	Client.reqBuffer = make([]ReqLog, 0)
	Client.lastFlushTime = time.Now()
}
