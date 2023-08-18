package out

const (
	DefaultWriteTimeout string = "20"
	DefaultReadTimeout  string = "10"

	DefaultBatchSize int = 1024
	DefaultFlushTime int = 10
)

var (
	insertSQL       = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, log, ts, trace, level, type, msg, req, ip, latency, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertSQLForReq = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, ts, trace, method, path, action, query, user_agent, status, form, ip, latency, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertSQLForSql = "INSERT INTO %s.%s(date, cluster, namespace, app, pod_name, container_name, host, ts, trace, sql, file, ms, rows, ip, user_id, username) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

var Client = NewClickHouseClient()
