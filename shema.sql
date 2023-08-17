--CREATE DATABASE IF NOT EXISTS scmp;
--CREATE TABLE IF NOT EXISTS scmp.logs(date Date DEFAULT toDate(0),cluster String,namespace String,app String,pod_name String,container_name String,host String,type String,trace String,level String,msg String,log String, ip String, req String, ts DateTime) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(date) ORDER BY (cluster, namespace, app, pod_name, container_name, host,ts);
--CREATE TABLE IF NOT EXISTS scmp.logs_all(date Date DEFAULT toDate(0),cluster String,namespace String,app String,pod_name String,container_name String,host String,log String,ts DateTime) ENGINE = Distributed(scmp-hulk-logs, scmp, logs);

CREATE TABLE park.logs
(
    `date`           Date DEFAULT toDate(0),
    `level`          String,
    `namespace`      String,
    `app`            String,
    `container_name` String,
    `type`           String,
    `req`            String,
    `latency`        Float64,
    `trace`          String,
    "user_id"        String,
    "username"       String,
    `msg`            String,
    `ip`             String,
    `host`           String,
    `cluster`        String,
    `pod_name`       String,
    `ts`             DateTime,
    `log`            String,
    INDEX idx_user_id user_id TYPE ngrambf_v1(3,
                         256,
                         2,
                         0) GRANULARITY 4,
    INDEX idx_username username TYPE ngrambf_v1(3,
                         256,
                         2,
                         0) GRANULARITY 4,
    INDEX idx_ip ip TYPE ngrambf_v1(3,
                         256,
                         2,
                         0) GRANULARITY 4,
    INDEX idx_req req TYPE ngrambf_v1(3,
                           256,
                           2,
                           0) GRANULARITY 4,
    INDEX idx_trace trace TYPE ngrambf_v1(3,
                               256,
                               2,
                               0) GRANULARITY 4
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(date)
        ORDER BY (cluster,
                  namespace,
                  app,
                  pod_name,
                  container_name,
                  host,
                  ts)
        SETTINGS index_granularity = 8192;

CREATE TABLE park.sql_log
(
    `date`           Date DEFAULT toDate(0),
    `cluster`        String,
    `namespace`      String,
    `app`            String,
    `container_name` String,
    `sql`            String,
    `file`           String,
    `ms`             Float64,
    `rows`           Int64,
    `trace`          String,
    "user_id"        String,
    "username"       String,
    `ip`             String,
    `host`           String,
    `pod_name`       String,
    `ts`             DateTime,
    INDEX idx_user_id user_id TYPE ngrambf_v1(3,
                                   256,
                                   2,
                                   0) GRANULARITY 4,
    INDEX idx_username username TYPE ngrambf_v1(3,
                                     256,
                                     2,
                                     0) GRANULARITY 4,
    INDEX idx_ms ms TYPE minmax GRANULARITY 4,
    INDEX idx_trace trace TYPE ngrambf_v1(3,
                               256,
                               2,
                               0) GRANULARITY 4
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(date)
        ORDER BY (ts)
        SETTINGS index_granularity = 8192;

CREATE TABLE park.req_log
(
    `date`           Date DEFAULT toDate(0),
    `namespace`      String,
    `app`            String,
    `container_name` String,
    `method`         String,
    `path`           String,
    `action`         String,
    `query`          String,
    `user_agent`     String,
    `status`         String,
    `form`           String,
    "user_id"        String,
    "username"       String,
    `ip`             String,
    `latency`        Float64,
    `host`           String,
    `cluster`        String,
    `pod_name`       String,
    `trace`          String,
    `ts`             DateTime,
    INDEX idx_user_id user_id TYPE ngrambf_v1(3,
                                   256,
                                   2,
                                   0) GRANULARITY 4,
    INDEX idx_username username TYPE ngrambf_v1(3,
                                     256,
                                     2,
                                     0) GRANULARITY 4,
    INDEX idx_path path TYPE ngrambf_v1(3,
                             256,
                             2,
                             0) GRANULARITY 4,
    INDEX idx_action action TYPE ngrambf_v1(3,
                                     256,
                                     2,
                                     0) GRANULARITY 4,
    INDEX idx_ms latency TYPE minmax GRANULARITY 4,
    INDEX idx_trace trace TYPE ngrambf_v1(3,
                               256,
                               2,
                               0) GRANULARITY 4
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(date)
        ORDER BY (ts)
        SETTINGS index_granularity = 8192;