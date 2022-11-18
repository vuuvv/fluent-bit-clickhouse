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
    `msg`            String,
    `ip`             String,
    `host`           String,
    `cluster`        String,
    `pod_name`       String,
    `ts`             DateTime,
    `log`            String,
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
        SETTINGS index_granularity = 8192