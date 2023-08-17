FROM golang:1.18 AS build-env
ADD ./  /go/src/github.com/iyacontrol/fluent-bit-clickhouse
WORKDIR /go/src/github.com/iyacontrol/fluent-bit-clickhouse
RUN go build -buildmode=c-shared -o clickhouse.so .

FROM fluent/fluent-bit:2.1.8
COPY --from=build-env /go/src/github.com/iyacontrol/fluent-bit-clickhouse/clickhouse.so /fluent-bit/
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf", "-e", "/fluent-bit/clickhouse.so"]