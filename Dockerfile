FROM golang:1.24 AS builder
WORKDIR /go/src/volcano.sh/
ADD . volcano
RUN cd volcano && mkdir -p _output/bin && CC=gcc CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o _output/bin/vc-scheduler ./cmd/scheduler;

FROM alpine:latest
COPY --from=builder /go/src/volcano.sh/volcano/_output/bin/vc-scheduler /vc-scheduler
ENTRYPOINT ["/vc-scheduler"]
