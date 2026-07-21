# Distributed Log Aggregation & Analytics System

A distributed log pipeline built with Go, Apache Kafka, Prometheus, and Grafana. Logs are routed by severity through dedicated Kafka topics, validated with malformed messages funnelled into a Dead Letter Queue, and visualized in real time.

## Architecture

```
Producer (Go)
    │  generates mock HTTP access logs
    │  routes by severity → logs.{info,warn,error}
    │  injects ~1% malformed messages to exercise DLQ
    ▼
Kafka
    ├── logs.info
    ├── logs.warn
    ├── logs.error
    └── logs.dlq  ◄──── Consumer (on parse/validation failure)
         ▲                     │
         │         DLQ Consumer retries parseable messages
         │         permanently malformed → dropped
         │
Consumer (Go)
    │  fans out across 3 topics
    │  validates schema, forwards bad messages to DLQ
    │  writes severity-split log files
    │  exposes /metrics on :8080
    ▼
Prometheus  (scrapes :8080 every 5s)
    ▼
Grafana  (System Health dashboard, pre-provisioned)

Ingest API (Go)  ← external HTTP clients push logs via POST /ingest
    │  infers severity from status_code if omitted
    └──► logs.{info,warn,error}
```

## Services

| Service       | Description                                                          | Port |
|---------------|----------------------------------------------------------------------|------|
| producer      | Generates and publishes mock HTTP access logs to Kafka               | —    |
| consumer      | Consumes all severity topics, validates, metrics, writes log files   | 8080 |
| dlq-consumer  | Retries recoverable DLQ messages; drops permanently malformed ones   | —    |
| ingest-api    | HTTP gateway for external services to publish logs                   | 8082 |
| kafka         | Message broker (Confluent 7.6 + Zookeeper)                          | 9092 |
| prometheus    | Metrics store, scrapes consumer every 5s                             | 9090 |
| grafana       | Pre-provisioned System Health dashboard                              | 3000 |

## Tech Stack

| Component     | Tool                         |
|---------------|------------------------------|
| Services      | Go 1.22                      |
| Messaging     | Apache Kafka (Confluent 7.6) |
| Metrics store | Prometheus 2.51              |
| Visualization | Grafana 10.4                 |
| Orchestration | Docker Compose               |

---

## Prerequisites

- Docker and Docker Compose installed
- Ports `3000`, `8080`, `8082`, `9090`, `9092` free on your machine

---

## Running the Project

### 1. Clone and enter the directory

```bash
git clone <repo-url>
cd log-aggregator
```

### 2. Start all services

```bash
docker compose up --build
```

This starts: Zookeeper, Kafka, Producer, Consumer, DLQ Consumer, Ingest API, Prometheus, Grafana.

First run takes ~2 minutes to pull images and build Go services. Subsequent runs are fast.

### 3. Verify logs are flowing

```bash
docker compose logs -f consumer
```

You should see lines like:
```
[consumed] GET /api/users 200 143ms
[consumed] POST /api/orders 500 312ms
```

Log files are written to `./logs/` on the host, split by severity:

```
logs/
├── producer.log   # everything the producer published
├── info.log       # 2xx logs
├── warn.log       # 3xx / 4xx logs
├── error.log      # 5xx logs
└── dlq.log        # malformed or invalid messages
```

### 4. Open the dashboards

| Service          | URL                             | Credentials   |
|------------------|---------------------------------|---------------|
| Grafana          | http://localhost:3000           | admin / admin |
| Prometheus       | http://localhost:9090           | —             |
| Consumer metrics | http://localhost:8080/metrics   | —             |
| Ingest API       | http://localhost:8082           | —             |

In Grafana, navigate to **Dashboards → System Health** to see the live charts.

### 5. Stop the system

```bash
docker compose down
```

---

## Log Schema

Every log entry (produced, consumed, and accepted by the ingest API) follows this JSON shape:

```json
{
  "timestamp":       "2024-01-15T12:00:00Z",
  "level":           "info | warn | error",
  "service":         "auth-service | order-service | ...",
  "ip":              "203.0.113.42",
  "method":          "GET | POST | PUT | DELETE",
  "path":            "/api/users",
  "status_code":     200,
  "response_time_ms": 143
}
```

The consumer validates that `level`, `service`, and `status_code` are present and well-formed. Messages that fail validation are forwarded to `logs.dlq`.

---

## Endpoints

### Ingest API — `http://localhost:8082`

| Method | Endpoint   | Description                                        |
|--------|------------|----------------------------------------------------|
| POST   | `/ingest`  | Publish a log entry to the appropriate Kafka topic |
| GET    | `/health`  | Health check                                       |

**POST `/ingest` example:**

```bash
curl -X POST http://localhost:8082/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "service": "my-service",
    "method": "POST",
    "path": "/api/checkout",
    "status_code": 500,
    "response_time_ms": 312
  }'
```

`timestamp` defaults to now if omitted. `level` is inferred from `status_code` (5xx → error, 4xx → warn, otherwise → info) if omitted.

Response:
```json
{"status":"accepted","topic":"logs.error"}
```

### Consumer — `http://localhost:8080`

| Endpoint   | Description                                       |
|------------|---------------------------------------------------|
| `/metrics` | Prometheus metrics (scraped automatically every 5s) |

### Prometheus — `http://localhost:9090`

| Endpoint        | Description                          |
|-----------------|--------------------------------------|
| `/`             | Prometheus UI — run PromQL queries   |
| `/targets`      | Scrape status for the consumer       |
| `/graph`        | Query and graph metrics manually     |
| `/api/v1/query` | HTTP API for querying metrics        |

### Grafana — `http://localhost:3000`

| Page                       | Description                           |
|----------------------------|---------------------------------------|
| `/dashboards`              | All dashboards                        |
| `/d/logagg-system-health`  | System Health dashboard (pre-built)   |
| `/alerting`                | Alert rules configuration             |
| `/connections/datasources` | Prometheus datasource config          |

### Kafka — `localhost:9092`

```bash
# list topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# watch raw messages on a topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic logs.error \
  --from-beginning
```

Topics: `logs.info`, `logs.warn`, `logs.error`, `logs.dlq`

---

## Metrics Reference

All metrics are exposed on `http://localhost:8080/metrics` and stored in Prometheus.

| Metric                      | Type      | Labels                             | Description                                  |
|-----------------------------|-----------|------------------------------------|----------------------------------------------|
| `logs_total`                | Counter   | `status_code`, `method`, `level`, `service` | Total logs consumed                 |
| `response_time_seconds`     | Histogram | `level`                            | Response time distribution (seconds)         |
| `logs_processed_per_second` | Gauge     | —                                  | Rolling logs/sec rate (last 1s)              |
| `error_rate`                | Gauge     | —                                  | 5xx ratio over the last 10s window           |
| `dlq_messages_total`        | Counter   | —                                  | Total messages forwarded to the DLQ          |
| `kafka_consumer_lag`        | Gauge     | `topic`                            | Messages behind latest offset, per topic     |

### Useful PromQL queries

```promql
# logs per second by status code
rate(logs_total[1m])

# error rate (5xx / total)
rate(logs_total{status_code=~"5.."}[1m]) / rate(logs_total[1m])

# p99 response time
histogram_quantile(0.99, rate(response_time_seconds_bucket[1m]))

# total errors in the last 5 minutes
increase(logs_total{status_code=~"5.."}[5m])

# DLQ ingestion rate
rate(dlq_messages_total[1m])

# consumer lag per topic
kafka_consumer_lag
```

---

## Dead Letter Queue

The consumer forwards messages to `logs.dlq` when they fail JSON parsing or schema validation (missing `level`, `service`, or `status_code`). The producer intentionally injects ~1% malformed messages to exercise this path.

The DLQ consumer (`dlq-consumer`) reads `logs.dlq` and:
- **Retries** messages that parse *and* pass the same schema validation the
  consumer applies → republishes to `logs.{info,warn,error}` with an incremented
  `x-retry-count` header
- **Drops** messages that are unparseable, or that parse but still fail
  validation — republishing those would only get them rejected again
- **Gives up** after `maxRetries` (3) redeliveries

The validation check here must stay in lockstep with the consumer's. If the DLQ
consumer's check is the weaker of the two, any message in the gap is forwarded,
rejected, and returned to the DLQ indefinitely — a feedback loop that is
invisible at low rates but saturates the broker at 30k/s.

All DLQ messages are also written to `./logs/dlq.log` with the origin topic and error reason.

---

## Chaos Engineering

Kill the producer and watch the Grafana charts flatline, then recover when it restarts:

```bash
# stop the producer
docker compose stop producer

# watch charts drop in Grafana at http://localhost:3000

# restart it
docker compose start producer
```

You can also stop the consumer to watch `kafka_consumer_lag` climb, then recover:

```bash
docker compose stop consumer
# let lag accumulate...
docker compose start consumer
```

---

## Configuration

Edit `.env` to tune behavior:

| Variable              | Default      | Description                                        |
|-----------------------|--------------|----------------------------------------------------|
| `KAFKA_BROKER`        | `kafka:9092` | Kafka broker address                               |
| `LOG_RATE_PER_SEC`    | `30000`      | Target produce rate (messages/sec)                 |
| `PRODUCER_WORKERS`    | `8`          | Goroutines generating and publishing logs          |
| `PRODUCER_BATCH_SIZE` | `500`        | Messages per Kafka write batch                     |
| `LOG_SAMPLE_N`        | `1000`       | Write 1-in-N produced entries to `producer.log`    |
| `CONSUMER_WORKERS`    | `8`          | Goroutines validating and recording logs           |
| `LOG_FILE_SAMPLE_N`   | `100`        | Write 1-in-N consumed entries to severity log files|
| `METRICS_PORT`        | `8080`       | Consumer metrics port                              |
| `INGEST_PORT`         | `8082`       | Host port for the Ingest API                       |
| `PROMETHEUS_PORT`     | `9090`       | Host port for Prometheus                           |
| `GRAFANA_PORT`        | `3000`       | Host port for Grafana                              |

### Throughput notes

The pipeline is tuned to sustain 30k messages/sec end to end. The settings that
matter most if you change the target:

- **Partitions.** `KAFKA_NUM_PARTITIONS: 6` in `docker-compose.yml` sets how many
  consumers in the group can read a topic in parallel. Auto-created topics
  otherwise get one partition, which pins each topic to a single reader.
- **Commit interval.** Both consumers set `CommitInterval: 1s`. Left unset,
  kafka-go commits offsets synchronously on every message — one broker
  round-trip per log, and the single hardest ceiling on this pipeline. The
  trade-off is that a restart may replay up to a second of messages.
- **File sampling.** At 30k/s, writing every entry to `logs/` is ~16GB/hour with
  no rotation. `LOG_FILE_SAMPLE_N` and `LOG_SAMPLE_N` bound that; Kafka and
  Prometheus still see 100% of the stream. Set either to `1` to log everything,
  and add rotation if you do.
- **Kafka retention** is capped by size (`KAFKA_LOG_RETENTION_BYTES`) so a long
  run cannot fill the host disk.

---

## Project Structure

```
log-aggregator/
├── .env                          # Environment variables
├── docker-compose.yml            # Orchestrates all services
├── logs/                         # Volume-mounted log files (written at runtime)
│   ├── producer.log
│   ├── info.log
│   ├── warn.log
│   ├── error.log
│   └── dlq.log
├── producer/
│   ├── main.go                   # Log generation + severity-routed Kafka publisher
│   ├── Dockerfile
│   └── go.mod
├── consumer/
│   ├── main.go                   # Multi-topic consumer, DLQ routing, Prometheus metrics
│   ├── Dockerfile
│   └── go.mod
├── dlq-consumer/
│   ├── main.go                   # DLQ reader: retry or drop malformed messages
│   ├── Dockerfile
│   └── go.mod
├── ingest-api/
│   ├── main.go                   # HTTP gateway: POST /ingest → Kafka
│   ├── Dockerfile
│   └── go.mod
└── infra/
    ├── prometheus/
    │   └── prometheus.yml        # Scrape config
    └── grafana/provisioning/
        ├── datasources/
        │   └── prometheus.yml    # Auto-registers Prometheus
        └── dashboards/
            ├── dashboards.yml    # Dashboard loader config
            └── system_health.json # Pre-built dashboard
```
