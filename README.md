# Sentinel-SRE

A high-performance, distributed monitoring platform engineered for reliability and real-time observability. Built on an asynchronous, event-driven microservices architecture to deliver fault-tolerant website health monitoring at scale — with millisecond-level precision.

[![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Node.js](https://img.shields.io/badge/Node.js-20.x-339933?style=flat-square&logo=nodedotjs)](https://nodejs.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?style=flat-square&logo=rabbitmq)](https://www.rabbitmq.com/)
[![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?style=flat-square&logo=redis)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C?style=flat-square&logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-dashboards-F46800?style=flat-square&logo=grafana)](https://grafana.com/)

---

## What is Sentinel-SRE?

Traditional monitoring tools run checks synchronously inside a monolithic process — one thread per check, poor fault isolation, and no recovery when a worker crashes.

Sentinel-SRE solves this with a fully decoupled, event-driven pipeline:

- A **Node.js Scheduler** identifies overdue probes and publishes tasks to a message queue
- A **Go Worker Pool** consumes tasks concurrently using goroutines — handling thousands of network I/O operations with near-zero overhead
- **RabbitMQ Manual ACKs** guarantee no check is lost, even if a worker crashes mid-execution
- **Redis Write-Through Cache** provides O(1) real-time status lookups, shielding PostgreSQL from dashboard read pressure
- **Prometheus + Grafana** expose P99 latency, queue depth, and worker throughput in real time

---

## Architecture

<img width="273" height="302" alt="image" src="https://github.com/user-attachments/assets/47b3ced2-7621-446b-9849-08bc30bb5d96" />
Here is the raw markdown — select all and copy everything between the lines:

# Sentinel-SRE

A high-performance, distributed monitoring platform engineered for reliability and real-time observability. Built on an asynchronous, event-driven microservices architecture to deliver fault-tolerant website health monitoring at scale — with millisecond-level precision.

[![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Node.js](https://img.shields.io/badge/Node.js-20.x-339933?style=flat-square&logo=nodedotjs)](https://nodejs.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?style=flat-square&logo=rabbitmq)](https://www.rabbitmq.com/)
[![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?style=flat-square&logo=redis)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C?style=flat-square&logo=prometheus)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Grafana-dashboards-F46800?style=flat-square&logo=grafana)](https://grafana.com/)

---

## Service Breakdown

### API Gateway · `api-gateway/` · Node.js

The single ingress point for monitor configuration. Accepts `POST /monitors` with a target URL and check interval, validates the payload, and persists to PostgreSQL.

```http
POST /monitors
{
  "url": "https://example.com",
  "interval": 60
}
```

| Responsibility | Implementation |
|---|---|
| Request validation | Express middleware |
| Configuration persistence | PostgreSQL `monitors` table |
| Response | Returns monitor ID + confirmation |

---

### Scheduler Service · `scheduler-service/` · Node.js

The system heartbeat. Runs on a fixed tick, queries PostgreSQL for monitors whose `next_check_at` timestamp has passed, and publishes each overdue probe as a task message to RabbitMQ.

```sql
-- Overdue probe query (simplified)
SELECT id, url FROM monitors
WHERE next_check_at <= NOW()
  AND is_active = true;
```

---

### Worker Service · `worker-service/` · Go

The execution engine. Consumes check tasks from RabbitMQ and performs concurrent HTTP probes using Go's M:N goroutine scheduler. Each worker handles multiple probes simultaneously with non-blocking I/O — a single worker process can manage thousands of network operations with negligible memory overhead compared to thread-per-connection models.

```go
// Each consumed message spawns a goroutine
go func(msg amqp.Delivery) {
    result := performCheck(msg)
    writeToCache(result)
    persistToDatabase(result)
    msg.Ack(false) // Manual ACK — only after successful processing
}(delivery)
```

---

## Reliability Engineering

### Guaranteed Delivery — Manual ACKs

RabbitMQ messages are acknowledged **only after the worker successfully completes a check and writes the result**. If a worker crashes, panics, or is killed mid-execution, RabbitMQ automatically re-queues the message. No check is silently dropped.
Worker receives message
│
▼
Perform HTTP check
│
▼
Write result to Redis + PostgreSQL
│
▼
msg.Ack(false)  ◄── only now is the message removed from the queue

---

### Exponential Backoff — Transient Failure Mitigation

Checks that fail due to transient network conditions are retried with an exponential delay formula `2ⁿ` seconds between attempts. This prevents thundering-herd retry storms and dramatically reduces false-positive alerts.
Attempt 1  →  fail  →  wait 2s
Attempt 2  →  fail  →  wait 4s
Attempt 3  →  fail  →  wait 8s
Attempt 4  →  fail  →  move to DLQ

---

### Dead Letter Queue — Poison Message Isolation

Messages that exceed the maximum retry count are automatically routed to `checks.dlq` instead of being discarded. This isolates unprocessable tasks from the primary pipeline while preserving them for manual audit.
checks.queue (primary)
│
│ maxRetries exceeded
▼
checks.dlq (dead letter)
│
└── available for manual inspection / replay

---

### Backpressure — QoS Prefetch

Each worker channel is configured with a QoS `prefetch_count` limit. This prevents a single worker from pulling the entire queue and getting overwhelmed, ensuring even load distribution across all active workers.

---

### Write-Through Cache — Redis Speed Layer

Every check result is written to Redis **and** PostgreSQL simultaneously. Dashboard queries hit Redis first — PostgreSQL is only read on a cache miss. This pattern eliminates read pressure on the relational store under high dashboard load.

---

## Data Model

```sql
-- Monitor configuration (static, low-write)
CREATE TABLE monitors (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url           TEXT NOT NULL,
    interval      INTEGER NOT NULL,
    is_active     BOOLEAN DEFAULT true,
    next_check_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Check history (time-series, high-write)
CREATE TABLE check_results (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    monitor_id  UUID REFERENCES monitors(id),
    status_code INTEGER,
    latency_ms  INTEGER,
    is_up       BOOLEAN,
    checked_at  TIMESTAMPTZ DEFAULT NOW()
);
```

Separating static configuration from time-series results optimizes write throughput and avoids storing redundant monitor metadata on every row.

---

## Observability

Prometheus metrics are exposed from the worker service and scraped into Grafana dashboards.

| Metric | Type | Description |
|---|---|---|
| `sentinel_check_duration_ms` | Histogram | HTTP check latency per endpoint (P50, P95, P99) |
| `sentinel_checks_total` | Counter | Total checks performed, labelled by status |
| `sentinel_queue_depth` | Gauge | Current RabbitMQ `checks.queue` message count |
| `sentinel_workers_active` | Gauge | Active goroutines in the worker pool |
| `sentinel_cache_hits_total` | Counter | Redis cache hit vs miss ratio |

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Node.js 20+

### 1. Start infrastructure

```bash
git clone https://github.com/Afshan738/Site_Reliability_Guardian
cd Site_Reliability_Guardian
docker-compose up -d
```

### 2. Start services

```bash
# API Gateway
cd api-gateway
npm install
npm start

# Scheduler Service
cd scheduler-service
npm install
npm start

# Worker Pool
cd worker-service
go run main.go
```

### 3. Access dashboards

| Service | URL |
|---|---|
| API Gateway | `http://localhost:3000` |
| RabbitMQ Management | `http://localhost:15672` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3001` |

### 4. Register a monitor

```bash
curl -X POST http://localhost:3000/monitors \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com", "interval": 60}'
```

---

## Environment Variables

| Variable | Service | Description |
|---|---|---|
| `POSTGRES_URL` | gateway, scheduler | PostgreSQL connection string |
| `RABBITMQ_URL` | scheduler, worker | RabbitMQ AMQP connection string |
| `REDIS_URL` | worker | Redis connection string |
| `CHECK_INTERVAL_MS` | scheduler | Scheduler poll frequency in ms |
| `WORKER_PREFETCH` | worker | RabbitMQ QoS prefetch count |
| `PROMETHEUS_PORT` | worker | Metrics scrape port (default: 9091) |

---

## Roadmap

- [ ] Kubernetes deployment — Helm chart with Horizontal Pod Autoscaling for elastic worker scaling
- [ ] Batch-insert buffering — buffer check results in memory and flush to PostgreSQL in batches
- [ ] Alerting — Alertmanager integration for Slack notifications on monitor state changes
- [ ] Multi-region workers — deploy worker pools across availability zones
- [ ] TLS / mTLS — mutual TLS between internal services for zero-trust networking

---

## Tech Stack

| Layer | Technology |
|---|---|
| Worker engine | Go 1.21 — goroutines, non-blocking I/O |
| API & Scheduler | Node.js 20, Express.js |
| Message broker | RabbitMQ 3.13 (AMQP 0-9-1) |
| Cache | Redis 7 (Write-Through pattern) |
| Database | PostgreSQL 16 (normalized schema) |
| Observability | Prometheus + Grafana |
| Load testing | k6 |
| Container runtime | Docker, Docker Compose |

---

## Author

**Afshan Qasim** · [GitHub](https://github.com/Afshan738) · [LinkedIn](https://www.linkedin.com/in/afshan-qasim-998917300)

> Engineering systems that survive real-world failure conditions.
