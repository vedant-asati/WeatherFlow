# Weather Telemetry Pipeline

A distributed data ingestion pipeline that fetches real-time weather telemetry for 500 geographical locations globally and stores it in a time-series database. Built with TypeScript/Node.js, Redis, and InfluxDB.

---

## Architecture

```
Open-Meteo API
      │  HTTP (8 req/s, token bucket rate limiter)
      ▼
 [Fetcher Service]
   node-cron scheduler — enqueues 500 locations every 60s
   50 async workers   — BRPOP from queue, fetch, XADD to stream
   Express server     — /metrics (Prometheus) + /healthz
      │  XADD
      ▼
 Redis Stream (weather:raw)
      │  XREADGROUP (consumer group, pending recovery on restart)
      ▼
 [Processor Service]
   reads messages → writes to InfluxDB → XACK
      │  write (idempotent by timestamp)
      ▼
 InfluxDB (weather_bucket)
   measurement: weather
   tags:   city_name, weather_condition
   fields: temperature, latitude, longitude
   retention: 30 days
```

---

## Components

### Fetcher Service (`services/fetcher/`)

Responsible for fetching weather data from the Open-Meteo API and publishing it to a Redis Stream.

**Scheduler (`scheduler.ts`)**
- Uses `node-cron` to enqueue all 500 locations into a Redis LIST every 60 seconds
- On startup, awaits the first enqueue before starting workers — ensures the queue is populated before any worker tries to pop from it
- Each cycle gets a monotonic ID and a start timestamp stored in Redis, used by the analytics reporter

**Workers (`worker.ts`)**
- 50 concurrent async workers, each running an infinite loop: `BRPOP` → `acquire token` → `fetch` → `XADD`
- All 50 start simultaneously — the rate limiter handles concurrency, not startup staggering
- Per-second analytics reporter prints live cycle progress: requests/sec, success/fail counts, avg + p99 latency

**Rate Limiter (`rate-limiter.ts`)**
- Token bucket algorithm implemented via an atomic Redis Lua script
- Lua ensures no two workers can "double-spend" the same token even with 50 concurrent goroutines hitting Redis simultaneously
- Cap: 8 tokens/sec — comfortably under Open-Meteo's 600 req/min free tier limit
- Cooldown mode: if a 429 slips through, a Redis key with a TTL is set — all workers sleep until it expires (`PTTL` for exact sleep duration, not polling)
- Backed by Redis so it works correctly across multiple fetcher replicas in production

**Fetcher (`fetcher.ts`)**
- Shared axios instance with IPv4 forced (`family: 4`) — avoids IPv6 DNS hangs on some network configs
- `axiosRetry` with full-jitter exponential backoff, up to 5 retries
- Respects `Retry-After` header from 429 responses
- `USE_MOCK=true` swaps the real HTTP call for `mockFetchWeather()` — same return type, no quota used

**Mock Weather (`mock-weather.ts`)**
- Drop-in replacement for the real API — produces realistic temperatures based on latitude and season
- Simulates network latency (80–350ms) so worker timing matches real conditions
- Used for local development when the Open-Meteo daily quota is exhausted

**Metrics & Server (`metrics.ts`, `server.ts`)**
- Prometheus counters: `api_calls_total`, `api_calls_success_total`, `api_calls_failed_total`, `rate_limiter_denials_total`
- Prometheus histogram: `api_response_latency_seconds` (buckets: 50ms → 10s)
- Express server on port 3000:
  - `GET /metrics` — Prometheus scrape endpoint
  - `GET /healthz` — probes Open-Meteo reachability, returns `200 ok` or `503 degraded`

---

### Processor Service (`services/processor/`)

Reads from the Redis Stream and writes to InfluxDB.

**Consumer (`consumer.ts`)**
- Uses Redis `XREADGROUP` with a named consumer group (`processor-group`)
- On startup, first reads with `"0"` to drain any pending (unacknowledged) messages from before a crash — ensures no data loss on restart
- Then switches to `">"` for new messages
- `XACK` is only sent after a successful InfluxDB write — if the write fails, the message stays pending and gets redelivered

**InfluxDB Writer (`influx-writer.ts`)**
- Connects to InfluxDB using `@influxdata/influxdb-client`
- Batches writes: flushes every 1 second or when 100 points accumulate — more efficient than one write per point
- Each point uses `recorded_at` as the timestamp — this is the actual observation time from Open-Meteo, not the ingestion time
- **Idempotency**: InfluxDB deduplicates by `(measurement + tags + timestamp)`. Writing the same point twice (e.g. after a processor restart) overwrites rather than duplicates — no UPSERT logic needed

---

### Redis

Acts as three things simultaneously:
- **Job queue** — a LIST (`weather:locations:queue`) that the scheduler pushes to and workers pop from
- **Stream broker** — a Stream (`weather:raw`) that workers publish to and the processor consumes from
- **Rate limiter state** — stores token bucket state (`rate_limiter:weather_api:bucket`) and cooldown flag (`rate_limiter:weather_api:cooldown`)

---

### InfluxDB

Chosen over PostgreSQL + TimescaleDB for this use case because:
- Native time-series data model — no schema migrations needed when adding fields
- Built-in retention policies — 30-day automatic data expiry configured at bucket creation
- Flux query language is purpose-built for time-series aggregations (windowed averages, downsampling)
- Free tier sufficient for development and demonstration

**Data model:**
```
measurement: weather
tags:         city_name (string), weather_condition (string)
fields:       temperature (float), latitude (float), longitude (float)
timestamp:    recorded_at (ms precision)
```

Tags are indexed — used for filtering and grouping. Fields are the measured values.

---

## Local Development

**Prerequisites:** Docker, Docker Compose

```bash
# Start all four services (Redis + InfluxDB + fetcher + processor)
docker compose up --build

# Use mock data instead of real API (no quota consumed)
# Edit docker-compose.yml, uncomment: USE_MOCK: "true"
# Then restart: docker compose up --build

# Stop
docker compose down

# Stop and wipe all persisted data (Redis + InfluxDB volumes)
docker compose down -v
```

**Endpoints once running:**
| URL | Description |
|-----|-------------|
| http://localhost:8086 | InfluxDB UI (admin / adminpassword) |
| http://localhost:3001/metrics | Prometheus metrics |
| http://localhost:3001/healthz | Health check |

---

## Querying the Data

Open InfluxDB UI → Data Explorer → Script Editor.

**Count of cities written in the last hour:**
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature")
  |> group()
  |> count()
```

**Temperature time series for a specific city:**
```flux
from(bucket: "weather_bucket")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature")
  |> filter(fn: (r) => r.city_name == "London")
  |> sort(columns: ["_time"])
```

**Full table — one row per observation, all fields:**
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather")
  |> pivot(rowKey: ["_time", "city_name"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "city_name", "weather_condition", "temperature", "latitude", "longitude"])
```

**Observations per minute (confirms cycle timing):**
```flux
from(bucket: "weather_bucket")
  |> range(start: -2h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature")
  |> aggregateWindow(every: 1m, fn: count, createEmpty: false)
```

---

## Production (Kubernetes)

**Prerequisites:** `kubectl`, a running cluster (minikube, GKE, EKS, etc.)

```bash
# Deploy everything
kubectl apply -f k8s/

# Check pod status
kubectl get pods

# Stream logs
kubectl logs -f deployment/fetcher
kubectl logs -f deployment/processor

# Tear down
kubectl delete -f k8s/
```

**K8s manifest overview:**

| File | What it creates |
|------|----------------|
| `configmap.yaml` | Shared env vars — Redis URL, InfluxDB URL, org, bucket |
| `secret.yaml` | Sensitive values — InfluxDB token + admin password |
| `redis-deployment.yaml` | Redis Deployment + internal Service (`redis-service:6379`) |
| `influxdb-deployment.yaml` | InfluxDB Deployment + internal Service (`influxdb-service:8086`) |
| `fetcher-deployment.yaml` | Fetcher Deployment + Service (exposes port 3000 for metrics) |
| `processor-deployment.yaml` | Processor Deployment |

Services use K8s internal DNS — pods reach each other by service name, not IP.

---

## Rate Limiting

Open-Meteo free tier limits: **600 req/min**, **10,000 req/day**.

| | Value |
|--|--|
| Pipeline rate | 8 req/s = 480 req/min |
| Per cycle | 500 locations |
| Cycle duration | ~62 seconds at 8 req/s |
| Daily usage (1 cycle/min) | ~720,000 req — exceeds free tier |
| Safe usage | Run in mock mode for development; use real API for demos only |

For sustained production use, either purchase the Open-Meteo commercial plan ($29/month for 10M req/month) or reduce cycle frequency to once per hour (500 req/hr = 12,000 req/day).

---

## Environment Variables

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `REDIS_URL` | both | `redis://localhost:6379` | Redis connection URL |
| `INFLUX_URL` | processor | `http://localhost:8086` | InfluxDB connection URL |
| `INFLUX_TOKEN` | processor | `my-super-secret-token` | InfluxDB API token |
| `INFLUX_ORG` | processor | `weather_org` | InfluxDB organisation |
| `INFLUX_BUCKET` | processor | `weather_bucket` | InfluxDB bucket name |
| `USE_MOCK` | fetcher | unset | Set to `"true"` to use mock data |
| `METRICS_PORT` | fetcher | `3000` | Port for `/metrics` and `/healthz` |
