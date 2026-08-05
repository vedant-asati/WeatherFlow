# Weather Telemetry Pipeline

A distributed data ingestion pipeline that fetches real-time weather telemetry for 500 geographical locations and stores it in a time-series database.

## Architecture

```
Open-Meteo API
      │  HTTP (8 req/s, token bucket rate limiter)
      ▼
 [Fetcher Service]
   50 async workers
   node-cron scheduler (every 60s)
      │  XADD
      ▼
 Redis Stream (weather:raw)
      │  XREADGROUP
      ▼
 [Processor Service]
   consumer group, XACK on success
      │  write
      ▼
 InfluxDB (weather_bucket)
   measurement: weather
   tags: city_name, weather_condition
   fields: temperature, latitude, longitude
```

## Local Development

**Prerequisites:** Docker, Docker Compose

```bash
# Start everything (Redis + InfluxDB + fetcher + processor)
docker compose up --build

# Use mock data instead of real API (no quota used)
# Edit docker-compose.yml and uncomment: USE_MOCK: "true"

# Stop
docker compose down

# Stop and wipe all data
docker compose down -v
```

InfluxDB UI: http://localhost:8086 (admin / adminpassword)

## Querying the Data

Open InfluxDB UI → Data Explorer → Script Editor.

**All cities written in the last hour:**
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature")
  |> group()
  |> count()
```

**Temperature time series for a city:**
```flux
from(bucket: "weather_bucket")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "weather" and r._field == "temperature")
  |> filter(fn: (r) => r.city_name == "London")
  |> sort(columns: ["_time"])
```

**Full table view (one row per observation):**
```flux
from(bucket: "weather_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "weather")
  |> pivot(rowKey: ["_time", "city_name"], columnKey: ["_field"], value: ["_value"])
  |> keep(columns: ["_time", "city_name", "weather_condition", "temperature", "latitude", "longitude"])
```

## Production (Kubernetes)

**Prerequisites:** kubectl, a running cluster (e.g. minikube, GKE, EKS)

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check status
kubectl get pods
kubectl logs -f deployment/fetcher
kubectl logs -f deployment/processor

# Tear down
kubectl delete -f k8s/
```

**To switch to mock data in K8s:**
Edit `k8s/fetcher-deployment.yaml` and uncomment the `USE_MOCK` env var.

## Services

| Service   | Description                                              |
|-----------|----------------------------------------------------------|
| fetcher   | Enqueues 500 locations every 60s, 50 workers fetch weather via Open-Meteo API |
| processor | Reads from Redis stream, writes to InfluxDB              |
| redis     | Job queue + rate limiter + stream broker                 |
| influxdb  | Time-series storage, 30-day retention                    |

## Rate Limiting

Open-Meteo free tier: 10,000 req/day, 600 req/min.
Pipeline runs at 8 req/s (480 req/min) — safely under the per-minute limit.
500 locations × 1 cycle/min = 720,000 req/day theoretical max.
For sustained use, either reduce cycle frequency or use the mock mode.

## Environment Variables

| Variable      | Service   | Default                  | Description              |
|---------------|-----------|--------------------------|--------------------------|
| REDIS_URL     | both      | redis://localhost:6379   | Redis connection URL     |
| INFLUX_URL    | processor | http://localhost:8086    | InfluxDB connection URL  |
| INFLUX_TOKEN  | processor | my-super-secret-token    | InfluxDB API token       |
| INFLUX_ORG    | processor | weather_org              | InfluxDB organisation    |
| INFLUX_BUCKET | processor | weather_bucket           | InfluxDB bucket          |
| USE_MOCK      | fetcher   | unset (real API)         | Set to "true" for mock   |
