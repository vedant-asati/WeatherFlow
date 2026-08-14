import express from "express";
import cors from "cors";
import path from "path";
import http from "http";
import Redis from "ioredis";
import { InfluxDB } from "@influxdata/influxdb-client";
import { createPipelineRouter } from "./routes/pipeline";
import { createWeatherRouter } from "./routes/weather";
import { createStreamRouter } from "./routes/stream";

const PORT          = parseInt(process.env.PORT ?? "4000");
const REDIS_URL     = process.env.REDIS_URL     ?? "redis://localhost:6379";
const INFLUX_URL    = process.env.INFLUX_URL     ?? "http://localhost:8086";
const INFLUX_TOKEN  = process.env.INFLUX_TOKEN   ?? "my-super-secret-token";
const INFLUX_ORG    = process.env.INFLUX_ORG     ?? "weather_org";
const INFLUX_BUCKET = process.env.INFLUX_BUCKET  ?? "weather_bucket";
// Fetcher metrics endpoint — internal Docker network; port 3000 is the fetcher's metrics server
const FETCHER_METRICS_URL = process.env.FETCHER_METRICS_URL ?? "http://fetcher:3000/metrics";

const QUEUE_KEY  = "weather:locations:queue";
const STREAM_KEY = "weather:raw";

// Ring buffer — last 60 pipeline snapshots (~5 min at 5s interval)
const HISTORY_MAX = 60;
interface PipelineSnapshot {
  ts:          number;  // Unix ms
  queueDepth:  number;
  streamLength: number;
}
const pipelineHistory: PipelineSnapshot[] = [];

// InfluxDB health state — updated every 15s
let influxHealthy = true;
export function getInfluxHealth(): boolean { return influxHealthy; }

// Fetcher Prometheus metrics — raw text, updated every 5s alongside pipeline poll
let fetcherMetricsRaw = "";
export function getFetcherMetricsRaw(): string { return fetcherMetricsRaw; }

/** Simple HTTP GET helper (no external dependency) */
function httpGet(url: string, timeoutMs = 3000): Promise<string> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { timeout: timeoutMs }, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve(data));
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
  });
}

async function main() {
  // Redis — used for pipeline status, stream inspection
  const redis = new Redis(REDIS_URL);

  await new Promise<void>((resolve, reject) => {
    redis.on("connect", () => { console.log("[redis] connected"); resolve(); });
    redis.on("error",   (err) => { console.error("[redis] error:", err); reject(err); });
  });

  // InfluxDB — read-only query client for weather data
  const influx = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
  const queryApi = influx.getQueryApi(INFLUX_ORG);
  console.log(`[influx] query client ready → ${INFLUX_URL} | org: ${INFLUX_ORG}`);

  // InfluxDB health check — ping every 15s
  const checkInflux = async () => {
    try {
      const body = await httpGet(`${INFLUX_URL}/health`, 3000);
      influxHealthy = body.includes("\"status\":\"pass\"") || body.includes("ready");
    } catch {
      influxHealthy = false;
    }
  };
  await checkInflux();
  setInterval(checkInflux, 15_000);

  // Fetcher Prometheus metrics — scraped every 5s
  const scrapeFetcher = async () => {
    try {
      fetcherMetricsRaw = await httpGet(FETCHER_METRICS_URL, 2000);
    } catch {
      fetcherMetricsRaw = "";
    }
  };
  await scrapeFetcher();             // prime immediately so first request isn't null
  setInterval(scrapeFetcher, 5_000);

  // Pipeline history sampler — runs every 5s, fills the ring buffer
  const samplePipeline = async () => {
    try {
      const [queueLen, streamLen] = await Promise.all([
        redis.llen(QUEUE_KEY),
        redis.xlen(STREAM_KEY),
      ]);
      pipelineHistory.push({ ts: Date.now(), queueDepth: queueLen, streamLength: streamLen });
      if (pipelineHistory.length > HISTORY_MAX) pipelineHistory.shift();
    } catch {
      // Redis may be temporarily unavailable; skip sample silently
    }
  };
  await samplePipeline();            // prime immediately so chart isn't blank on first load
  setInterval(samplePipeline, 5000);

  // Express
  const app = express();
  app.use(cors());

  // Static files — the dashboard UI
  app.use(express.static(path.join(__dirname, "..", "public")));

  // Pipeline history — served from the ring buffer.
  // Registered BEFORE the pipeline router to guarantee Express resolves it first,
  // regardless of what routes the router internally handles.
  app.get("/api/pipeline/history", (_req, res) => {
    res.json(pipelineHistory);
  });

  // API routes
  app.use("/api/pipeline", createPipelineRouter(redis));
  app.use("/api/weather",  createWeatherRouter(queryApi, INFLUX_BUCKET));
  app.use("/api/stream",   createStreamRouter(redis));

  app.listen(PORT, () => {
    console.log(`[dashboard] listening on port ${PORT}`);
    console.log(`[dashboard] UI:  http://localhost:${PORT}/`);
    console.log(`[dashboard] API: http://localhost:${PORT}/api/pipeline/status`);
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log("[dashboard] shutting down...");
    await redis.quit();
    process.exit(0);
  };
  process.on("SIGINT",  shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[dashboard] fatal:", err);
  process.exit(1);
});
