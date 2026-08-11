import express from "express";
import cors from "cors";
import path from "path";
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

  // Express
  const app = express();
  app.use(cors());

  // Static files — the dashboard UI
  app.use(express.static(path.join(__dirname, "..", "public")));

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
