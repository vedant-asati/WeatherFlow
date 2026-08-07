import express from "express";
import { registry } from "./metrics";
import { probeOpenMeteo } from "./fetcher";

const PORT = parseInt(process.env.METRICS_PORT ?? "3000");

export function startServer(): void {
  const app = express();

  // Prometheus metrics — scraped by Prometheus every 15s
  app.get("/metrics", async (_req, res) => {
    res.set("Content-Type", registry.contentType);
    res.send(await registry.metrics());
  });

  // Health check — verifies upstream API is reachable
  app.get("/healthz", async (_req, res) => {
    const apiOk = process.env.USE_MOCK === "true"
      ? true                  // mock mode — no real API to probe
      : await probeOpenMeteo();

    const status = apiOk ? 200 : 503;
    res.status(status).json({
      status:    apiOk ? "ok" : "degraded",
      api:       apiOk ? "reachable" : "unreachable",
      timestamp: new Date().toISOString(),
    });
  });

  app.listen(PORT, () => {
    console.log(`[server] /metrics and /healthz listening on port ${PORT}`);
  });
}
