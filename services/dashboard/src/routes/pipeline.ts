import { Router } from "express";
import Redis from "ioredis";
import { getInfluxHealth, getFetcherMetricsRaw } from "../main";

const QUEUE_KEY            = "weather:locations:queue";
const STREAM_KEY           = "weather:raw";
const CYCLE_KEY            = "weather:cycle:id";
const CYCLE_START          = "weather:cycle:start_ms";
const COOLDOWN_KEY         = "rate_limiter:weather_api:cooldown";
const COOLDOWN_COUNT_KEY   = "rate_limiter:weather_api:cooldown_count";
const GROUP_NAME           = "processor-group";
const E2E_LATENCY_KEY      = "weather:metrics:e2e_avg_ms";
const E2E_STALE_KEY        = "weather:metrics:e2e_stale";

const BACKPRESSURE_THRESHOLD = parseInt(process.env.BACKPRESSURE_THRESHOLD ?? "5000");
const STREAM_MAXLEN          = parseInt(process.env.STREAM_MAXLEN ?? "10000");

/** Parse a single counter value from Prometheus text format */
function parseCounter(text: string, name: string): number {
  const match = text.match(new RegExp(`^${name}\\s+([\\d.e+]+)`, "m"));
  return match ? parseFloat(match[1]) : 0;
}

/** Parse histogram avg latency from _sum / _count */
function parseHistogramAvg(text: string, name: string): number | null {
  const sumMatch   = text.match(new RegExp(`^${name}_sum\\s+([\\d.e+]+)`, "m"));
  const countMatch = text.match(new RegExp(`^${name}_count\\s+([\\d.e+]+)`, "m"));
  if (!sumMatch || !countMatch) return null;
  const count = parseFloat(countMatch[1]);
  if (count === 0) return null;
  return Math.round((parseFloat(sumMatch[1]) / count) * 1000); // seconds → ms
}

export function createPipelineRouter(redis: Redis): Router {
  const router = Router();

  router.get("/status", async (_req, res) => {
    try {
      const [cycleIdStr, cycleStartStr, queueLen, streamLen, cooldownTtl, e2eAvgStr, cooldownCountStr, e2eStaleStr] = await Promise.all([
        redis.get(CYCLE_KEY),
        redis.get(CYCLE_START),
        redis.llen(QUEUE_KEY),
        redis.xlen(STREAM_KEY),
        redis.pttl(COOLDOWN_KEY),
        redis.get(E2E_LATENCY_KEY),
        redis.get(COOLDOWN_COUNT_KEY),
        redis.get(E2E_STALE_KEY),
      ]);

      const cycleId    = parseInt(cycleIdStr ?? "0");
      const cycleStart = cycleStartStr ? parseInt(cycleStartStr) : null;

      // Stream consumer group info — pending count tells us how far behind the processor is
      let pending = 0;
      let lastDeliveredId: string | null = null;
      try {
        const groups = await redis.xinfo("GROUPS", STREAM_KEY) as any[];
        const group = groups.find((g: any) => {
          // xinfo returns flat arrays: [field, value, field, value, ...]
          if (Array.isArray(g)) {
            const nameIdx = g.indexOf("name");
            return nameIdx !== -1 && g[nameIdx + 1] === GROUP_NAME;
          }
          return g?.name === GROUP_NAME;
        });

        if (group) {
          if (Array.isArray(group)) {
            const pelIdx = group.indexOf("pel-count");
            if (pelIdx !== -1) pending = parseInt(group[pelIdx + 1]) || 0;
            const lidIdx = group.indexOf("last-delivered-id");
            if (lidIdx !== -1) lastDeliveredId = group[lidIdx + 1];
          } else {
            pending = group["pel-count"] ?? 0;
            lastDeliveredId = group["last-delivered-id"] ?? null;
          }
        }
      } catch {
        // Stream or group may not exist yet
      }

      // Backpressure state — based on pending (un-ACKed) count, not stream length.
      // Stream length includes processed history; pending = actual processor lag.
      let backpressure: "ok" | "warning" | "critical" = "ok";
      if (pending > BACKPRESSURE_THRESHOLD) {
        backpressure = "critical";
      } else if (pending > BACKPRESSURE_THRESHOLD * 0.7) {
        backpressure = "warning";
      }

      // Parse Prometheus metrics from fetcher
      const raw = getFetcherMetricsRaw();
      const successTotal  = parseCounter(raw, "weather_api_calls_success_total");
      const failedTotal   = parseCounter(raw, "weather_api_calls_failed_total");
      const denials       = parseCounter(raw, "weather_rate_limiter_denials_total");
      const fetchAvgMs    = parseHistogramAvg(raw, "weather_api_response_latency_seconds");
      const callsTotal    = successTotal + failedTotal;
      const successRate   = callsTotal > 0 ? Math.round((successTotal / callsTotal) * 100) : null;

      // E2E latency from Redis (written by processor)
      const e2eAvgMs = e2eAvgStr ? parseInt(e2eAvgStr) : null;

      // Pipeline health — derived composite signal
      const influxOk  = getInfluxHealth();
      const noBackpressure = backpressure === "ok";
      const noCooldown     = cooldownTtl <= 0;
      const health: "healthy" | "degraded" =
        influxOk && noBackpressure && noCooldown ? "healthy" : "degraded";

      res.json({
        cycle: {
          id:        cycleId,
          startedAt: cycleStart ? new Date(cycleStart).toISOString() : null,
          elapsed:   cycleStart ? Math.round((Date.now() - cycleStart) / 1000) : null,
        },
        queue: {
          depth: queueLen,
        },
        stream: {
          length:          streamLen,
          maxLen:          STREAM_MAXLEN,
          pending:         pending,
          lastDeliveredId: lastDeliveredId,
        },
        rateLimiter: {
          rps:            8,  // token bucket capacity
          cooldownActive: cooldownTtl > 0,
          cooldownTtlMs:  cooldownTtl > 0 ? cooldownTtl : 0,
          cooldownCount:  parseInt(cooldownCountStr ?? "0"),
          denials:        denials,
        },
        backpressure: {
          state:     backpressure,
          threshold: BACKPRESSURE_THRESHOLD,
          streamLen: streamLen,
        },
        fetchMetrics: {
          successTotal:  successTotal,
          failedTotal:   failedTotal,
          successRate:   successRate,   // percentage or null if no data yet
          fetchAvgMs:    fetchAvgMs,    // avg API latency ms or null
        },
        processor: {
          e2eAvgMs:  e2eAvgMs,           // observation → InfluxDB ms or null
          e2eStale:  e2eStaleStr === "1", // true = catch-up mode, value not meaningful
        },
        services: {
          influxdb: influxOk ? "ok" : "error",
        },
        health: health,
        timestamp: new Date().toISOString(),
      });
    } catch (err: any) {
      console.error("[pipeline] status error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
