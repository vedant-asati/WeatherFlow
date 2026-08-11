import { Router } from "express";
import Redis from "ioredis";

const QUEUE_KEY    = "weather:locations:queue";
const STREAM_KEY   = "weather:raw";
const CYCLE_KEY    = "weather:cycle:id";
const CYCLE_START  = "weather:cycle:start_ms";
const COOLDOWN_KEY = "rate_limiter:weather_api:cooldown";
const GROUP_NAME   = "processor-group";

const BACKPRESSURE_THRESHOLD = parseInt(process.env.BACKPRESSURE_THRESHOLD ?? "5000");

export function createPipelineRouter(redis: Redis): Router {
  const router = Router();

  router.get("/status", async (_req, res) => {
    try {
      const [cycleIdStr, cycleStartStr, queueLen, streamLen, cooldownTtl] = await Promise.all([
        redis.get(CYCLE_KEY),
        redis.get(CYCLE_START),
        redis.llen(QUEUE_KEY),
        redis.xlen(STREAM_KEY),
        redis.pttl(COOLDOWN_KEY),
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

      // Backpressure state
      let backpressure: "ok" | "warning" | "critical" = "ok";
      if (streamLen > BACKPRESSURE_THRESHOLD) {
        backpressure = "critical";
      } else if (streamLen > BACKPRESSURE_THRESHOLD * 0.7) {
        backpressure = "warning";
      }

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
          pending:         pending,
          lastDeliveredId: lastDeliveredId,
        },
        rateLimiter: {
          cooldownActive: cooldownTtl > 0,
          cooldownTtlMs:  cooldownTtl > 0 ? cooldownTtl : 0,
        },
        backpressure: {
          state:     backpressure,
          threshold: BACKPRESSURE_THRESHOLD,
          streamLen: streamLen,
        },
        timestamp: new Date().toISOString(),
      });
    } catch (err: any) {
      console.error("[pipeline] status error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
