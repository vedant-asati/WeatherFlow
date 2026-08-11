import { Router } from "express";
import Redis from "ioredis";

const STREAM_KEY = "weather:raw";

export function createStreamRouter(redis: Redis): Router {
  const router = Router();

  // GET /api/stream/recent?count=20
  router.get("/recent", async (req, res) => {
    try {
      const count = Math.min(parseInt(req.query.count as string) || 20, 100);

      // XREVRANGE returns newest-first
      const entries = await redis.xrevrange(STREAM_KEY, "+", "-", "COUNT", count);

      const results = entries.map(([id, fields]) => {
        const data: Record<string, string> = {};
        for (let i = 0; i < fields.length; i += 2) {
          data[fields[i]] = fields[i + 1];
        }
        return { id, ...data };
      });

      res.json(results);
    } catch (err: any) {
      console.error("[stream] recent error:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
