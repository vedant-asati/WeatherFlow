import Redis from "ioredis";
import { startScheduler } from "./scheduler";
import { startWorkers } from "./worker";
import { RateLimiter } from "./rate-limiter";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

async function main() {
  const redis = new Redis(REDIS_URL);

  await new Promise<void>((resolve, reject) => {
    redis.on("connect", () => { console.log("[redis] connected"); resolve(); });
    redis.on("error",   (err) => { console.error("[redis] error:", err); reject(err); });
  });

  const limiter = new RateLimiter(redis);

  await startScheduler(redis);

  startWorkers(redis, limiter).catch(err => console.error("[workers] fatal:", err));

  console.log("[main] fetcher running — press Ctrl+C to stop");

  const shutdown = async () => {
    console.log("[main] shutting down...");
    await redis.quit();
    process.exit(0);
  };

  process.on("SIGINT",  shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[main] fatal:", err);
  process.exit(1);
});
