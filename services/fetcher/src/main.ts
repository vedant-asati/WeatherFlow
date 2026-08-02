import Redis from "ioredis";
import { startScheduler } from "./scheduler";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

async function main() {
  // Connect to Redis
  const redis = new Redis(REDIS_URL);

  redis.on("connect", () => console.log("[redis] connected"));
  redis.on("error", (err) => console.error("[redis] error:", err));

  // Start scheduler — pushes all locations into queue every 60s
  startScheduler(redis);

  // Keep the process alive
  // Workers (CP2b) and healthz (CP2d) will be added here next
  console.log("[main] fetcher running — press Ctrl+C to stop");

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("[main] shutting down...");
    await redis.quit();
    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    console.log("[main] shutting down...");
    await redis.quit();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("[main] fatal:", err);
  process.exit(1);
});
