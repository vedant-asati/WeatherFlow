import cron from "node-cron";
import Redis from "ioredis";
import { ALL_LOCATIONS } from "./locations";

const QUEUE_KEY = "weather:locations:queue";

/**
 * Pushes all locations into the Redis queue as a single pipeline batch.
 * Deletes the previous batch first so stale jobs never pile up.
 */
export async function enqueueLocations(redis: Redis): Promise<void> {
  const pipeline = redis.pipeline();
  pipeline.del(QUEUE_KEY);
  for (const loc of ALL_LOCATIONS) {
    pipeline.lpush(QUEUE_KEY, JSON.stringify(loc));
  }
  await pipeline.exec();
  console.log(`[scheduler] enqueued ${ALL_LOCATIONS.length} locations`);
}

/**
 * Starts the cron job — fires every 60s and once immediately on startup.
 */
export function startScheduler(redis: Redis): void {
  enqueueLocations(redis); // fire immediately, don't wait 60s

  cron.schedule("* * * * *", () => {
    enqueueLocations(redis).catch(err =>
      console.error("[scheduler] enqueue error:", err)
    );
  });

  console.log("[scheduler] started — enqueuing every 60s");
}
