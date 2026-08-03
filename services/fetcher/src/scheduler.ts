import cron from "node-cron";
import Redis from "ioredis";
import { ALL_LOCATIONS } from "./locations";

const QUEUE_KEY   = "weather:locations:queue";
const CYCLE_KEY   = "weather:cycle:id";
const CYCLE_START = "weather:cycle:start_ms";

export async function enqueueLocations(redis: Redis): Promise<void> {
  const cycleId = await redis.incr(CYCLE_KEY);
  const startMs = Date.now();

  const pipeline = redis.pipeline();
  pipeline.set(CYCLE_START, String(startMs));
  pipeline.del(QUEUE_KEY);
  for (const loc of ALL_LOCATIONS) {
    pipeline.lpush(QUEUE_KEY, JSON.stringify(loc));
  }
  await pipeline.exec();

  console.log(`\n${"━".repeat(56)}`);
  console.log(` [scheduler] Cycle #${cycleId} started — ${ALL_LOCATIONS.length} locations enqueued`);
  console.log(`${"━".repeat(56)}\n`);
}

export async function startScheduler(redis: Redis): Promise<void> {
  await enqueueLocations(redis);

  cron.schedule("* * * * *", () => {
    enqueueLocations(redis).catch(err =>
      console.error("[scheduler] enqueue error:", err)
    );
  });

  console.log("[scheduler] started — enqueuing every 60 seconds");
}
