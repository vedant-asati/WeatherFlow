import cron from "node-cron";
import Redis from "ioredis";
import { ALL_LOCATIONS } from "./locations";

const QUEUE_KEY   = "weather:locations:queue";
const STREAM_KEY  = "weather:raw";
const CYCLE_KEY   = "weather:cycle:id";
const CYCLE_START = "weather:cycle:start_ms";

const BACKPRESSURE_THRESHOLD = parseInt(process.env.BACKPRESSURE_THRESHOLD ?? "5000");

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
  // First cycle always runs unconditionally so the pipeline starts immediately
  await enqueueLocations(redis);

  cron.schedule("* * * * *", async () => {
    try {
      const streamLen = await redis.xlen(STREAM_KEY);
      if (streamLen > BACKPRESSURE_THRESHOLD) {
        console.warn(
          `[scheduler] backpressure: stream depth ${streamLen} > ${BACKPRESSURE_THRESHOLD}, skipping cycle`
        );
        return;
      }
      await enqueueLocations(redis);
    } catch (err) {
      console.error("[scheduler] enqueue error:", err);
    }
  });

  console.log(`[scheduler] started — enqueuing every 60s (backpressure threshold: ${BACKPRESSURE_THRESHOLD})`);
}
