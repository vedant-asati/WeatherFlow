import cron from "node-cron";
import Redis from "ioredis";
import { ALL_LOCATIONS } from "./locations";

const QUEUE_KEY   = "weather:locations:queue";
const STREAM_KEY  = "weather:raw";
const CYCLE_KEY   = "weather:cycle:id";
const CYCLE_START = "weather:cycle:start_ms";
const GROUP_NAME  = "processor-group";

const BACKPRESSURE_THRESHOLD = parseInt(process.env.BACKPRESSURE_THRESHOLD ?? "5000");

/**
 * Get the number of un-ACKed (pending) messages in the processor consumer group.
 * This is the correct backpressure signal — unlike XLEN, it only counts
 * messages that the processor hasn't finished processing yet.
 */
async function getPendingCount(redis: Redis): Promise<number> {
  try {
    const groups = await redis.xinfo("GROUPS", STREAM_KEY) as any[];
    const group = groups.find((g: any) => {
      if (Array.isArray(g)) {
        const nameIdx = g.indexOf("name");
        return nameIdx !== -1 && g[nameIdx + 1] === GROUP_NAME;
      }
      return g?.name === GROUP_NAME;
    });

    if (!group) return 0;

    if (Array.isArray(group)) {
      const pelIdx = group.indexOf("pel-count");
      return pelIdx !== -1 ? parseInt(group[pelIdx + 1]) || 0 : 0;
    }
    return group["pel-count"] ?? 0;
  } catch {
    return 0; // Stream or group may not exist yet
  }
}

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
      const pending = await getPendingCount(redis);
      if (pending > BACKPRESSURE_THRESHOLD) {
        console.warn(
          `[scheduler] backpressure: ${pending} pending messages > ${BACKPRESSURE_THRESHOLD}, skipping cycle`
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
