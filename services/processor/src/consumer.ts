import Redis from "ioredis";
import os from "os";

const STREAM_KEY    = "weather:raw";
const GROUP_NAME    = "processor-group";
// Using hostname allows running multiple processor instances in the same consumer group.
// Each gets a unique consumer identity — messages distribute between them automatically.
// If you always run one instance, any fixed name works; hostname is future-proof.
const CONSUMER_NAME = process.env.CONSUMER_NAME ?? `processor-${os.hostname()}`;
const BATCH_SIZE    = 50;
const BLOCK_MS      = 5000;

export interface WeatherRecord {
  id:                string;
  city_name:         string;
  latitude:          number;
  longitude:         number;
  temperature:       number;
  weather_condition: string;
  recorded_at:       string;
}

async function ensureGroup(redis: Redis): Promise<void> {
  try {
    await redis.xgroup("CREATE", STREAM_KEY, GROUP_NAME, "$", "MKSTREAM");
    console.log(`[consumer] created consumer group "${GROUP_NAME}"`);
  } catch (err: any) {
    if (err.message?.includes("BUSYGROUP")) {
      console.log(`[consumer] consumer group "${GROUP_NAME}" already exists`);
    } else {
      throw err;
    }
  }
}

function parseMessage(id: string, fields: string[]): WeatherRecord {
  const map: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    map[fields[i]] = fields[i + 1];
  }
  return {
    id,
    city_name:         map.city_name         ?? "unknown",
    latitude:          parseFloat(map.latitude   ?? "0"),
    longitude:         parseFloat(map.longitude  ?? "0"),
    temperature:       parseFloat(map.temperature ?? "0"),
    weather_condition: map.weather_condition ?? "unknown",
    recorded_at:       map.recorded_at       ?? new Date().toISOString(),
  };
}

const E2E_LATENCY_KEY  = "weather:metrics:e2e_avg_ms";
const E2E_STALE_KEY    = "weather:metrics:e2e_stale";   // set when catch-up mode detected
const E2E_ALPHA        = 0.1;  // EMA smoothing factor
// Messages older than 30s are considered backlog catch-up, not steady-state E2E.
// Tracking their latency would skew the metric by minutes.
const E2E_STALE_THRESHOLD_MS = 30_000;

async function updateE2eLatency(redis: Redis, recordedAt: string): Promise<void> {
  const latencyMs = Date.now() - new Date(recordedAt).getTime();
  if (latencyMs < 0) return; // clock skew guard

  if (latencyMs > E2E_STALE_THRESHOLD_MS) {
    // Catch-up mode: processor is processing old messages, not real-time.
    // Don't pollute E2E metric. Set stale flag so dashboard can show context.
    await redis.set(E2E_STALE_KEY, "1", "EX", 15);
    return;
  }

  // Clear stale flag — we're back to steady-state
  await redis.del(E2E_STALE_KEY);
  const prev  = await redis.get(E2E_LATENCY_KEY);
  const prevMs = prev ? parseFloat(prev) : latencyMs;
  const ema    = prevMs + E2E_ALPHA * (latencyMs - prevMs); // exponential moving average
  await redis.set(E2E_LATENCY_KEY, ema.toFixed(0), "EX", 60);
}

async function processMessages(
  redis: Redis,
  messages: [string, string[]][],
  onRecord: (record: WeatherRecord) => Promise<void>,
  errorState: { consecutive: number }
): Promise<void> {
  for (const [id, fields] of messages) {
    try {
      const record = parseMessage(id, fields);
      await onRecord(record);
      // Note: write() enqueues in a 1s buffer — XACK happens after enqueue, not after flush.
      // A crash within the 1s flush window may lose that message from InfluxDB.
      // Acceptable for telemetry. True at-least-once would require per-write flush (kills batching).
      // InfluxDB deduplicates on (measurement + tags + timestamp), so reprocessed messages
      // from PEL on restart are safe — they just overwrite the same point.

      // Compute E2E latency locally, then pipeline XACK + Redis metric update together
      // to reduce round-trips from 3+ sequential calls to 1-2.
      const latencyMs = Date.now() - new Date(record.recorded_at).getTime();
      const pl = redis.pipeline();
      pl.xack(STREAM_KEY, GROUP_NAME, id);

      if (latencyMs < 0) {
        // Clock skew — skip metric, just XACK
      } else if (latencyMs > E2E_STALE_THRESHOLD_MS) {
        // Catch-up mode: old backlog message. Mark stale so dashboard can show context.
        pl.set(E2E_STALE_KEY, "1", "EX", 15);
      } else {
        // Steady-state: clear stale flag in same pipeline as XACK
        pl.del(E2E_STALE_KEY);
      }
      await pl.exec();

      // EMA update requires reading prev value — can't pipeline a conditional read+write.
      // Only runs in steady-state (non-stale) path.
      if (latencyMs >= 0 && latencyMs <= E2E_STALE_THRESHOLD_MS) {
        const prev   = await redis.get(E2E_LATENCY_KEY);
        const prevMs = prev ? parseFloat(prev) : latencyMs;
        const ema    = prevMs + E2E_ALPHA * (latencyMs - prevMs);
        await redis.set(E2E_LATENCY_KEY, ema.toFixed(0), "EX", 60);
      }

      errorState.consecutive = 0;
    } catch (err: any) {
      console.error(`[consumer] failed to process message ${id}: ${err.message}`);
      errorState.consecutive++;
      // Exponential backoff when InfluxDB is consistently failing.
      // Without this, the consumer would tight-loop logging errors for every message.
      if (errorState.consecutive > 5) {
        const backoffMs = Math.min(errorState.consecutive * 1000, 30_000);
        console.warn(`[consumer] ${errorState.consecutive} consecutive errors — backing off ${backoffMs}ms`);
        await new Promise(r => setTimeout(r, backoffMs));
      }
    }
  }
}

export async function startConsumer(
  redis: Redis,
  onRecord: (record: WeatherRecord) => Promise<void>
): Promise<void> {
  await ensureGroup(redis);

  // On startup, drain any pending (unacknowledged) messages first.
  // These are messages delivered before a previous crash but never XACKed.
  // "0" returns pending messages for this consumer instead of new ones.
  // Shared error state across both drain and normal read loops
  const errorState = { consecutive: 0 };

  // On startup, drain any pending (unacknowledged) messages first.
  // These are messages delivered before a previous crash but never XACKed.
  // "0" returns pending messages for this consumer instead of new ones.
  let pendingCount = 0;
  while (true) {
    const pending = await redis.xreadgroup(
      "GROUP", GROUP_NAME, CONSUMER_NAME,
      "COUNT", BATCH_SIZE,
      "STREAMS", STREAM_KEY,
      "0"
    ) as [string, [string, string[]][]][] | null;

    if (!pending) break;
    const [, messages] = pending[0];
    if (!messages || messages.length === 0) break;

    await processMessages(redis, messages, onRecord, errorState);
    pendingCount += messages.length;
  }

  if (pendingCount > 0) {
    console.log(`[consumer] recovered ${pendingCount} pending messages from before last restart`);
  }

  console.log(`[consumer] listening on stream "${STREAM_KEY}"...`);

  // Read new messages normally
  while (true) {
    const response = await redis.xreadgroup(
      "GROUP", GROUP_NAME, CONSUMER_NAME,
      "COUNT", BATCH_SIZE,
      "BLOCK", BLOCK_MS,
      "STREAMS", STREAM_KEY,
      ">"
    ) as [string, [string, string[]][]][] | null;

    if (!response) continue;

    const [, messages] = response[0];
    if (!messages || messages.length === 0) continue;

    await processMessages(redis, messages, onRecord, errorState);
  }
}
