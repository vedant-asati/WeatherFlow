import Redis from "ioredis";

const STREAM_KEY    = "weather:raw";
const GROUP_NAME    = "processor-group";
const CONSUMER_NAME = "processor-1";
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
  onRecord: (record: WeatherRecord) => Promise<void>
): Promise<void> {
  for (const [id, fields] of messages) {
    try {
      const record = parseMessage(id, fields);
      await onRecord(record);
      // XACK only after successful write — if we crash before this,
      // the message stays pending and gets redelivered on restart.
      // InfluxDB deduplicates by (measurement + tags + timestamp) so
      // reprocessing the same message is safe — it just overwrites.
      await redis.xack(STREAM_KEY, GROUP_NAME, id);
      // Track end-to-end latency: observation time → InfluxDB write
      await updateE2eLatency(redis, record.recorded_at);
    } catch (err: any) {
      console.error(`[consumer] failed to process message ${id}: ${err.message}`);
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

    await processMessages(redis, messages, onRecord);
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

    await processMessages(redis, messages, onRecord);
  }
}
