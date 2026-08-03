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

export async function startConsumer(
  redis: Redis,
  onRecord: (record: WeatherRecord) => Promise<void>
): Promise<void> {
  await ensureGroup(redis);
  console.log(`[consumer] listening on stream "${STREAM_KEY}"...`);

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

    for (const [id, fields] of messages) {
      try {
        const record = parseMessage(id, fields);
        await onRecord(record);
        await redis.xack(STREAM_KEY, GROUP_NAME, id);
      } catch (err: any) {
        console.error(`[consumer] failed to process message ${id}: ${err.message}`);
      }
    }
  }
}
