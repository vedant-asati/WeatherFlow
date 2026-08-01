/**
 * Full-scale load test — all locations, 50 concurrent workers, no rate limiting.
 * Run: npm run test:all
 */
import axios from "axios";
import * as https from "https";
import { ALL_LOCATIONS, Location } from "./locations";

const CONCURRENCY = 50;

const httpClient = axios.create({
  timeout: 10_000,
  httpsAgent: new https.Agent({ family: 4, maxSockets: CONCURRENCY }),
});

const WMO_CODES: Record<number, string> = {
  0: "Clear sky",
  1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
  45: "Fog", 48: "Rime fog",
  51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
  61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
  71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
  80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
  95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail",
};

interface Result {
  city_name: string;
  ok: boolean;
  latencyMs: number;
  temperature?: number;
  condition?: string;
  error?: string;
}

async function fetchOne(location: Location): Promise<Result> {
  const start = Date.now();
  const url = `https://api.open-meteo.com/v1/forecast?latitude=${location.latitude}&longitude=${location.longitude}&current_weather=true&timeformat=unixtime`;
  try {
    const { data } = await httpClient.get(url);
    const cw = data.current_weather;
    return {
      city_name: location.city_name,
      ok: true,
      latencyMs: Date.now() - start,
      temperature: cw.temperature,
      condition: WMO_CODES[cw.weathercode] ?? `WMO-${cw.weathercode}`,
    };
  } catch (err: any) {
    return { city_name: location.city_name, ok: false, latencyMs: Date.now() - start, error: err.message };
  }
}

/** Fixed-size worker pool — each worker pulls from the queue until empty. */
async function runWithWorkerPool(locations: Location[]): Promise<Result[]> {
  const results: Result[] = [];
  const queue = [...locations];
  let completed = 0;
  const total = locations.length;

  async function worker(id: number): Promise<void> {
    while (true) {
      const location = queue.shift();
      if (!location) return;

      const result = await fetchOne(location);
      results.push(result);
      completed++;

      if (result.ok) {
        process.stdout.write(`\r[${String(completed).padStart(4)}/${total}] ✓ ${result.city_name.padEnd(22)} ${String(result.temperature).padEnd(6)}°C  ${(result.condition ?? "").padEnd(20)}  [${result.latencyMs}ms]  (worker ${id})\n`);
      } else {
        process.stdout.write(`\r[${String(completed).padStart(4)}/${total}] ✗ ${result.city_name.padEnd(22)} ${result.error}\n`);
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i + 1)));
  return results;
}

async function main() {
  console.log(`\n🌍 ${ALL_LOCATIONS.length} locations — ${CONCURRENCY} workers\n`);

  const wallStart = Date.now();
  const results = await runWithWorkerPool(ALL_LOCATIONS);
  const wallMs = Date.now() - wallStart;

  const ok = results.filter(r => r.ok);
  const failed = results.filter(r => !r.ok);
  const latencies = ok.map(r => r.latencyMs).sort((a, b) => a - b);
  const avg = latencies.length ? Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length) : 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] ?? 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)] ?? 0;

  console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 Total     : ${ALL_LOCATIONS.length}
 ✓ Success : ${ok.length}
 ✗ Failed  : ${failed.length}
 Wall time : ${(wallMs / 1000).toFixed(1)}s
 Throughput: ${Math.round(ALL_LOCATIONS.length / (wallMs / 1000))} req/s
 Latency   : avg ${avg}ms  p95 ${p95}ms  p99 ${p99}ms
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  if (failed.length > 0) {
    console.log("\nFailed:");
    failed.forEach(r => console.log(`  ✗ ${r.city_name}: ${r.error}`));
  }
}

main();
