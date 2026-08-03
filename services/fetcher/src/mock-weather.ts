import { Location } from "./locations";
import { WeatherPayload } from "./fetcher";
import { apiCallsTotal, apiCallsSuccess, apiLatency } from "./metrics";

// Weighted pool of weather conditions — clear skies most common, storms rare
const CONDITIONS: [string, number][] = [
  ["Clear sky",          20],
  ["Mainly clear",       18],
  ["Partly cloudy",      16],
  ["Overcast",           12],
  ["Fog",                 4],
  ["Light drizzle",       6],
  ["Moderate rain",       7],
  ["Heavy rain",          4],
  ["Slight showers",      5],
  ["Thunderstorm",        3],
  ["Slight snow",         3],
  ["Moderate snow",       1],
  ["Rime fog",            1],
];

const TOTAL_WEIGHT = CONDITIONS.reduce((s, [, w]) => s + w, 0);

function pickCondition(lat: number): string {
  const canSnow = Math.abs(lat) > 45;
  let roll = Math.random() * TOTAL_WEIGHT;
  for (const [condition, weight] of CONDITIONS) {
    roll -= weight;
    if (roll <= 0) {
      if (condition.includes("snow") && !canSnow) return "Moderate rain";
      return condition;
    }
  }
  return "Clear sky";
}

function mockTemperature(lat: number): number {
  const base     = 30 - Math.abs(lat) * 0.5;
  const month    = new Date().getMonth();
  const isSummer = month >= 5 && month <= 8;
  const isNH     = lat >= 0;
  const seasonal = (isSummer === isNH) ? 5 : -5;
  const noise    = (Math.random() - 0.5) * 8;
  return Math.round((base + seasonal + noise) * 10) / 10;
}

// Drop-in replacement for fetchWeather() — same signature, no HTTP calls
export async function mockFetchWeather(location: Location): Promise<WeatherPayload> {
  const end = apiLatency.startTimer();
  apiCallsTotal.inc();

  await sleep(80 + Math.random() * 270); // simulate realistic network latency

  apiCallsSuccess.inc();
  end();

  return {
    city_name:         location.city_name,
    latitude:          location.latitude,
    longitude:         location.longitude,
    temperature:       mockTemperature(location.latitude),
    weather_condition: pickCondition(location.latitude),
    recorded_at:       new Date().toISOString(),
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
