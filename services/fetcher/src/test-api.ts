/**
 * Smoke test — fetches 5 cities to verify the API is reachable.
 * Run: npm run test:api
 */
import axios from "axios";
import * as https from "https";
import { ALL_LOCATIONS } from "./locations";

const httpClient = axios.create({
  timeout: 10_000,
  httpsAgent: new https.Agent({ family: 4 }),
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

async function main() {
  console.log(`\nLocations loaded: ${ALL_LOCATIONS.length}`);
  console.log("Fetching 5 sample cities...\n");

  const sample = [
    ALL_LOCATIONS.find(l => l.city_name === "Bangalore")!,
    ALL_LOCATIONS.find(l => l.city_name === "London")!,
    ALL_LOCATIONS.find(l => l.city_name === "New York")!,
    ALL_LOCATIONS.find(l => l.city_name === "Tokyo")!,
    ALL_LOCATIONS.find(l => l.city_name === "Sydney")!,
  ];

  for (const location of sample) {
    try {
      const start = Date.now();
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${location.latitude}&longitude=${location.longitude}&current_weather=true&timeformat=unixtime`;
      const { data } = await httpClient.get(url);
      const cw = data.current_weather;
      const condition = WMO_CODES[cw.weathercode] ?? `WMO-${cw.weathercode}`;
      const recordedAt = new Date(cw.time * 1000).toISOString();
      console.log(`✓ ${location.city_name.padEnd(20)} ${String(cw.temperature).padEnd(6)}°C  ${condition.padEnd(20)}  ${recordedAt}  [${Date.now() - start}ms]`);
    } catch (err: any) {
      console.error(`✗ ${location.city_name}: ${err.message}`);
    }
  }
}

main();
