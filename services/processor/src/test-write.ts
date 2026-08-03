/**
 * CP3b smoke test — writes 5 hardcoded weather points to InfluxDB
 * and verifies the connection works before we wire real data through.
 *
 * Run: npm run test:write
 *
 * After running, go to:
 *   http://localhost:8086 → Data Explorer
 *   Filter: bucket=weather_bucket, measurement=weather
 *   You should see 5 points appear.
 */

import { InfluxWriter } from "./influx-writer";

const TEST_POINTS = [
  { city_name: "London",    latitude: 51.51,  longitude: -0.13,   temperature: 13.5, weather_condition: "Overcast",     recorded_at: new Date().toISOString() },
  { city_name: "Tokyo",     latitude: 35.69,  longitude: 139.69,  temperature: 18.2, weather_condition: "Partly cloudy", recorded_at: new Date().toISOString() },
  { city_name: "Bangalore", latitude: 12.97,  longitude: 77.59,   temperature: 28.4, weather_condition: "Clear sky",     recorded_at: new Date().toISOString() },
  { city_name: "New York",  latitude: 40.71,  longitude: -74.01,  temperature: 9.7,  weather_condition: "Mainly clear",  recorded_at: new Date().toISOString() },
  { city_name: "Sydney",    latitude: -33.87, longitude: 151.21,  temperature: 22.1, weather_condition: "Slight rain",   recorded_at: new Date().toISOString() },
];

async function main() {
  console.log("━".repeat(50));
  console.log(" CP3b — InfluxDB write test");
  console.log("━".repeat(50));

  const writer = new InfluxWriter();

  for (const point of TEST_POINTS) {
    writer.write(point);
    console.log(`  ✓ wrote  ${point.city_name.padEnd(12)} ${point.temperature}°C  ${point.weather_condition}`);
  }

  // close() flushes all buffered points before disconnecting
  await writer.close();

  console.log("\n━".repeat(50));
  console.log(" Done! Check InfluxDB UI:");
  console.log("   http://localhost:8086");
  console.log("   Data Explorer → bucket: weather_bucket → measurement: weather");
  console.log("━".repeat(50));
}

main().catch(err => {
  console.error("Fatal:", err.message);
  process.exit(1);
});
