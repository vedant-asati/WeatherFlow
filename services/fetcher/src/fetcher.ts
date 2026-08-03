import axios, { AxiosInstance } from "axios";
import axiosRetry from "axios-retry";
import * as https from "https";
import { Location } from "./locations";
import { apiCallsTotal, apiCallsSuccess, apiCallsFailed, apiLatency } from "./metrics";
import { mockFetchWeather } from "./mock-weather";

// Set USE_MOCK=true to skip real HTTP calls (quota exhausted, offline dev, CI).
// Everything downstream — Redis stream, processor, InfluxDB — is unaffected.
const USE_MOCK = process.env.USE_MOCK === "true";

export interface WeatherPayload {
  city_name: string;
  latitude: number;
  longitude: number;
  temperature: number;
  weather_condition: string;
  recorded_at: string;
}

// WMO code → weather description mapping
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

// Shared axios instance — IPv4 forced, max 50 concurrent sockets
const httpClient: AxiosInstance = axios.create({
  timeout: 10_000,
  httpsAgent: new https.Agent({ family: 4, maxSockets: 50, keepAlive: true }),
});

// Full-jitter exponential backoff, up to 5 retries
axiosRetry(httpClient, {
  retries: 5,
  retryCondition: (err) => {
    const status = err.response?.status ?? 0;
    return axiosRetry.isNetworkError(err) || [429, 500, 502, 503, 504].includes(status);
  },
  retryDelay: (retryCount, err) => {
    const retryAfter = err.response?.headers?.["retry-after"];
    if (retryAfter) return parseFloat(retryAfter) * 1000;
    const cap = Math.min(32_000, 1000 * Math.pow(2, retryCount));
    return Math.random() * cap;
  },
  onRetry: (retryCount, err) => {
    console.warn(`[fetcher] retry ${retryCount}/5 — ${err.message}`);
  },
});

/** Fetch current weather for a single location from Open-Meteo. */
export async function fetchWeather(location: Location): Promise<WeatherPayload> {
  if (USE_MOCK) return mockFetchWeather(location);

  const end = apiLatency.startTimer();
  apiCallsTotal.inc();

  try {
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${location.latitude}&longitude=${location.longitude}&current_weather=true&timeformat=unixtime`;
    const { data } = await httpClient.get(url);

    const cw = data.current_weather;
    if (!cw) throw new Error(`Empty response for ${location.city_name}`);

    apiCallsSuccess.inc();
    return {
      city_name: location.city_name,
      latitude: location.latitude,
      longitude: location.longitude,
      temperature: cw.temperature,
      weather_condition: WMO_CODES[cw.weathercode as number] ?? `WMO-${cw.weathercode}`,
      recorded_at: new Date(cw.time * 1000).toISOString(),
    };
  } catch (err) {
    apiCallsFailed.inc();
    throw err;
  } finally {
    end();
  }
}

/** Lightweight probe used by /healthz to verify Open-Meteo is reachable. */
export async function probeOpenMeteo(): Promise<boolean> {
  try {
    const { status } = await httpClient.get(
      "https://api.open-meteo.com/v1/forecast?latitude=12.97&longitude=77.59&current_weather=true",
      { timeout: 5000 }
    );
    return status === 200;
  } catch {
    return false;
  }
}
