import { Counter, Histogram, Registry } from "prom-client";

export const registry = new Registry();

export const apiCallsTotal = new Counter({
  name: "weather_api_calls_total",
  help: "Total API calls to Open-Meteo",
  registers: [registry],
});

export const apiCallsSuccess = new Counter({
  name: "weather_api_calls_success_total",
  help: "Successful API calls",
  registers: [registry],
});

export const apiCallsFailed = new Counter({
  name: "weather_api_calls_failed_total",
  help: "Failed API calls after all retries",
  registers: [registry],
});

export const apiLatency = new Histogram({
  name: "weather_api_response_latency_seconds",
  help: "API response latency in seconds",
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
  registers: [registry],
});

export const rateLimiterDenials = new Counter({
  name: "weather_rate_limiter_denials_total",
  help: "Rate limiter token denials",
  registers: [registry],
});
