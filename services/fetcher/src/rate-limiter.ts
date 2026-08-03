import Redis from "ioredis";
import { rateLimiterDenials } from "./metrics";

// Token bucket: 8 req/s cap across all workers via atomic Lua script.
// Cooldown: if a 429 slips through, all workers pause for COOLDOWN_S seconds.

const BUCKET_KEY   = "rate_limiter:weather_api:bucket";
const COOLDOWN_KEY = "rate_limiter:weather_api:cooldown";

const MAX_RPS    = 8;
const CAPACITY   = 8;
const COOLDOWN_S = 30;

// Atomic token consume + time-based refill. Returns 1 if granted, 0 if denied.
const CONSUME_LUA = `
local key         = KEYS[1]
local capacity    = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now         = tonumber(ARGV[3])

local data        = redis.call("HMGET", key, "tokens", "last_refill")
local tokens      = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

local elapsed    = math.max(0, now - last_refill)
local new_tokens = math.min(capacity, tokens + elapsed * refill_rate)

if new_tokens >= 1 then
  new_tokens = new_tokens - 1
  redis.call("HMSET", key, "tokens", new_tokens, "last_refill", now)
  redis.call("EXPIRE", key, 60)
  return 1
else
  redis.call("HMSET", key, "tokens", new_tokens, "last_refill", now)
  redis.call("EXPIRE", key, 60)
  return 0
end
`;

export class RateLimiter {
  private readonly redis: Redis;

  constructor(redis: Redis) {
    this.redis = redis;
  }

  async acquire(): Promise<void> {
    while (true) {
      // Sleep exactly as long as the cooldown has left (PTTL = ms remaining)
      const ttlMs = await this.redis.pttl(COOLDOWN_KEY);
      if (ttlMs > 0) { await sleep(ttlMs); continue; }

      const now = Date.now() / 1000;
      const granted = await this.redis.eval(
        CONSUME_LUA, 1,
        BUCKET_KEY, String(CAPACITY), String(MAX_RPS), String(now),
      ) as number;

      if (granted === 1) return;

      rateLimiterDenials.inc();
      await sleep(40);
    }
  }

  async notifyThrottled(): Promise<void> {
    await this.redis.set(COOLDOWN_KEY, "1", "EX", COOLDOWN_S, "NX");
    console.warn(`[rate-limiter] 429 — cooling down all workers for ${COOLDOWN_S}s`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
