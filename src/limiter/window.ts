import { BaseLimiter } from "./base.js";
import { LUA_WINDOW_ACQUIRE } from "./lua-scripts.js";
import type {
  AcquireResult,
  RedisProvider,
  TimeInterval,
  WindowLimiterOptions,
} from "./types.js";

const INTERVAL_SECONDS: Record<TimeInterval, number> = {
  second: 1,
  minute: 60,
  hour: 3600,
  day: 86_400,
};

/**
 * Limits operations within a rolling time window.
 * Window starts from first operation and slides continuously.
 */
export class WindowLimiter extends BaseLimiter {
  private readonly maxCount: number;
  private readonly windowSeconds: number;

  constructor(
    name: string,
    maxCount: number,
    interval: TimeInterval | number,
    options: WindowLimiterOptions = {},
    redisProvider?: RedisProvider
  ) {
    super(name, options, redisProvider);
    this.maxCount = maxCount;
    this.windowSeconds =
      typeof interval === "number" ? interval : INTERVAL_SECONDS[interval];
  }

  private generateRequestId(): string {
    return `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  protected async tryAcquire(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const now = Date.now() / 1000;
    const requestId = this.generateRequestId();

    const result = (await redis.eval(LUA_WINDOW_ACQUIRE, {
      keys: [this.redisKey],
      arguments: [
        String(this.maxCount),
        String(this.windowSeconds),
        String(now),
        requestId,
      ],
    })) as [number, number, number];

    const [allowed, current, retryAfter] = result;

    return {
      allowed: allowed === 1,
      current,
      limit: this.maxCount,
      retryAfter: retryAfter > 0 ? retryAfter : undefined,
    };
  }

  async check(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const now = Date.now() / 1000;
    const windowStart = now - this.windowSeconds;

    // Remove expired entries and count
    await redis.zRemRangeByScore(this.redisKey, "-inf", String(windowStart));
    const current = await redis.zCard(this.redisKey);

    return {
      allowed: current < this.maxCount,
      current,
      limit: this.maxCount,
    };
  }
}
