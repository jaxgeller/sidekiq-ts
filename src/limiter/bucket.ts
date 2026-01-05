import { BaseLimiter } from "./base.js";
import { LUA_BUCKET_ACQUIRE } from "./lua-scripts.js";
import type {
  AcquireResult,
  BucketLimiterOptions,
  RedisProvider,
  TimeInterval,
} from "./types.js";

const INTERVAL_SECONDS: Record<TimeInterval, number> = {
  second: 1,
  minute: 60,
  hour: 3600,
  day: 86_400,
};

/**
 * Limits operations per time boundary (e.g., 100 per minute).
 * Resets at fixed time boundaries (start of minute, hour, etc).
 */
export class BucketLimiter extends BaseLimiter {
  private readonly maxCount: number;
  private readonly intervalSeconds: number;

  constructor(
    name: string,
    maxCount: number,
    interval: TimeInterval | number,
    options: BucketLimiterOptions = {},
    redisProvider?: RedisProvider
  ) {
    super(name, options, redisProvider);
    this.maxCount = maxCount;
    this.intervalSeconds =
      typeof interval === "number" ? interval : INTERVAL_SECONDS[interval];
  }

  private getBucketTimestamp(): string {
    const now = Math.floor(Date.now() / 1000);
    const bucketStart =
      Math.floor(now / this.intervalSeconds) * this.intervalSeconds;
    return String(bucketStart);
  }

  protected async tryAcquire(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const bucketTs = this.getBucketTimestamp();

    const result = (await redis.eval(LUA_BUCKET_ACQUIRE, {
      keys: [this.redisKey],
      arguments: [
        String(this.maxCount),
        bucketTs,
        String(this.intervalSeconds + 1),
      ],
    })) as [number, number, number];

    const [allowed, current] = result;

    // Calculate actual retry time until next bucket
    const now = Date.now() / 1000;
    const bucketEnd =
      Math.floor(now / this.intervalSeconds) * this.intervalSeconds +
      this.intervalSeconds;
    const actualRetry = bucketEnd - now;

    return {
      allowed: allowed === 1,
      current,
      limit: this.maxCount,
      retryAfter: allowed === 0 ? actualRetry : undefined,
    };
  }

  async check(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const bucketTs = this.getBucketTimestamp();

    const current = Number(await redis.hGet(this.redisKey, bucketTs)) || 0;

    return {
      allowed: current < this.maxCount,
      current,
      limit: this.maxCount,
    };
  }
}
