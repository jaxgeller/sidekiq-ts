import { BaseLimiter } from "./base.js";
import {
  LUA_CONCURRENT_ACQUIRE,
  LUA_CONCURRENT_RELEASE,
} from "./lua-scripts.js";
import type {
  AcquireResult,
  ConcurrentLimiterOptions,
  RedisProvider,
} from "./types.js";

/**
 * Limits concurrent executions across all processes.
 * Uses Redis ZSET with expiring locks for distributed mutex behavior.
 */
export class ConcurrentLimiter extends BaseLimiter {
  private readonly maxConcurrent: number;
  private readonly lockTimeout: number;
  private currentLockId?: string;

  constructor(
    name: string,
    maxConcurrent: number,
    options: ConcurrentLimiterOptions = {},
    redisProvider?: RedisProvider
  ) {
    super(name, options, redisProvider);
    this.maxConcurrent = maxConcurrent;
    this.lockTimeout = options.lockTimeout ?? 180;
  }

  private generateLockId(): string {
    return `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  protected async tryAcquire(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const lockId = this.generateLockId();
    const now = Date.now() / 1000;

    const result = (await redis.eval(LUA_CONCURRENT_ACQUIRE, {
      keys: [this.redisKey],
      arguments: [
        String(this.maxConcurrent),
        lockId,
        String(now),
        String(this.lockTimeout),
      ],
    })) as [number, number, number];

    const [allowed, current, retryAfter] = result;

    if (allowed === 1) {
      this.currentLockId = lockId;
    }

    return {
      allowed: allowed === 1,
      current,
      limit: this.maxConcurrent,
      retryAfter: retryAfter > 0 ? retryAfter : undefined,
    };
  }

  protected async release(): Promise<void> {
    if (!this.currentLockId) {
      return;
    }

    const redis = await this.getRedis();
    await redis.eval(LUA_CONCURRENT_RELEASE, {
      keys: [this.redisKey],
      arguments: [this.currentLockId],
    });
    this.currentLockId = undefined;
  }

  async check(): Promise<AcquireResult> {
    const redis = await this.getRedis();
    const now = Date.now() / 1000;

    // Remove expired and count without acquiring
    await redis.zRemRangeByScore(this.redisKey, "-inf", String(now));
    const current = await redis.zCard(this.redisKey);

    return {
      allowed: current < this.maxConcurrent,
      current,
      limit: this.maxConcurrent,
    };
  }
}
