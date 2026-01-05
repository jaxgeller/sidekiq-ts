import { Sidekiq } from "../sidekiq.js";
import { OverLimitError } from "./errors.js";
import type {
  AcquireResult,
  ILimiter,
  LimiterOptions,
  RedisProvider,
} from "./types.js";

export abstract class BaseLimiter implements ILimiter {
  readonly name: string;
  protected readonly keyPrefix: string;
  protected readonly getRedis: RedisProvider;

  constructor(
    name: string,
    options: LimiterOptions = {},
    redisProvider?: RedisProvider
  ) {
    this.name = name;
    this.keyPrefix = options.keyPrefix ?? "limiter";
    this.getRedis =
      redisProvider ?? (() => Sidekiq.defaultConfiguration.getRedisClient());
  }

  protected get redisKey(): string {
    return `${this.keyPrefix}:${this.name}`;
  }

  /** Attempt to acquire the limit. Implemented by subclasses. */
  protected abstract tryAcquire(): Promise<AcquireResult>;

  /** Release any held resources. Override in ConcurrentLimiter. */
  protected async release(): Promise<void> {
    // Default: no-op
  }

  check(): Promise<AcquireResult> {
    return this.tryAcquire();
  }

  async reset(): Promise<void> {
    const redis = await this.getRedis();
    await redis.del(this.redisKey);
  }

  async withinLimit<T>(fn: () => Promise<T> | T): Promise<T> {
    const result = await this.tryAcquire();

    if (!result.allowed) {
      throw new OverLimitError(this.name, {
        retryAfter: result.retryAfter,
        current: result.current,
        limit: result.limit,
      });
    }

    try {
      return await fn();
    } finally {
      await this.release();
    }
  }
}
