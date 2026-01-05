import type { RedisClient } from "../redis.js";

/** Time intervals for bucket and window limiters */
export type TimeInterval = "second" | "minute" | "hour" | "day";

/** Result of an acquire attempt */
export interface AcquireResult {
  allowed: boolean;
  /** Seconds until the limit resets */
  retryAfter?: number;
  /** Current usage count */
  current?: number;
  /** Maximum allowed */
  limit?: number;
}

/** Common options for all limiters */
export interface LimiterOptions {
  /** Custom Redis key prefix. Default: 'limiter' */
  keyPrefix?: string;
}

/** Options for ConcurrentLimiter */
export interface ConcurrentLimiterOptions extends LimiterOptions {
  /** How long a lock is held before auto-release (seconds). Default: 180 */
  lockTimeout?: number;
}

/** Options for BucketLimiter */
export interface BucketLimiterOptions extends LimiterOptions {}

/** Options for WindowLimiter */
export interface WindowLimiterOptions extends LimiterOptions {}

/** The limiter interface - all limiters implement this */
export interface ILimiter {
  readonly name: string;

  /** Execute callback within rate limit. Throws OverLimitError if limit exceeded. */
  withinLimit<T>(fn: () => Promise<T> | T): Promise<T>;

  /** Check current limit status without consuming */
  check(): Promise<AcquireResult>;

  /** Reset the limiter state */
  reset(): Promise<void>;
}

/** Redis connection provider function */
export type RedisProvider = () => Promise<RedisClient>;
