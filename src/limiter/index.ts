import { BucketLimiter } from "./bucket.js";
import { ConcurrentLimiter } from "./concurrent.js";
import type {
  BucketLimiterOptions,
  ConcurrentLimiterOptions,
  ILimiter,
  RedisProvider,
  TimeInterval,
  WindowLimiterOptions,
} from "./types.js";
import { WindowLimiter } from "./window.js";

export { BucketLimiter } from "./bucket.js";
export { ConcurrentLimiter } from "./concurrent.js";
export * from "./errors.js";
export {
  ensureRateLimitMiddleware,
  RateLimitMiddleware,
} from "./middleware.js";
export * from "./types.js";
export { WindowLimiter } from "./window.js";

/**
 * Factory class for creating rate limiters.
 * Provides a Sidekiq Enterprise-compatible API.
 *
 * @example
 * ```typescript
 * import { Limiter } from "sidekiq-ts";
 *
 * // Limit concurrent API calls to 50
 * const apiLimiter = Limiter.concurrent("external-api", 50);
 *
 * // Limit to 100 emails per minute
 * const emailLimiter = Limiter.bucket("email-send", 100, "minute");
 *
 * // Limit to 1000 requests per hour (rolling window)
 * const rateLimiter = Limiter.window("api-requests", 1000, "hour");
 *
 * // Use in a job
 * class ApiCallJob extends Job<[string]> {
 *   async perform(endpoint: string) {
 *     await apiLimiter.withinLimit(async () => {
 *       await fetch(endpoint);
 *     });
 *   }
 * }
 * ```
 */
// biome-ignore lint/complexity/noStaticOnlyClass: Factory class provides namespace for limiter creation
export class Limiter {
  /**
   * Create a concurrent limiter (distributed mutex with count).
   * Limits concurrent executions across all processes.
   *
   * @param name - Unique identifier for the limiter
   * @param maxCount - Maximum concurrent operations allowed
   * @param options - Additional options (lockTimeout, keyPrefix)
   * @param redisProvider - Optional custom Redis provider
   */
  static concurrent(
    name: string,
    maxCount: number,
    options?: ConcurrentLimiterOptions,
    redisProvider?: RedisProvider
  ): ILimiter {
    return new ConcurrentLimiter(name, maxCount, options, redisProvider);
  }

  /**
   * Create a bucket limiter (count per time boundary).
   * Resets at fixed time boundaries (start of minute, hour, etc).
   *
   * @param name - Unique identifier for the limiter
   * @param count - Operations allowed per interval
   * @param interval - Time interval ("second", "minute", "hour", "day") or seconds
   * @param options - Additional options (keyPrefix)
   * @param redisProvider - Optional custom Redis provider
   */
  static bucket(
    name: string,
    count: number,
    interval: TimeInterval | number,
    options?: BucketLimiterOptions,
    redisProvider?: RedisProvider
  ): ILimiter {
    return new BucketLimiter(name, count, interval, options, redisProvider);
  }

  /**
   * Create a window limiter (rolling window).
   * Window starts from first operation and slides continuously.
   *
   * @param name - Unique identifier for the limiter
   * @param count - Operations allowed within window
   * @param interval - Window size ("second", "minute", "hour", "day") or seconds
   * @param options - Additional options (keyPrefix)
   * @param redisProvider - Optional custom Redis provider
   */
  static window(
    name: string,
    count: number,
    interval: TimeInterval | number,
    options?: WindowLimiterOptions,
    redisProvider?: RedisProvider
  ): ILimiter {
    return new WindowLimiter(name, count, interval, options, redisProvider);
  }
}
