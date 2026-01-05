import type { Config } from "../config.js";
import { dumpJson } from "../json.js";
import type { JobPayload } from "../types.js";
import { OverLimitError } from "./errors.js";

/** Maximum number of times a job can be rescheduled due to rate limiting */
const MAX_OVERRATED = 20;

/** Base delay in seconds for rate limit reschedule (5 minutes) */
const BASE_DELAY_SECONDS = 300;

/**
 * Server middleware that catches OverLimitError and reschedules jobs.
 * Follows Sidekiq Enterprise behavior:
 * - Linear backoff starting at ~5 minutes
 * - Maximum 20 reschedules (~1 day total)
 * - After max reschedules, job fails normally
 */
export class RateLimitMiddleware {
  config?: Config;

  async call(
    _instance: unknown,
    payload: JobPayload,
    _queue: string,
    next: () => Promise<unknown> | unknown
  ): Promise<unknown> {
    try {
      return await next();
    } catch (error) {
      if (error instanceof OverLimitError) {
        return this.handleOverLimit(payload, error);
      }
      throw error;
    }
  }

  private async handleOverLimit(
    payload: JobPayload,
    error: OverLimitError
  ): Promise<void> {
    const overrated = ((payload.overrated as number) ?? 0) + 1;
    payload.overrated = overrated;

    if (overrated > MAX_OVERRATED) {
      // Exceeded max reschedules, let it fail normally
      throw error;
    }

    // Calculate backoff: linear ~5 min + jitter
    const delay = this.calculateBackoff(overrated, error.retryAfter);
    const retryAt = Date.now() / 1000 + delay;

    // Add to retry queue
    const redis = await this.config?.getRedisClient();
    if (!redis) {
      throw error;
    }

    await redis.zAdd("retry", [{ score: retryAt, value: dumpJson(payload) }]);

    this.config?.logger.debug(
      () =>
        `Rate limited: ${error.limiterName}, rescheduled in ${Math.round(delay)}s (attempt ${overrated}/${MAX_OVERRATED})`
    );
  }

  private calculateBackoff(overrated: number, retryAfter?: number): number {
    // Use limiter's retryAfter if available and reasonable
    if (retryAfter !== undefined && retryAfter > 0 && retryAfter < 3600) {
      return retryAfter + Math.random() * 30;
    }

    // Linear backoff: 5 min * attempt + jitter
    return BASE_DELAY_SECONDS * overrated + Math.random() * BASE_DELAY_SECONDS;
  }
}

/**
 * Ensures the RateLimitMiddleware is registered.
 * Call this in your configuration if using rate limiters in jobs.
 */
export const ensureRateLimitMiddleware = (config: Config): void => {
  if (!config.serverMiddleware.exists(RateLimitMiddleware)) {
    config.serverMiddleware.add(RateLimitMiddleware);
  }
};
