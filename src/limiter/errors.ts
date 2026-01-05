/**
 * Thrown when a rate limit is exceeded.
 * Can be caught by RateLimitMiddleware to reschedule jobs.
 */
export class OverLimitError extends Error {
  readonly limiterName: string;
  readonly retryAfter?: number;
  readonly current?: number;
  readonly limit?: number;

  constructor(
    limiterName: string,
    options?: { retryAfter?: number; current?: number; limit?: number }
  ) {
    super(`Rate limit exceeded for limiter: ${limiterName}`);
    this.name = "OverLimitError";
    this.limiterName = limiterName;
    this.retryAfter = options?.retryAfter;
    this.current = options?.current;
    this.limit = options?.limit;
  }
}
