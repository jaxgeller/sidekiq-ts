import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Config } from "../src/config.js";
import {
  BucketLimiter,
  ConcurrentLimiter,
  Limiter,
  OverLimitError,
  WindowLimiter,
} from "../src/limiter/index.js";
import type { RedisClient } from "../src/redis.js";

const silentLogger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  level: "silent" as const,
  with: () => silentLogger,
};

describe("Rate Limiters", () => {
  let config: Config;
  let redis: RedisClient;
  const getRedis = () => config.getRedisClient();

  beforeEach(async () => {
    config = new Config({ logger: silentLogger });
    redis = await config.getRedisClient();
    // Clean up any limiter keys
    const keys = await redis.keys("limiter:*");
    if (keys.length > 0) {
      await redis.del(keys);
    }
  });

  afterEach(async () => {
    const keys = await redis.keys("limiter:*");
    if (keys.length > 0) {
      await redis.del(keys);
    }
    await redis.quit();
  });

  describe("Limiter factory", () => {
    it("creates a ConcurrentLimiter", () => {
      const limiter = Limiter.concurrent("test", 5);
      expect(limiter).toBeInstanceOf(ConcurrentLimiter);
      expect(limiter.name).toBe("test");
    });

    it("creates a BucketLimiter", () => {
      const limiter = Limiter.bucket("test", 100, "minute");
      expect(limiter).toBeInstanceOf(BucketLimiter);
      expect(limiter.name).toBe("test");
    });

    it("creates a WindowLimiter", () => {
      const limiter = Limiter.window("test", 50, "hour");
      expect(limiter).toBeInstanceOf(WindowLimiter);
      expect(limiter.name).toBe("test");
    });
  });

  describe("ConcurrentLimiter", () => {
    it("allows operations up to max concurrent", async () => {
      const limiter = Limiter.concurrent("concurrent-test", 2, {}, getRedis);

      const results: number[] = [];

      // Start 2 concurrent operations
      const p1 = limiter.withinLimit(async () => {
        results.push(1);
        await new Promise((r) => setTimeout(r, 50));
        return 1;
      });

      const p2 = limiter.withinLimit(async () => {
        results.push(2);
        await new Promise((r) => setTimeout(r, 50));
        return 2;
      });

      const [r1, r2] = await Promise.all([p1, p2]);
      expect(r1).toBe(1);
      expect(r2).toBe(2);
      expect(results).toHaveLength(2);
    });

    it("throws OverLimitError when at max concurrent", async () => {
      const limiter = Limiter.concurrent("concurrent-block", 1, {}, getRedis);

      let secondStarted = false;

      const p1 = limiter.withinLimit(async () => {
        await new Promise((r) => setTimeout(r, 100));
        return "first";
      });

      // Give p1 time to acquire
      await new Promise((r) => setTimeout(r, 20));

      await expect(
        limiter.withinLimit(() => {
          secondStarted = true;
          return "second";
        })
      ).rejects.toThrow(OverLimitError);

      expect(secondStarted).toBe(false);
      await p1;
    });

    it("releases lock when operation completes", async () => {
      const limiter = Limiter.concurrent("concurrent-release", 1, {}, getRedis);

      await limiter.withinLimit(() => "first");

      // Should be able to acquire again immediately
      const result = await limiter.withinLimit(() => "second");
      expect(result).toBe("second");
    });

    it("releases lock even on error", async () => {
      const limiter = Limiter.concurrent("concurrent-error", 1, {}, getRedis);

      await expect(
        limiter.withinLimit(() => {
          throw new Error("test error");
        })
      ).rejects.toThrow("test error");

      // Should be able to acquire again
      const result = await limiter.withinLimit(() => "recovered");
      expect(result).toBe("recovered");
    });

    it("check() returns current status without acquiring", async () => {
      const limiter = Limiter.concurrent("concurrent-check", 2, {}, getRedis);

      const status1 = await limiter.check();
      expect(status1.allowed).toBe(true);
      expect(status1.current).toBe(0);
      expect(status1.limit).toBe(2);

      // Acquire a slot
      const p1 = limiter.withinLimit(async () => {
        await new Promise((r) => setTimeout(r, 100));
      });

      await new Promise((r) => setTimeout(r, 10));

      const status2 = await limiter.check();
      expect(status2.allowed).toBe(true);
      expect(status2.current).toBe(1);

      await p1;
    });

    it("reset() clears all locks", async () => {
      const limiter = Limiter.concurrent("concurrent-reset", 1, {}, getRedis);

      // Acquire and hold the lock
      const p1 = limiter.withinLimit(async () => {
        await new Promise((r) => setTimeout(r, 500));
      });

      await new Promise((r) => setTimeout(r, 10));

      // Reset should clear it
      await limiter.reset();

      // Should be able to acquire now
      const result = await limiter.withinLimit(() => "after-reset");
      expect(result).toBe("after-reset");

      await p1;
    });
  });

  describe("BucketLimiter", () => {
    it("allows operations up to max per interval", async () => {
      const limiter = Limiter.bucket("bucket-test", 3, 60, {}, getRedis);

      const results: number[] = [];
      for (let i = 0; i < 3; i++) {
        results.push(await limiter.withinLimit(() => i));
      }

      expect(results).toEqual([0, 1, 2]);
    });

    it("throws OverLimitError when bucket is full", async () => {
      const limiter = Limiter.bucket("bucket-full", 2, 60, {}, getRedis);

      await limiter.withinLimit(() => 1);
      await limiter.withinLimit(() => 2);

      await expect(limiter.withinLimit(() => 3)).rejects.toThrow(
        OverLimitError
      );
    });

    it("provides retryAfter when over limit", async () => {
      const limiter = Limiter.bucket("bucket-retry", 1, 60, {}, getRedis);

      await limiter.withinLimit(() => 1);

      try {
        await limiter.withinLimit(() => 2);
      } catch (error) {
        expect(error).toBeInstanceOf(OverLimitError);
        expect((error as OverLimitError).retryAfter).toBeGreaterThan(0);
        expect((error as OverLimitError).retryAfter).toBeLessThanOrEqual(60);
      }
    });

    it("check() returns current count", async () => {
      const limiter = Limiter.bucket("bucket-check", 5, 60, {}, getRedis);

      const status1 = await limiter.check();
      expect(status1.allowed).toBe(true);
      expect(status1.current).toBe(0);

      await limiter.withinLimit(() => 1);
      await limiter.withinLimit(() => 2);

      const status2 = await limiter.check();
      expect(status2.current).toBe(2);
      expect(status2.limit).toBe(5);
    });

    it("accepts numeric interval (seconds)", async () => {
      const limiter = Limiter.bucket("bucket-numeric", 5, 30, {}, getRedis);

      await limiter.withinLimit(() => "ok");
      const status = await limiter.check();
      expect(status.current).toBe(1);
    });
  });

  describe("WindowLimiter", () => {
    it("allows operations up to max within window", async () => {
      const limiter = Limiter.window("window-test", 3, 60, {}, getRedis);

      const results: number[] = [];
      for (let i = 0; i < 3; i++) {
        results.push(await limiter.withinLimit(() => i));
      }

      expect(results).toEqual([0, 1, 2]);
    });

    it("throws OverLimitError when window is full", async () => {
      const limiter = Limiter.window("window-full", 2, 60, {}, getRedis);

      await limiter.withinLimit(() => 1);
      await limiter.withinLimit(() => 2);

      await expect(limiter.withinLimit(() => 3)).rejects.toThrow(
        OverLimitError
      );
    });

    it("provides retryAfter based on oldest entry", async () => {
      const limiter = Limiter.window("window-retry", 1, 10, {}, getRedis);

      await limiter.withinLimit(() => 1);

      try {
        await limiter.withinLimit(() => 2);
      } catch (error) {
        expect(error).toBeInstanceOf(OverLimitError);
        expect((error as OverLimitError).retryAfter).toBeGreaterThan(0);
        expect((error as OverLimitError).retryAfter).toBeLessThanOrEqual(10);
      }
    });

    it("check() returns current count without consuming", async () => {
      const limiter = Limiter.window("window-check", 5, 60, {}, getRedis);

      await limiter.withinLimit(() => 1);
      await limiter.withinLimit(() => 2);

      const status = await limiter.check();
      expect(status.current).toBe(2);
      expect(status.limit).toBe(5);
      expect(status.allowed).toBe(true);
    });

    it("accepts numeric interval (seconds)", async () => {
      const limiter = Limiter.window("window-numeric", 5, 30, {}, getRedis);

      await limiter.withinLimit(() => "ok");
      const status = await limiter.check();
      expect(status.current).toBe(1);
    });
  });

  describe("OverLimitError", () => {
    it("includes limiter name and details", async () => {
      const limiter = Limiter.bucket("error-details", 1, 60, {}, getRedis);

      await limiter.withinLimit(() => 1);

      try {
        await limiter.withinLimit(() => 2);
      } catch (error) {
        expect(error).toBeInstanceOf(OverLimitError);
        const e = error as OverLimitError;
        expect(e.limiterName).toBe("error-details");
        expect(e.current).toBe(1);
        expect(e.limit).toBe(1);
        expect(e.message).toContain("error-details");
      }
    });
  });

  describe("Custom key prefix", () => {
    it("uses custom prefix for Redis keys", async () => {
      const limiter = Limiter.concurrent(
        "custom-prefix",
        5,
        { keyPrefix: "myapp" },
        getRedis
      );

      // Check Redis key while operation is in progress
      const p1 = limiter.withinLimit(async () => {
        // Check that key exists while we hold the lock
        const keys = await redis.keys("myapp:*");
        expect(keys.length).toBeGreaterThan(0);
        expect(keys[0]).toBe("myapp:custom-prefix");
        return "test";
      });

      await p1;
    });
  });
});
