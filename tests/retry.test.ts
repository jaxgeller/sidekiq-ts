import { afterAll, beforeAll, beforeEach, describe, it, expect } from "vitest";
import { Sidekiq, Job } from "../src/index.js";
import { setTimeout as sleep } from "node:timers/promises";

class FailingJob extends Job<[number]> {
  static sidekiqOptions = { retry: 1 };

  async perform(_value: number) {
    throw new Error("boom");
  }
}

class DeadJob extends Job<[string]> {
  static sidekiqOptions = { retry: 0 };

  async perform(_value: string) {
    throw new Error("nope");
  }
}

const redisUrl = "redis://localhost:6379/0";

const waitFor = async (condition: () => Promise<boolean> | boolean, timeoutMs = 1000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) {
      return;
    }
    await sleep(20);
  }
  throw new Error("Timed out waiting for condition");
};

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
  Sidekiq.defaultConfiguration.concurrency = 1;
  Sidekiq.defaultConfiguration.queues = ["default"];
  Sidekiq.defaultConfiguration.averageScheduledPollInterval = 1;
  Sidekiq.registerJob(FailingJob);
  Sidekiq.registerJob(DeadJob);
  FailingJob.retryIn(() => 60);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Retry handling", () => {
  it("moves failed jobs to the retry set", async () => {
    const runner = await Sidekiq.run();
    try {
      await FailingJob.performAsync(1);

      const redis = await Sidekiq.defaultConfiguration.getRedisClient();
      await waitFor(async () => {
        const count = Number(await redis.sendCommand(["ZCARD", "retry"]));
        return count >= 1;
      });

      const result = (await redis.sendCommand([
        "ZRANGE",
        "retry",
        "0",
        "0",
      ])) as string[];
      const payload = JSON.parse(result[0]);

      expect(payload.class).toBe("FailingJob");
      expect(payload.retry_count).toBe(0);
      expect(payload.error_message).toBe("boom");
      expect(payload.queue).toBe("default");
    } finally {
      await runner.stop();
    }
  });

  it("sends exhausted jobs to the dead set", async () => {
    const runner = await Sidekiq.run();
    try {
      await DeadJob.performAsync("x");

      const redis = await Sidekiq.defaultConfiguration.getRedisClient();
      await waitFor(async () => {
        const count = Number(await redis.sendCommand(["ZCARD", "dead"]));
        return count >= 1;
      });

      const result = (await redis.sendCommand([
        "ZRANGE",
        "dead",
        "0",
        "0",
      ])) as string[];
      const payload = JSON.parse(result[0]);

      expect(payload.class).toBe("DeadJob");
      expect(payload.retry_count).toBe(0);
      expect(payload.error_message).toBe("nope");
    } finally {
      await runner.stop();
    }
  });
});
