import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Job, Sidekiq } from "../src/index.js";

class RecorderJob extends Job<[number]> {
  static performed: number[] = [];

  async perform(value: number) {
    RecorderJob.performed.push(value);
  }
}

const redisUrl = "redis://localhost:6379/0";

const waitFor = async (condition: () => boolean, timeoutMs = 1000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (condition()) {
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
  Sidekiq.registerJob(RecorderJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  RecorderJob.performed = [];
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Runner", () => {
  it("executes enqueued jobs", async () => {
    const runner = await Sidekiq.run();
    try {
      await RecorderJob.performAsync(42);
      await waitFor(() => RecorderJob.performed.length === 1);
      expect(RecorderJob.performed).toEqual([42]);
    } finally {
      await runner.stop();
    }
  });
});
