import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Job, Queue, Sidekiq } from "../src/index.js";

let release: (() => void) | null = null;
let started = false;

class BlockingJob extends Job<[]> {
  async perform() {
    started = true;
    await new Promise<void>((resolve) => {
      release = resolve;
    });
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
  Sidekiq.defaultConfiguration.timeout = 0.1;
  Sidekiq.registerJob(BlockingJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  release = null;
  started = false;
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Shutdown requeue", () => {
  it("requeues in-progress jobs on stop", async () => {
    const runner = await Sidekiq.run();
    try {
      await BlockingJob.performAsync();
      await waitFor(() => started === true);

      await runner.stop();

      const queue = new Queue("default");
      expect(await queue.size()).toBe(1);
    } finally {
      release?.();
    }
  });

  it("captures in-progress jobs in snapshot", async () => {
    const runner = await Sidekiq.run();
    try {
      await BlockingJob.performAsync();
      await waitFor(() => started === true);

      const snapshot = runner.snapshotWork();
      expect(snapshot).toHaveLength(1);
      expect(snapshot[0].queue).toBe("default");
      expect(snapshot[0].payload?.class).toBe("BlockingJob");
      expect(snapshot[0].elapsed).toBeGreaterThanOrEqual(0);
    } finally {
      release?.();
      await runner.stop();
    }
  });
});
