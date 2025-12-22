import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ProcessSet, Workers } from "../src/api.js";
import { Job } from "../src/job.js";
import { Sidekiq } from "../src/sidekiq.js";

class SleepJob extends Job<[number]> {
  async perform(ms: number) {
    await sleep(ms);
  }
}

const redisUrl = "redis://localhost:6379/0";

const waitFor = async (
  condition: () => Promise<boolean> | boolean,
  timeoutMs = 1000
) => {
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
  Sidekiq.defaultConfiguration.heartbeatInterval = 0.1;
  Sidekiq.registerJob(SleepJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Process/worker tracking", () => {
  it("records process heartbeat and work state", async () => {
    const runner = await Sidekiq.run();
    try {
      await SleepJob.performAsync(200);

      const processSet = new ProcessSet();
      await waitFor(async () => (await processSet.entries()).length === 1);
      const processes = await processSet.entries();
      expect(processes[0].identity).toContain(":");
      const info = processes[0].info as { pid?: number };
      expect(info.pid).toBeDefined();

      const workers = new Workers();
      await waitFor(async () => (await workers.entries()).length === 1);
      const entries = await workers.entries();
      expect(entries[0].queue).toBe("default");
      expect(entries[0].payload.class).toBe("SleepJob");
    } finally {
      await runner.stop();
    }
  });

  it("cleans up stale process entries", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.sAdd("processes", ["stale-process"]);

    const processSet = new ProcessSet();
    const cleaned = await processSet.cleanup();
    expect(cleaned).toBe(1);

    const remaining = await redis.sMembers("processes");
    expect(remaining).not.toContain("stale-process");
  });
});
