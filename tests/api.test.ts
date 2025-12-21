import { afterAll, beforeAll, beforeEach, describe, it, expect } from "vitest";
import {
  Sidekiq,
  Job,
  Stats,
  StatsHistory,
  Queue,
  ScheduledSet,
  RetrySet,
  DeadSet,
} from "../src/index.js";

class ApiJob extends Job<[number]> {
  async perform(_value: number) {
    // no-op
  }
}

const redisUrl = "redis://localhost:6379/0";

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
  Sidekiq.registerJob(ApiJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Data API", () => {
  it("exposes stats and queue sizes", async () => {
    await ApiJob.performAsync(1);

    const stats = new Stats();
    expect(await stats.enqueued()).toBe(1);
    const queues = await stats.queues();
    expect(queues.default).toBe(1);

    const queue = new Queue("default");
    expect(await queue.size()).toBe(1);
    const entries = await queue.entries();
    expect(entries[0].class).toBe("ApiJob");
  });

  it("resets processed/failed stats", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.set("stat:processed", "5");
    await redis.set("stat:failed", "2");

    const stats = new Stats();
    await stats.reset();

    expect(await stats.processed()).toBe(0);
    expect(await stats.failed()).toBe(0);
  });

  it("reads historical stats", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const today = new Date().toISOString().slice(0, 10);
    await redis.set(`stat:processed:${today}`, "3");
    await redis.set(`stat:failed:${today}`, "1");

    const history = new StatsHistory(1);
    const processed = await history.processed();
    const failed = await history.failed();

    expect(processed[today]).toBe(3);
    expect(failed[today]).toBe(1);
  });

  it("exposes scheduled, retry, and dead sets", async () => {
    await ApiJob.performIn(60, 2);

    const scheduled = new ScheduledSet();
    expect(await scheduled.size()).toBe(1);
    const scheduledEntries = await scheduled.entries();
    expect(scheduledEntries[0].payload.class).toBe("ApiJob");

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.zAdd("retry", [
      {
        score: Date.now() / 1000 + 30,
        value: Sidekiq.dumpJson({
          class: "ApiJob",
          args: [1],
          queue: "default",
          jid: "abc",
        }),
      },
    ]);
    await redis.zAdd("dead", [
      {
        score: Date.now() / 1000 + 60,
        value: Sidekiq.dumpJson({
          class: "ApiJob",
          args: [1],
          queue: "default",
          jid: "def",
        }),
      },
    ]);

    const retrySet = new RetrySet();
    const deadSet = new DeadSet();
    expect(await retrySet.size()).toBe(1);
    expect(await deadSet.size()).toBe(1);
  });
});
