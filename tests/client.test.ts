import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Client } from "../src/client.js";
import { Config } from "../src/config.js";
import { Job } from "../src/job.js";
import { Sidekiq } from "../src/sidekiq.js";

class HardJob extends Job<[number, number]> {
  static sidekiqOptions = { queue: "critical", retry: 5 };

  async perform(_a: number, _b: number) {
    // no-op
  }
}

const redisUrl = "redis://localhost:6379/0";

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Client", () => {
  it("pushes jobs to a queue with Sidekiq-compatible payloads", async () => {
    const jid = await HardJob.performAsync(1, 2);
    expect(jid).toHaveLength(24);

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const queues = await redis.sMembers("queues");
    expect(queues).toContain("critical");

    const entries = await redis.lRange("queue:critical", 0, -1);
    expect(entries).toHaveLength(1);

    const payload = JSON.parse(entries[0]);
    expect(payload.class).toBe("HardJob");
    expect(payload.args).toEqual([1, 2]);
    expect(payload.queue).toBe("critical");
    expect(typeof payload.enqueued_at).toBe("number");
    expect(typeof payload.created_at).toBe("number");
  });

  it("pushes scheduled jobs into the schedule set", async () => {
    const now = Date.now() / 1000;
    await HardJob.performIn(60, 3, 4);

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const range = (await redis.sendCommand([
      "ZRANGE",
      "schedule",
      "0",
      "-1",
      "WITHSCORES",
    ])) as string[];

    expect(range).toHaveLength(2);
    const [value, score] = range;
    const payload = JSON.parse(value);

    expect(payload.class).toBe("HardJob");
    expect(payload.args).toEqual([3, 4]);
    expect(payload.at).toBeUndefined();
    expect(payload.enqueued_at).toBeUndefined();
    expect(Number(score)).toBeGreaterThan(now + 30);
  });

  it("pushes bulk jobs in a single call", async () => {
    const client = new Client();
    const result = await client.pushBulk({
      class: HardJob,
      args: [
        [10, 11],
        [12, 13],
      ],
    });

    expect(result).toHaveLength(2);
    for (const jid of result) {
      expect(jid).toHaveLength(24);
    }

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const entries = await redis.lRange("queue:critical", 0, -1);
    expect(entries).toHaveLength(2);
  });
});

describe("Reliable Client", () => {
  // Redis options that fail fast (no retries)
  const badRedisOptions = {
    url: "redis://localhost:9999",
    socket: {
      connectTimeout: 100,
      reconnectStrategy: false as const,
    },
  };

  beforeEach(() => {
    Client.clearLocalQueue();
  });

  it("queues jobs locally when Redis is unavailable", async () => {
    const badConfig = new Config({
      redis: badRedisOptions,
      reliableClientMaxQueue: 100,
    });

    const client = new Client({ config: badConfig });
    const jid = await client.push({
      class: "TestJob",
      args: [1, 2],
      queue: "default",
    });

    // Push should succeed (return JID) even though Redis is down
    expect(jid).toHaveLength(24);
    expect(Client.localQueueSize()).toBe(1);

    await badConfig.close();
  });

  it("drains local queue on successful push", async () => {
    const badConfig = new Config({
      redis: badRedisOptions,
      reliableClientMaxQueue: 100,
    });

    const badClient = new Client({ config: badConfig });
    await badClient.push({ class: "TestJob", args: [1], queue: "drain_test" });
    await badClient.push({ class: "TestJob", args: [2], queue: "drain_test" });
    expect(Client.localQueueSize()).toBe(2);
    await badConfig.close();

    // Now push with a good connection - should drain the local queue
    const goodClient = new Client();
    await goodClient.push({ class: "TestJob", args: [3], queue: "drain_test" });

    expect(Client.localQueueSize()).toBe(0);

    // All 3 jobs should be in Redis
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const entries = await redis.lRange("queue:drain_test", 0, -1);
    expect(entries).toHaveLength(3);
  });

  it("respects max queue size", async () => {
    const badConfig = new Config({
      redis: badRedisOptions,
      reliableClientMaxQueue: 3,
    });

    const client = new Client({ config: badConfig });

    // Push 5 jobs, only 3 should be queued
    for (let i = 0; i < 5; i++) {
      await client.push({ class: "TestJob", args: [i], queue: "default" });
    }

    expect(Client.localQueueSize()).toBe(3);
    await badConfig.close();
  });

  it("throws when reliability is disabled", async () => {
    const badConfig = new Config({
      redis: badRedisOptions,
      reliableClientMaxQueue: 0,
    });

    const client = new Client({ config: badConfig });

    await expect(
      client.push({ class: "TestJob", args: [1], queue: "default" })
    ).rejects.toThrow();

    expect(Client.localQueueSize()).toBe(0);
    await badConfig.close();
  });

  it("handles scheduled jobs in local queue", async () => {
    const badConfig = new Config({
      redis: badRedisOptions,
      reliableClientMaxQueue: 100,
    });

    const client = new Client({ config: badConfig });
    const futureTime = Date.now() / 1000 + 3600;
    await client.push({
      class: "TestJob",
      args: [1],
      queue: "default",
      at: futureTime,
    });

    expect(Client.localQueueSize()).toBe(1);
    await badConfig.close();

    // Drain with good connection
    const goodClient = new Client();
    await goodClient.push({ class: "TestJob", args: [2], queue: "default" });

    expect(Client.localQueueSize()).toBe(0);

    // Check schedule set has the job
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const scheduled = await redis.zRange("schedule", 0, -1);
    expect(scheduled.length).toBeGreaterThanOrEqual(1);

    const found = scheduled.some((entry) => {
      const payload = JSON.parse(entry);
      return payload.class === "TestJob" && payload.args[0] === 1;
    });
    expect(found).toBe(true);
  });
});
