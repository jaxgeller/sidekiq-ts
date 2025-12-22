import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  DeadSet,
  ProcessSet,
  Queue,
  RetrySet,
  ScheduledSet,
  Sidekiq,
} from "../src/index.js";

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

describe("Data API actions", () => {
  it("iterates queue entries and deletes by job record", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const payloads = [
      Sidekiq.dumpJson({ class: "JobA", args: [], queue: "default", jid: "jid-1" }),
      Sidekiq.dumpJson({ class: "JobA", args: [], queue: "default", jid: "jid-2" }),
    ];
    await redis.rPush("queue:default", payloads);
    await redis.sAdd("queues", ["default"]);

    const queue = new Queue("default");
    const records = [];
    for await (const record of queue.each()) {
      records.push(record);
    }
    expect(records).toHaveLength(2);

    const record = await queue.findJob("jid-1");
    expect(record?.payload.jid).toBe("jid-1");
    const removed = await record?.delete();
    expect(removed).toBe(true);
    expect(await queue.size()).toBe(1);
  });

  it("reschedules scheduled entries and enqueues them", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const set = new ScheduledSet();
    const now = Date.now() / 1000;
    const payload = {
      class: "ScheduledJob",
      args: [],
      queue: "default",
      jid: "jid-3",
    };
    await set.schedule(now + 10, payload);

    const entries = await set.entries();
    expect(entries).toHaveLength(1);
    const entry = entries[0];
    await entry.reschedule(now + 20);
    const score = await redis.zScore("schedule", entry.value);
    expect(Number(score)).toBeCloseTo(now + 20, 2);

    await entry.addToQueue();
    expect(await redis.zCard("schedule")).toBe(0);
    const queued = await redis.lRange("queue:default", 0, -1);
    expect(queued).toHaveLength(1);
  });

  it("retries entries and decrements retry_count", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const set = new RetrySet();
    const payload = {
      class: "RetryJob",
      args: [],
      queue: "default",
      jid: "jid-4",
      retry_count: 1,
      error_class: "Error",
      error_message: "boom",
      failed_at: Date.now(),
    };
    await redis.zAdd("retry", [{ score: Date.now() / 1000, value: Sidekiq.dumpJson(payload) }]);

    const entry = (await set.entries())[0];
    await entry.retry();
    expect(await redis.zCard("retry")).toBe(0);
    const queued = await redis.lRange("queue:default", 0, -1);
    const queuedPayload = JSON.parse(queued[0]);
    expect(queuedPayload.retry_count).toBe(0);
  });

  it("kills all retry jobs into the dead set", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const set = new RetrySet();
    const payloads = [
      Sidekiq.dumpJson({ class: "RetryJob", args: [], queue: "default", jid: "jid-5" }),
      Sidekiq.dumpJson({ class: "RetryJob", args: [], queue: "default", jid: "jid-6" }),
    ];
    await redis.zAdd("retry", [
      { score: Date.now() / 1000, value: payloads[0] },
      { score: Date.now() / 1000 + 1, value: payloads[1] },
    ]);

    await set.killAll();
    expect(await redis.zCard("retry")).toBe(0);
    expect(await redis.zCard("dead")).toBe(2);
  });

  it("adds jobs to the dead set and finds them", async () => {
    const dead = new DeadSet();
    const payload = Sidekiq.dumpJson({
      class: "DeadJob",
      args: [],
      queue: "default",
      jid: "jid-7",
    });
    await dead.kill(payload);
    const entry = await dead.findJob("jid-7");
    expect(entry?.payload.jid).toBe("jid-7");
  });

  it("summarizes process totals", async () => {
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.sAdd("processes", ["p1", "p2"]);
    await redis.hSet("p1", {
      info: JSON.stringify({ concurrency: 2 }),
      busy: "1",
      beat: "1",
      quiet: "false",
      rtt_us: "0",
      rss: "100",
    });
    await redis.hSet("p2", {
      info: JSON.stringify({ concurrency: 3 }),
      busy: "0",
      beat: "1",
      quiet: "false",
      rtt_us: "0",
      rss: "200",
    });

    const processes = new ProcessSet();
    expect(await processes.size()).toBe(2);
    expect(await processes.totalConcurrency()).toBe(5);
    expect(await processes.totalRss()).toBe(300);
    const entry = await ProcessSet.get("p1");
    expect(entry?.info.concurrency).toBe(2);
  });
});
