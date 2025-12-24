import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { DeadSet } from "../src/api.js";
import { dumpJson } from "../src/json.js";
import { Sidekiq } from "../src/sidekiq.js";
import type { JobPayload } from "../src/types.js";

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

describe("DeadSet", () => {
  it("puts passed job to the dead sorted set", async () => {
    const payload: JobPayload = {
      jid: "123123",
      class: "SomeJob",
      args: [],
      queue: "default",
    };

    const deadSet = new DeadSet();
    await deadSet.kill(payload);

    const found = await deadSet.findJob("123123");
    expect(found).not.toBeNull();
    expect(found?.payload.jid).toBe("123123");
    expect(found?.payload.class).toBe("SomeJob");
  });

  it("puts passed serialized job string to the dead sorted set", async () => {
    const payload: JobPayload = {
      jid: "456456",
      class: "StringJob",
      args: [1, 2, 3],
      queue: "default",
    };
    const serialized = dumpJson(payload);

    const deadSet = new DeadSet();
    await deadSet.kill(serialized);

    const found = await deadSet.findJob("456456");
    expect(found).not.toBeNull();
    expect(found?.payload.class).toBe("StringJob");
  });

  it("removes dead jobs older than timeout", async () => {
    const originalTimeout = Sidekiq.defaultConfiguration.deadTimeoutInSeconds;
    Sidekiq.defaultConfiguration.deadTimeoutInSeconds = 10;

    try {
      const redis = await Sidekiq.defaultConfiguration.getRedisClient();
      const nowSeconds = Date.now() / 1000;

      // Add old job (older than timeout)
      const oldPayload: JobPayload = {
        jid: "000103",
        class: "MyJob3",
        args: [],
        queue: "default",
      };
      await redis.zAdd("dead", [
        { score: nowSeconds - 11, value: dumpJson(oldPayload) },
      ]);

      // Add recent job (within timeout)
      const recentPayload: JobPayload = {
        jid: "000102",
        class: "MyJob2",
        args: [],
        queue: "default",
      };
      await redis.zAdd("dead", [
        { score: nowSeconds - 9, value: dumpJson(recentPayload) },
      ]);

      // Add current job
      const currentPayload: JobPayload = {
        jid: "000101",
        class: "MyJob1",
        args: [],
        queue: "default",
      };
      await redis.zAdd("dead", [
        { score: nowSeconds, value: dumpJson(currentPayload) },
      ]);

      // Trigger trim
      const deadSet = new DeadSet();
      await deadSet.trim();

      // Old job should be removed
      expect(await deadSet.findJob("000103")).toBeNull();
      // Recent jobs should still exist
      expect(await deadSet.findJob("000102")).not.toBeNull();
      expect(await deadSet.findJob("000101")).not.toBeNull();
    } finally {
      Sidekiq.defaultConfiguration.deadTimeoutInSeconds = originalTimeout;
    }
  });

  it("removes all but last max_jobs-1 jobs", async () => {
    const originalMaxJobs = Sidekiq.defaultConfiguration.deadMaxJobs;
    // With max_jobs=3, zRemRangeByRank(0, -3) keeps 2 entries (max_jobs-1)
    // This matches Ruby Sidekiq behavior
    Sidekiq.defaultConfiguration.deadMaxJobs = 3;

    try {
      const deadSet = new DeadSet();

      // Add 3 jobs
      await deadSet.kill({
        jid: "000101",
        class: "MyJob1",
        args: [],
        queue: "default",
      });
      await sleep(10);
      await deadSet.kill({
        jid: "000102",
        class: "MyJob2",
        args: [],
        queue: "default",
      });
      await sleep(10);
      await deadSet.kill({
        jid: "000103",
        class: "MyJob3",
        args: [],
        queue: "default",
      });

      // With max_jobs=3, keeps max_jobs-1=2 entries, oldest is removed
      expect(await deadSet.findJob("000101")).toBeNull();
      expect(await deadSet.findJob("000102")).not.toBeNull();
      expect(await deadSet.findJob("000103")).not.toBeNull();
    } finally {
      Sidekiq.defaultConfiguration.deadMaxJobs = originalMaxJobs;
    }
  });

  it("iterates through dead set entries", async () => {
    const deadSet = new DeadSet();

    await deadSet.kill({
      jid: "iter1",
      class: "Job1",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "iter2",
      class: "Job2",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "iter3",
      class: "Job3",
      args: [],
      queue: "default",
    });

    const entries = await deadSet.entries();
    expect(entries.length).toBe(3);

    const jids = entries.map((e) => e.jid);
    expect(jids).toContain("iter1");
    expect(jids).toContain("iter2");
    expect(jids).toContain("iter3");
  });

  it("clears all dead jobs", async () => {
    const deadSet = new DeadSet();

    await deadSet.kill({
      jid: "clear1",
      class: "Job1",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "clear2",
      class: "Job2",
      args: [],
      queue: "default",
    });

    expect(await deadSet.size()).toBe(2);

    await deadSet.clear();

    expect(await deadSet.size()).toBe(0);
  });

  it("allows deleting individual entries", async () => {
    const deadSet = new DeadSet();

    await deadSet.kill({
      jid: "del1",
      class: "Job1",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "del2",
      class: "Job2",
      args: [],
      queue: "default",
    });

    const entry = await deadSet.findJob("del1");
    expect(entry).not.toBeNull();

    await entry?.delete();

    expect(await deadSet.findJob("del1")).toBeNull();
    expect(await deadSet.findJob("del2")).not.toBeNull();
    expect(await deadSet.size()).toBe(1);
  });

  it("allows retrying dead jobs", async () => {
    const deadSet = new DeadSet();
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();

    await deadSet.kill({
      jid: "retry1",
      class: "RetryJob",
      args: [1, 2],
      queue: "default",
      retry_count: 5,
    });

    const entry = await deadSet.findJob("retry1");
    expect(entry).not.toBeNull();

    await entry?.retry();

    // Job should be removed from dead set
    expect(await deadSet.findJob("retry1")).toBeNull();

    // Job should be in the queue with decremented retry_count
    const queueLen = await redis.lLen("queue:default");
    expect(queueLen).toBe(1);

    const queueEntry = await redis.lRange("queue:default", 0, 0);
    const payload = JSON.parse(queueEntry[0]);
    expect(payload.jid).toBe("retry1");
    expect(payload.retry_count).toBe(4);
  });

  it("allows adding job back to queue", async () => {
    const deadSet = new DeadSet();
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();

    await deadSet.kill({
      jid: "queue1",
      class: "QueueJob",
      args: ["test"],
      queue: "critical",
    });

    const entry = await deadSet.findJob("queue1");
    await entry?.addToQueue();

    // Job should be removed from dead set
    expect(await deadSet.findJob("queue1")).toBeNull();

    // Job should be in its original queue
    const queueLen = await redis.lLen("queue:critical");
    expect(queueLen).toBe(1);
  });

  it("respects trim: false option to skip trimming", async () => {
    const originalMaxJobs = Sidekiq.defaultConfiguration.deadMaxJobs;
    // Note: zRemRangeByRank with -maxJobs keeps maxJobs-1 entries
    Sidekiq.defaultConfiguration.deadMaxJobs = 3;

    try {
      const deadSet = new DeadSet();

      // Add jobs without trimming
      await deadSet.kill(
        { jid: "notrim1", class: "Job1", args: [], queue: "default" },
        { trim: false }
      );
      await deadSet.kill(
        { jid: "notrim2", class: "Job2", args: [], queue: "default" },
        { trim: false }
      );
      await deadSet.kill(
        { jid: "notrim3", class: "Job3", args: [], queue: "default" },
        { trim: false }
      );

      // All 3 should exist since we didn't trim
      expect(await deadSet.size()).toBe(3);

      // Now trim - with maxJobs=3, keeps maxJobs-1=2 entries
      await deadSet.trim();

      // Should now be limited to maxJobs-1 (this matches Ruby Sidekiq behavior)
      expect(await deadSet.size()).toBe(2);
    } finally {
      Sidekiq.defaultConfiguration.deadMaxJobs = originalMaxJobs;
    }
  });

  it("scan finds jobs matching pattern", async () => {
    const deadSet = new DeadSet();

    await deadSet.kill({
      jid: "scan-abc-1",
      class: "ScanJob",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "scan-xyz-2",
      class: "ScanJob",
      args: [],
      queue: "default",
    });
    await deadSet.kill({
      jid: "other-3",
      class: "OtherJob",
      args: [],
      queue: "default",
    });

    const matches: string[] = [];
    for await (const entry of deadSet.scan("scan-")) {
      if (entry.jid) {
        matches.push(entry.jid);
      }
    }

    expect(matches).toContain("scan-abc-1");
    expect(matches).toContain("scan-xyz-2");
    expect(matches).not.toContain("other-3");
  });
});
