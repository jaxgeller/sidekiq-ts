import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Job } from "../src/job.js";
import type { RedisClient } from "../src/redis.js";
import { Sidekiq } from "../src/sidekiq.js";

class MiddlewareJob extends Job<[number]> {
  async perform(_value: number) {
    // no-op
  }
}

class QueueSwitchMiddleware {
  call(
    _jobClass: unknown,
    payload: { args: number[]; queue: string },
    _queue: string,
    _redis: RedisClient,
    next: () => Promise<unknown>
  ) {
    payload.queue = payload.args[0] % 2 === 0 ? "even" : "odd";
    return next();
  }
}

class StopMiddleware {
  call(
    _jobClass: unknown,
    _payload: unknown,
    _queue: string,
    _redis: RedisClient,
    _next: () => Promise<unknown>
  ) {
    return null;
  }
}

class ServerMiddleware {
  static events: string[] = [];

  async call(
    _jobInstance: unknown,
    _payload: unknown,
    _queue: string,
    next: () => Promise<unknown>
  ) {
    ServerMiddleware.events.push("before");
    await next();
    ServerMiddleware.events.push("after");
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
  Sidekiq.defaultConfiguration.queues = ["default", "odd", "even"];
  Sidekiq.registerJob(MiddlewareJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  Sidekiq.defaultConfiguration.clientMiddleware.clear();
  Sidekiq.defaultConfiguration.serverMiddleware.clear();
  ServerMiddleware.events = [];
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Middleware", () => {
  it("allows client middleware to modify payloads", async () => {
    Sidekiq.defaultConfiguration.clientMiddleware.add(QueueSwitchMiddleware);
    await MiddlewareJob.performAsync(3);

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const entries = await redis.lRange("queue:odd", 0, -1);
    expect(entries).toHaveLength(1);
    const payload = JSON.parse(entries[0]);
    expect(payload.queue).toBe("odd");
  });

  it("allows client middleware to stop enqueueing", async () => {
    Sidekiq.defaultConfiguration.clientMiddleware.add(StopMiddleware);
    const jid = await MiddlewareJob.performAsync(4);
    expect(jid).toBeNull();

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const entries = await redis.lRange("queue:default", 0, -1);
    expect(entries).toHaveLength(0);
  });

  it("wraps server execution with middleware", async () => {
    Sidekiq.defaultConfiguration.serverMiddleware.add(ServerMiddleware);
    const runner = await Sidekiq.run();
    try {
      await MiddlewareJob.performAsync(1);
      await waitFor(() => ServerMiddleware.events.length === 2);
      expect(ServerMiddleware.events).toEqual(["before", "after"]);
    } finally {
      await runner.stop();
    }
  });
});
