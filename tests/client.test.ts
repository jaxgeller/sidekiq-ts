import { beforeAll, afterAll, beforeEach, describe, it, expect } from "vitest";
import { Sidekiq, Job, Client } from "../src/index.js";

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
    result.forEach((jid) => expect(jid).toHaveLength(24));

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const entries = await redis.lRange("queue:critical", 0, -1);
    expect(entries).toHaveLength(2);
  });
});
