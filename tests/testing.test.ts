import { afterAll, afterEach, beforeAll, beforeEach, describe, it, expect } from "vitest";
import { Sidekiq, Job, Testing, Queues } from "../src/index.js";

class TestJob extends Job<[number]> {
  static performed: number[] = [];

  async perform(value: number) {
    TestJob.performed.push(value);
  }
}

const redisUrl = "redis://localhost:6379/0";

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
  Sidekiq.registerJob(TestJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  TestJob.performed = [];
  Queues.clearAll();
  Testing.disable();
});

afterEach(() => {
  Testing.disable();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Testing mode", () => {
  it("stores jobs in fake mode", async () => {
    Testing.fake();
    await TestJob.performAsync(10);

    expect(TestJob.jobs().length).toBe(1);
    expect(Queues.get("default").length).toBe(1);
    expect(TestJob.performed).toEqual([]);

    await TestJob.drain();
    expect(TestJob.performed).toEqual([10]);
    expect(TestJob.jobs().length).toBe(0);
  });

  it("executes jobs immediately in inline mode", async () => {
    Testing.inline();
    await TestJob.performAsync(5);

    expect(TestJob.performed).toEqual([5]);
    expect(TestJob.jobs().length).toBe(0);
  });
});
