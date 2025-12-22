import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Job } from "../src/job.js";
import { DefaultJobLogger } from "../src/job-logger.js";
import { Sidekiq } from "../src/sidekiq.js";
import type { JobLogger, JobPayload } from "../src/types.js";

class LogJob extends Job<[number]> {
  async perform(_value: number) {
    await sleep(10);
  }
}

class RecorderLogger implements JobLogger {
  prepareCalls = 0;
  callCalls = 0;

  async prepare<T>(_payload: JobPayload, fn: () => Promise<T> | T): Promise<T> {
    this.prepareCalls += 1;
    return await fn();
  }

  async call<T>(
    _payload: JobPayload,
    _queue: string,
    fn: () => Promise<T> | T
  ): Promise<T> {
    this.callCalls += 1;
    return await fn();
  }
}

const redisUrl = "redis://localhost:6379/0";

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
  Sidekiq.defaultConfiguration.concurrency = 1;
  Sidekiq.defaultConfiguration.queues = ["default"];
  Sidekiq.registerJob(LogJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Job logging and profiling", () => {
  it("uses custom job logger and profiler hook", async () => {
    const logger = new RecorderLogger();
    Sidekiq.defaultConfiguration.jobLogger = logger;
    let profilerCalls = 0;
    Sidekiq.defaultConfiguration.profiler = async (_payload, fn) => {
      profilerCalls += 1;
      await fn();
    };

    const runner = await Sidekiq.run();
    try {
      await LogJob.set({ profile: true }).performAsync(1);
      await sleep(50);
      expect(logger.prepareCalls).toBeGreaterThan(0);
      expect(logger.callCalls).toBeGreaterThan(0);
      expect(profilerCalls).toBeGreaterThan(0);
    } finally {
      await runner.stop();
      Sidekiq.defaultConfiguration.jobLogger = new DefaultJobLogger(
        Sidekiq.defaultConfiguration
      );
      Sidekiq.defaultConfiguration.profiler = undefined;
    }
  });
});
