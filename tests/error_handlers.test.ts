import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Sidekiq, Job } from "../src/index.js";
import { setTimeout as sleep } from "node:timers/promises";
import type { Config } from "../src/config.js";

class ErrorJob extends Job<[]> {
  static sidekiqOptions = { retry: false };

  async perform() {
    throw new Error("boom");
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
  Sidekiq.registerJob(ErrorJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Error handlers", () => {
  let originalHandlers: typeof Sidekiq.defaultConfiguration.errorHandlers;

  beforeEach(() => {
    originalHandlers = Sidekiq.defaultConfiguration.errorHandlers;
  });

  afterEach(() => {
    Sidekiq.defaultConfiguration.errorHandlers = originalHandlers;
  });

  it("passes job context to error handlers", async () => {
    const calls: Array<{
      error: Error;
      context: Record<string, unknown>;
      config?: Config;
    }> = [];
    Sidekiq.defaultConfiguration.errorHandlers = [
      (error, context, config) => {
        calls.push({ error, context, config });
      },
    ];

    const runner = await Sidekiq.run();
    try {
      await ErrorJob.performAsync();
      await waitFor(() => calls.length > 0);
      const { error, context, config } = calls[0];
      expect(error.message).toBe("boom");
      expect(context.context).toBe("Job raised exception");
      expect(context.queue).toBe("default");
      expect(context.job).toMatchObject({ class: "ErrorJob", args: [] });
      expect(config).toBe(Sidekiq.defaultConfiguration);
    } finally {
      await runner.stop();
    }
  });

  it("passes invalid json context to error handlers", async () => {
    const calls: Array<{
      error: Error;
      context: Record<string, unknown>;
      config?: Config;
    }> = [];
    Sidekiq.defaultConfiguration.errorHandlers = [
      (error, context, config) => {
        calls.push({ error, context, config });
      },
    ];

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.lPush("queue:default", "{invalid");

    const runner = await Sidekiq.run();
    try {
      await waitFor(() => calls.length > 0);
      const { error, context } = calls[0];
      expect(error).toBeInstanceOf(Error);
      expect(context.context).toBe("Invalid JSON for job");
      expect(context.queue).toBe("default");
      expect(context.jobstr).toBe("{invalid");
    } finally {
      await runner.stop();
    }
  });
});
