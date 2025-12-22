import { setTimeout as sleep } from "node:timers/promises";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import type { Config } from "../src/config.js";
import { Job } from "../src/job.js";
import { PlainFormatter, SidekiqLogger } from "../src/logger.js";
import { Sidekiq } from "../src/sidekiq.js";

class ErrorJob extends Job<[]> {
  static sidekiqOptions = { retry: false };

  perform(): void {
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
  let originalLogger: typeof Sidekiq.defaultConfiguration.logger;

  beforeEach(() => {
    originalHandlers = Sidekiq.defaultConfiguration.errorHandlers;
    originalLogger = Sidekiq.defaultConfiguration.logger;
  });

  afterEach(() => {
    Sidekiq.defaultConfiguration.errorHandlers = originalHandlers;
    Sidekiq.defaultConfiguration.logger = originalLogger;
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

  it("logs errors with the default handler", async () => {
    const messages: string[] = [];
    const base = {
      debug: (message: string) => messages.push(message),
      info: (message: string) => messages.push(message),
      warn: (message: string) => messages.push(message),
      error: (message: string) => messages.push(message),
    } as unknown as Console;

    Sidekiq.defaultConfiguration.logger = new SidekiqLogger(
      base,
      new PlainFormatter()
    );
    const runner = await Sidekiq.run();
    try {
      await ErrorJob.performAsync();
      await waitFor(() =>
        messages.some(
          (message) =>
            message.includes("Error: boom") &&
            message.includes("context=Job raised exception")
        )
      );
    } finally {
      await runner.stop();
    }
  });
});
