import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { DeadSet } from "../src/api.js";
import { Job } from "../src/job.js";
import { Sidekiq } from "../src/sidekiq.js";
import type { JobPayload } from "../src/types.js";

const redisUrl = "redis://localhost:6379/0";

const waitFor = async (
  condition: () => Promise<boolean> | boolean,
  timeoutMs = 2000
) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) {
      return;
    }
    await sleep(20);
  }
  throw new Error("Timed out waiting for condition");
};

// Test job that always succeeds
class SuccessJob extends Job<[number]> {
  static sidekiqOptions = { retry: 1 };
  static exhaustedCalled = false;
  static exhaustedJob: JobPayload | undefined;
  static exhaustedError: Error | undefined;

  static {
    this.retriesExhausted((job, error) => {
      SuccessJob.exhaustedCalled = true;
      SuccessJob.exhaustedJob = job;
      SuccessJob.exhaustedError = error;
    });
  }

  static reset() {
    this.exhaustedCalled = false;
    this.exhaustedJob = undefined;
    this.exhaustedError = undefined;
  }

  perform(_value: number): void {
    // success
  }
}

// Test job that always fails
class FailJob extends Job<[number]> {
  static sidekiqOptions = { retry: 1 };
  static exhaustedCalled = false;
  static exhaustedJob: JobPayload | undefined;
  static exhaustedError: Error | undefined;

  static {
    this.retriesExhausted((job, error) => {
      FailJob.exhaustedCalled = true;
      FailJob.exhaustedJob = job;
      FailJob.exhaustedError = error;
    });
  }

  static reset() {
    this.exhaustedCalled = false;
    this.exhaustedJob = undefined;
    this.exhaustedError = undefined;
  }

  perform(_value: number): never {
    throw new Error("kerblammo!");
  }
}

// Job that fails immediately (retry: 0)
class ImmediateDeadJob extends Job<[string]> {
  static sidekiqOptions = { retry: 0 };
  static exhaustedCalled = false;

  static {
    this.retriesExhausted(() => {
      ImmediateDeadJob.exhaustedCalled = true;
    });
  }

  static reset() {
    this.exhaustedCalled = false;
  }

  perform(_value: string): never {
    throw new Error("immediate death");
  }
}

// Job that returns :discard from exhausted handler
class DiscardJob extends Job<[number]> {
  static sidekiqOptions = { retry: 0 };
  static exhaustedCalled = false;

  static {
    this.retriesExhausted(() => {
      DiscardJob.exhaustedCalled = true;
      return "discard";
    });
  }

  static reset() {
    this.exhaustedCalled = false;
  }

  perform(_value: number): never {
    throw new Error("discarded!");
  }
}

// Job without exhausted handler
class PlainFailJob extends Job<[number]> {
  static sidekiqOptions = { retry: 0 };

  perform(_value: number): never {
    throw new Error("plain fail");
  }
}

// Job with dead: false option
class NoDeadJob extends Job<[number]> {
  static sidekiqOptions = { retry: 0, dead: false };

  perform(_value: number): never {
    throw new Error("no dead");
  }
}

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
  Sidekiq.defaultConfiguration.concurrency = 1;
  Sidekiq.defaultConfiguration.queues = ["default"];
  Sidekiq.defaultConfiguration.averageScheduledPollInterval = 1;
  Sidekiq.registerJob(SuccessJob);
  Sidekiq.registerJob(FailJob);
  Sidekiq.registerJob(ImmediateDeadJob);
  Sidekiq.registerJob(DiscardJob);
  Sidekiq.registerJob(PlainFailJob);
  Sidekiq.registerJob(NoDeadJob);
  FailJob.retryIn(() => 60);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  SuccessJob.reset();
  FailJob.reset();
  ImmediateDeadJob.reset();
  DiscardJob.reset();
  Sidekiq.defaultConfiguration.deathHandlers.length = 0;
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("sidekiq_retries_exhausted", () => {
  it("does not run exhausted block when job successful on first run", async () => {
    const runner = await Sidekiq.run();
    try {
      await SuccessJob.performAsync(1);
      await waitFor(async () => {
        const redis = await Sidekiq.defaultConfiguration.getRedisClient();
        const queueLen = await redis.lLen("queue:default");
        return queueLen === 0;
      });
      await sleep(100);
      expect(SuccessJob.exhaustedCalled).toBe(false);
    } finally {
      await runner.stop();
    }
  });

  it("does not run exhausted block when retries not exhausted yet", async () => {
    const runner = await Sidekiq.run();
    try {
      await FailJob.performAsync(1);

      // Wait for job to be moved to retry set
      const redis = await Sidekiq.defaultConfiguration.getRedisClient();
      await waitFor(async () => {
        const retryCount = Number(await redis.sendCommand(["ZCARD", "retry"]));
        return retryCount >= 1;
      });

      expect(FailJob.exhaustedCalled).toBe(false);
    } finally {
      await runner.stop();
    }
  });

  it("runs exhausted block when retries exhausted", async () => {
    const runner = await Sidekiq.run();
    try {
      await ImmediateDeadJob.performAsync("x");

      await waitFor(() => ImmediateDeadJob.exhaustedCalled);

      expect(ImmediateDeadJob.exhaustedCalled).toBe(true);
    } finally {
      await runner.stop();
    }
  });

  it("passes job and exception to retries exhausted block", async () => {
    const runner = await Sidekiq.run();
    try {
      await ImmediateDeadJob.performAsync("test-arg");

      // Use a local handler to capture the data
      let capturedJob: JobPayload | undefined;
      let capturedError: Error | undefined;

      // Reset and set a new handler
      class CaptureJob extends Job<[string]> {
        static sidekiqOptions = { retry: 0 };
        perform(_: string): never {
          throw new Error("captured!");
        }
      }
      CaptureJob.retriesExhausted((job, error) => {
        capturedJob = job;
        capturedError = error;
      });
      Sidekiq.registerJob(CaptureJob);
      await CaptureJob.performAsync("capture-arg");

      await waitFor(() => capturedJob !== undefined);

      expect(capturedJob).toBeDefined();
      expect(capturedJob?.class).toBe("CaptureJob");
      expect(capturedJob?.error_message).toBe("captured!");
      expect(capturedError).toBeDefined();
      expect(capturedError?.message).toBe("captured!");
    } finally {
      await runner.stop();
    }
  });

  it("allows global death handlers", async () => {
    let deathJob: JobPayload | undefined;
    let deathError: Error | undefined;

    Sidekiq.defaultConfiguration.deathHandlers.push((job, error) => {
      deathJob = job;
      deathError = error;
    });

    const runner = await Sidekiq.run();
    try {
      await PlainFailJob.performAsync(42);

      await waitFor(() => deathJob !== undefined);

      expect(deathJob).toBeDefined();
      expect(deathJob?.class).toBe("PlainFailJob");
      expect(deathError).toBeDefined();
      expect(deathError?.message).toBe("plain fail");
    } finally {
      await runner.stop();
    }
  });

  it("adds jobs to the dead set", async () => {
    const runner = await Sidekiq.run();
    try {
      await ImmediateDeadJob.performAsync("dead");

      const redis = await Sidekiq.defaultConfiguration.getRedisClient();
      await waitFor(async () => {
        const deadCount = Number(await redis.sendCommand(["ZCARD", "dead"]));
        return deadCount >= 1;
      });

      const deadSet = new DeadSet();
      expect(await deadSet.size()).toBe(1);
    } finally {
      await runner.stop();
    }
  });

  it("allows disabling dead set with dead: false", async () => {
    const runner = await Sidekiq.run();
    try {
      await NoDeadJob.performAsync(1);

      // Wait for job processing
      await waitFor(async () => {
        const redis = await Sidekiq.defaultConfiguration.getRedisClient();
        const queueLen = await redis.lLen("queue:default");
        return queueLen === 0;
      });
      await sleep(100);

      const deadSet = new DeadSet();
      expect(await deadSet.size()).toBe(0);
    } finally {
      await runner.stop();
    }
  });

  it("still calls death handlers when dead set disabled", async () => {
    let deathHandlerCalled = false;
    let deathPayload: JobPayload | undefined;

    Sidekiq.defaultConfiguration.deathHandlers.push((job) => {
      deathHandlerCalled = true;
      deathPayload = job;
    });

    const runner = await Sidekiq.run();
    try {
      await NoDeadJob.performAsync(1);

      await waitFor(() => deathHandlerCalled);

      expect(deathHandlerCalled).toBe(true);
      expect(deathPayload).toBeDefined();
      expect(deathPayload?.discarded_at).toBeDefined();
    } finally {
      await runner.stop();
    }
  });

  it(":discard return still calls death handlers", async () => {
    let deathHandlerCalled = false;
    let deathPayload: JobPayload | undefined;

    Sidekiq.defaultConfiguration.deathHandlers.push((job) => {
      deathHandlerCalled = true;
      deathPayload = job;
    });

    const runner = await Sidekiq.run();
    try {
      await DiscardJob.performAsync(1);

      await waitFor(() => deathHandlerCalled);

      expect(DiscardJob.exhaustedCalled).toBe(true);
      expect(deathHandlerCalled).toBe(true);
      expect(deathPayload?.discarded_at).toBeDefined();

      // Should not be in dead set
      const deadSet = new DeadSet();
      expect(await deadSet.size()).toBe(0);
    } finally {
      await runner.stop();
    }
  });
});
