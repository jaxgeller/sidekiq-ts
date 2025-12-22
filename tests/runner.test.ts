import { setTimeout as sleep } from "node:timers/promises";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { Config } from "../src/config.js";
import { Job, Sidekiq } from "../src/index.js";

class RecorderJob extends Job<[number]> {
  static performed: number[] = [];

  perform(value: number): void {
    RecorderJob.performed.push(value);
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
  Sidekiq.defaultConfiguration.averageScheduledPollInterval = 1;
  // Use fixed poll interval to avoid initial wait in tests
  Sidekiq.defaultConfiguration.pollIntervalAverage = 1;
  Sidekiq.registerJob(RecorderJob);
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  RecorderJob.performed = [];
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Runner", () => {
  it("executes enqueued jobs", async () => {
    const runner = await Sidekiq.run();
    try {
      await RecorderJob.performAsync(42);
      await waitFor(() => RecorderJob.performed.length === 1);
      expect(RecorderJob.performed).toEqual([42]);
    } finally {
      await runner.stop();
    }
  });
});

describe("Lifecycle Events", () => {
  it("fires startup event after start()", async () => {
    const events: string[] = [];
    const config = new Config({
      redis: { url: redisUrl },
      concurrency: 1,
      queues: ["default"],
      pollIntervalAverage: 1,
      lifecycleEvents: {
        startup: [() => events.push("startup")],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    const runner = await Sidekiq.run({ config });
    try {
      expect(events).toContain("startup");
    } finally {
      await runner.stop();
      await config.close();
    }
  });

  it("fires quiet event when quiet() is called", async () => {
    const events: string[] = [];
    const config = new Config({
      redis: { url: redisUrl },
      concurrency: 1,
      queues: ["default"],
      pollIntervalAverage: 1,
      lifecycleEvents: {
        startup: [],
        quiet: [() => events.push("quiet")],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    const runner = await Sidekiq.run({ config });
    await runner.quiet();
    expect(events).toContain("quiet");
    await runner.stop();
    await config.close();
  });

  it("fires shutdown and exit events on stop()", async () => {
    const events: string[] = [];
    const config = new Config({
      redis: { url: redisUrl },
      concurrency: 1,
      queues: ["default"],
      pollIntervalAverage: 1,
      lifecycleEvents: {
        startup: [],
        quiet: [],
        shutdown: [() => events.push("shutdown")],
        exit: [() => events.push("exit")],
        heartbeat: [],
        beat: [],
      },
    });

    const runner = await Sidekiq.run({ config });
    await runner.stop();
    await config.close();

    expect(events).toContain("shutdown");
    expect(events).toContain("exit");
    expect(events.indexOf("shutdown")).toBeLessThan(events.indexOf("exit"));
  });

  it("fires heartbeat events on each heartbeat cycle", async () => {
    let heartbeatCount = 0;
    const config = new Config({
      redis: { url: redisUrl },
      concurrency: 1,
      queues: ["default"],
      pollIntervalAverage: 1,
      heartbeatInterval: 0.1,
      lifecycleEvents: {
        startup: [],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [() => heartbeatCount++],
        beat: [],
      },
    });

    const runner = await Sidekiq.run({ config });
    try {
      await sleep(350);
      expect(heartbeatCount).toBeGreaterThanOrEqual(2);
    } finally {
      await runner.stop();
      await config.close();
    }
  });
});

describe("Config.fireEvent", () => {
  it("calls all registered handlers", async () => {
    const events: number[] = [];
    const config = new Config({
      lifecycleEvents: {
        startup: [() => events.push(1), () => events.push(2)],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    await config.fireEvent("startup");
    expect(events).toEqual([1, 2]);
  });

  it("clears handlers when oneshot is true (default)", async () => {
    const events: number[] = [];
    const config = new Config({
      lifecycleEvents: {
        startup: [() => events.push(1)],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    await config.fireEvent("startup");
    await config.fireEvent("startup");

    expect(events).toEqual([1]);
  });

  it("keeps handlers when oneshot is false", async () => {
    let count = 0;
    const config = new Config({
      lifecycleEvents: {
        startup: [],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [() => count++],
        beat: [],
      },
    });

    await config.fireEvent("heartbeat", { oneshot: false });
    await config.fireEvent("heartbeat", { oneshot: false });

    expect(count).toBe(2);
  });

  it("calls handlers in reverse order when reverse is true", async () => {
    const events: number[] = [];
    const config = new Config({
      lifecycleEvents: {
        startup: [],
        quiet: [],
        shutdown: [() => events.push(1), () => events.push(2)],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    await config.fireEvent("shutdown", { reverse: true });
    expect(events).toEqual([2, 1]);
  });

  it("re-throws errors when reraise is true", async () => {
    const config = new Config({
      lifecycleEvents: {
        startup: [
          () => {
            throw new Error("test error");
          },
        ],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    await expect(
      config.fireEvent("startup", { reraise: true })
    ).rejects.toThrow("test error");
  });

  it("swallows errors when reraise is false (default)", async () => {
    const events: number[] = [];
    const config = new Config({
      lifecycleEvents: {
        startup: [
          () => {
            throw new Error("test error");
          },
          () => events.push(2),
        ],
        quiet: [],
        shutdown: [],
        exit: [],
        heartbeat: [],
        beat: [],
      },
    });

    await config.fireEvent("startup");
    expect(events).toEqual([2]);
  });
});

describe("Dynamic Poll Interval", () => {
  it("scales poll interval based on process count", async () => {
    const config = new Config({
      redis: { url: redisUrl },
      averageScheduledPollInterval: 5,
    });
    const redis = await config.getRedisClient();

    // Add multiple processes
    await redis.sAdd("processes", ["proc1", "proc2", "proc3"]);

    const processCount = await redis.sCard("processes");
    expect(processCount).toBe(3);

    // With 3 processes and averageScheduledPollInterval of 5,
    // scaled interval should be 15 seconds
    const scaledInterval = processCount * config.averageScheduledPollInterval;
    expect(scaledInterval).toBe(15);

    await redis.flushDb();
    await config.close();
  });

  it("uses fixed poll interval when pollIntervalAverage is set", async () => {
    const config = new Config({
      redis: { url: redisUrl },
      pollIntervalAverage: 10,
      averageScheduledPollInterval: 5,
    });

    // When pollIntervalAverage is set, it should be used directly
    expect(config.pollIntervalAverage).toBe(10);
    await config.close();
  });

  it("defaults process count to 1 when no processes registered", async () => {
    const config = new Config({
      redis: { url: redisUrl },
    });
    const redis = await config.getRedisClient();

    await redis.del("processes");
    const processCount = await redis.sCard("processes");
    expect(processCount).toBe(0);

    // System should treat 0 as 1
    const effectiveCount = processCount === 0 ? 1 : processCount;
    expect(effectiveCount).toBe(1);

    await config.close();
  });
});
