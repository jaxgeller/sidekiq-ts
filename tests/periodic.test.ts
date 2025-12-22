import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Config } from "../src/config.js";
import { Job } from "../src/job.js";
import { LeaderElector } from "../src/leader.js";
import { PeriodicScheduler } from "../src/periodic.js";

class TestJob extends Job<[string]> {
  perform(_message: string) {
    // noop
  }
}

const silentLogger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  level: "silent" as const,
  with: () => silentLogger,
};

describe("PeriodicScheduler", () => {
  let config: Config;
  let redis: Awaited<ReturnType<Config["getRedisClient"]>>;
  let leaderElector: LeaderElector;

  beforeEach(async () => {
    config = new Config({
      logger: silentLogger,
    });
    redis = await config.getRedisClient();
    await redis.del("leader");
    await redis.del("cron");

    leaderElector = new LeaderElector(config, {
      identity: "test-process",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });
    await leaderElector.start();
  });

  afterEach(async () => {
    await leaderElector.stop();
    await redis.del("leader");
    await redis.del("cron");
    await redis.quit();
  });

  it("registers a periodic job", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    const lid = scheduler.register("*/5 * * * *", TestJob);

    expect(lid).toContain("testjob");
    expect(scheduler.list()).toHaveLength(1);
    expect(scheduler.list()[0].cron).toBe("*/5 * * * *");
    expect(scheduler.list()[0].class).toBe("TestJob");
  });

  it("registers multiple periodic jobs", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    scheduler.register("*/5 * * * *", TestJob);
    scheduler.register("0 * * * *", TestJob, { queue: "hourly" });

    expect(scheduler.list()).toHaveLength(2);
  });

  it("unregisters a periodic job", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    const lid = scheduler.register("*/5 * * * *", TestJob);
    expect(scheduler.list()).toHaveLength(1);

    const removed = scheduler.unregister(lid);
    expect(removed).toBe(true);
    expect(scheduler.list()).toHaveLength(0);
  });

  it("syncs jobs to Redis on start", async () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    scheduler.register("*/5 * * * *", TestJob);
    await scheduler.start();

    const stored = await redis.hGetAll("cron");
    expect(Object.keys(stored)).toHaveLength(1);

    const jobConfig = JSON.parse(Object.values(stored)[0]);
    expect(jobConfig.cron).toBe("*/5 * * * *");
    expect(jobConfig.class).toBe("TestJob");

    await scheduler.stop();
  });

  it("removes jobs from Redis on stop", async () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    scheduler.register("*/5 * * * *", TestJob);
    await scheduler.start();

    let stored = await redis.hGetAll("cron");
    expect(Object.keys(stored)).toHaveLength(1);

    await scheduler.stop();

    stored = await redis.hGetAll("cron");
    expect(Object.keys(stored)).toHaveLength(0);
  });

  it("uses custom queue when specified", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    scheduler.register("0 * * * *", TestJob, { queue: "reports" });

    const jobs = scheduler.list();
    expect(jobs[0].queue).toBe("reports");
  });

  it("uses job args when specified", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    scheduler.register("0 * * * *", TestJob, { args: ["hello"] });

    const jobs = scheduler.list();
    expect(jobs[0].args).toEqual(["hello"]);
  });

  it("generates deterministic lid for same configuration", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    const lid1 = scheduler.register("*/5 * * * *", TestJob);
    scheduler.unregister(lid1);
    const lid2 = scheduler.register("*/5 * * * *", TestJob);

    expect(lid1).toBe(lid2);
  });

  it("generates different lid for different cron", () => {
    const scheduler = new PeriodicScheduler(config, leaderElector);

    const lid1 = scheduler.register("*/5 * * * *", TestJob);
    const lid2 = scheduler.register("*/10 * * * *", TestJob);

    expect(lid1).not.toBe(lid2);
  });
});
