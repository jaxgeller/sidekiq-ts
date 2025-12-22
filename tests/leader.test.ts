import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Config } from "../src/config.js";
import { LeaderElector } from "../src/leader.js";

const silentLogger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  level: "silent" as const,
  with: () => silentLogger,
};

describe("LeaderElector", () => {
  let config: Config;
  let redis: Awaited<ReturnType<Config["getRedisClient"]>>;

  beforeEach(async () => {
    config = new Config({
      logger: silentLogger,
    });
    redis = await config.getRedisClient();
    await redis.del("leader");
  });

  afterEach(async () => {
    await redis.del("leader");
    await redis.quit();
  });

  it("acquires leadership when no leader exists", async () => {
    const elector = new LeaderElector(config, {
      identity: "test-process-1",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector.start();
    expect(elector.leader()).toBe(true);

    const currentLeader = await elector.currentLeader();
    expect(currentLeader).toBe("test-process-1");

    await elector.stop();
    expect(elector.leader()).toBe(false);
  });

  it("does not acquire when another process is leader", async () => {
    // First process becomes leader
    await redis.set("leader", "existing-leader", { EX: 60 });

    const elector = new LeaderElector(config, {
      identity: "test-process-2",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector.start();
    expect(elector.leader()).toBe(false);

    const currentLeader = await elector.currentLeader();
    expect(currentLeader).toBe("existing-leader");

    await elector.stop();
  });

  it("releases leadership on clean shutdown", async () => {
    const elector = new LeaderElector(config, {
      identity: "test-process-3",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector.start();
    expect(elector.leader()).toBe(true);

    await elector.stop();

    const currentLeader = await redis.get("leader");
    expect(currentLeader).toBeNull();
  });

  it("returns leader status via leader() method", async () => {
    const elector = new LeaderElector(config, {
      identity: "test-process-4",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    expect(elector.leader()).toBe(false);

    await elector.start();
    expect(elector.leader()).toBe(true);

    await elector.stop();
    expect(elector.leader()).toBe(false);
  });

  it("multiple electors - only one becomes leader", async () => {
    const elector1 = new LeaderElector(config, {
      identity: "process-1",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    const elector2 = new LeaderElector(config, {
      identity: "process-2",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector1.start();
    await elector2.start();

    // Only one should be leader
    const leaders = [elector1.leader(), elector2.leader()].filter(Boolean);
    expect(leaders).toHaveLength(1);

    // The first one should be the leader
    expect(elector1.leader()).toBe(true);
    expect(elector2.leader()).toBe(false);

    await elector1.stop();
    await elector2.stop();
  });
});
