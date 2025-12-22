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

  it("follower becomes leader when leader stops", async () => {
    const elector1 = new LeaderElector(config, {
      identity: "leader-process",
      refreshInterval: 100,
      checkInterval: 200,
      ttl: 2,
    });

    const elector2 = new LeaderElector(config, {
      identity: "follower-process",
      refreshInterval: 100,
      checkInterval: 200,
      ttl: 2,
    });

    await elector1.start();
    await elector2.start();

    expect(elector1.leader()).toBe(true);
    expect(elector2.leader()).toBe(false);

    // Stop the leader
    await elector1.stop();

    // Wait for follower to acquire leadership
    await new Promise((r) => setTimeout(r, 500));

    expect(elector2.leader()).toBe(true);

    await elector2.stop();
  });

  it("follower becomes leader when leader key is deleted", async () => {
    const elector1 = new LeaderElector(config, {
      identity: "dying-leader",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    const elector2 = new LeaderElector(config, {
      identity: "waiting-follower",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector1.start();
    expect(elector1.leader()).toBe(true);

    await elector2.start();
    expect(elector2.leader()).toBe(false);

    // Stop elector1 (which releases the key)
    await elector1.stop();

    // Wait for follower to check and acquire
    await new Promise((r) => setTimeout(r, 300));

    expect(elector2.leader()).toBe(true);

    await elector2.stop();
  });

  it("fires leader lifecycle event when becoming leader", async () => {
    let leaderEventFired = false;
    const testConfig = new Config({
      logger: silentLogger,
      lifecycleEvents: {
        leader: [
          () => {
            leaderEventFired = true;
          },
        ],
      },
    });

    const elector = new LeaderElector(testConfig, {
      identity: "event-test",
      refreshInterval: 100,
      checkInterval: 100,
      ttl: 5,
    });

    await elector.start();
    expect(leaderEventFired).toBe(true);

    await elector.stop();
    await testConfig.close();
  });

  it("fires follower lifecycle event when losing leadership", async () => {
    let followerEventFired = false;
    const testConfig = new Config({
      logger: silentLogger,
      lifecycleEvents: {
        follower: [
          () => {
            followerEventFired = true;
          },
        ],
      },
    });

    const elector = new LeaderElector(testConfig, {
      identity: "follower-event-test",
      refreshInterval: 50, // Fast refresh
      checkInterval: 50,
      ttl: 1,
    });

    await elector.start();
    expect(elector.leader()).toBe(true);

    // Delete the key and set a different leader to simulate another process taking over
    await redis.del("leader");
    await redis.set("leader", "other-leader", { EX: 10 });

    // Wait for elector to try to refresh and notice it lost leadership
    await new Promise((r) => setTimeout(r, 200));

    expect(followerEventFired).toBe(true);
    expect(elector.leader()).toBe(false);

    await elector.stop();
    await testConfig.close();
  });

  it("uses custom config options from Config.leaderElection", async () => {
    const customConfig = new Config({
      logger: silentLogger,
      leaderElection: {
        refreshInterval: 500,
        checkInterval: 1000,
        ttl: 10,
      },
    });

    // Verify the options are passed through
    expect(customConfig.leaderElection.refreshInterval).toBe(500);
    expect(customConfig.leaderElection.checkInterval).toBe(1000);
    expect(customConfig.leaderElection.ttl).toBe(10);

    await customConfig.close();
  });
});
