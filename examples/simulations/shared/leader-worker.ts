#!/usr/bin/env npx tsx
/**
 * Leader election worker process for stress testing.
 * Reports leader status periodically via stdout JSON messages.
 */

import { Config } from "../../../src/config.js";
import type { RedisClient } from "../../../src/redis.js";
import type { Runner } from "../../../src/runner.js";
import { Sidekiq } from "../../../src/sidekiq.js";

import { registerAllJobs } from "./jobs.js";

interface WorkerMessage {
  type: "lifecycle" | "status" | "error";
  event?: string;
  isLeader?: boolean;
  pid?: number;
  data?: Record<string, unknown>;
}

function sendMessage(msg: WorkerMessage) {
  console.log(JSON.stringify(msg));
}

async function main() {
  const concurrency = Number.parseInt(process.env.CONCURRENCY ?? "1", 10);
  const timeout = Number.parseInt(process.env.TIMEOUT ?? "5", 10);

  const statusInterval = Number.parseInt(
    process.env.STATUS_INTERVAL ?? "500",
    10
  );

  // Leader election config for faster testing
  const leaderRefreshInterval = Number.parseInt(
    process.env.LEADER_REFRESH_INTERVAL ?? "2000",
    10
  );
  const leaderCheckInterval = Number.parseInt(
    process.env.LEADER_CHECK_INTERVAL ?? "3000",
    10
  );
  const leaderTtl = Number.parseInt(process.env.LEADER_TTL ?? "5", 10);

  registerAllJobs();

  const config = new Config({
    concurrency,
    queues: ["simulation"],
    timeout,
    pollIntervalAverage: 1,
    heartbeatInterval: 1,
    leaderElection: {
      refreshInterval: leaderRefreshInterval,
      checkInterval: leaderCheckInterval,
      ttl: leaderTtl,
    },
    lifecycleEvents: {
      startup: [() => sendMessage({ type: "lifecycle", event: "startup" })],
      quiet: [() => sendMessage({ type: "lifecycle", event: "quiet" })],
      shutdown: [() => sendMessage({ type: "lifecycle", event: "shutdown" })],
      exit: [() => sendMessage({ type: "lifecycle", event: "exit" })],
      leader: [
        () =>
          sendMessage({
            type: "lifecycle",
            event: "became_leader",
            pid: process.pid,
          }),
      ],
      follower: [
        () =>
          sendMessage({
            type: "lifecycle",
            event: "became_follower",
            pid: process.pid,
          }),
      ],
    },
  });

  let runner: Runner | null = null;
  let redisClient: RedisClient | null = null;

  const ensureRedisClient = async (): Promise<RedisClient> => {
    if (redisClient) {
      return redisClient;
    }
    redisClient = await config.getRedisClient();
    return redisClient;
  };

  const disconnectRedis = async (): Promise<void> => {
    try {
      const client = await ensureRedisClient();
      if (client.isOpen) {
        await client.disconnect();
      }
      sendMessage({
        type: "lifecycle",
        event: "redis_disconnected",
        pid: process.pid,
      });
    } catch (error) {
      sendMessage({
        type: "error",
        data: { message: `redis_disconnected: ${(error as Error).message}` },
      });
    }
  };

  const reconnectRedis = async (): Promise<void> => {
    try {
      const client = await ensureRedisClient();
      if (!client.isOpen) {
        await client.connect();
      }
      sendMessage({
        type: "lifecycle",
        event: "redis_reconnected",
        pid: process.pid,
      });
    } catch (error) {
      sendMessage({
        type: "error",
        data: { message: `redis_reconnected: ${(error as Error).message}` },
      });
    }
  };

  const shutdown = async (signal: string) => {
    sendMessage({ type: "lifecycle", event: `signal:${signal}` });
    if (runner) {
      await runner.stop();
    }
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGUSR1", () => {
    disconnectRedis().catch(() => {
      // Intentionally ignore errors
    });
  });
  process.on("SIGUSR2", () => {
    reconnectRedis().catch(() => {
      // Intentionally ignore errors
    });
  });

  try {
    runner = await Sidekiq.run({ config });

    await ensureRedisClient();

    sendMessage({
      type: "lifecycle",
      event: "running",
      data: { pid: process.pid },
    });

    // Periodically report leader status
    const statusReporter = setInterval(() => {
      const isLeader = runner?.leader() ?? false;
      sendMessage({
        type: "status",
        isLeader,
        pid: process.pid,
      });
    }, statusInterval);

    // Keep running until stopped
    await new Promise(() => {
      // Intentionally empty - blocks forever until process is killed
    });

    clearInterval(statusReporter);
  } catch (error) {
    sendMessage({ type: "error", data: { message: (error as Error).message } });
    process.exit(1);
  }
}

main();
