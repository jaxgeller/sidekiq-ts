/**
 * Leader election for Sidekiq processes.
 *
 * Follows Sidekiq Enterprise's approach:
 * - Leaders refresh every 15 seconds
 * - Followers check/attempt acquisition every 60 seconds
 * - Redis is the source of truth (no Raft/Paxos)
 * - 20 second TTL on leader key for automatic failover
 */

import { hostname } from "node:os";
import type { Config } from "./config.js";
import type { RedisClient } from "./redis.js";

const LEADER_KEY = "leader";
const LEADER_TTL_SECONDS = 20;
const LEADER_REFRESH_INTERVAL_MS = 15_000;
const FOLLOWER_CHECK_INTERVAL_MS = 60_000;

const LUA_REFRESH_LEADER = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("set", KEYS[1], ARGV[1], "ex", ARGV[2])
end
return nil
`;

export interface LeaderElectorOptions {
  /** Leader refresh interval in milliseconds (default: 15000) */
  refreshInterval?: number;
  /** Follower check interval in milliseconds (default: 60000) */
  checkInterval?: number;
  /** Leader key TTL in seconds (default: 20) */
  ttl?: number;
  /** Override process identity (default: hostname:pid) */
  identity?: string;
}

export class LeaderElector {
  private readonly config: Config;
  private readonly identity: string;
  private readonly refreshInterval: number;
  private readonly checkInterval: number;
  private readonly ttl: number;

  private redis?: RedisClient;
  private isLeader = false;
  private running = false;
  private loopPromise?: Promise<void>;
  private stopController?: AbortController;

  constructor(config: Config, options: LeaderElectorOptions = {}) {
    this.config = config;
    this.identity = options.identity ?? `${hostname()}:${process.pid}`;
    this.refreshInterval =
      options.refreshInterval ?? LEADER_REFRESH_INTERVAL_MS;
    this.checkInterval = options.checkInterval ?? FOLLOWER_CHECK_INTERVAL_MS;
    this.ttl = options.ttl ?? LEADER_TTL_SECONDS;
  }

  /**
   * Start the leader election loop.
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    this.redis = await this.config.getRedisClient();
    this.running = true;
    this.stopController = new AbortController();

    // Try to acquire leadership immediately on start
    await this.tryAcquire();

    // Start the background loop
    this.loopPromise = this.leaderLoop();
  }

  /**
   * Stop the leader election loop and release leadership if held.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;
    this.stopController?.abort();

    // Release leadership on clean shutdown
    if (this.isLeader && this.redis) {
      try {
        const current = await this.redis.get(LEADER_KEY);
        if (current === this.identity) {
          await this.redis.del(LEADER_KEY);
        }
      } catch {
        // Ignore errors during shutdown
      }
    }

    this.isLeader = false;

    // Wait for loop to finish
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  /**
   * Returns true if this process is currently the leader.
   */
  leader(): boolean {
    return this.isLeader;
  }

  /**
   * Get the identity of the current leader (may be another process).
   */
  async currentLeader(): Promise<string | null> {
    const redis = this.redis ?? (await this.config.getRedisClient());
    return redis.get(LEADER_KEY);
  }

  /**
   * Try to acquire leadership. Returns true if successful.
   */
  private async tryAcquire(): Promise<boolean> {
    if (!this.redis) {
      return false;
    }

    try {
      const result = await this.redis.set(LEADER_KEY, this.identity, {
        NX: true,
        EX: this.ttl,
      });

      if (result === "OK") {
        if (!this.isLeader) {
          this.isLeader = true;
          await this.config.fireEvent("leader", { oneshot: false });
          this.config.logger.info(() => `Became leader: ${this.identity}`);
        }
        return true;
      }
    } catch (error) {
      this.config.logger.error(
        () => `Leader acquisition error: ${(error as Error).message}`
      );
    }

    return false;
  }

  /**
   * Refresh leadership (extend TTL). Returns true if successful.
   */
  private async tryRefresh(): Promise<boolean> {
    if (!this.redis) {
      return false;
    }

    try {
      const result = await this.redis.eval(LUA_REFRESH_LEADER, {
        keys: [LEADER_KEY],
        arguments: [this.identity, String(this.ttl)],
      });
      return result === "OK";
    } catch (error) {
      this.config.logger.error(
        () => `Leader refresh error: ${(error as Error).message}`
      );
      return false;
    }
  }

  /**
   * Background loop for leader election.
   */
  private async leaderLoop(): Promise<void> {
    while (this.running) {
      try {
        if (this.isLeader) {
          // Leader: try to refresh
          const refreshed = await this.tryRefresh();
          if (!refreshed) {
            this.isLeader = false;
            await this.config.fireEvent("follower", { oneshot: false });
            this.config.logger.info(() => `Lost leadership: ${this.identity}`);
          }
        } else {
          // Follower: try to acquire
          await this.tryAcquire();
        }
      } catch (error) {
        // On unexpected errors, assume we lost leadership
        if (this.isLeader) {
          this.isLeader = false;
          await this.config.fireEvent("follower", { oneshot: false });
          this.config.logger.warn(
            () => `Lost leadership due to error: ${(error as Error).message}`
          );
        }
      }

      // Sleep for appropriate interval
      const interval = this.isLeader
        ? this.refreshInterval
        : this.checkInterval;
      await this.sleepWithAbort(interval);
    }
  }

  private async sleepWithAbort(ms: number): Promise<void> {
    const controller = this.stopController;
    if (!controller || controller.signal.aborted) {
      return;
    }
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        controller.signal.removeEventListener("abort", onAbort);
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(timeout);
        controller.signal.removeEventListener("abort", onAbort);
        resolve();
      };
      controller.signal.addEventListener("abort", onAbort, { once: true });
    });
  }
}
