/**
 * Periodic job scheduler for Sidekiq.
 *
 * Allows registering jobs that run on a cron schedule.
 * Only the leader process enqueues periodic jobs to prevent duplicates.
 */

import { Client } from "./client.js";
import type { Config } from "./config.js";
import { type CronSchedule, parseCron, shouldRunAt } from "./cron.js";
import type { JobConstructor } from "./job.js";
import { dumpJson } from "./json.js";
import type { LeaderElector } from "./leader.js";
import type { RedisClient } from "./redis.js";
import type { JobOptions } from "./types.js";

const CRON_KEY = "cron";
const CRON_LOCK_PREFIX = "cron:lock:";
const CRON_LOCK_TTL_SECONDS = 60;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface PeriodicJobOptions extends JobOptions {
  /** Job arguments */
  args?: unknown[];
}

export interface PeriodicJobConfig {
  /** Unique identifier for this periodic job */
  lid: string;
  /** Cron expression */
  cron: string;
  /** Job class name */
  class: string;
  /** Target queue */
  queue: string;
  /** Job arguments */
  args: unknown[];
  /** Additional job options */
  options: PeriodicJobOptions;
}

interface RegisteredJob {
  config: PeriodicJobConfig;
  schedule: CronSchedule;
  jobClass: JobConstructor;
}

export class PeriodicScheduler {
  private readonly config: Config;
  private readonly leaderElector: LeaderElector;
  private readonly jobs: Map<string, RegisteredJob> = new Map();
  private readonly client: Client;

  private redis?: RedisClient;
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(config: Config, leaderElector: LeaderElector) {
    this.config = config;
    this.leaderElector = leaderElector;
    this.client = new Client({ config });
  }

  /**
   * Register a periodic job.
   *
   * @param cron - Standard 5-field cron expression
   * @param jobClass - Job class to execute
   * @param options - Additional job options
   * @returns The unique lid for this periodic job
   */
  register(
    cron: string,
    jobClass: JobConstructor,
    options: PeriodicJobOptions = {}
  ): string {
    const schedule = parseCron(cron);
    const queue =
      options.queue ?? jobClass.getSidekiqOptions().queue ?? "default";
    const args = options.args ?? [];

    // Generate a deterministic lid based on class name and cron
    const lid = this.generateLid(jobClass.name, cron, queue, args);

    const config: PeriodicJobConfig = {
      lid,
      cron,
      class: jobClass.name,
      queue,
      args,
      options,
    };

    this.jobs.set(lid, { config, schedule, jobClass });

    this.config.logger.debug(
      () => `Registered periodic job: ${jobClass.name} (${cron})`
    );

    return lid;
  }

  /**
   * Unregister a periodic job by its lid.
   */
  unregister(lid: string): boolean {
    const existed = this.jobs.delete(lid);
    if (existed) {
      this.config.logger.debug(() => `Unregistered periodic job: ${lid}`);
    }
    return existed;
  }

  /**
   * List all registered periodic jobs.
   */
  list(): PeriodicJobConfig[] {
    return Array.from(this.jobs.values()).map((job) => job.config);
  }

  /**
   * Start the periodic job loop.
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    this.redis = await this.config.getRedisClient();
    this.running = true;

    // Persist job configs to Redis for visibility
    await this.syncToRedis();

    // Start the background loop
    this.loopPromise = this.periodicLoop();
  }

  /**
   * Stop the periodic job loop.
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    // Remove job configs from Redis
    if (this.redis && this.jobs.size > 0) {
      try {
        const lids = Array.from(this.jobs.keys());
        await this.redis.hDel(CRON_KEY, lids);
      } catch {
        // Ignore errors during shutdown
      }
    }

    // Wait for loop to finish
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  /**
   * Generate a deterministic lid for a periodic job.
   */
  private generateLid(
    className: string,
    cron: string,
    queue: string,
    args: unknown[]
  ): string {
    // Create a simple hash of the job configuration
    const base = `${className}:${cron}:${queue}:${dumpJson(args)}`;
    let hash = 0;
    for (let i = 0; i < base.length; i += 1) {
      const char = base.charCodeAt(i);
      // biome-ignore lint/suspicious/noBitwiseOperators: intentional hash computation
      hash = ((hash << 5) - hash + char) | 0;
    }
    return `${className.toLowerCase()}-${Math.abs(hash).toString(36)}`;
  }

  /**
   * Sync registered jobs to Redis for visibility.
   */
  private async syncToRedis(): Promise<void> {
    if (!this.redis || this.jobs.size === 0) {
      return;
    }

    const entries: Record<string, string> = {};
    for (const [lid, job] of this.jobs) {
      entries[lid] = dumpJson(job.config);
    }

    try {
      await this.redis.hSet(CRON_KEY, entries);
    } catch (error) {
      this.config.logger.error(
        () =>
          `Failed to sync periodic jobs to Redis: ${(error as Error).message}`
      );
    }
  }

  /**
   * Background loop for periodic job scheduling.
   */
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: scheduler loop requires multiple conditional checks
  private async periodicLoop(): Promise<void> {
    // Wait for initial sync with jitter to avoid thundering herd
    await sleep(Math.random() * 5000);

    while (this.running) {
      // Wait until the next minute boundary plus jitter
      const now = Date.now();
      const nextMinute = Math.ceil(now / 60_000) * 60_000;
      const jitter = Math.random() * 2000; // 0-2 seconds
      const waitMs = Math.max(0, nextMinute - now + jitter);

      await sleep(waitMs);

      if (!this.running) {
        break;
      }

      // Only leader enqueues jobs
      if (!this.leaderElector.leader()) {
        continue;
      }

      // Check each registered job
      const checkTime = new Date();
      for (const [lid, job] of this.jobs) {
        if (!this.running) {
          break;
        }

        try {
          if (shouldRunAt(job.schedule, checkTime)) {
            await this.enqueueIfNotRecent(lid, job);
          }
        } catch (error) {
          this.config.logger.error(
            () =>
              `Error checking periodic job ${lid}: ${(error as Error).message}`
          );
        }
      }
    }
  }

  /**
   * Enqueue a periodic job if it hasn't been enqueued recently.
   */
  private async enqueueIfNotRecent(
    lid: string,
    job: RegisteredJob
  ): Promise<void> {
    if (!this.redis) {
      return;
    }

    // Try to acquire the lock (prevents duplicate enqueue)
    const lockKey = `${CRON_LOCK_PREFIX}${lid}`;
    const result = await this.redis.set(lockKey, "1", {
      NX: true,
      EX: CRON_LOCK_TTL_SECONDS,
    });

    if (result !== "OK") {
      // Lock already held, job was already enqueued this minute
      return;
    }

    // Enqueue the job
    const { queue, retry, tags, ...restOptions } = job.config.options;
    const jid = await this.client.push({
      class: job.jobClass,
      args: job.config.args,
      queue: job.config.queue,
      retry,
      tags,
      ...restOptions,
    });

    this.config.logger.info(
      () => `Enqueued periodic job ${job.config.class} (${lid}) with jid ${jid}`
    );
  }
}
