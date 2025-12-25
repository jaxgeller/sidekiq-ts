import { AsyncLocalStorage } from "node:async_hooks";
import type { Config } from "./config.js";
import { ITERATION_STATE_TTL_SECONDS } from "./iterable-constants.js";
import {
  generateJid,
  normalizeItem,
  nowInMillis,
  verifyJson,
} from "./job-util.js";
import { dumpJson } from "./json.js";
import type { RedisClient } from "./redis.js";
import { Sidekiq } from "./sidekiq.js";
import { Testing } from "./testing.js";
import type { BulkPayload, JobClassLike, JobPayload } from "./types.js";

const redisContext = new AsyncLocalStorage<RedisClient>();

// Local queue for reliable push - holds jobs when Redis is unavailable
const localQueue: JobPayload[] = [];

export class Client {
  private readonly config: Config;
  private readonly redisClient?: RedisClient;

  constructor({
    config,
    redis,
  }: { config?: Config; redis?: RedisClient } = {}) {
    this.config = config ?? Sidekiq.defaultConfiguration;
    this.redisClient = redis;
  }

  private async getRedis(): Promise<RedisClient> {
    return (
      this.redisClient ??
      redisContext.getStore() ??
      (await this.config.getRedisClient())
    );
  }

  middleware(
    fn?: (chain: Config["clientMiddleware"]) => void
  ): Config["clientMiddleware"] {
    if (fn) {
      fn(this.config.clientMiddleware);
    }
    return this.config.clientMiddleware;
  }

  async push(item: JobPayload): Promise<string | null> {
    const normalized = normalizeItem(item, Sidekiq.defaultJobOptions());
    const queue = normalized.queue ?? "default";

    try {
      const redis = await this.getRedis();
      const result = await this.config.clientMiddleware.invoke(
        item.class,
        normalized,
        queue,
        redis,
        async () => normalized
      );
      if (!result) {
        return null;
      }
      const payload = result as JobPayload;
      verifyJson(payload.args, this.config.strictArgs);
      await this.rawPush([payload]);
      return payload.jid ?? null;
    } catch (error) {
      // Redis unavailable - queue locally (skipping middleware)
      verifyJson(normalized.args, this.config.strictArgs);
      this.queueLocally([normalized], error);
      return normalized.jid ?? null;
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: bulk push validation is inherently complex
  async pushBulk(items: BulkPayload): Promise<(string | null)[]> {
    const batchSize = items.batch_size ?? 1000;
    const args = items.args;
    const at = items.at;

    if (!Array.isArray(args)) {
      throw new Error("Bulk arguments must be an Array of Arrays");
    }

    if (at !== undefined) {
      const atArray = Array.isArray(at) ? at : [at];
      if (
        atArray.length === 0 ||
        !atArray.every((entry) => typeof entry === "number")
      ) {
        throw new Error("Job 'at' must be a number or array of numbers");
      }
      if (Array.isArray(at) && at.length !== args.length) {
        throw new Error("Job 'at' array must match args array size");
      }
    }

    const jid = items.jid;
    if (jid && args.length > 1) {
      throw new Error("Explicit 'jid' is not supported for bulk jobs");
    }

    const spreadInterval = items.spread_interval;
    if (
      spreadInterval !== undefined &&
      (typeof spreadInterval !== "number" || spreadInterval <= 0)
    ) {
      throw new Error("Jobs 'spread_interval' must be a positive number");
    }
    if (at !== undefined && spreadInterval !== undefined) {
      throw new Error("Only one of 'at' or 'spread_interval' can be provided");
    }

    let resolvedAt = at;
    if (resolvedAt === undefined && spreadInterval !== undefined) {
      const interval = Math.max(spreadInterval, 5);
      const now = Date.now() / 1000;
      resolvedAt = args.map(() => now + Math.random() * interval);
    }

    const base: JobPayload = {
      ...(items as Record<string, unknown>),
      args,
    } as JobPayload;
    (base as Record<string, unknown>).batch_size = undefined;
    (base as Record<string, unknown>).spread_interval = undefined;
    (base as Record<string, unknown>).at = undefined;
    (base as Record<string, unknown>).jid = undefined;
    const normalized = normalizeItem(base, Sidekiq.defaultJobOptions());

    const results: Array<string | null> = [];

    try {
      const redis = await this.getRedis();
      for (let i = 0; i < args.length; i += batchSize) {
        const slice = args.slice(i, i + batchSize);
        if (slice.length === 0) {
          break;
        }
        if (!slice.every((entry) => Array.isArray(entry))) {
          throw new Error(
            "Bulk arguments must be an Array of Arrays: [[1], [2]]"
          );
        }

        const payloads = await Promise.all(
          slice.map(async (jobArgs, index) => {
            const payload: JobPayload = {
              ...normalized,
              args: jobArgs,
              jid: generateJid(),
            };
            if (resolvedAt !== undefined) {
              payload.at = Array.isArray(resolvedAt)
                ? resolvedAt[i + index]
                : resolvedAt;
            }
            const result = await this.config.clientMiddleware.invoke(
              items.class,
              payload,
              payload.queue ?? "default",
              redis,
              async () => payload
            );
            if (!result) {
              return null;
            }
            const finalPayload = result as JobPayload;
            verifyJson(finalPayload.args, this.config.strictArgs);
            return finalPayload;
          })
        );

        const toPush = payloads.filter((payload): payload is JobPayload =>
          Boolean(payload)
        );
        await this.rawPush(toPush);
        results.push(...payloads.map((payload) => payload?.jid ?? null));
      }
    } catch (error) {
      // Redis unavailable - build payloads without middleware and queue locally
      for (let i = results.length; i < args.length; i++) {
        const jobArgs = args[i];
        if (!Array.isArray(jobArgs)) {
          throw new Error(
            "Bulk arguments must be an Array of Arrays: [[1], [2]]"
          );
        }
        const payload: JobPayload = {
          ...normalized,
          args: jobArgs,
          jid: generateJid(),
        };
        if (resolvedAt !== undefined) {
          payload.at = Array.isArray(resolvedAt) ? resolvedAt[i] : resolvedAt;
        }
        verifyJson(payload.args, this.config.strictArgs);
        this.queueLocally([payload], error);
        results.push(payload.jid ?? null);
      }
    }

    return results;
  }

  async cancel(jid: string): Promise<boolean> {
    const redis = await this.getRedis();
    const key = `it-${jid}`;
    const now = String(Math.floor(Date.now() / 1000));
    const pipeline = redis.multi();
    pipeline.hSetNX(key, "cancelled", now);
    pipeline.hGet(key, "cancelled");
    pipeline.expire(key, ITERATION_STATE_TTL_SECONDS);
    const result = await pipeline.exec();
    const cancelled = result?.[1] as unknown as string | null | undefined;
    return Boolean(Number(cancelled));
  }

  // biome-ignore lint/suspicious/useAdjacentOverloadSignatures: static methods are grouped separately from instance methods
  static push(item: JobPayload): Promise<string | null> {
    return new Client().push(item);
  }

  static pushBulk(items: BulkPayload): Promise<(string | null)[]> {
    return new Client().pushBulk(items);
  }

  static enqueue<TArgs extends unknown[]>(
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    return Client.push({ class: klass, args });
  }

  static enqueueTo<TArgs extends unknown[]>(
    queue: string,
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    return Client.push({ class: klass, queue, args });
  }

  static enqueueToIn<TArgs extends unknown[]>(
    queue: string,
    interval: number,
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    const now = Date.now() / 1000;
    const ts = interval < 1_000_000_000 ? now + interval : interval;
    const payload: JobPayload = { class: klass, queue, args };
    if (ts > now) {
      payload.at = ts;
    }
    return Client.push(payload);
  }

  static enqueueIn<TArgs extends unknown[]>(
    interval: number,
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    const queue =
      typeof klass !== "string" && klass.getSidekiqOptions
        ? (klass.getSidekiqOptions().queue ?? "default")
        : "default";
    return Client.enqueueToIn(queue, interval, klass, ...args);
  }

  static via<T>(redis: RedisClient, fn: () => Promise<T>): Promise<T> {
    return redisContext.run(redis, fn);
  }

  /**
   * Returns the number of jobs currently in the local queue.
   * Jobs are queued locally when Redis is unavailable.
   */
  static localQueueSize(): number {
    return localQueue.length;
  }

  /**
   * Clears the local queue. Primarily for testing.
   */
  static clearLocalQueue(): void {
    localQueue.length = 0;
  }

  private async rawPush(payloads: JobPayload[]): Promise<void> {
    if (payloads.length === 0) {
      return;
    }
    const testMode = Testing.mode();
    if (testMode === "fake") {
      for (const payload of payloads) {
        Testing.enqueue(payload);
      }
      return;
    }
    if (testMode === "inline") {
      for (const payload of payloads) {
        await Testing.performInline(payload, this.config);
      }
      return;
    }

    try {
      const redis = await this.getRedis();

      // Drain any locally queued jobs first
      await this.drainLocalQueue(redis);

      // Push the current batch
      await this.pushToRedis(redis, payloads);
    } catch (error) {
      // Queue locally on Redis failure (silent success)
      this.queueLocally(payloads, error);
    }
  }

  private async pushToRedis(
    redis: RedisClient,
    payloads: JobPayload[]
  ): Promise<void> {
    const pipeline = redis.multi();

    if (payloads[0].at !== undefined) {
      for (const payload of payloads) {
        const at = payload.at as number;
        const { at: _at, enqueued_at: _enqueuedAt, ...copy } = payload;
        pipeline.zAdd("schedule", [{ score: at, value: dumpJson(copy) }]);
      }
    } else {
      const now = nowInMillis();
      const grouped = new Map<string, JobPayload[]>();
      for (const payload of payloads) {
        const queue = payload.queue as string;
        const bucket = grouped.get(queue) ?? [];
        bucket.push(payload);
        grouped.set(queue, bucket);
      }

      const queueNames = Array.from(grouped.keys());
      if (queueNames.length > 0) {
        pipeline.sAdd("queues", queueNames);
      }

      for (const [queue, entries] of grouped.entries()) {
        const toPush = entries.map((entry) => {
          entry.enqueued_at = now;
          return dumpJson(entry);
        });
        pipeline.lPush(`queue:${queue}`, toPush);
      }
    }

    await pipeline.exec();
  }

  private async drainLocalQueue(redis: RedisClient): Promise<void> {
    if (localQueue.length === 0) {
      return;
    }

    const drainCount = localQueue.length;
    this.config.logger.info(
      () => `Draining ${drainCount} locally queued jobs to Redis`
    );

    // Process in batches to avoid huge pipelines
    while (localQueue.length > 0) {
      const batch = localQueue.splice(0, 100);
      try {
        // Separate scheduled and immediate jobs
        const scheduled = batch.filter((p) => p.at !== undefined);
        const immediate = batch.filter((p) => p.at === undefined);

        if (scheduled.length > 0) {
          await this.pushToRedis(redis, scheduled);
        }
        if (immediate.length > 0) {
          await this.pushToRedis(redis, immediate);
        }
      } catch {
        // Put back and abort drain - Redis still unavailable
        localQueue.unshift(...batch);
        throw new Error("Failed to drain local queue");
      }
    }
  }

  private queueLocally(payloads: JobPayload[], error: unknown): void {
    const maxQueue = this.config.reliableClientMaxQueue;
    if (maxQueue === 0) {
      // Reliability disabled - re-throw
      throw error;
    }

    const available = maxQueue - localQueue.length;
    if (available <= 0) {
      this.config.logger.warn(
        () =>
          `Local queue full (${maxQueue} jobs), dropping ${payloads.length} jobs`
      );
      return;
    }

    const toQueue = payloads.slice(0, available);
    localQueue.push(...toQueue);

    const dropped = payloads.length - toQueue.length;
    if (dropped > 0) {
      this.config.logger.warn(
        () =>
          `Queued ${toQueue.length} jobs locally, dropped ${dropped} (queue full at ${maxQueue})`
      );
    } else {
      this.config.logger.warn(
        () =>
          `Redis unavailable, queued ${toQueue.length} jobs locally (${localQueue.length}/${maxQueue})`
      );
    }
  }
}
