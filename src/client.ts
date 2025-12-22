import { AsyncLocalStorage } from "node:async_hooks";
import type { RedisClient } from "./redis.js";
import { Sidekiq } from "./sidekiq.js";
import { dumpJson } from "./json.js";
import {
  normalizeItem,
  nowInMillis,
  verifyJson,
  generateJid,
} from "./job_util.js";
import { ITERATION_STATE_TTL_SECONDS } from "./iterable_constants.js";
import { Testing } from "./testing.js";
import type {
  BulkPayload,
  JobPayload,
  JobClassLike,
  JobOptions,
} from "./types.js";
import type { Config } from "./config.js";

const redisContext = new AsyncLocalStorage<RedisClient>();

export class Client {
  private config: Config;
  private redisClient?: RedisClient;

  constructor({ config, redis }: { config?: Config; redis?: RedisClient } = {}) {
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

  middleware(fn?: (chain: Config["clientMiddleware"]) => void): Config["clientMiddleware"] {
    if (fn) {
      fn(this.config.clientMiddleware);
    }
    return this.config.clientMiddleware;
  }

  async push(item: JobPayload): Promise<string | null> {
    const normalized = normalizeItem(item, Sidekiq.defaultJobOptions());
    const queue = normalized.queue ?? "default";
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
  }

  async pushBulk(items: BulkPayload): Promise<(string | null)[]> {
    const batchSize = items.batch_size ?? 1000;
    const args = items.args;
    const at = items.at;

    if (!Array.isArray(args)) {
      throw new Error("Bulk arguments must be an Array of Arrays");
    }

    if (at !== undefined) {
      const atArray = Array.isArray(at) ? at : [at];
      if (atArray.length === 0 || !atArray.every((entry) => typeof entry === "number")) {
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

    const base: JobPayload = { ...(items as Record<string, unknown>), args } as JobPayload;
    delete (base as Record<string, unknown>).batch_size;
    delete (base as Record<string, unknown>).spread_interval;
    delete (base as Record<string, unknown>).at;
    delete (base as Record<string, unknown>).jid;
    const normalized = normalizeItem(base, Sidekiq.defaultJobOptions());

    const results: Array<string | null> = [];
    const redis = await this.getRedis();
    for (let i = 0; i < args.length; i += batchSize) {
      const slice = args.slice(i, i + batchSize);
      if (slice.length === 0) {
        break;
      }
      if (!slice.every((entry) => Array.isArray(entry))) {
        throw new Error("Bulk arguments must be an Array of Arrays: [[1], [2]]");
      }

      const payloads = await Promise.all(slice.map(async (jobArgs, index) => {
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
      }));

      const toPush = payloads.filter((payload): payload is JobPayload => Boolean(payload));
      await this.rawPush(toPush);
      results.push(...payloads.map((payload) => payload?.jid ?? null));
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
    pipeline.expire(key, ITERATION_STATE_TTL_SECONDS, "NX");
    const result = await pipeline.exec();
    const cancelled = result?.[1] as unknown as string | null | undefined;
    return Boolean(Number(cancelled));
  }

  static async push(item: JobPayload): Promise<string | null> {
    return new Client().push(item);
  }

  static async pushBulk(items: BulkPayload): Promise<(string | null)[]> {
    return new Client().pushBulk(items);
  }

  static async enqueue<TArgs extends unknown[]>(
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    return Client.push({ class: klass, args });
  }

  static async enqueueTo<TArgs extends unknown[]>(
    queue: string,
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    return Client.push({ class: klass, queue, args });
  }

  static async enqueueToIn<TArgs extends unknown[]>(
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

  static async enqueueIn<TArgs extends unknown[]>(
    interval: number,
    klass: JobClassLike,
    ...args: TArgs
  ): Promise<string | null> {
    const queue =
      typeof klass !== "string" && klass.getSidekiqOptions
        ? klass.getSidekiqOptions().queue ?? "default"
        : "default";
    return Client.enqueueToIn(queue, interval, klass, ...args);
  }

  static async via<T>(redis: RedisClient, fn: () => Promise<T>): Promise<T> {
    return redisContext.run(redis, fn);
  }

  private async rawPush(payloads: JobPayload[]): Promise<void> {
    if (payloads.length === 0) {
      return;
    }
    const testMode = Testing.mode();
    if (testMode === "fake") {
      payloads.forEach((payload) => Testing.enqueue(payload));
      return;
    }
    if (testMode === "inline") {
      for (const payload of payloads) {
        await Testing.performInline(payload, this.config);
      }
      return;
    }
    const redis = await this.getRedis();
    const pipeline = redis.multi();

    if (payloads[0].at !== undefined) {
      for (const payload of payloads) {
        const at = payload.at as number;
        const copy = { ...payload };
        delete copy.at;
        delete copy.enqueued_at;
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
}
