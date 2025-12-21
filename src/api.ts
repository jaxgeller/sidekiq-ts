import { Sidekiq } from "./sidekiq.js";
import { loadJson } from "./json.js";
import type { Config } from "./config.js";
import type { JobPayload } from "./types.js";
import type { RedisClient } from "./redis.js";

export interface SortedEntry {
  score: number;
  payload: JobPayload;
}

const getRedis = async (config?: Config): Promise<RedisClient> =>
  (config ?? Sidekiq.defaultConfiguration).getRedisClient();

export class Stats {
  private config?: Config;

  constructor(config?: Config) {
    this.config = config;
  }

  async processed(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.get("stat:processed")) || 0;
  }

  async failed(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.get("stat:failed")) || 0;
  }

  async workersSize(): Promise<number> {
    const redis = await getRedis(this.config);
    const processes = await redis.sMembers("processes");
    if (processes.length === 0) {
      return 0;
    }
    const pipeline = redis.multi();
    processes.forEach((key) => {
      pipeline.hGet(key, "busy");
    });
    const result = await pipeline.exec();
    return (result ?? []).reduce((sum, value) => sum + Number(value ?? 0), 0);
  }

  async scheduledSize(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sendCommand(["ZCARD", "schedule"])) || 0;
  }

  async retrySize(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sendCommand(["ZCARD", "retry"])) || 0;
  }

  async deadSize(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sendCommand(["ZCARD", "dead"])) || 0;
  }

  async processesSize(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sendCommand(["SCARD", "processes"])) || 0;
  }

  async enqueued(): Promise<number> {
    const redis = await getRedis(this.config);
    const queues = await redis.sMembers("queues");
    if (queues.length === 0) {
      return 0;
    }
    const pipeline = redis.multi();
    queues.forEach((queue) => {
      pipeline.lLen(`queue:${queue}`);
    });
    const result = await pipeline.exec();
    return (result ?? []).reduce((sum, value) => sum + Number(value ?? 0), 0);
  }

  async defaultQueueLatency(): Promise<number> {
    const redis = await getRedis(this.config);
    const entry = (await redis.sendCommand([
      "LINDEX",
      "queue:default",
      "-1",
    ])) as string | null;

    if (!entry) {
      return 0;
    }

    let payload: JobPayload;
    try {
      payload = loadJson(entry) as JobPayload;
    } catch {
      return 0;
    }

    const enqueuedAt = Number(payload.enqueued_at);
    if (!Number.isFinite(enqueuedAt) || enqueuedAt <= 0) {
      return 0;
    }

    const nowMs = Date.now();
    const diffMs = enqueuedAt > 1_000_000_000_000 ? nowMs - enqueuedAt : nowMs - enqueuedAt * 1000;
    return diffMs / 1000;
  }

  async reset(...stats: Array<string | number | symbol>): Promise<void> {
    const targets = stats.length === 0 ? ["processed", "failed"] : stats.map(String);
    const allowed = targets.filter((stat) => stat === "processed" || stat === "failed");
    if (allowed.length === 0) {
      return;
    }
    const redis = await getRedis(this.config);
    const args: string[] = [];
    allowed.forEach((stat) => {
      args.push(`stat:${stat}`, "0");
    });
    await redis.mSet(args);
  }

  async queues(): Promise<Record<string, number>> {
    const redis = await getRedis(this.config);
    const queues = await redis.sMembers("queues");
    if (queues.length === 0) {
      return {};
    }
    const pipeline = redis.multi();
    queues.forEach((queue) => {
      pipeline.lLen(`queue:${queue}`);
    });
    const result = await pipeline.exec();
    const pairs: Array<[string, number]> = queues.map((queue, index) => [
      queue,
      Number(result?.[index] ?? 0),
    ]);
    pairs.sort((a, b) => b[1] - a[1]);
    return Object.fromEntries(pairs);
  }
}

export class StatsHistory {
  private config?: Config;
  private daysPrevious: number;
  private startDate: Date;

  constructor(daysPrevious: number, startDate: Date = new Date(), config?: Config) {
    if (daysPrevious < 1 || daysPrevious > 5 * 365) {
      throw new Error("daysPrevious must be between 1 and 1825");
    }
    this.config = config;
    this.daysPrevious = daysPrevious;
    this.startDate = new Date(Date.UTC(
      startDate.getUTCFullYear(),
      startDate.getUTCMonth(),
      startDate.getUTCDate()
    ));
  }

  async processed(): Promise<Record<string, number>> {
    return this.dateStat("processed");
  }

  async failed(): Promise<Record<string, number>> {
    return this.dateStat("failed");
  }

  private async dateStat(stat: "processed" | "failed"): Promise<Record<string, number>> {
    const dates: string[] = [];
    for (let i = 0; i < this.daysPrevious; i += 1) {
      const date = new Date(this.startDate);
      date.setUTCDate(this.startDate.getUTCDate() - i);
      dates.push(date.toISOString().slice(0, 10));
    }
    const keys = dates.map((date) => `stat:${stat}:${date}`);
    const redis = await getRedis(this.config);
    const values = await redis.mGet(keys);
    const result: Record<string, number> = {};
    dates.forEach((date, index) => {
      result[date] = Number(values[index] ?? 0);
    });
    return result;
  }
}

export class Queue {
  private config?: Config;
  readonly name: string;

  constructor(name: string = "default", config?: Config) {
    this.name = name;
    this.config = config;
  }

  async size(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.lLen(`queue:${this.name}`));
  }

  async clear(): Promise<void> {
    const redis = await getRedis(this.config);
    await redis.del(`queue:${this.name}`);
  }

  async entries(start = 0, stop = -1): Promise<JobPayload[]> {
    const redis = await getRedis(this.config);
    const range = await redis.lRange(`queue:${this.name}`, start, stop);
    return range.map((entry) => loadJson(entry) as JobPayload);
  }

  async latency(): Promise<number> {
    const redis = await getRedis(this.config);
    const entry = (await redis.sendCommand([
      "LINDEX",
      `queue:${this.name}`,
      "-1",
    ])) as string | null;

    if (!entry) {
      return 0;
    }

    let payload: JobPayload;
    try {
      payload = loadJson(entry) as JobPayload;
    } catch {
      return 0;
    }

    const enqueuedAt = Number(payload.enqueued_at);
    if (!Number.isFinite(enqueuedAt) || enqueuedAt <= 0) {
      return 0;
    }
    const nowMs = Date.now();
    const diffMs = enqueuedAt > 1_000_000_000_000 ? nowMs - enqueuedAt : nowMs - enqueuedAt * 1000;
    return diffMs / 1000;
  }
}

class SortedSet {
  protected config?: Config;
  protected key: string;

  constructor(key: string, config?: Config) {
    this.key = key;
    this.config = config;
  }

  async size(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sendCommand(["ZCARD", this.key]));
  }

  async clear(): Promise<void> {
    const redis = await getRedis(this.config);
    await redis.sendCommand(["ZREMRANGEBYRANK", this.key, "0", "-1"]);
  }

  async entries(start = 0, stop = -1): Promise<SortedEntry[]> {
    const redis = await getRedis(this.config);
    const response = (await redis.sendCommand([
      "ZRANGE",
      this.key,
      String(start),
      String(stop),
      "WITHSCORES",
    ])) as string[];
    const entries: SortedEntry[] = [];
    for (let i = 0; i < response.length; i += 2) {
      const payload = loadJson(response[i]) as JobPayload;
      const score = Number(response[i + 1]);
      entries.push({ score, payload });
    }
    return entries;
  }
}

export class ScheduledSet extends SortedSet {
  constructor(config?: Config) {
    super("schedule", config);
  }
}

export class RetrySet extends SortedSet {
  constructor(config?: Config) {
    super("retry", config);
  }
}

export class DeadSet extends SortedSet {
  constructor(config?: Config) {
    super("dead", config);
  }
}

export interface ProcessInfoEntry {
  identity: string;
  info: Record<string, unknown>;
  busy: number;
  beat: number;
  quiet: boolean;
  rtt_us: number;
  rss: number;
}

export class ProcessSet {
  private config?: Config;

  constructor(config?: Config) {
    this.config = config;
  }

  async entries(): Promise<ProcessInfoEntry[]> {
    const redis = await getRedis(this.config);
    const processes = await redis.sMembers("processes");
    if (processes.length === 0) {
      return [];
    }
    const pipeline = redis.multi();
    processes.forEach((key) => {
      pipeline.hGetAll(key);
    });
    const result = await pipeline.exec();
    return processes.map((identity, index) => {
      const raw = (result?.[index] ?? {}) as unknown as Record<string, string>;
      const info = raw.info ? (loadJson(raw.info) as Record<string, unknown>) : {};
      return {
        identity,
        info,
        busy: Number(raw.busy ?? 0),
        beat: Number(raw.beat ?? 0),
        quiet: raw.quiet === "true",
        rtt_us: Number(raw.rtt_us ?? 0),
        rss: Number(raw.rss ?? 0),
      };
    });
  }

  async cleanup(): Promise<number> {
    const redis = await getRedis(this.config);
    const lock = await redis.set("process_cleanup", "1", { NX: true, EX: 60 });
    if (lock !== "OK") {
      return 0;
    }
    const processes = await redis.sMembers("processes");
    if (processes.length === 0) {
      return 0;
    }
    const pipeline = redis.multi();
    processes.forEach((key) => {
      pipeline.hGet(key, "info");
    });
    const result = await pipeline.exec();
    const toPrune = processes.filter((_, index) => !result?.[index]);
    if (toPrune.length === 0) {
      return 0;
    }
    await redis.sRem("processes", toPrune);
    return toPrune.length;
  }
}

export interface WorkerEntry {
  process: string;
  thread: string;
  queue: string;
  payload: JobPayload;
  run_at: number;
}

export class Workers {
  private config?: Config;

  constructor(config?: Config) {
    this.config = config;
  }

  async entries(): Promise<WorkerEntry[]> {
    const redis = await getRedis(this.config);
    const processes = await redis.sMembers("processes");
    if (processes.length === 0) {
      return [];
    }
    const pipeline = redis.multi();
    processes.forEach((key) => {
      pipeline.hGetAll(`${key}:work`);
    });
    const result = await pipeline.exec();
    const entries: WorkerEntry[] = [];
    processes.forEach((identity, index) => {
      const work = (result?.[index] ?? {}) as unknown as Record<string, string>;
      Object.entries(work).forEach(([thread, value]) => {
        const parsed = loadJson(value) as { queue: string; payload: string; run_at: number };
        const payload = loadJson(parsed.payload) as JobPayload;
        entries.push({
          process: identity,
          thread,
          queue: parsed.queue,
          payload,
          run_at: parsed.run_at,
        });
      });
    });
    return entries;
  }
}
