import { Sidekiq } from "./sidekiq.js";
import { dumpJson, loadJson } from "./json.js";
import { Client } from "./client.js";
import type { Config } from "./config.js";
import type { JobPayload } from "./types.js";
import type { RedisClient } from "./redis.js";

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

  static async all(config?: Config): Promise<Queue[]> {
    const redis = await getRedis(config);
    const queues = await redis.sMembers("queues");
    return queues.sort().map((queue) => new Queue(queue, config));
  }

  async size(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.lLen(`queue:${this.name}`));
  }

  async clear(): Promise<void> {
    const redis = await getRedis(this.config);
    const pipeline = redis.multi();
    pipeline.unlink(`queue:${this.name}`);
    pipeline.sRem("queues", [this.name]);
    await pipeline.exec();
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

  async *each(pageSize = 50): AsyncGenerator<JobRecord> {
    const redis = await getRedis(this.config);
    const initialSize = await redis.lLen(`queue:${this.name}`);
    let deletedSize = 0;
    let page = 0;
    while (true) {
      const start = Math.max(page * pageSize - deletedSize, 0);
      const end = start + pageSize - 1;
      const entries = await redis.lRange(`queue:${this.name}`, start, end);
      if (entries.length === 0) {
        break;
      }
      page += 1;
      for (const entry of entries) {
        yield new JobRecord(entry, this.name, this.config);
      }
      const size = await redis.lLen(`queue:${this.name}`);
      deletedSize = Math.max(initialSize - size, 0);
    }
  }

  async findJob(jid: string): Promise<JobRecord | null> {
    for await (const record of this.each()) {
      if (record.jid === jid) {
        return record;
      }
    }
    return null;
  }
}

export class JobRecord {
  readonly queue: string;
  readonly value: string;
  readonly payload: JobPayload;
  private config?: Config;

  constructor(value: string, queue: string, config?: Config) {
    this.value = value;
    this.queue = queue;
    this.config = config;
    this.payload = loadJson(value) as JobPayload;
  }

  get jid(): string | undefined {
    return typeof this.payload.jid === "string" ? this.payload.jid : undefined;
  }

  async delete(): Promise<boolean> {
    const redis = await getRedis(this.config);
    const removed = await redis.lRem(`queue:${this.queue}`, 1, this.value);
    return removed > 0;
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
}

export class SortedEntry {
  readonly score: number;
  readonly payload: JobPayload;
  readonly value: string;
  private parent: JobSet;

  constructor(parent: JobSet, score: number, value: string) {
    this.parent = parent;
    this.score = score;
    this.value = value;
    this.payload = loadJson(value) as JobPayload;
  }

  get jid(): string | undefined {
    return typeof this.payload.jid === "string" ? this.payload.jid : undefined;
  }

  async delete(): Promise<void> {
    await this.parent.deleteByValue(this.value);
  }

  async reschedule(at: number): Promise<void> {
    await this.parent.rescheduleValue(this.value, at);
  }

  async addToQueue(): Promise<void> {
    await this.parent.addToQueueValue(this.value);
  }

  async retry(): Promise<void> {
    await this.parent.retryValue(this.value);
  }

  async kill(): Promise<void> {
    await this.parent.killValue(this.value);
  }
}

class JobSet extends SortedSet {
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
      const value = response[i];
      const score = Number(response[i + 1]);
      entries.push(new SortedEntry(this, score, value));
    }
    return entries;
  }

  async *scan(match: string, count = 100): AsyncGenerator<SortedEntry> {
    const redis = await getRedis(this.config);
    const pattern = match.includes("*") ? match : `*${match}*`;
    let cursor = "0";
    do {
      const response = (await redis.sendCommand([
        "ZSCAN",
        this.key,
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        String(count),
      ])) as [string, string[]];
      cursor = response[0];
      const items = response[1] ?? [];
      for (let i = 0; i < items.length; i += 2) {
        const value = items[i];
        const score = Number(items[i + 1]);
        yield new SortedEntry(this, score, value);
      }
    } while (cursor !== "0");
  }

  async *each(): AsyncGenerator<SortedEntry> {
    yield* this.scan("*");
  }

  async findJob(jid: string): Promise<SortedEntry | null> {
    for await (const entry of this.scan(jid)) {
      if (entry.jid === jid) {
        return entry;
      }
    }
    return null;
  }

  async schedule(timestamp: number, payload: JobPayload): Promise<void> {
    const redis = await getRedis(this.config);
    await redis.zAdd(this.key, [{ score: timestamp, value: dumpJson(payload) }]);
  }

  async retryAll(): Promise<void> {
    const client = new Client({ config: this.config });
    await this.popEach(async (value) => {
      const payload = loadJson(value) as JobPayload;
      if (typeof payload.retry_count === "number") {
        payload.retry_count -= 1;
      }
      await client.push(payload);
    });
  }

  async killAll(): Promise<void> {
    const dead = new DeadSet(this.config);
    await this.popEach(async (value) => {
      await dead.kill(value, { trim: false });
    });
    await dead.trim();
  }

  async deleteByValue(value: string): Promise<void> {
    const redis = await getRedis(this.config);
    await redis.zRem(this.key, value);
  }

  async rescheduleValue(value: string, at: number): Promise<void> {
    const redis = await getRedis(this.config);
    await redis.zAdd(this.key, [{ score: at, value }]);
  }

  async addToQueueValue(value: string): Promise<void> {
    const redis = await getRedis(this.config);
    const removed = await redis.zRem(this.key, value);
    if (removed === 0) {
      return;
    }
    const payload = loadJson(value) as JobPayload;
    const client = new Client({ config: this.config });
    await client.push(payload);
  }

  async retryValue(value: string): Promise<void> {
    const redis = await getRedis(this.config);
    const removed = await redis.zRem(this.key, value);
    if (removed === 0) {
      return;
    }
    const payload = loadJson(value) as JobPayload;
    if (typeof payload.retry_count === "number") {
      payload.retry_count -= 1;
    }
    const client = new Client({ config: this.config });
    await client.push(payload);
  }

  async killValue(value: string): Promise<void> {
    const redis = await getRedis(this.config);
    const removed = await redis.zRem(this.key, value);
    if (removed === 0) {
      return;
    }
    const dead = new DeadSet(this.config);
    await dead.kill(value);
  }

  private async popEach(fn: (value: string) => Promise<void>): Promise<void> {
    const redis = await getRedis(this.config);
    while (true) {
      const response = (await redis.sendCommand([
        "ZPOPMIN",
        this.key,
        "1",
      ])) as string[];
      if (!response || response.length === 0) {
        break;
      }
      const value = response[0];
      await fn(value);
    }
  }
}

export class ScheduledSet extends JobSet {
  constructor(config?: Config) {
    super("schedule", config);
  }
}

export class RetrySet extends JobSet {
  constructor(config?: Config) {
    super("retry", config);
  }
}

export class DeadSet extends JobSet {
  constructor(config?: Config) {
    super("dead", config);
  }

  async kill(payload: string | JobPayload, options: { trim?: boolean } = {}): Promise<void> {
    const redis = await getRedis(this.config);
    const nowSeconds = Date.now() / 1000;
    const value = typeof payload === "string" ? payload : dumpJson(payload);
    await redis.zAdd(this.key, [{ score: nowSeconds, value }]);
    if (options.trim !== false) {
      await this.trim();
    }
  }

  async trim(): Promise<void> {
    const redis = await getRedis(this.config);
    const maxJobs = this.config?.deadMaxJobs ?? Sidekiq.defaultConfiguration.deadMaxJobs;
    const timeout = this.config?.deadTimeoutInSeconds ?? Sidekiq.defaultConfiguration.deadTimeoutInSeconds;
    const cutoff = Date.now() / 1000 - timeout;
    const pipeline = redis.multi();
    pipeline.zRemRangeByScore(this.key, 0, cutoff);
    pipeline.zRemRangeByRank(this.key, 0, -maxJobs);
    await pipeline.exec();
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

  static async get(identity: string, config?: Config): Promise<ProcessInfoEntry | null> {
    const redis = await getRedis(config);
    const pipeline = redis.multi();
    pipeline.sIsMember("processes", identity);
    pipeline.hGetAll(identity);
    const result = await pipeline.exec();
    const exists = Number(result?.[0] ?? 0);
    const raw = (result?.[1] ?? {}) as unknown as Record<string, string>;
    if (exists === 0 || Object.keys(raw).length === 0) {
      return null;
    }
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

  async size(): Promise<number> {
    const redis = await getRedis(this.config);
    return Number(await redis.sCard("processes"));
  }

  async totalConcurrency(): Promise<number> {
    const entries = await this.entries();
    return entries.reduce(
      (sum, entry) => sum + Number(entry.info.concurrency ?? 0),
      0
    );
  }

  async totalRss(): Promise<number> {
    const entries = await this.entries();
    return entries.reduce((sum, entry) => sum + Number(entry.rss ?? 0), 0);
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
