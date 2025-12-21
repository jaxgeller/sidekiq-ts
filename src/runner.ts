import { loadJson } from "./json.js";
import { Client } from "./client.js";
import { resolveJob } from "./registry.js";
import type { Config } from "./config.js";
import type { JobPayload } from "./types.js";

const FETCH_TIMEOUT_SECONDS = 2;

const LUA_ZPOPBYSCORE = `
local key, now = KEYS[1], ARGV[1]
local jobs = redis.call("zrange", key, "-inf", now, "byscore", "limit", 0, 1)
if jobs[1] then
  redis.call("zrem", key, jobs[1])
  return jobs[1]
end
`;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type QueueMode = "strict" | "random" | "weighted";

const parseQueues = (
  queues: string[] | Array<[string, number]>
): { list: string[]; mode: QueueMode } => {
  const weights = new Map<string, number>();
  const list: string[] = [];

  for (const entry of queues) {
    if (Array.isArray(entry)) {
      const [name, weight] = entry;
      const weightValue = Number(weight ?? 0);
      weights.set(name, weightValue);
      const count = Math.max(weightValue, 1);
      for (let i = 0; i < count; i += 1) {
        list.push(name);
      }
    } else {
      const [name, weightString] = entry.split(",", 2);
      const weightValue = Number(weightString ?? 0);
      weights.set(name, weightValue);
      const count = Math.max(weightValue, 1);
      for (let i = 0; i < count; i += 1) {
        list.push(name);
      }
    }
  }

  const allWeights = Array.from(weights.values());
  const mode = allWeights.every((weight) => weight === 0)
    ? "strict"
    : allWeights.every((weight) => weight === 1)
      ? "random"
      : "weighted";

  return { list, mode };
};

const shuffle = <T>(values: T[]): T[] => {
  const result = [...values];
  for (let i = result.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
};

const unique = (values: string[]): string[] => {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      result.push(value);
    }
  }
  return result;
};

class QueueStrategy {
  private list: string[];
  private mode: QueueMode;

  constructor(queues: string[] | Array<[string, number]>) {
    const parsed = parseQueues(queues);
    this.list = parsed.list;
    this.mode = parsed.mode;
  }

  queueKeys(): string[] {
    if (this.list.length === 0) {
      return [];
    }
    if (this.mode === "strict") {
      return this.list.map((queue) => `queue:${queue}`);
    }
    const shuffled = shuffle(this.list);
    return unique(shuffled).map((queue) => `queue:${queue}`);
  }
}

export class Runner {
  private config: Config;
  private quieting = false;
  private stopping = false;
  private workers: Array<Promise<void>> = [];
  private schedulerHandle?: NodeJS.Timeout;
  private queueStrategy: QueueStrategy;

  constructor(config: Config) {
    this.config = config;
    this.queueStrategy = new QueueStrategy(config.queues);
  }

  async start(): Promise<void> {
    this.startScheduler();
    for (let i = 0; i < this.config.concurrency; i += 1) {
      this.workers.push(this.workLoop(i));
    }
  }

  async quiet(): Promise<void> {
    this.quieting = true;
  }

  async stop(): Promise<void> {
    this.quieting = true;
    this.stopping = true;
    this.stopScheduler();
    await Promise.all(this.workers);
  }

  private startScheduler(): void {
    const intervalMs = this.config.averageScheduledPollInterval * 1000;
    this.schedulerHandle = setInterval(() => {
      void this.enqueueScheduled();
    }, intervalMs);
  }

  private stopScheduler(): void {
    if (this.schedulerHandle) {
      clearInterval(this.schedulerHandle);
      this.schedulerHandle = undefined;
    }
  }

  private async enqueueScheduled(): Promise<void> {
    if (this.stopping) {
      return;
    }
    const redis = await this.config.getRedisClient();
    const client = new Client({ config: this.config });
    const now = Date.now() / 1000;

    while (!this.stopping) {
      const job = (await redis.sendCommand([
        "EVAL",
        LUA_ZPOPBYSCORE,
        "1",
        "schedule",
        String(now),
      ])) as string | null;

      if (!job) {
        break;
      }

      const payload = loadJson(job) as JobPayload;
      await client.push(payload);
    }
  }

  private async workLoop(_index: number): Promise<void> {
    while (!this.stopping) {
      if (this.quieting) {
        await sleep(50);
        continue;
      }

      const unit = await this.fetchWork();
      if (!unit) {
        continue;
      }

      await this.processJob(unit.queue, unit.payload);
    }
  }

  private async fetchWork(): Promise<
    | {
        queue: string;
        payload: JobPayload;
      }
    | null
  > {
    const queueKeys = this.queueStrategy.queueKeys();
    if (queueKeys.length === 0) {
      await sleep(FETCH_TIMEOUT_SECONDS * 1000);
      return null;
    }

    const redis = await this.config.getRedisClient();
    const result = (await redis.sendCommand([
      "BRPOP",
      ...queueKeys,
      String(FETCH_TIMEOUT_SECONDS),
    ])) as [string, string] | null;

    if (!result) {
      return null;
    }

    const [queue, job] = result;
    const payload = loadJson(job) as JobPayload;

    return { queue, payload };
  }

  private async processJob(queue: string, payload: JobPayload): Promise<void> {
    const redis = await this.config.getRedisClient();
    const className = String(payload.class);
    const klass = resolveJob(className);

    if (!klass) {
      this.config.logger.error(
        () => `Unknown job class ${className} for ${queue}`
      );
      await redis.incr("stat:failed");
      return;
    }

    const job = new klass();
    job.jid = payload.jid;
    job._context = { stopping: () => this.stopping };

    try {
      await job.perform(...(payload.args ?? []));
      await redis.incr("stat:processed");
    } catch (error) {
      const message =
        error instanceof Error ? error.message : `Unknown error: ${String(error)}`;
      this.config.logger.error(
        () => `Job ${className} failed on ${queue}: ${message}`
      );
      await redis.incr("stat:failed");
    }
  }
}
