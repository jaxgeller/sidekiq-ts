import { dumpJson, loadJson } from "./json.js";
import { Client } from "./client.js";
import { resolveJob } from "./registry.js";
import type { JobConstructor, RetriesExhaustedHandler, RetryInHandler } from "./job.js";
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
  private baseRedis?: Awaited<ReturnType<Config["getRedisClient"]>>;
  private workerRedis: Array<Awaited<ReturnType<Config["getRedisClient"]>>> = [];

  constructor(config: Config) {
    this.config = config;
    this.queueStrategy = new QueueStrategy(config.queues);
  }

  async start(): Promise<void> {
    this.baseRedis = await this.config.getRedisClient();
    this.startScheduler();
    for (let i = 0; i < this.config.concurrency; i += 1) {
      const client = this.baseRedis.duplicate();
      await client.connect();
      this.workerRedis[i] = client;
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
    await Promise.all(
      this.workerRedis.map(async (client) => {
        if (client.isOpen) {
          await client.quit();
        }
      })
    );
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
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const client = new Client({ config: this.config });
    const now = Date.now() / 1000;
    const sets = ["schedule", "retry"];

    for (const set of sets) {
      while (!this.stopping) {
        const job = (await redis.sendCommand([
          "EVAL",
          LUA_ZPOPBYSCORE,
          "1",
          set,
          String(now),
        ])) as string | null;

        if (!job) {
          break;
        }

        const payload = loadJson(job) as JobPayload;
        await client.push(payload);
      }
    }
  }

  private async workLoop(index: number): Promise<void> {
    while (!this.stopping) {
      if (this.quieting) {
        await sleep(50);
        continue;
      }

      const unit = await this.fetchWork(index);
      if (!unit) {
        continue;
      }

      await this.processJob(unit.queue, unit.payload);
    }
  }

  private async fetchWork(index: number): Promise<
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

    const redis = this.workerRedis[index] ?? (await this.config.getRedisClient());
    const result = (await redis.sendCommand([
      "BRPOP",
      ...queueKeys,
      String(FETCH_TIMEOUT_SECONDS),
    ])) as [string, string] | null;

    if (!result) {
      return null;
    }

    const [queueKey, job] = result;
    const payload = loadJson(job) as JobPayload;
    const queue = queueKey.startsWith("queue:") ? queueKey.slice(6) : queueKey;

    return { queue, payload };
  }

  private async processJob(queue: string, payload: JobPayload): Promise<void> {
    const redis = await this.config.getRedisClient();
    const className = String(payload.class);
    const klass = resolveJob(className) as JobConstructor | undefined;

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
      let executed = false;
      await this.config.serverMiddleware.invoke(job, payload, queue, async () => {
        executed = true;
        await job.perform(...(payload.args ?? []));
      });
      if (executed) {
        await redis.incr("stat:processed");
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      await this.handleFailure(queue, payload, klass, err);
    }
  }

  private async handleFailure(
    queue: string,
    payload: JobPayload,
    klass: JobConstructor,
    error: Error
  ): Promise<void> {
    const redis = await this.config.getRedisClient();
    const className = String(payload.class);
    const message = error.message ?? `Unknown error: ${String(error)}`;
    this.config.logger.error(
      () => `Job ${className} failed on ${queue}: ${message}`
    );

    await redis.incr("stat:failed");

    const retryOption =
      payload.retry !== undefined
        ? payload.retry
        : klass.getSidekiqOptions().retry;

    const retryDisabled =
      retryOption === false || retryOption === null || retryOption === undefined;
    if (retryDisabled) {
      await this.runDeathHandlers(payload, error);
      await this.runErrorHandlers(payload, error, queue);
      return;
    }

    const maxRetries =
      typeof retryOption === "number" ? retryOption : this.config.maxRetries;

    const nowMs = Date.now();
    const nowSeconds = nowMs / 1000;
    payload.queue = payload.retry_queue ?? payload.queue ?? queue;
    payload.error_message = message.slice(0, 10_000);
    payload.error_class = error.name;

    if (payload.retry_count !== undefined) {
      payload.retry_count += 1;
      payload.retried_at = nowMs;
    } else {
      payload.retry_count = 0;
      payload.failed_at = nowMs;
    }

    const retryFor = payload.retry_for;
    if (
      typeof retryFor === "number" &&
      payload.failed_at !== undefined
    ) {
      const deadline = payload.failed_at / 1000 + retryFor;
      if (deadline < nowSeconds) {
        await this.retriesExhausted(payload, error, klass.sidekiqRetriesExhausted);
        await this.runErrorHandlers(payload, error, queue);
        return;
      }
    } else if (payload.retry_count >= maxRetries) {
      await this.retriesExhausted(payload, error, klass.sidekiqRetriesExhausted);
      await this.runErrorHandlers(payload, error, queue);
      return;
    }

    const retryIn = klass.sidekiqRetryIn;
    const delayResult = retryIn
      ? this.safeRetryIn(retryIn, payload.retry_count, error, payload)
      : "default";
    if (delayResult === "discard") {
      payload.discarded_at = nowMs;
      await this.runDeathHandlers(payload, error);
      return;
    }
    if (delayResult === "kill") {
      await this.retriesExhausted(payload, error, klass.sidekiqRetriesExhausted);
      await this.runErrorHandlers(payload, error, queue);
      return;
    }

    const delaySeconds =
      typeof delayResult === "number"
        ? delayResult
        : Math.pow(payload.retry_count, 4) + 15;
    const jitter = Math.random() * 10 * (payload.retry_count + 1);
    const retryAt = nowSeconds + delaySeconds + jitter;

    await redis.zAdd("retry", [{ score: retryAt, value: dumpJson(payload) }]);
    await this.runErrorHandlers(payload, error, queue);
  }

  private safeRetryIn(
    handler: RetryInHandler,
    count: number,
    error: Error,
    payload: JobPayload
  ): number | "discard" | "kill" | "default" {
    try {
      return handler(count, error, payload) ?? "default";
    } catch (handlerError) {
      const err =
        handlerError instanceof Error
          ? handlerError
          : new Error(String(handlerError));
      this.config.logger.error(
        () => `Error in retryIn handler: ${err.message}`
      );
      return "default";
    }
  }

  private async retriesExhausted(
    payload: JobPayload,
    error: Error,
    handler?: RetriesExhaustedHandler
  ): Promise<void> {
    let handlerResult: "discard" | void = undefined;
    if (handler) {
      try {
        handlerResult = handler(payload, error);
      } catch (handlerError) {
        const err =
          handlerError instanceof Error
            ? handlerError
            : new Error(String(handlerError));
        this.config.logger.error(
          () => `Error calling retriesExhausted handler: ${err.message}`
        );
      }
    }

    const discard = payload.dead === false || handlerResult === "discard";
    if (discard) {
      payload.discarded_at = Date.now();
    } else {
      await this.sendToMorgue(payload);
    }

    await this.runDeathHandlers(payload, error);
  }

  private async sendToMorgue(payload: JobPayload): Promise<void> {
    const redis = await this.config.getRedisClient();
    const nowSeconds = Date.now() / 1000;
    const cutoff = nowSeconds - this.config.deadTimeoutInSeconds;

    const pipeline = redis.multi();
    pipeline.zAdd("dead", [{ score: nowSeconds, value: dumpJson(payload) }]);
    pipeline.zRemRangeByScore("dead", 0, cutoff);
    pipeline.zRemRangeByRank("dead", 0, -this.config.deadMaxJobs);
    await pipeline.exec();
  }

  private async runDeathHandlers(payload: JobPayload, error: Error): Promise<void> {
    for (const handler of this.config.deathHandlers) {
      try {
        await handler(payload, error);
      } catch (handlerError) {
        const err =
          handlerError instanceof Error
            ? handlerError
            : new Error(String(handlerError));
        this.config.logger.error(
          () => `Error calling death handler: ${err.message}`
        );
      }
    }
  }

  private async runErrorHandlers(
    payload: JobPayload,
    error: Error,
    queue: string
  ): Promise<void> {
    if (this.config.errorHandlers.length === 0) {
      return;
    }
    for (const handler of this.config.errorHandlers) {
      try {
        await handler(error, { job: payload, queue });
      } catch (handlerError) {
        const err =
          handlerError instanceof Error
            ? handlerError
            : new Error(String(handlerError));
        this.config.logger.error(
          () => `Error calling error handler: ${err.message}`
        );
      }
    }
  }
}
