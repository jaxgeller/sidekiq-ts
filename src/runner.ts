import { hostname } from "node:os";
import { compressBacktrace, extractBacktrace } from "./backtrace.js";
import type { Config } from "./config.js";
import { ensureInterruptHandler } from "./interrupt-handler.js";
import { JobSkipError } from "./iterable-errors.js";
import type {
  JobConstructor,
  RetriesExhaustedHandler,
  RetryInHandler,
} from "./job.js";
import { dumpJson, loadJson } from "./json.js";
import { LeaderElector } from "./leader.js";
import { PeriodicScheduler } from "./periodic.js";
import { resolveJob } from "./registry.js";
import type { JobPayload } from "./types.js";

const FETCH_TIMEOUT_SECONDS = 2;
const STATS_TTL_SECONDS = 5 * 365 * 24 * 60 * 60;
const INITIAL_WAIT_SECONDS = 10;
const PAUSE_TIME_MS = 500;

// Reliable scheduler: atomically pop from sorted set AND enqueue to queue
// This eliminates the data loss window between pop and push operations
const LUA_RELIABLE_ENQUEUE = `
local key, now_seconds, now_millis = KEYS[1], ARGV[1], ARGV[2]
local jobs = redis.call("zrange", key, "-inf", now_seconds, "byscore", "limit", 0, 1)
if jobs[1] then
  redis.call("zrem", key, jobs[1])
  local job = cjson.decode(jobs[1])
  local queue = job.queue or "default"
  job.enqueued_at = tonumber(now_millis)
  local payload = cjson.encode(job)
  redis.call("sadd", "queues", queue)
  redis.call("lpush", "queue:" .. queue, payload)
  return jobs[1]
end
`;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type QueueMode = "strict" | "random" | "weighted";

const parseQueues = (
  queues: string[] | [string, number][]
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
  let mode: QueueMode;
  if (allWeights.every((weight) => weight === 0)) {
    mode = "strict";
  } else if (allWeights.every((weight) => weight === 1)) {
    mode = "random";
  } else {
    mode = "weighted";
  }

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
  private readonly list: string[];
  private readonly mode: QueueMode;

  constructor(queues: string[] | [string, number][]) {
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

export interface WorkSnapshot {
  workerId: string;
  queue: string;
  payloadRaw: string;
  payload?: JobPayload;
  runAt: number;
  elapsed: number;
}

export class Runner {
  private readonly config: Config;
  private quieting = false;
  private stopping = false;
  private readonly workers: Promise<void>[] = [];
  private schedulerHandle?: NodeJS.Timeout;
  private schedulerRunning = false;
  private heartbeatHandle?: NodeJS.Timeout;
  private readonly queueStrategy: QueueStrategy;
  private baseRedis?: Awaited<ReturnType<Config["getRedisClient"]>>;
  private readonly workerRedis: Awaited<
    ReturnType<Config["getRedisClient"]>
  >[] = [];
  private readonly identity: string;
  private readonly startedAt: number;
  private readonly workState = new Map<
    string,
    { queue: string; payload: string; runAt: number }
  >();
  private readonly inProgress = new Map<
    string,
    { queue: string; payload: string }
  >();
  private lastCleanupAt = 0;
  private readonly rttReadings: number[] = [];
  private readonly jobLogger: Config["jobLogger"];
  private leaderElector?: LeaderElector;
  private _periodicScheduler?: PeriodicScheduler;

  constructor(config: Config) {
    this.config = config;
    this.queueStrategy = new QueueStrategy(config.queues);
    this.startedAt = Date.now() / 1000;
    this.identity = `${hostname()}:${process.pid}`;
    this.jobLogger = config.jobLogger;
  }

  async start(): Promise<void> {
    this.baseRedis = await this.config.getRedisClient();
    ensureInterruptHandler(this.config);

    // Start leader election
    this.leaderElector = new LeaderElector(this.config, {
      identity: this.identity,
      ...this.config.leaderElection,
    });
    await this.leaderElector.start();

    // Start periodic scheduler
    this._periodicScheduler = new PeriodicScheduler(
      this.config,
      this.leaderElector
    );
    await this._periodicScheduler.start();

    await this.heartbeat();
    this.startHeartbeat();
    this.startScheduler();
    for (let i = 0; i < this.config.concurrency; i += 1) {
      const client = this.baseRedis.duplicate();
      await client.connect();
      this.workerRedis[i] = client;
      this.workers.push(this.workLoop(i));
    }
    await this.config.fireEvent("startup");
  }

  async quiet(): Promise<void> {
    this.quieting = true;
    await this.config.fireEvent("quiet");
  }

  async stop(): Promise<void> {
    if (!this.quieting) {
      await this.quiet();
    }
    this.stopping = true;
    await this.config.fireEvent("shutdown", { reverse: true });

    // Stop periodic scheduler and leader election
    await this._periodicScheduler?.stop();
    await this.leaderElector?.stop();

    this.stopHeartbeat();
    this.stopScheduler();

    // Brief pause to allow idle processors to finish
    await sleep(PAUSE_TIME_MS);

    const deadline = Date.now() + this.config.timeout * 1000;

    // Early exit if no jobs are running
    if (this.inProgress.size === 0) {
      await this.waitForWorkers(deadline);
      await this.clearHeartbeat();
      await Promise.all(
        this.workerRedis.map(async (client) => {
          if (client.isOpen) {
            await client.quit();
          }
        })
      );
      await this.config.fireEvent("exit", { reverse: true });
      return;
    }

    this.config.logger.info(() => "Pausing to allow jobs to finish...");

    await this.waitForDrain(deadline);
    if (this.inProgress.size > 0) {
      await this.requeueInProgress();
    }
    await this.waitForWorkers(deadline);
    await this.clearHeartbeat();
    await Promise.all(
      this.workerRedis.map(async (client) => {
        if (client.isOpen) {
          await client.quit();
        }
      })
    );
    await this.config.fireEvent("exit", { reverse: true });
  }

  /**
   * Returns true if this process is currently the leader.
   */
  leader(): boolean {
    return this.leaderElector?.leader() ?? false;
  }

  /**
   * Get the periodic scheduler for registering cron jobs.
   */
  get periodicScheduler(): PeriodicScheduler | undefined {
    return this._periodicScheduler;
  }

  snapshotWork(): WorkSnapshot[] {
    const now = Date.now();
    const entries: WorkSnapshot[] = [];
    for (const [workerId, value] of this.workState.entries()) {
      let payload: JobPayload | undefined;
      try {
        payload = loadJson(value.payload) as JobPayload;
      } catch {
        payload = undefined;
      }
      entries.push({
        workerId,
        queue: value.queue,
        payloadRaw: value.payload,
        payload,
        runAt: value.runAt,
        elapsed: (now - value.runAt) / 1000,
      });
    }
    return entries;
  }

  private startScheduler(): void {
    this.schedulerRunning = true;
    this.runSchedulerLoop().catch(() => undefined);
  }

  private async runSchedulerLoop(): Promise<void> {
    // Initial wait: give time for heartbeats to register so process count is accurate
    await this.initialWait();

    while (this.schedulerRunning && !this.stopping) {
      await this.enqueueScheduled().catch(() => undefined);
      const intervalMs = await this.randomPollInterval();
      await sleep(intervalMs);
    }
  }

  private async initialWait(): Promise<void> {
    // Wait 10-15 seconds before first poll to let heartbeats register
    // and to avoid thundering herd on restart
    const useDynamicInterval = this.config.pollIntervalAverage === null;
    let waitSeconds = 0;

    if (useDynamicInterval) {
      waitSeconds += INITIAL_WAIT_SECONDS;
    }
    waitSeconds += Math.random() * 5; // 0-5 seconds jitter

    if (waitSeconds > 0) {
      await sleep(waitSeconds * 1000);
    }

    // Run cleanup after initial wait
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    await this.cleanupProcesses(redis);
  }

  private async processCount(): Promise<number> {
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const count = await redis.sCard("processes");
    return count === 0 ? 1 : count;
  }

  private scaledPollInterval(processCount: number): number {
    return processCount * this.config.averageScheduledPollInterval;
  }

  private async randomPollInterval(): Promise<number> {
    // If user set a fixed poll interval, use it directly
    if (this.config.pollIntervalAverage !== null) {
      return this.config.pollIntervalAverage * 1000;
    }

    const count = await this.processCount();
    const interval = this.scaledPollInterval(count);

    let intervalSeconds: number;
    if (count < 10) {
      // For small clusters, calculate a random interval that is ±50% the desired average
      intervalSeconds = interval * Math.random() + interval / 2;
    } else {
      // With 10+ processes, we have enough randomness for decent polling spread
      intervalSeconds = interval * Math.random() * 2;
    }

    return intervalSeconds * 1000;
  }

  private stopScheduler(): void {
    this.schedulerRunning = false;
    if (this.schedulerHandle) {
      clearInterval(this.schedulerHandle);
      this.schedulerHandle = undefined;
    }
  }

  private async heartbeat(): Promise<void> {
    if (this.stopping) {
      return;
    }
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const now = Date.now() / 1000;
    const info = this.processInfo();
    const rssKb = Math.round(process.memoryUsage().rss / 1024);

    const workKey = `${this.identity}:work`;
    const workEntries: Record<string, string> = {};
    for (const [key, value] of this.workState.entries()) {
      workEntries[key] = dumpJson({
        queue: value.queue,
        payload: value.payload,
        run_at: Math.floor(value.runAt / 1000),
      });
    }

    try {
      await this.cleanupProcesses(redis);
      const rttUs = await this.checkRtt(redis);
      const pipeline = redis.multi();
      pipeline.unlink(workKey);
      if (Object.keys(workEntries).length > 0) {
        pipeline.hSet(workKey, workEntries);
        pipeline.expire(workKey, 60);
      }
      pipeline.sAdd("processes", [this.identity]);
      pipeline.hSet(this.identity, {
        info: dumpJson(info),
        busy: String(this.workState.size),
        beat: String(now),
        quiet: String(this.quieting),
        rtt_us: String(rttUs),
        rss: String(rssKb),
      });
      pipeline.expire(this.identity, 60);
      pipeline.rPop(`${this.identity}-signals`);
      const result = await pipeline.exec();

      // Check for remote signals (last result from pipeline)
      const signalResult = result?.[result.length - 1];
      if (signalResult && typeof signalResult === "string") {
        await this.handleSignal(signalResult);
      }

      // Fire heartbeat/beat events (not oneshot - they fire repeatedly)
      await this.config.fireEvent("heartbeat", { oneshot: false });
      await this.config.fireEvent("beat", { oneshot: false });
    } catch (error) {
      this.config.logger.error(
        () =>
          `heartbeat: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  private async handleSignal(signal: string): Promise<void> {
    this.config.logger.info(() => `Received remote signal: ${signal}`);
    switch (signal) {
      case "TSTP":
        await this.quiet();
        break;
      case "TERM":
        // Trigger stop - this will be picked up by the main loop
        this.stopping = true;
        this.quieting = true;
        await this.config.fireEvent("shutdown", { reverse: true });
        break;
      case "TTIN":
        this.dumpWorkState();
        break;
      default:
        this.config.logger.warn(() => `Unknown signal: ${signal}`);
    }
  }

  private dumpWorkState(): void {
    const snapshot = this.snapshotWork();
    if (snapshot.length === 0) {
      this.config.logger.info(() => "No active workers");
      return;
    }
    this.config.logger.info(() => `Active workers: ${snapshot.length}`);
    for (const work of snapshot) {
      this.config.logger.info(
        () =>
          `  ${work.workerId}: queue=${work.queue} ` +
          `jid=${work.payload?.jid ?? "unknown"} ` +
          `class=${work.payload?.class ?? "unknown"} ` +
          `elapsed=${work.elapsed.toFixed(1)}s`
      );
    }
  }

  private async clearHeartbeat(): Promise<void> {
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    try {
      const pipeline = redis.multi();
      pipeline.sRem("processes", [this.identity]);
      pipeline.unlink(`${this.identity}:work`);
      pipeline.unlink(this.identity);
      await pipeline.exec();
    } catch {
      // best effort
    }
  }

  private processInfo(): Record<string, unknown> {
    return {
      hostname: hostname(),
      started_at: this.startedAt,
      pid: process.pid,
      tag: this.config.tag,
      concurrency: this.config.concurrency,
      queues: this.config.queueNames(),
      labels: this.config.labels,
      identity: this.identity,
      embedded: false,
    };
  }

  private async checkRtt(
    redis: Awaited<ReturnType<Config["getRedisClient"]>>
  ): Promise<number> {
    const start = process.hrtime.bigint();
    await redis.ping();
    const end = process.hrtime.bigint();
    const rtt = Number((end - start) / 1000n);
    this.recordRtt(rtt);
    return rtt;
  }

  private recordRtt(rtt: number): void {
    const MAX_READINGS = 5;
    const WARNING_LEVEL = 50_000;
    this.rttReadings.push(rtt);
    if (this.rttReadings.length > MAX_READINGS) {
      this.rttReadings.shift();
    }
    if (
      this.rttReadings.length === MAX_READINGS &&
      this.rttReadings.every((value) => value > WARNING_LEVEL)
    ) {
      this.config.logger.warn(
        () =>
          `Redis RTT is high (${this.rttReadings.join(", ")} us). ` +
          "Consider lowering concurrency or colocating Redis."
      );
      this.rttReadings.length = 0;
    }
  }

  private async waitForDrain(deadline: number): Promise<void> {
    while (this.inProgress.size > 0 && Date.now() < deadline) {
      await sleep(50);
    }
  }

  private async waitForWorkers(deadline: number): Promise<void> {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      return;
    }
    await Promise.race([Promise.all(this.workers), sleep(remaining)]);
  }

  private async requeueInProgress(): Promise<void> {
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const grouped = new Map<string, string[]>();
    for (const entry of this.inProgress.values()) {
      const list = grouped.get(entry.queue) ?? [];
      list.push(entry.payload);
      grouped.set(entry.queue, list);
    }
    if (grouped.size === 0) {
      return;
    }
    const pipeline = redis.multi();
    for (const [queue, payloads] of grouped.entries()) {
      pipeline.rPush(`queue:${queue}`, payloads);
    }
    await pipeline.exec();
  }

  private async sendRawToMorgue(payload: string): Promise<void> {
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const now = Date.now() / 1000;
    const cutoff = now - this.config.deadTimeoutInSeconds;
    const pipeline = redis.multi();
    pipeline.zAdd("dead", [{ score: now, value: payload }]);
    pipeline.zRemRangeByScore("dead", 0, cutoff);
    pipeline.zRemRangeByRank("dead", 0, -this.config.deadMaxJobs);
    await pipeline.exec();
  }

  private async runWithProfiler(
    payload: JobPayload,
    fn: () => Promise<void>
  ): Promise<void> {
    if (payload.profile && this.config.profiler) {
      await this.config.profiler(payload, fn);
      return;
    }
    await fn();
  }

  private async cleanupProcesses(
    redis: Awaited<ReturnType<Config["getRedisClient"]>>
  ): Promise<void> {
    const now = Date.now();
    if (now - this.lastCleanupAt < 60_000) {
      return;
    }
    const lock = await redis.set("process_cleanup", "1", {
      NX: true,
      EX: 60,
    });
    if (lock !== "OK") {
      return;
    }
    this.lastCleanupAt = now;
    const processes = await redis.sMembers("processes");
    if (processes.length === 0) {
      return;
    }
    const pipeline = redis.multi();
    for (const key of processes) {
      pipeline.hGet(key, "info");
    }
    const result = await pipeline.exec();
    const toPrune = processes.filter((_, index) => !result?.[index]);
    if (toPrune.length > 0) {
      await redis.sRem("processes", toPrune);
    }
  }

  private startHeartbeat(): void {
    const intervalMs = this.config.heartbeatInterval * 1000;
    this.heartbeatHandle = setInterval(() => {
      this.heartbeat().catch(() => undefined);
    }, intervalMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatHandle) {
      clearInterval(this.heartbeatHandle);
      this.heartbeatHandle = undefined;
    }
  }

  private async enqueueScheduled(): Promise<void> {
    if (this.stopping) {
      return;
    }
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const nowMs = Date.now();
    const nowSeconds = nowMs / 1000;
    const sets = ["schedule", "retry"];

    for (const set of sets) {
      while (!this.stopping) {
        // Atomic operation: pop from sorted set AND push to queue in one step
        // This eliminates data loss if process crashes between operations
        const job = (await redis.sendCommand([
          "EVAL",
          LUA_RELIABLE_ENQUEUE,
          "1",
          set,
          String(nowSeconds),
          String(nowMs),
        ])) as string | null;

        if (!job) {
          break;
        }
        // Job was atomically moved to its queue - no additional action needed
      }
    }
  }

  private async workLoop(index: number): Promise<void> {
    while (true) {
      // Check stopping BEFORE fetching - don't start new work
      if (this.stopping) {
        break;
      }

      if (this.quieting) {
        await sleep(50);
        continue;
      }

      const unit = await this.fetchWork(index);
      if (!unit) {
        continue;
      }

      // Process the job - even if stopping became true during fetchWork
      // Once we've popped a job from Redis, we MUST process it
      const workerId = `worker-${index}`;
      this.inProgress.set(workerId, {
        queue: unit.queue,
        payload: unit.payload,
      });
      this.workState.set(workerId, {
        queue: unit.queue,
        payload: unit.payload,
        runAt: Date.now(),
      });
      this.heartbeat().catch(() => undefined);
      try {
        await this.processJob(unit.queue, unit.payload);
      } finally {
        this.inProgress.delete(workerId);
        this.workState.delete(workerId);
        this.heartbeat().catch(() => undefined);
      }
    }
  }

  private async fetchWork(index: number): Promise<{
    queue: string;
    payload: string;
  } | null> {
    const queueKeys = this.queueStrategy.queueKeys();
    if (queueKeys.length === 0) {
      await sleep(FETCH_TIMEOUT_SECONDS * 1000);
      return null;
    }

    const redis =
      this.workerRedis[index] ?? (await this.config.getRedisClient());
    const result = (await redis.sendCommand([
      "BRPOP",
      ...queueKeys,
      String(FETCH_TIMEOUT_SECONDS),
    ])) as [string, string] | null;

    if (!result) {
      return null;
    }

    const [queueKey, job] = result;
    const queue = queueKey.startsWith("queue:") ? queueKey.slice(6) : queueKey;

    return { queue, payload: job };
  }

  private async processJob(queue: string, payloadRaw: string): Promise<void> {
    let payload: JobPayload;
    try {
      payload = loadJson(payloadRaw) as JobPayload;
    } catch (error) {
      await this.sendRawToMorgue(payloadRaw);
      await this.updateStat("failed");
      this.config.logger.error(
        () => `Invalid JSON for job on ${queue}: ${String(error)}`
      );
      const err = error instanceof Error ? error : new Error(String(error));
      await this.runErrorHandlers(
        err,
        this.buildErrorContext(
          "Invalid JSON for job",
          undefined,
          queue,
          payloadRaw
        )
      );
      return;
    }

    const className = String(payload.class);
    const klass = resolveJob(className) as JobConstructor | undefined;

    if (!klass) {
      this.config.logger.error(
        () => `Unknown job class ${className} for ${queue}`
      );
      await this.updateStat("failed");
      await this.runErrorHandlers(
        new Error(`Unknown job class ${className}`),
        this.buildErrorContext("Unknown job class", payload, queue)
      );
      return;
    }

    const job = new klass();
    job.jid = payload.jid;
    job._context = { stopping: () => this.stopping };

    try {
      let executed = false;
      await this.jobLogger.prepare(payload, async () => {
        await this.jobLogger.call(payload, queue, async () => {
          await this.runWithProfiler(payload, async () => {
            await this.config.serverMiddleware.invoke(
              job,
              payload,
              queue,
              async () => {
                executed = true;
                await job.perform(...(payload.args ?? []));
              }
            );
          });
        });
      });
      if (executed) {
        await this.updateStat("processed");
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      if (err instanceof JobSkipError) {
        return;
      }
      await this.handleFailure(queue, payload, klass, err);
    }
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: retry logic requires handling many edge cases
  private async handleFailure(
    queue: string,
    payload: JobPayload,
    klass: JobConstructor,
    error: Error
  ): Promise<void> {
    const redis = await this.config.getRedisClient();
    const className = String(payload.class);
    const message = this.safeErrorMessage(error);
    this.config.logger.error(
      () => `Job ${className} failed on ${queue}: ${message}`
    );

    await this.updateStat("failed");

    const retryOption =
      payload.retry !== undefined
        ? payload.retry
        : klass.getSidekiqOptions().retry;

    const retryDisabled =
      retryOption === false ||
      retryOption === null ||
      retryOption === undefined;
    if (retryDisabled) {
      await this.runDeathHandlers(payload, error);
      await this.runErrorHandlers(
        error,
        this.buildErrorContext("Job raised exception", payload, queue)
      );
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

    if (payload.backtrace) {
      const rawLines = extractBacktrace(error);
      const cleaned = this.config.backtraceCleaner(rawLines);
      const limit =
        payload.backtrace === true ? cleaned.length : Number(payload.backtrace);
      const lines = cleaned.slice(0, Math.max(limit, 0));
      payload.error_backtrace = compressBacktrace(lines);
    }

    const retryFor = payload.retry_for;
    if (typeof retryFor === "number" && payload.failed_at !== undefined) {
      const deadline = payload.failed_at / 1000 + retryFor;
      if (deadline < nowSeconds) {
        await this.retriesExhausted(
          payload,
          error,
          klass.sidekiqRetriesExhausted
        );
        await this.runErrorHandlers(
          error,
          this.buildErrorContext("Job raised exception", payload, queue)
        );
        return;
      }
    } else if (payload.retry_count >= maxRetries) {
      await this.retriesExhausted(
        payload,
        error,
        klass.sidekiqRetriesExhausted
      );
      await this.runErrorHandlers(
        error,
        this.buildErrorContext("Job raised exception", payload, queue)
      );
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
      await this.retriesExhausted(
        payload,
        error,
        klass.sidekiqRetriesExhausted
      );
      await this.runErrorHandlers(
        error,
        this.buildErrorContext("Job raised exception", payload, queue)
      );
      return;
    }

    const delaySeconds =
      typeof delayResult === "number"
        ? delayResult
        : payload.retry_count ** 4 + 15;
    const jitter = Math.random() * 10 * (payload.retry_count + 1);
    const retryAt = nowSeconds + delaySeconds + jitter;

    await redis.zAdd("retry", [{ score: retryAt, value: dumpJson(payload) }]);
    await this.runErrorHandlers(
      error,
      this.buildErrorContext("Job raised exception", payload, queue)
    );
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
    let handlerResult: "discard" | undefined;
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

  private async runDeathHandlers(
    payload: JobPayload,
    error: Error
  ): Promise<void> {
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
    error: Error,
    context: Record<string, unknown>
  ): Promise<void> {
    if (this.config.errorHandlers.length === 0) {
      return;
    }
    for (const handler of this.config.errorHandlers) {
      try {
        await handler(error, context, this.config);
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

  private buildErrorContext(
    message: string,
    payload?: JobPayload,
    queue?: string,
    jobstr?: string
  ): Record<string, unknown> {
    const context: Record<string, unknown> = { context: message };
    if (payload) {
      context.job = payload;
    }
    if (queue) {
      context.queue = queue;
    }
    if (jobstr) {
      context.jobstr = jobstr;
    }
    return context;
  }

  private safeErrorMessage(error: Error): string {
    try {
      return String(error.message ?? "Unknown error");
    } catch {
      return "!!! ERROR MESSAGE THREW AN ERROR !!!";
    }
  }

  private async updateStat(
    stat: "processed" | "failed",
    count = 1
  ): Promise<void> {
    const redis = this.baseRedis ?? (await this.config.getRedisClient());
    const date = new Date().toISOString().slice(0, 10);
    const key = `stat:${stat}`;
    const dailyKey = `${key}:${date}`;
    const pipeline = redis.multi();
    pipeline.incrBy(key, count);
    pipeline.incrBy(dailyKey, count);
    pipeline.expire(dailyKey, STATS_TTL_SECONDS);
    await pipeline.exec();
  }
}
