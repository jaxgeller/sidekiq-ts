import type { JobOptions, JobPayload, JobSetterOptions, BulkOptions } from "./types.js";
import { Sidekiq } from "./sidekiq.js";
import { Client } from "./client.js";
import { normalizeItem, verifyJson } from "./job_util.js";
import { EmptyQueueError, Queues } from "./testing.js";

const OPTIONS_KEY = Symbol("sidekiqOptions");

export type JobConstructor<TArgs extends unknown[] = unknown[]> = {
  new (): Job<TArgs>;
  name: string;
  sidekiqOptions?: JobOptions;
  sidekiqRetryIn?: RetryInHandler;
  sidekiqRetriesExhausted?: RetriesExhaustedHandler;
  getSidekiqOptions(): JobOptions;
  setSidekiqOptions(options: JobOptions): void;
  queueAs(queue: string): void;
  retryIn(handler: RetryInHandler): void;
  retriesExhausted(handler: RetriesExhaustedHandler): void;
  set(options: JobSetterOptions): JobSetter<TArgs>;
  performAsync(...args: TArgs): Promise<string | null>;
  performIn(interval: number, ...args: TArgs): Promise<string | null>;
  performAt(timestamp: number, ...args: TArgs): Promise<string | null>;
  performBulk(args: TArgs[][], options?: BulkOptions): Promise<(string | null)[]>;
  performInline(...args: TArgs): Promise<boolean | null>;
  jobs(): JobPayload[];
  clear(): void;
  drain(): Promise<void>;
  performOne(): Promise<void>;
  processJob(payload: JobPayload): Promise<void>;
  clientPush(item: JobPayload): Promise<string | null>;
};

export abstract class Job<TArgs extends unknown[] = unknown[]> {
  jid?: string;
  _context?: { stopping: () => boolean };

  static getSidekiqOptions(this: JobConstructor): JobOptions {
    const base = Sidekiq.defaultJobOptions();
    const explicit = this.sidekiqOptions ?? {};
    const stored = (this as unknown as Record<symbol, JobOptions>)[OPTIONS_KEY] ?? {};
    return {
      ...base,
      ...explicit,
      ...stored,
    };
  }

  static setSidekiqOptions(this: JobConstructor, options: JobOptions): void {
    const stored = (this as unknown as Record<symbol, JobOptions>)[OPTIONS_KEY] ?? {};
    (this as unknown as Record<symbol, JobOptions>)[OPTIONS_KEY] = {
      ...stored,
      ...options,
    };
  }

  static queueAs(this: JobConstructor, queue: string): void {
    this.setSidekiqOptions({ queue });
  }

  static retryIn(this: JobConstructor, handler: RetryInHandler): void {
    this.sidekiqRetryIn = handler;
  }

  static retriesExhausted(this: JobConstructor, handler: RetriesExhaustedHandler): void {
    this.sidekiqRetriesExhausted = handler;
  }

  static set<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    options: JobSetterOptions
  ): JobSetter<TArgs> {
    return new JobSetter(this, options);
  }

  static performAsync<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    ...args: TArgs
  ): Promise<string | null> {
    return new JobSetter(this, {}).performAsync(...args);
  }

  static performIn<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    interval: number,
    ...args: TArgs
  ): Promise<string | null> {
    return new JobSetter(this, {}).performIn(interval, ...args);
  }

  static performAt<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    timestamp: number,
    ...args: TArgs
  ): Promise<string | null> {
    return new JobSetter(this, {}).performAt(timestamp, ...args);
  }

  static performBulk<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    args: TArgs[][],
    options?: BulkOptions
  ): Promise<(string | null)[]> {
    return new JobSetter(this, {}).performBulk(args, options);
  }

  static performInline<TArgs extends unknown[]>(
    this: JobConstructor<TArgs>,
    ...args: TArgs
  ): Promise<boolean | null> {
    return new JobSetter(this, {}).performInline(...args);
  }

  static jobs(this: JobConstructor | typeof Job): JobPayload[] {
    if (this === Job) {
      return Queues.jobs();
    }
    return Queues.jobsForClass(this.name);
  }

  static clear(this: JobConstructor): void {
    const queue = this.getSidekiqOptions().queue ?? "default";
    Queues.clearFor(queue, this.name);
  }

  static async drain(this: JobConstructor): Promise<void> {
    while (this.jobs().length > 0) {
      const nextJob = this.jobs()[0];
      Queues.deleteFor(nextJob.jid ?? "", nextJob.queue ?? "default", this.name);
      await this.processJob(nextJob);
    }
  }

  static async performOne(this: JobConstructor): Promise<void> {
    if (this.jobs().length === 0) {
      throw new EmptyQueueError();
    }
    const nextJob = this.jobs()[0];
    Queues.deleteFor(nextJob.jid ?? "", nextJob.queue ?? "default", this.name);
    await this.processJob(nextJob);
  }

  static async processJob(this: JobConstructor, payload: JobPayload): Promise<void> {
    const instance = new this();
    instance.jid = payload.jid;
    instance._context = { stopping: () => false };
    await Sidekiq.defaultConfiguration.serverMiddleware.invoke(
      instance,
      payload,
      payload.queue ?? "default",
      async () => {
        await instance.perform(...(payload.args ?? []));
      }
    );
  }

  static clearAll(): void {
    Queues.clearAll();
  }

  static async drainAll(): Promise<void> {
    while (Queues.jobs().length > 0) {
      const jobs = Queues.jobs();
      const classes = Array.from(new Set(jobs.map((job) => String(job.class))));
      for (const className of classes) {
        const klass = Sidekiq.registeredJobClass(className);
        if (!klass) {
          continue;
        }
        await (klass as JobConstructor).drain();
      }
    }
  }

  static async clientPush(this: JobConstructor, item: JobPayload): Promise<string | null> {
    const config = Sidekiq.defaultConfiguration;
    const normalized = normalizeItem(item, Sidekiq.defaultJobOptions());
    verifyJson(normalized.args, config.strictArgs);
    const client = new Client({ config });
    return client.push(normalized);
  }

  logger() {
    return Sidekiq.logger();
  }

  interrupted(): boolean {
    return this._context?.stopping() ?? false;
  }

  abstract perform(...args: TArgs): Promise<void> | void;
}

export type RetryInHandler = (
  count: number,
  error: Error,
  payload: JobPayload
) => number | "discard" | "kill" | "default";

export type RetriesExhaustedHandler = (
  payload: JobPayload,
  error: Error
) => "discard" | void;

export class JobSetter<TArgs extends unknown[] = unknown[]> {
  private klass: JobConstructor<TArgs>;
  private options: JobSetterOptions;

  constructor(klass: JobConstructor<TArgs>, options: JobSetterOptions) {
    this.klass = klass;
    this.options = { ...options };

    const interval = this.options.waitUntil ?? this.options.wait;
    if (interval !== undefined) {
      this.at(interval);
      delete this.options.wait;
      delete this.options.waitUntil;
    }
  }

  set(options: JobSetterOptions): JobSetter<TArgs> {
    const merged = { ...options };
    const interval = merged.waitUntil ?? merged.wait;
    if (interval !== undefined) {
      this.at(interval);
      delete merged.wait;
      delete merged.waitUntil;
    }
    this.options = {
      ...this.options,
      ...merged,
    };
    return this;
  }

  async performAsync(...args: TArgs): Promise<string | null> {
    if (this.options.sync === true) {
      return (await this.performInline(...args)) ? "inline" : null;
    }
    const payload: JobPayload = {
      ...this.options,
      class: this.klass,
      args,
    };
    return this.klass.clientPush(payload);
  }

  async performInline(...args: TArgs): Promise<boolean | null> {
    const payload: JobPayload = {
      ...this.options,
      class: this.klass,
      args,
    };
    const normalized = normalizeItem(payload, Sidekiq.defaultJobOptions());
    verifyJson(normalized.args, Sidekiq.defaultConfiguration.strictArgs);

    const instance = new this.klass();
    instance.jid = normalized.jid;
    await instance.perform(...(normalized.args as TArgs));
    return true;
  }

  performBulk(args: TArgs[][], options?: BulkOptions): Promise<(string | null)[]> {
    const payload: JobPayload = {
      ...this.options,
      class: this.klass,
      args: [],
    };
    const client = new Client({ config: Sidekiq.defaultConfiguration });
    return client.pushBulk({
      ...payload,
      args,
      batch_size: options?.batchSize,
      spread_interval: options?.spreadInterval,
      at: options?.at,
    });
  }

  performIn(interval: number, ...args: TArgs): Promise<string | null> {
    return this.at(interval).performAsync(...args);
  }

  performAt(timestamp: number, ...args: TArgs): Promise<string | null> {
    return this.at(timestamp).performAsync(...args);
  }

  private at(interval: number): JobSetter<TArgs> {
    const int = Number(interval);
    const now = Date.now() / 1000;
    const ts = int < 1_000_000_000 ? now + int : int;
    if (ts > now) {
      this.options.at = ts;
    } else {
      delete this.options.at;
    }
    return this;
  }
}
