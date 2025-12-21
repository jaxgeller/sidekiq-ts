import type { RedisClientOptions } from "redis";
import type { Logger } from "./logger.js";

export type StrictArgsMode = "raise" | "warn" | "none";

export type LifecycleHandler = () => void | Promise<void>;

export interface LifecycleEvents {
  startup: LifecycleHandler[];
  quiet: LifecycleHandler[];
  shutdown: LifecycleHandler[];
  exit: LifecycleHandler[];
  heartbeat: LifecycleHandler[];
  beat: LifecycleHandler[];
}

export type ErrorHandler = (
  error: Error,
  context: Record<string, unknown>
) => void | Promise<void>;

export type DeathHandler = (
  payload: JobPayload,
  error: Error
) => void | Promise<void>;

export interface ConfigOptions {
  redis?: RedisClientOptions;
  concurrency?: number;
  queues?: string[] | Array<[string, number]>;
  timeout?: number;
  pollIntervalAverage?: number | null;
  averageScheduledPollInterval?: number;
  strictArgs?: StrictArgsMode;
  errorHandlers?: ErrorHandler[];
  deathHandlers?: DeathHandler[];
  lifecycleEvents?: Partial<LifecycleEvents>;
  logger?: Logger;
  redisIdleTimeout?: number | null;
}

export type JobRetryOption = boolean | number;

export interface JobOptions {
  queue?: string;
  retry?: JobRetryOption;
  backtrace?: boolean | number;
  retry_for?: number;
  tags?: string[];
}

export interface JobSetterOptions extends JobOptions {
  wait?: number;
  waitUntil?: number;
  at?: number;
  sync?: boolean;
}

export interface BulkOptions {
  batchSize?: number;
  at?: number | number[];
  spreadInterval?: number;
}

export interface JobClassLike {
  name?: string;
  getSidekiqOptions?: () => JobOptions;
}

export interface JobPayload extends JobOptions {
  class: string | JobClassLike;
  args: unknown[];
  queue?: string;
  at?: number;
  jid?: string;
  created_at?: number;
  enqueued_at?: number;
  wrapped?: string | JobClassLike;
  [key: string]: unknown;
}

export interface BulkPayload extends JobOptions {
  class: string | JobClassLike;
  args: unknown[][];
  queue?: string;
  at?: number | number[];
  spread_interval?: number;
  batch_size?: number;
  jid?: string;
  [key: string]: unknown;
}
