import type { RedisClientOptions } from "redis";
import type { Config } from "./config.js";
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
  leader: LifecycleHandler[];
  follower: LifecycleHandler[];
}

export type ErrorHandler = (
  error: Error,
  context: Record<string, unknown>,
  config?: Config
) => void | Promise<void>;

export type DeathHandler = (
  payload: JobPayload,
  error: Error
) => void | Promise<void>;

export type SidekiqEventName = keyof LifecycleEvents | "error" | "death";

export type SidekiqEventHandler<TEvent extends SidekiqEventName> =
  TEvent extends "error"
    ? ErrorHandler
    : TEvent extends "death"
      ? DeathHandler
      : LifecycleHandler;

export interface LeaderElectionConfig {
  /** Leader refresh interval in milliseconds (default: 15000) */
  refreshInterval?: number;
  /** Follower check interval in milliseconds (default: 60000) */
  checkInterval?: number;
  /** Leader key TTL in seconds (default: 20) */
  ttl?: number;
}

export interface ConfigOptions {
  redis?: RedisClientOptions;
  concurrency?: number;
  queues?: string[] | [string, number][];
  timeout?: number;
  pollIntervalAverage?: number | null;
  averageScheduledPollInterval?: number;
  heartbeatInterval?: number;
  tag?: string;
  labels?: string[];
  maxRetries?: number;
  deadMaxJobs?: number;
  deadTimeoutInSeconds?: number;
  backtraceCleaner?: (backtrace: string[]) => string[];
  maxIterationRuntime?: number | null;
  skipDefaultJobLogging?: boolean;
  loggedJobAttributes?: string[];
  profiler?: (payload: JobPayload, fn: () => Promise<void>) => Promise<void>;
  jobLogger?: JobLogger;
  strictArgs?: StrictArgsMode;
  errorHandlers?: ErrorHandler[];
  deathHandlers?: DeathHandler[];
  lifecycleEvents?: Partial<LifecycleEvents>;
  logger?: Logger;
  redisIdleTimeout?: number | null;
  /** Leader election configuration */
  leaderElection?: LeaderElectionConfig;
  /** Maximum jobs to hold locally when Redis is unavailable (default: 1000, 0 to disable) */
  reliableClientMaxQueue?: number;
}

export type JobRetryOption = boolean | number;

export interface JobOptions {
  queue?: string;
  retry?: JobRetryOption;
  backtrace?: boolean | number;
  retry_for?: number;
  retry_queue?: string;
  dead?: boolean;
  tags?: string[];
  log_level?: string;
  profile?: boolean | string;
  profiler_options?: Record<string, unknown>;
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
  error_message?: string;
  error_class?: string;
  failed_at?: number;
  retried_at?: number;
  retry_count?: number;
  error_backtrace?: string;
  discarded_at?: number;
  [key: string]: unknown;
}

export interface JobLogger {
  prepare<T>(payload: JobPayload, fn: () => Promise<T> | T): Promise<T> | T;
  call<T>(
    payload: JobPayload,
    queue: string,
    fn: () => Promise<T> | T
  ): Promise<T> | T;
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
