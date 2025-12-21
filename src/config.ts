import { createClient } from "redis";
import type { RedisClientOptions } from "redis";
import { createLogger, type Logger } from "./logger.js";
import type { RedisClient } from "./redis.js";
import { MiddlewareChain } from "./middleware.js";
import type { JobPayload } from "./types.js";
import { DefaultJobLogger } from "./job_logger.js";
import type { JobLogger } from "./types.js";
import type {
  ConfigOptions,
  LifecycleEvents,
  ErrorHandler,
  DeathHandler,
  StrictArgsMode,
} from "./types.js";

const DEFAULT_LIFECYCLE_EVENTS: LifecycleEvents = {
  startup: [],
  quiet: [],
  shutdown: [],
  exit: [],
  heartbeat: [],
  beat: [],
};

export class Config {
  redis: RedisClientOptions;
  concurrency: number;
  queues: string[] | Array<[string, number]>;
  timeout: number;
  pollIntervalAverage: number | null;
  averageScheduledPollInterval: number;
  heartbeatInterval: number;
  tag: string;
  labels: string[];
  maxRetries: number;
  deadMaxJobs: number;
  deadTimeoutInSeconds: number;
  backtraceCleaner: (backtrace: string[]) => string[];
  skipDefaultJobLogging: boolean;
  loggedJobAttributes: string[];
  profiler?: (payload: JobPayload, fn: () => Promise<void>) => Promise<void>;
  strictArgs: StrictArgsMode;
  errorHandlers: ErrorHandler[];
  deathHandlers: DeathHandler[];
  lifecycleEvents: LifecycleEvents;
  logger: Logger;
  redisIdleTimeout: number | null;
  jobLogger: JobLogger;
  clientMiddleware: MiddlewareChain<
    [string | unknown, JobPayload, string, RedisClient],
    JobPayload | false | null | undefined
  >;
  serverMiddleware: MiddlewareChain<
    [unknown, JobPayload, string],
    unknown
  >;
  private redisClient?: RedisClient;

  constructor(options: ConfigOptions = {}) {
    this.redis = options.redis ?? {
      url: process.env.REDIS_URL ?? "redis://localhost:6379/0",
    };
    this.concurrency = options.concurrency ?? 5;
    this.queues = options.queues ?? ["default"];
    this.timeout = options.timeout ?? 25;
    this.pollIntervalAverage = options.pollIntervalAverage ?? null;
    this.averageScheduledPollInterval =
      options.averageScheduledPollInterval ?? 5;
    this.heartbeatInterval = options.heartbeatInterval ?? 10;
    this.tag = options.tag ?? "";
    this.labels = options.labels ?? [];
    this.maxRetries = options.maxRetries ?? 25;
    this.deadMaxJobs = options.deadMaxJobs ?? 10_000;
    this.deadTimeoutInSeconds =
      options.deadTimeoutInSeconds ?? 180 * 24 * 60 * 60;
    this.backtraceCleaner = options.backtraceCleaner ?? ((backtrace) => backtrace);
    this.skipDefaultJobLogging = options.skipDefaultJobLogging ?? false;
    this.loggedJobAttributes = options.loggedJobAttributes ?? ["tags"];
    this.profiler = options.profiler;
    this.strictArgs = options.strictArgs ?? "raise";
    this.errorHandlers = options.errorHandlers ?? [];
    this.deathHandlers = options.deathHandlers ?? [];
    this.lifecycleEvents = {
      ...DEFAULT_LIFECYCLE_EVENTS,
      ...(options.lifecycleEvents ?? {}),
    };
    this.logger = options.logger ?? createLogger();
    this.redisIdleTimeout = options.redisIdleTimeout ?? null;
    this.jobLogger = options.jobLogger ?? new DefaultJobLogger(this);
    this.clientMiddleware = new MiddlewareChain(this);
    this.serverMiddleware = new MiddlewareChain(this);
  }

  async getRedisClient(): Promise<RedisClient> {
    if (this.redisClient && this.redisClient.isOpen) {
      return this.redisClient;
    }
    const client = createClient(this.redis);
    client.on("error", (error: Error) => {
      this.logger.error(() => `Redis error: ${error.message}`);
    });
    await client.connect();
    this.redisClient = client;
    return client;
  }

  async close(): Promise<void> {
    if (this.redisClient && this.redisClient.isOpen) {
      await this.redisClient.quit();
    }
  }

  queueNames(): string[] {
    const names = new Set<string>();
    for (const entry of this.queues) {
      if (Array.isArray(entry)) {
        names.add(entry[0]);
      } else {
        const [name] = entry.split(",", 1);
        names.add(name);
      }
    }
    return Array.from(names);
  }
}
