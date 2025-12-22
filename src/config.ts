import type { RedisClientOptions } from "redis";
import { createClient } from "redis";
import { Context } from "./context.js";
import { DefaultJobLogger } from "./job-logger.js";
import { createLogger, type Logger } from "./logger.js";
import { MiddlewareChain } from "./middleware.js";
import type { RedisClient } from "./redis.js";
import type {
  ConfigOptions,
  DeathHandler,
  ErrorHandler,
  JobLogger,
  JobPayload,
  LifecycleEvents,
  StrictArgsMode,
} from "./types.js";

const DEFAULT_LIFECYCLE_EVENTS: LifecycleEvents = {
  startup: [],
  quiet: [],
  shutdown: [],
  exit: [],
  heartbeat: [],
  beat: [],
  leader: [],
  follower: [],
};

export class Config {
  redis: RedisClientOptions;
  concurrency: number;
  queues: string[] | [string, number][];
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
  maxIterationRuntime: number | null;
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
  serverMiddleware: MiddlewareChain<[unknown, JobPayload, string], unknown>;
  private redisClient?: RedisClient;

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: config initialization requires many defaults
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
    this.backtraceCleaner =
      options.backtraceCleaner ?? ((backtrace) => backtrace);
    this.maxIterationRuntime = options.maxIterationRuntime ?? null;
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

    if (this.errorHandlers.length === 0) {
      this.errorHandlers.push((error, context) => {
        Context.with(context, () => {
          const message =
            error.stack ?? `${error.name}: ${error.message ?? "Unknown error"}`;
          this.logger.info(() => message);
        });
      });
    }
  }

  async getRedisClient(): Promise<RedisClient> {
    if (this.redisClient?.isOpen) {
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
    if (this.redisClient?.isOpen) {
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

  /**
   * Fire a lifecycle event, calling all registered handlers.
   *
   * @param event - The event name to fire
   * @param options - Options for firing the event
   * @param options.oneshot - If true, clear handlers after firing (default: true)
   * @param options.reverse - If true, call handlers in reverse order
   * @param options.reraise - If true, re-throw any errors from handlers
   */
  async fireEvent(
    event: keyof LifecycleEvents,
    options: { oneshot?: boolean; reverse?: boolean; reraise?: boolean } = {}
  ): Promise<void> {
    const { oneshot = true, reverse = false, reraise = false } = options;

    if (oneshot) {
      this.logger.debug(() => `Firing ${event} event`);
    }

    const handlers = [...this.lifecycleEvents[event]];
    if (reverse) {
      handlers.reverse();
    }

    for (const handler of handlers) {
      try {
        await handler();
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        this.logger.error(
          () =>
            `Exception during Sidekiq lifecycle event ${event}: ${err.message}`
        );
        if (reraise) {
          throw err;
        }
      }
    }

    if (oneshot) {
      this.lifecycleEvents[event] = [];
    }
  }
}
