import { createClient } from "redis";
import type { RedisClientOptions } from "redis";
import { createLogger, type Logger } from "./logger.js";
import type { RedisClient } from "./redis.js";
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
  maxRetries: number;
  deadMaxJobs: number;
  deadTimeoutInSeconds: number;
  strictArgs: StrictArgsMode;
  errorHandlers: ErrorHandler[];
  deathHandlers: DeathHandler[];
  lifecycleEvents: LifecycleEvents;
  logger: Logger;
  redisIdleTimeout: number | null;
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
    this.maxRetries = options.maxRetries ?? 25;
    this.deadMaxJobs = options.deadMaxJobs ?? 10_000;
    this.deadTimeoutInSeconds =
      options.deadTimeoutInSeconds ?? 180 * 24 * 60 * 60;
    this.strictArgs = options.strictArgs ?? "raise";
    this.errorHandlers = options.errorHandlers ?? [];
    this.deathHandlers = options.deathHandlers ?? [];
    this.lifecycleEvents = {
      ...DEFAULT_LIFECYCLE_EVENTS,
      ...(options.lifecycleEvents ?? {}),
    };
    this.logger = options.logger ?? createLogger();
    this.redisIdleTimeout = options.redisIdleTimeout ?? null;
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
}
