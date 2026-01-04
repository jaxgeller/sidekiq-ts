import { createShutdownHandler } from "./cli-helpers.js";
import { Config } from "./config.js";
import { dumpJson, loadJson } from "./json.js";
import type { MiddlewareConstructor } from "./middleware.js";
import type { RedisClient } from "./redis.js";
import {
  type RegisteredJobClass,
  registeredJob,
  registerJob,
} from "./registry.js";
import type { Runner } from "./runner.js";
import type {
  ConfigOptions,
  DeathHandler,
  ErrorHandler,
  JobOptions,
  JobPayload,
  LifecycleEvents,
  LifecycleHandler,
  SidekiqEventHandler,
  SidekiqEventName,
  StrictArgsMode,
} from "./types.js";

interface RunOptions {
  config?: Config;
  /** Auto-register SIGINT/SIGTERM handlers for graceful shutdown */
  signals?: boolean;
}

// biome-ignore lint/complexity/noStaticOnlyClass: Sidekiq class provides namespace for configuration and utilities
export class Sidekiq {
  static readonly NAME = "Sidekiq";
  static readonly LICENSE =
    "See LICENSE and the LGPL-3.0 for licensing details.";

  private static config = new Config();
  private static serverConfigQueue: ConfigOptions[] = [];
  private static isServerMode = false;
  private static defaultJobOpts: JobOptions = {
    retry: true,
    queue: "default",
  };

  static get defaultConfiguration(): Config {
    return this.config;
  }

  static configureServer(options: ConfigOptions): void {
    this.serverConfigQueue.push(options);
    if (this.isServerMode) {
      this.applyConfigOptions(options);
    }
  }

  static configureClient(options: ConfigOptions): void {
    if (!this.isServerMode) {
      this.applyConfigOptions(options);
    }
  }

  static on<TEvent extends SidekiqEventName>(
    event: TEvent,
    handler: SidekiqEventHandler<TEvent>
  ): void {
    if (event === "error") {
      this.config.errorHandlers.push(handler as ErrorHandler);
      return;
    }
    if (event === "death") {
      this.config.deathHandlers.push(handler as DeathHandler);
      return;
    }
    this.config.lifecycleEvents[event as keyof LifecycleEvents].push(
      handler as LifecycleHandler
    );
  }

  static useClientMiddleware(
    klass: MiddlewareConstructor<
      [string | unknown, JobPayload, string, RedisClient],
      JobPayload | false | null | undefined
    >,
    ...args: unknown[]
  ): void {
    this.config.clientMiddleware.use(klass, ...args);
  }

  static useServerMiddleware(
    klass: MiddlewareConstructor<[unknown, JobPayload, string], unknown>,
    ...args: unknown[]
  ): void {
    this.config.serverMiddleware.use(klass, ...args);
  }

  static markServerMode(): void {
    this.isServerMode = true;
    for (const options of this.serverConfigQueue) {
      this.applyConfigOptions(options);
    }
  }

  static defaultJobOptions(): JobOptions {
    return { ...this.defaultJobOpts };
  }

  static setDefaultJobOptions(options: JobOptions): void {
    this.defaultJobOpts = {
      ...this.defaultJobOpts,
      ...options,
    };
  }

  static strictArgs(mode: StrictArgsMode = "raise"): void {
    this.config.strictArgs = mode;
  }

  static logger() {
    return this.config.logger;
  }

  private static applyConfigOptions(options: ConfigOptions): void {
    this.applyScalarOptions(options);
    this.applyHandlerOptions(options);
    this.applyLifecycleOptions(options.lifecycleEvents);
  }

  private static applyScalarOptions(options: ConfigOptions): void {
    this.setIfDefined(options.redis, (value) => {
      this.config.redis = value;
    });
    this.setIfDefined(options.concurrency, (value) => {
      this.config.concurrency = value;
    });
    this.setIfDefined(options.queues, (value) => {
      this.config.queues = value;
    });
    this.setIfDefined(options.timeout, (value) => {
      this.config.timeout = value;
    });
    this.setIfDefined(options.pollIntervalAverage, (value) => {
      this.config.pollIntervalAverage = value;
    });
    this.setIfDefined(options.averageScheduledPollInterval, (value) => {
      this.config.averageScheduledPollInterval = value;
    });
    this.setIfDefined(options.heartbeatInterval, (value) => {
      this.config.heartbeatInterval = value;
    });
    this.setIfDefined(options.tag, (value) => {
      this.config.tag = value;
    });
    this.setIfDefined(options.labels, (value) => {
      this.config.labels = value;
    });
    this.setIfDefined(options.maxRetries, (value) => {
      this.config.maxRetries = value;
    });
    this.setIfDefined(options.deadMaxJobs, (value) => {
      this.config.deadMaxJobs = value;
    });
    this.setIfDefined(options.deadTimeoutInSeconds, (value) => {
      this.config.deadTimeoutInSeconds = value;
    });
    this.setIfDefined(options.backtraceCleaner, (value) => {
      this.config.backtraceCleaner = value;
    });
    this.setIfDefined(options.maxIterationRuntime, (value) => {
      this.config.maxIterationRuntime = value;
    });
    this.setIfDefined(options.skipDefaultJobLogging, (value) => {
      this.config.skipDefaultJobLogging = value;
    });
    this.setIfDefined(options.loggedJobAttributes, (value) => {
      this.config.loggedJobAttributes = value;
    });
    this.setIfDefined(options.profiler, (value) => {
      this.config.profiler = value;
    });
    this.setIfDefined(options.jobLogger, (value) => {
      this.config.jobLogger = value;
    });
    this.setIfDefined(options.strictArgs, (value) => {
      this.config.strictArgs = value;
    });
    this.setIfDefined(options.logger, (value) => {
      this.config.logger = value;
    });
    this.setIfDefined(options.redisIdleTimeout, (value) => {
      this.config.redisIdleTimeout = value;
    });
    this.setIfDefined(options.leaderElection, (value) => {
      this.config.leaderElection = value;
    });
  }

  private static applyHandlerOptions(options: ConfigOptions): void {
    this.setIfDefined(options.errorHandlers, (value) => {
      this.config.errorHandlers.push(...value);
    });
    this.setIfDefined(options.deathHandlers, (value) => {
      this.config.deathHandlers.push(...value);
    });
  }

  private static applyLifecycleOptions(
    events?: Partial<LifecycleEvents>
  ): void {
    if (!events) {
      return;
    }
    for (const [event, handlers] of Object.entries(events)) {
      if (handlers !== undefined) {
        this.config.lifecycleEvents[event as keyof LifecycleEvents].push(
          ...handlers
        );
      }
    }
  }

  private static setIfDefined<T>(
    value: T | undefined,
    apply: (value: T) => void
  ): void {
    if (value !== undefined) {
      apply(value);
    }
  }

  static dumpJson(value: unknown): string {
    return dumpJson(value);
  }

  static loadJson(value: string): unknown {
    return loadJson(value);
  }

  static async redis<T>(
    fn: (client: Awaited<ReturnType<Config["getRedisClient"]>>) => Promise<T>
  ): Promise<T> {
    const client = await this.config.getRedisClient();
    return fn(client);
  }

  static registerJob(klass: RegisteredJobClass): void {
    registerJob(klass);
  }

  static registeredJobClass(name: string): RegisteredJobClass | undefined {
    return registeredJob(name);
  }

  static async run({ config, signals }: RunOptions = {}): Promise<Runner> {
    if (config) {
      this.config = config;
    }
    this.markServerMode();
    const { Runner } = await import("./runner.js");
    const runner = new Runner(this.config);
    await runner.start();

    if (signals) {
      this.registerSignalHandlers(runner);
    }

    return runner;
  }

  private static registerSignalHandlers(runner: Runner): void {
    const shutdown = createShutdownHandler({
      logger: this.config.logger,
      stop: () => runner.stop(),
      exit: (code) => process.exit(code),
    });

    const registerSignal = (
      signal: string,
      handler: () => void | Promise<void>
    ) => {
      try {
        process.on(signal as NodeJS.Signals, () => {
          Promise.resolve(handler()).catch(() => undefined);
        });
      } catch {
        // Signal not supported on this platform
      }
    };

    registerSignal("SIGINT", () => shutdown("SIGINT"));
    registerSignal("SIGTERM", () => shutdown("SIGTERM"));
    registerSignal("SIGTSTP", () => runner.quiet());
  }
}
