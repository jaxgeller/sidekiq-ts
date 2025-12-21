import { Config } from "./config.js";
import { dumpJson, loadJson } from "./json.js";
import { registerJob, type RegisteredJobClass } from "./registry.js";
import type { JobOptions, StrictArgsMode } from "./types.js";

export class Sidekiq {
  static readonly NAME = "Sidekiq";
  static readonly LICENSE = "See LICENSE and the LGPL-3.0 for licensing details.";

  private static config = new Config();
  private static configBlocks: Array<(config: Config) => void> = [];
  private static isServerMode = false;
  private static defaultJobOpts: JobOptions = {
    retry: true,
    queue: "default",
  };

  static get defaultConfiguration(): Config {
    return this.config;
  }

  static configureServer(block: (config: Config) => void): void {
    this.configBlocks.push(block);
    if (this.isServerMode) {
      block(this.config);
    }
  }

  static configureClient(block: (config: Config) => void): void {
    if (!this.isServerMode) {
      block(this.config);
    }
  }

  static markServerMode(): void {
    this.isServerMode = true;
    for (const block of this.configBlocks) {
      block(this.config);
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

  static dumpJson(value: unknown): string {
    return dumpJson(value);
  }

  static loadJson(value: string): unknown {
    return loadJson(value);
  }

  static async redis<T>(fn: (client: Awaited<ReturnType<Config["getRedisClient"]>>) => Promise<T>): Promise<T> {
    const client = await this.config.getRedisClient();
    return fn(client);
  }

  static registerJob(klass: RegisteredJobClass): void {
    registerJob(klass);
  }

  static async run({ config }: { config?: Config } = {}) {
    if (config) {
      this.config = config;
    }
    this.markServerMode();
    const { Runner } = await import("./runner.js");
    const runner = new Runner(this.config);
    await runner.start();
    return runner;
  }
}
