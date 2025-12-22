import type { Config } from "./config.js";
import { Context } from "./context.js";
import type { JobLogger, JobPayload } from "./types.js";

export class DefaultJobLogger implements JobLogger {
  private config: Config;
  private skip: boolean;

  constructor(config: Config) {
    this.config = config;
    this.skip = Boolean(this.config.skipDefaultJobLogging);
  }

  async prepare<T>(payload: JobPayload, fn: () => Promise<T> | T): Promise<T> {
    const klass = payload.wrapped ?? payload.class;
    const context: Record<string, unknown> = {
      jid: payload.jid,
      class: typeof klass === "string" ? klass : String(klass),
    };

    for (const attr of this.config.loggedJobAttributes) {
      const value = payload[attr];
      if (value !== undefined) {
        context[attr] = value;
      }
    }

    return await Context.with(context, fn);
  }

  async call<T>(
    payload: JobPayload,
    queue: string,
    fn: () => Promise<T> | T
  ): Promise<T> {
    const start = process.hrtime.bigint();
    Context.add("queue", queue);
    this.log("info", payload, queue, "start");
    try {
      const result = await fn();
      this.log("info", payload, queue, "done", start);
      return result;
    } catch (error) {
      this.log("warn", payload, queue, "fail", start);
      throw error;
    }
  }

  private log(
    fallbackLevel: "debug" | "info" | "warn" | "error",
    payload: JobPayload,
    queue: string,
    phase: "start" | "done" | "fail",
    start?: bigint
  ) {
    if (this.skip) {
      return;
    }
    const elapsed = start
      ? Number((process.hrtime.bigint() - start) / 1_000_000n) / 1000
      : null;
    const level = this.resolveLevel(payload.log_level) ?? fallbackLevel;
    const logger = this.config.logger;
    if (elapsed !== null) {
      Context.add("elapsed", Number(elapsed.toFixed(3)));
    }
    logger[level](() => phase);
  }

  private resolveLevel(level?: string) {
    if (!level) {
      return null;
    }
    const normalized = level.toLowerCase();
    if (
      normalized === "debug" ||
      normalized === "info" ||
      normalized === "warn" ||
      normalized === "error"
    ) {
      return normalized as "debug" | "info" | "warn" | "error";
    }
    return null;
  }
}
