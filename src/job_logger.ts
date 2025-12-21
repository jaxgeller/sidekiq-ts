import type { Config } from "./config.js";
import type { JobPayload, JobLogger } from "./types.js";

export class DefaultJobLogger implements JobLogger {
  private config: Config;
  private skip: boolean;

  constructor(config: Config) {
    this.config = config;
    this.skip = Boolean(this.config.skipDefaultJobLogging);
  }

  async prepare<T>(payload: JobPayload, fn: () => Promise<T> | T): Promise<T> {
    return await fn();
  }

  async call<T>(payload: JobPayload, queue: string, fn: () => Promise<T> | T): Promise<T> {
    const start = process.hrtime.bigint();
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
    const parts = [phase, payload.class, `jid=${payload.jid}`, `queue=${queue}`];
    if (elapsed !== null) {
      parts.push(`elapsed=${elapsed.toFixed(3)}s`);
    }
    const message = parts.filter(Boolean).join(" ");
    logger[level](() => message);
  }

  private resolveLevel(level?: string) {
    if (!level) {
      return null;
    }
    const normalized = level.toLowerCase();
    if (normalized === "debug" || normalized === "info" || normalized === "warn" || normalized === "error") {
      return normalized as "debug" | "info" | "warn" | "error";
    }
    return null;
  }
}
