import { threadId } from "node:worker_threads";
import { Context } from "./context.js";

export type LogMessage = string | (() => string);
export type LogLevel = "debug" | "info" | "warn" | "error";

export interface Logger {
  debug(message: LogMessage): void;
  info(message: LogMessage): void;
  warn(message: LogMessage): void;
  error(message: LogMessage): void;
}

export interface Formatter {
  format(level: LogLevel, message: string, time?: Date): string;
}

const resolveMessage = (message: LogMessage): string =>
  typeof message === "function" ? message() : message;

const formatContext = (context?: Record<string, unknown>): string => {
  if (!context) {
    return "";
  }
  const entries = Object.entries(context).filter(
    ([, value]) => value !== undefined
  );
  if (entries.length === 0) {
    return "";
  }
  return entries
    .map(([key, value]) => {
      if (Array.isArray(value)) {
        return `${key}=${value.join(",")}`;
      }
      return `${key}=${String(value)}`;
    })
    .join(" ");
};

const upperLevel = (level: LogLevel): string =>
  level === "debug"
    ? "DEBUG"
    : level === "info"
      ? "INFO"
      : level === "warn"
        ? "WARN"
        : "ERROR";

const threadToken = (): string => {
  const token = process.pid ^ (threadId + 1);
  return (token >>> 0).toString(36);
};

export class BaseFormatter {
  private tidToken = threadToken();

  protected tid(): string {
    return this.tidToken;
  }

  protected contextString(): string {
    return formatContext(Context.peek());
  }
}

export class PrettyFormatter extends BaseFormatter implements Formatter {
  protected colors: Record<string, string>;
  protected useColors: boolean;

  constructor({ useColors = Boolean(process.stdout.isTTY) } = {}) {
    super();
    this.useColors = useColors;
    this.colors = {
      DEBUG: "\u001b[1;32mDEBUG\u001b[0m",
      INFO: "\u001b[1;34mINFO \u001b[0m",
      WARN: "\u001b[1;33mWARN \u001b[0m",
      ERROR: "\u001b[1;31mERROR\u001b[0m",
    };
  }

  protected label(severity: string): string {
    return this.useColors ? this.colors[severity] : severity;
  }

  format(level: LogLevel, message: string, time = new Date()): string {
    const severity = upperLevel(level);
    const label = this.label(severity);
    const context = this.contextString();
    const suffix = context ? ` ${context}` : "";
    return `${label} ${time.toISOString()} pid=${process.pid} tid=${this.tid()}${suffix}: ${message}`;
  }
}

export class PlainFormatter extends BaseFormatter implements Formatter {
  format(level: LogLevel, message: string, time = new Date()): string {
    const severity = upperLevel(level);
    const context = this.contextString();
    const suffix = context ? ` ${context}` : "";
    return `${severity} ${time.toISOString()} pid=${process.pid} tid=${this.tid()}${suffix}: ${message}`;
  }
}

export class WithoutTimestampFormatter extends PrettyFormatter {
  format(level: LogLevel, message: string): string {
    const severity = upperLevel(level);
    const label = this.label(severity);
    const context = this.contextString();
    const suffix = context ? ` ${context}` : "";
    return `${label} pid=${process.pid} tid=${this.tid()}${suffix}: ${message}`;
  }
}

export class JsonFormatter extends BaseFormatter implements Formatter {
  format(level: LogLevel, message: string, time = new Date()): string {
    const hash: Record<string, unknown> = {
      ts: time.toISOString(),
      pid: process.pid,
      tid: this.tid(),
      lvl: upperLevel(level),
      msg: message,
    };
    const context = Context.peek();
    if (context && Object.keys(context).length > 0) {
      hash.ctx = context;
    }
    return JSON.stringify(hash);
  }
}

export const Formatters = {
  Base: BaseFormatter,
  Pretty: PrettyFormatter,
  Plain: PlainFormatter,
  WithoutTimestamp: WithoutTimestampFormatter,
  JSON: JsonFormatter,
};

export class SidekiqLogger implements Logger {
  formatter: Formatter;
  private base: Console;

  constructor(
    base: Console = console,
    formatter: Formatter = defaultFormatter()
  ) {
    this.base = base;
    this.formatter = formatter;
  }

  debug(message: LogMessage): void {
    this.log("debug", message);
  }

  info(message: LogMessage): void {
    this.log("info", message);
  }

  warn(message: LogMessage): void {
    this.log("warn", message);
  }

  error(message: LogMessage): void {
    this.log("error", message);
  }

  private log(level: LogLevel, message: LogMessage): void {
    const resolved = resolveMessage(message);
    const formatted = this.formatter.format(level, resolved);
    this.base[level](formatted);
  }
}

const defaultFormatter = (): Formatter => {
  if (process.env.DYNO) {
    return new WithoutTimestampFormatter();
  }
  return new PrettyFormatter();
};

export const createLogger = (base: Console = console): Logger =>
  new SidekiqLogger(base, defaultFormatter());
