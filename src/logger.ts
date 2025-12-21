import { Context } from "./context.js";

export type LogMessage = string | (() => string);

export interface Logger {
  debug(message: LogMessage): void;
  info(message: LogMessage): void;
  warn(message: LogMessage): void;
  error(message: LogMessage): void;
}

const resolveMessage = (message: LogMessage): string =>
  typeof message === "function" ? message() : message;

const formatContext = (context?: Record<string, unknown>): string => {
  if (!context) {
    return "";
  }
  const entries = Object.entries(context).filter(([, value]) => value !== undefined);
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

const withContext = (message: LogMessage): string => {
  const resolved = resolveMessage(message);
  const context = formatContext(Context.peek());
  return context ? `${resolved} ${context}` : resolved;
};

export const createLogger = (base: Console = console): Logger => ({
  debug: (message) => base.debug(withContext(message)),
  info: (message) => base.info(withContext(message)),
  warn: (message) => base.warn(withContext(message)),
  error: (message) => base.error(withContext(message)),
});
