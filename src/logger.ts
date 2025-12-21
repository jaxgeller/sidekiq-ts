export type LogMessage = string | (() => string);

export interface Logger {
  debug(message: LogMessage): void;
  info(message: LogMessage): void;
  warn(message: LogMessage): void;
  error(message: LogMessage): void;
}

const resolveMessage = (message: LogMessage): string =>
  typeof message === "function" ? message() : message;

export const createLogger = (base: Console = console): Logger => ({
  debug: (message) => base.debug(resolveMessage(message)),
  info: (message) => base.info(resolveMessage(message)),
  warn: (message) => base.warn(resolveMessage(message)),
  error: (message) => base.error(resolveMessage(message)),
});
