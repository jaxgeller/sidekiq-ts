import type { Config } from "./config.js";
import type { Logger } from "./logger.js";

export const DEFAULT_CONFIG_PATH = "sidekiq.json";

export interface CliOptions {
  configPath: string;
  configPathProvided: boolean;
  concurrency?: number;
  environment?: string;
  queues?: string[];
  requirePaths?: string[];
  tag?: string;
  timeout?: number;
  verbose: boolean;
  showHelp: boolean;
  showVersion: boolean;
}

const parseNumber = (value: string, flag: string): number => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} expects a positive number`);
  }
  return parsed;
};

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: CLI argument parsing is inherently complex
export const parseArgs = (argv: string[]): CliOptions => {
  const args = argv.slice(2);
  const options: CliOptions = {
    configPath: DEFAULT_CONFIG_PATH,
    configPathProvided: false,
    verbose: false,
    showHelp: false,
    showVersion: false,
  };

  for (let i = 0; i < args.length; i += 1) {
    const raw = args[i];
    const [arg, inlineValue] =
      raw.startsWith("--") && raw.includes("=")
        ? raw.split("=", 2)
        : [raw, undefined];
    const value = inlineValue ?? args[i + 1];

    switch (arg) {
      case "-h":
      case "--help":
        options.showHelp = true;
        break;
      case "-V":
      case "--version":
        options.showVersion = true;
        break;
      case "-C":
      case "--config":
        if (!value) {
          throw new Error(`${arg} expects a path`);
        }
        options.configPath = value;
        options.configPathProvided = true;
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-c":
      case "--concurrency":
        if (!value) {
          throw new Error(`${arg} expects a number`);
        }
        options.concurrency = parseNumber(value, arg);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-e":
      case "--environment":
        if (!value) {
          throw new Error(`${arg} expects a value`);
        }
        options.environment = value;
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-g":
      case "--tag":
        if (!value) {
          throw new Error(`${arg} expects a value`);
        }
        options.tag = value;
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-q":
      case "--queue":
        if (!value) {
          throw new Error(`${arg} expects a queue name`);
        }
        options.queues ??= [];
        options.queues.push(value);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-r":
      case "--require":
        if (!value) {
          throw new Error(`${arg} expects a path`);
        }
        options.requirePaths ??= [];
        options.requirePaths.push(value);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-t":
      case "--timeout":
        if (!value) {
          throw new Error(`${arg} expects a number`);
        }
        options.timeout = parseNumber(value, arg);
        if (!inlineValue) {
          i += 1;
        }
        break;
      case "-v":
      case "--verbose":
        options.verbose = true;
        break;
      default:
        if (arg.startsWith("-")) {
          throw new Error(`Unknown option ${arg}`);
        }
        break;
    }
  }

  return options;
};

export const resolveEnvironment = (cliEnv?: string): string | undefined =>
  cliEnv ??
  process.env.APP_ENV ??
  process.env.NODE_ENV ??
  process.env.RACK_ENV ??
  process.env.RAILS_ENV;

const suppressDebug = (logger: Logger): Logger => ({
  debug: () => undefined,
  info: logger.info.bind(logger),
  warn: logger.warn.bind(logger),
  error: logger.error.bind(logger),
});

export const applyCliOptions = (
  config: Config,
  options: CliOptions,
  loadedRequirePaths: string[] = []
): { config: Config; requirePaths: string[] } => {
  let requirePaths = loadedRequirePaths;

  if (options.concurrency !== undefined) {
    config.concurrency = options.concurrency;
  }
  if (options.queues && options.queues.length > 0) {
    config.queues = options.queues;
  }
  if (options.tag) {
    config.tag = options.tag;
  }
  if (options.timeout !== undefined) {
    config.timeout = options.timeout;
  }
  if (!options.verbose) {
    config.logger = suppressDebug(config.logger);
  }
  if (options.requirePaths && options.requirePaths.length > 0) {
    requirePaths = options.requirePaths;
  }

  return { config, requirePaths };
};

export interface ShutdownDeps {
  logger: Logger;
  stop: () => Promise<void>;
  exit: (code: number) => void;
}

export interface ShutdownHandler {
  (signal: string): Promise<void>;
  isShuttingDown: () => boolean;
}

export const createShutdownHandler = (deps: ShutdownDeps): ShutdownHandler => {
  let shuttingDown = false;

  const handler = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      deps.logger.info(() => `Received ${signal}, forcing exit`);
      deps.exit(1);
      return;
    }
    shuttingDown = true;
    deps.logger.info(() => `Received ${signal}, shutting down`);
    await deps.stop();
    deps.exit(0);
  };

  handler.isShuttingDown = () => shuttingDown;

  return handler;
};
