#!/usr/bin/env node
import { access } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Config } from "./config.js";
import { loadConfigFile } from "./config_loader.js";
import type { Logger } from "./logger.js";
import { Sidekiq } from "./sidekiq.js";

const DEFAULT_CONFIG_PATH = "sidekiq.json";

interface CliOptions {
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

const parseArgs = (argv: string[]): CliOptions => {
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
    const [arg, inlineValue] = raw.startsWith("--") && raw.includes("=")
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

const printHelp = () => {
  console.log(`Usage: sidekiq-ts [options]

Options:
  -C, --config PATH       Path to JSON config file (default: sidekiq.json if present)
  -c, --concurrency NUM   Processor threads to use
  -e, --environment ENV   Application environment
  -g, --tag TAG           Process tag
  -q, --queue QUEUE[,WT]  Queue with optional weight (repeatable)
  -r, --require PATH      File or directory to import before startup (repeatable)
  -t, --timeout NUM       Shutdown timeout seconds
  -v, --verbose           Enable debug logging
  -V, --version           Print version and exit
  -h, --help              Show this help message
`);
};

const fileExists = async (path: string): Promise<boolean> => {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
};

const suppressDebug = (logger: Logger): Logger => ({
  debug: () => {},
  info: logger.info.bind(logger),
  warn: logger.warn.bind(logger),
  error: logger.error.bind(logger),
});

const resolveEnvironment = (cliEnv?: string): string | undefined =>
  cliEnv ?? process.env.APP_ENV ?? process.env.NODE_ENV ?? process.env.RACK_ENV ?? process.env.RAILS_ENV;

const printVersion = () => {
  const require = createRequire(import.meta.url);
  const pkg = require("../package.json") as { version?: string; name?: string };
  const name = pkg.name ?? "sidekiq-ts";
  const version = pkg.version ?? "0.0.0";
  console.log(`${name} ${version}`);
};

const main = async () => {
  const options = parseArgs(process.argv);
  if (options.showHelp) {
    printHelp();
    return;
  }
  if (options.showVersion) {
    printVersion();
    return;
  }

  const environment = resolveEnvironment(options.environment);
  if (environment && !process.env.NODE_ENV) {
    process.env.NODE_ENV = environment;
  }

  let config = new Config();
  let requirePaths: string[] = [];
  const configExists = await fileExists(options.configPath);
  if (options.configPathProvided || configExists) {
    if (!configExists) {
      throw new Error(`No such file ${options.configPath}`);
    }
    const loaded = await loadConfigFile(options.configPath, { environment });
    config = loaded.config;
    requirePaths = loaded.requirePaths;
  }

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
  for (const entry of requirePaths) {
    const fullPath = resolve(entry);
    await import(pathToFileURL(fullPath).href);
  }

  const runner = await Sidekiq.run({ config });

  const shutdown = async () => {
    await runner.stop();
    process.exit(0);
  };

  const quiet = async () => {
    config.logger.info(() => "Received TSTP, no longer accepting new work");
    await runner.quiet();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  process.on("SIGTSTP", quiet);
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
