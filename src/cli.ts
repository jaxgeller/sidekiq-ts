#!/usr/bin/env node
import { access } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  applyCliOptions,
  parseArgs,
  resolveEnvironment,
} from "./cli-helpers.js";
import { Config } from "./config.js";
import { loadConfigFile } from "./config-loader.js";
import { Sidekiq } from "./sidekiq.js";

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

const printVersion = () => {
  const require = createRequire(import.meta.url);
  const pkg = require("../package.json") as { version?: string; name?: string };
  const name = pkg.name ?? "sidekiq-ts";
  const version = pkg.version ?? "0.0.0";
  console.log(`${name} ${version}`);
};

const registerSignal = (
  signal: string,
  handler: () => void | Promise<void>
) => {
  try {
    process.on(signal as NodeJS.Signals, () => {
      Promise.resolve(handler()).catch(() => undefined);
    });
  } catch {
    // Signal not supported on this platform.
  }
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

  ({ config, requirePaths } = applyCliOptions(config, options, requirePaths));
  for (const entry of requirePaths) {
    const fullPath = resolve(entry);
    await import(pathToFileURL(fullPath).href);
  }

  const runner = await Sidekiq.run({ config });
  let shuttingDown = false;
  let quieting = false;

  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    config.logger.info(() => `Received ${signal}, shutting down`);
    await runner.stop();
    process.exit(0);
  };

  const quiet = async (signal: string) => {
    if (quieting) {
      return;
    }
    quieting = true;
    config.logger.info(
      () => `Received ${signal}, no longer accepting new work`
    );
    await runner.quiet();
  };

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: signal handling needs detailed logging
  const dumpState = (signal: string) => {
    config.logger.warn(() => `Received ${signal}, dumping state`);
    const snapshot = runner.snapshotWork();
    if (snapshot.length === 0) {
      config.logger.warn(() => "No active jobs");
    } else {
      config.logger.warn(() => `Active jobs: ${snapshot.length}`);
      for (const entry of snapshot) {
        const payload = entry.payload;
        let className: string;
        if (!payload) {
          className = "unknown";
        } else if (typeof payload.class === "string") {
          className = payload.class;
        } else {
          className = payload.class?.name ?? String(payload.class);
        }
        const jid = payload?.jid ?? "unknown";
        const elapsed = entry.elapsed.toFixed(3);
        config.logger.warn(
          () =>
            `Worker ${entry.workerId} class=${className} jid=${jid} queue=${entry.queue} elapsed=${elapsed}s`
        );
      }
    }
    const stack = new Error("Signal stack").stack;
    if (stack) {
      const lines = stack.split("\n").slice(1).join("\n");
      config.logger.warn(() => `Current stack:\n${lines}`);
    }
  };

  registerSignal("SIGINT", () => shutdown("SIGINT"));
  registerSignal("SIGTERM", () => shutdown("SIGTERM"));
  registerSignal("SIGTSTP", () => quiet("SIGTSTP"));
  registerSignal("SIGTTIN", () => dumpState("SIGTTIN"));
  registerSignal("SIGINFO", () => dumpState("SIGINFO"));
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
