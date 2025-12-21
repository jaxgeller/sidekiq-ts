#!/usr/bin/env node
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Sidekiq } from "./sidekiq.js";
import { loadConfigFile } from "./config_loader.js";

const parseArgs = (argv: string[]) => {
  const args = argv.slice(2);
  let configPath = "sidekiq.json";
  let showHelp = false;

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "-h" || arg === "--help") {
      showHelp = true;
    } else if (arg === "-c" || arg === "--config") {
      configPath = args[i + 1] ?? configPath;
      i += 1;
    }
  }

  return { configPath, showHelp };
};

const printHelp = () => {
  console.log(`Usage: sidekiq-ts --config path/to/sidekiq.json

Options:
  -c, --config   Path to JSON config file (default: sidekiq.json)
  -h, --help     Show this help message
`);
};

const main = async () => {
  const { configPath, showHelp } = parseArgs(process.argv);
  if (showHelp) {
    printHelp();
    return;
  }

  const { config, requirePaths } = await loadConfigFile(configPath);
  for (const entry of requirePaths) {
    const fullPath = resolve(entry);
    await import(pathToFileURL(fullPath).href);
  }

  const runner = await Sidekiq.run({ config });

  const shutdown = async () => {
    await runner.stop();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
