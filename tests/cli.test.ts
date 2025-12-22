import { afterEach, describe, expect, it, vi } from "vitest";
import {
  applyCliOptions,
  type CliOptions,
  DEFAULT_CONFIG_PATH,
  parseArgs,
  resolveEnvironment,
} from "../src/cli-helpers.js";
import { Config } from "../src/config.js";
import type { Logger } from "../src/logger.js";

const baseOptions = (): CliOptions => ({
  configPath: DEFAULT_CONFIG_PATH,
  configPathProvided: false,
  verbose: false,
  showHelp: false,
  showVersion: false,
});

const snapshotEnv = () => ({ ...process.env });

const restoreEnv = (snapshot: NodeJS.ProcessEnv) => {
  Object.keys(process.env).forEach((key) => {
    if (!(key in snapshot)) {
      delete process.env[key];
    }
  });
  Object.entries(snapshot).forEach(([key, value]) => {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  });
};

let envSnapshot = snapshotEnv();

afterEach(() => {
  restoreEnv(envSnapshot);
  envSnapshot = snapshotEnv();
});

describe("CLI helpers", () => {
  it("parses flags and inline values", () => {
    const options = parseArgs([
      "node",
      "sidekiq-ts",
      "--config=custom.json",
      "-c",
      "10",
      "-q",
      "critical,2",
      "-q",
      "default",
      "-r",
      "./jobs.js",
      "-t",
      "30",
      "-e",
      "production",
      "-g",
      "tag",
      "-v",
    ]);

    expect(options.configPath).toBe("custom.json");
    expect(options.configPathProvided).toBe(true);
    expect(options.concurrency).toBe(10);
    expect(options.queues).toEqual(["critical,2", "default"]);
    expect(options.requirePaths).toEqual(["./jobs.js"]);
    expect(options.timeout).toBe(30);
    expect(options.environment).toBe("production");
    expect(options.tag).toBe("tag");
    expect(options.verbose).toBe(true);
  });

  it("uses default config path when none provided", () => {
    const options = parseArgs(["node", "sidekiq-ts"]);
    expect(options.configPath).toBe(DEFAULT_CONFIG_PATH);
    expect(options.configPathProvided).toBe(false);
  });

  it("rejects unknown flags", () => {
    expect(() => parseArgs(["node", "sidekiq-ts", "--bogus"])).toThrow(
      "Unknown option --bogus"
    );
  });

  it("resolves environment from CLI or env vars", () => {
    process.env.APP_ENV = "app";
    expect(resolveEnvironment()).toBe("app");
    expect(resolveEnvironment("cli")).toBe("cli");

    delete process.env.APP_ENV;
    process.env.NODE_ENV = "node";
    expect(resolveEnvironment()).toBe("node");

    delete process.env.NODE_ENV;
    process.env.RACK_ENV = "rack";
    expect(resolveEnvironment()).toBe("rack");

    delete process.env.RACK_ENV;
    process.env.RAILS_ENV = "rails";
    expect(resolveEnvironment()).toBe("rails");
  });

  it("applies CLI overrides to config and require paths", () => {
    const logger: Logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const config = new Config({
      concurrency: 5,
      queues: ["default"],
      timeout: 25,
    });
    config.tag = "base";
    config.logger = logger;

    const options: CliOptions = {
      ...baseOptions(),
      concurrency: 12,
      queues: ["critical", "default"],
      timeout: 60,
      tag: "cli",
      requirePaths: ["./jobs.js"],
    };

    const result = applyCliOptions(config, options, ["./from-config.js"]);
    expect(config.concurrency).toBe(12);
    expect(config.queues).toEqual(["critical", "default"]);
    expect(config.timeout).toBe(60);
    expect(config.tag).toBe("cli");
    expect(result.requirePaths).toEqual(["./jobs.js"]);
    expect(config.logger).not.toBe(logger);

    config.logger.debug("skip");
    expect(logger.debug).not.toHaveBeenCalled();
    config.logger.info("hello");
    expect(logger.info).toHaveBeenCalled();
  });

  it("keeps logger when verbose is enabled", () => {
    const logger: Logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const config = new Config();
    config.logger = logger;

    const options: CliOptions = {
      ...baseOptions(),
      verbose: true,
    };

    const result = applyCliOptions(config, options, []);
    expect(result.requirePaths).toEqual([]);
    expect(config.logger).toBe(logger);
  });
});
