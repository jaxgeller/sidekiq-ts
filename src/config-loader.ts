import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Config } from "./config.js";
import type { ConfigOptions } from "./types.js";

export interface JsonConfig extends ConfigOptions {
  require?: string | string[];
  environments?: Record<string, JsonConfig>;
}

export interface LoadedConfig {
  config: Config;
  requirePaths: string[];
  sourcePath: string;
}

export interface LoadConfigOptions {
  environment?: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const normalizeRequire = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.filter((entry): entry is string => typeof entry === "string");
  }
  if (typeof value === "string") {
    return [value];
  }
  return [];
};

const resolveEnvironmentOverrides = (
  parsed: Record<string, unknown>,
  environment?: string
): Record<string, unknown> => {
  if (!environment) {
    return {};
  }
  const direct = isRecord(parsed[environment]) ? parsed[environment] : {};
  const envMap = isRecord(parsed.environments) ? parsed.environments : {};
  const mapped = isRecord(envMap[environment]) ? envMap[environment] : {};
  return { ...direct, ...mapped };
};

export const loadConfigFile = async (
  path: string,
  options: LoadConfigOptions = {}
): Promise<LoadedConfig> => {
  const fullPath = resolve(path);
  const raw = await readFile(fullPath, "utf8");
  const parsed = JSON.parse(raw) as Record<string, unknown>;
  const envOverrides = resolveEnvironmentOverrides(parsed, options.environment);
  const merged = { ...parsed, ...envOverrides } as Record<string, unknown>;
  const requirePaths = normalizeRequire(merged.require);
  merged.require = undefined;
  merged.environments = undefined;
  if (options.environment) {
    merged[options.environment] = undefined;
  }
  const config = new Config(merged as ConfigOptions);
  return { config, requirePaths, sourcePath: fullPath };
};
