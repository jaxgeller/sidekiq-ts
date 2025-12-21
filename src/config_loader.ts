import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Config } from "./config.js";
import type { ConfigOptions } from "./types.js";

export interface JsonConfig extends ConfigOptions {
  require?: string | string[];
}

export interface LoadedConfig {
  config: Config;
  requirePaths: string[];
  sourcePath: string;
}

export const loadConfigFile = async (path: string): Promise<LoadedConfig> => {
  const fullPath = resolve(path);
  const raw = await readFile(fullPath, "utf8");
  const parsed = JSON.parse(raw) as JsonConfig;
  const requirePaths = Array.isArray(parsed.require)
    ? parsed.require
    : parsed.require
      ? [parsed.require]
      : [];
  const { require: _require, ...options } = parsed;
  const config = new Config(options);
  return { config, requirePaths, sourcePath: fullPath };
};
