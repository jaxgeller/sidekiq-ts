import { writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { loadConfigFile } from "../src/config-loader.js";

const writeConfig = async (data: object) => {
  const path = join(tmpdir(), `sidekiq-ts-${Date.now()}.json`);
  await writeFile(path, JSON.stringify(data), "utf8");
  return path;
};

describe("config loader", () => {
  it("loads JSON config into Config", async () => {
    const path = await writeConfig({
      redis: { url: "redis://localhost:6379/0" },
      concurrency: 7,
      queues: ["critical", "default"],
      maxRetries: 12,
    });

    const { config, requirePaths, sourcePath } = await loadConfigFile(path);

    expect(sourcePath.endsWith(path)).toBe(true);
    expect(config.redis.url).toBe("redis://localhost:6379/0");
    expect(config.concurrency).toBe(7);
    expect(config.queues).toEqual(["critical", "default"]);
    expect(config.maxRetries).toBe(12);
    expect(requirePaths).toEqual([]);
  });

  it("normalizes require paths", async () => {
    const path = await writeConfig({
      require: ["./jobs/example.js"],
    });

    const { requirePaths } = await loadConfigFile(path);
    expect(requirePaths).toEqual(["./jobs/example.js"]);
  });

  it("applies environment overrides from top-level keys", async () => {
    const path = await writeConfig({
      concurrency: 5,
      require: "./jobs/base.js",
      development: {
        concurrency: 12,
        require: "./jobs/dev.js",
      },
    });

    const { config, requirePaths } = await loadConfigFile(path, {
      environment: "development",
    });
    expect(config.concurrency).toBe(12);
    expect(requirePaths).toEqual(["./jobs/dev.js"]);
  });

  it("applies environment overrides from environments map", async () => {
    const path = await writeConfig({
      timeout: 25,
      environments: {
        production: {
          timeout: 60,
        },
      },
    });

    const { config } = await loadConfigFile(path, {
      environment: "production",
    });
    expect(config.timeout).toBe(60);
  });
});
