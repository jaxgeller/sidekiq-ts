import { describe, it, expect } from "vitest";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeFile } from "node:fs/promises";
import { loadConfigFile } from "../src/config_loader.js";

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
});
