#!/usr/bin/env npx tsx
/**
 * Worker process for multiprocess simulation.
 * Spawned by multiprocess-test.ts controller.
 *
 * Communicates lifecycle events back to controller via stdout JSON messages.
 */

import { Config } from "../../../src/config.js";
import type { Runner } from "../../../src/runner.js";
import { Sidekiq } from "../../../src/sidekiq.js";
import { registerAllJobs } from "./jobs.js";

interface WorkerMessage {
  type: "lifecycle" | "error";
  event?: string;
  data?: Record<string, unknown>;
}

function sendMessage(msg: WorkerMessage) {
  console.log(JSON.stringify(msg));
}

async function main() {
  const concurrency = Number.parseInt(process.env.CONCURRENCY ?? "5", 10);
  const timeout = Number.parseInt(process.env.TIMEOUT ?? "10", 10);
  const heartbeatInterval = Number.parseFloat(
    process.env.HEARTBEAT_INTERVAL ?? "0.5"
  );

  registerAllJobs();

  const config = new Config({
    concurrency,
    queues: ["simulation"],
    timeout,
    pollIntervalAverage: 0.5,
    heartbeatInterval,
    lifecycleEvents: {
      startup: [() => sendMessage({ type: "lifecycle", event: "startup" })],
      quiet: [() => sendMessage({ type: "lifecycle", event: "quiet" })],
      shutdown: [() => sendMessage({ type: "lifecycle", event: "shutdown" })],
      exit: [() => sendMessage({ type: "lifecycle", event: "exit" })],
    },
  });

  let runner: Runner | null = null;

  const shutdown = async (signal: string) => {
    sendMessage({ type: "lifecycle", event: `signal:${signal}` });
    if (runner) {
      await runner.stop();
    }
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  try {
    runner = await Sidekiq.run({ config });
    sendMessage({
      type: "lifecycle",
      event: "running",
      data: { pid: process.pid },
    });

    // Keep running until stopped
    await new Promise(() => {
      // Intentionally empty - blocks forever until process is killed
    });
  } catch (error) {
    sendMessage({ type: "error", data: { message: (error as Error).message } });
    process.exit(1);
  }
}

main();
