/**
 * Failure Modes Simulation
 *
 * Tests retry exhaustion, dead queue handling, and graceful shutdown behavior.
 *
 * Run with: npx tsx examples/simulations/failure-modes.ts
 */

import { DeadSet, Queue, RetrySet, Stats } from "../../src/api.js";
import { Config } from "../../src/config.js";
import { Sidekiq } from "../../src/sidekiq.js";
import { AlwaysFailJob, DelayJob, registerAllJobs } from "./shared/jobs.js";
import {
  formatResult,
  MetricsCollector,
  type SimulationResult,
} from "./shared/metrics.js";

async function testRetryExhaustion(): Promise<SimulationResult> {
  const errors: string[] = [];
  const deathEvents: string[] = [];
  const retriesExhaustedCalls: string[] = [];
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 5,
    queues: ["simulation"],
    timeout: 5,
    pollIntervalAverage: 1,
    averageScheduledPollInterval: 1, // Fast retry polling
    errorHandlers: [
      (error, ctx) => {
        errors.push(`${error.message} (retry=${ctx?.job?.retry_count ?? 0})`);
      },
    ],
    deathHandlers: [
      (payload) => {
        deathEvents.push(payload.jid ?? "unknown");
      },
    ],
  });

  // Register retriesExhausted handler
  AlwaysFailJob.retriesExhausted((payload, _error) => {
    retriesExhaustedCalls.push(payload.jid ?? "unknown");
    // Return undefined to send to dead queue
    return undefined;
  });

  try {
    const redis = await config.getRedisClient();
    // Clear state
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Enqueue jobs that will always fail
    const jobCount = 3;
    console.log(`Enqueueing ${jobCount} AlwaysFailJob instances...`);

    const jids: string[] = [];
    for (let i = 0; i < jobCount; i++) {
      const jid = await AlwaysFailJob.performAsync(`fail-${i}`);
      if (jid) {
        jids.push(jid);
      }
    }

    console.log("Starting worker...");
    const runner = await Sidekiq.run({ config });

    // Wait for retries to exhaust (3 retries * 1s delay + up to 30s jitter each)
    console.log("Waiting for retries to exhaust...");
    const retrySet = new RetrySet(config);
    const queue = new Queue("simulation", config);

    // Wait up to 120 seconds - retries have random jitter up to 10*(retry_count+1) seconds
    const maxWait = 120_000;
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const retrySize = await retrySet.size();
      const queueSize = await queue.size();
      if (retrySize === 0 && queueSize === 0) {
        // Give a bit more time for death handlers to fire
        await new Promise((r) => setTimeout(r, 1000));
        break;
      }
      await new Promise((r) => setTimeout(r, 500));
    }

    await runner.stop();
    const totalTime = metrics.stop();

    // Check dead queue
    const deadSet = new DeadSet(config);
    const deadSize = await deadSet.size();
    const stats = new Stats(config);

    const passed =
      deadSize === jobCount &&
      deathEvents.length === jobCount &&
      retriesExhaustedCalls.length === jobCount;

    return {
      name: "Retry Exhaustion Test",
      passed,
      durationMs: totalTime,
      metrics: {
        "Jobs enqueued": jobCount,
        "Dead queue size": deadSize,
        "Death events fired": deathEvents.length,
        "retriesExhausted calls": retriesExhaustedCalls.length,
        "Total error attempts": errors.length,
        "Failed stat": await stats.failed(),
      },
      errors: passed ? [] : ["Not all jobs reached dead queue correctly"],
    };
  } finally {
    await config.close();
  }
}

async function testGracefulShutdown(): Promise<SimulationResult> {
  const lifecycleOrder: string[] = [];
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 3,
    queues: ["simulation"],
    timeout: 3, // Short timeout for faster test
    pollIntervalAverage: 1,
    lifecycleEvents: {
      startup: [() => lifecycleOrder.push("startup")],
      quiet: [() => lifecycleOrder.push("quiet")],
      shutdown: [() => lifecycleOrder.push("shutdown")],
      exit: [() => lifecycleOrder.push("exit")],
    },
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Enqueue slow jobs that take longer than timeout
    const jobCount = 5;
    console.log(`Enqueueing ${jobCount} slow jobs (10s each)...`);
    for (let i = 0; i < jobCount; i++) {
      await DelayJob.performAsync(i, 10_000); // 10 second jobs
    }

    console.log("Starting worker...");
    const runner = await Sidekiq.run({ config });

    // Wait for jobs to start processing
    await new Promise((r) => setTimeout(r, 500));

    // Trigger shutdown
    console.log("Triggering graceful shutdown...");
    const shutdownStart = Date.now();
    await runner.stop();
    const shutdownDuration = Date.now() - shutdownStart;

    const totalTime = metrics.stop();

    // Check queue for requeued jobs
    const queue = new Queue("simulation", config);
    const remainingJobs = await queue.size();
    const stats = new Stats(config);
    const processed = await stats.processed();

    // Verify lifecycle order
    const expectedOrder = ["startup", "quiet", "shutdown", "exit"];
    const orderCorrect =
      lifecycleOrder.length === expectedOrder.length &&
      lifecycleOrder.every((v, i) => v === expectedOrder[i]);

    // Shutdown should timeout around 3 seconds (config.timeout)
    const timeoutRespected = shutdownDuration < 5000;

    const passed = orderCorrect && timeoutRespected;

    return {
      name: "Graceful Shutdown Test",
      passed,
      durationMs: totalTime,
      metrics: {
        "Shutdown duration (ms)": shutdownDuration,
        "Lifecycle order": lifecycleOrder.join(" -> "),
        "Jobs processed": processed,
        "Jobs remaining in queue": remainingJobs,
        "Timeout respected": timeoutRespected ? "Yes" : "No",
        "Order correct": orderCorrect ? "Yes" : "No",
      },
      errors: passed
        ? []
        : [
            orderCorrect
              ? ""
              : `Expected order: ${expectedOrder.join(" -> ")}, got: ${lifecycleOrder.join(" -> ")}`,
            timeoutRespected
              ? ""
              : `Shutdown took ${shutdownDuration}ms, expected < 5000ms`,
          ].filter(Boolean),
    };
  } finally {
    await config.close();
  }
}

async function main() {
  console.log("=== FAILURE MODES SIMULATION ===\n");

  registerAllJobs();

  const results: SimulationResult[] = [];

  console.log("\n--- Running: Retry Exhaustion Test ---");
  results.push(await testRetryExhaustion());
  console.log(formatResult(results.at(-1)));

  await new Promise((r) => setTimeout(r, 1000));

  console.log("\n--- Running: Graceful Shutdown Test ---");
  results.push(await testGracefulShutdown());
  console.log(formatResult(results.at(-1)));

  // Summary
  console.log(`\n${"=".repeat(60)}`);
  console.log("SUMMARY");
  console.log("=".repeat(60));

  const allPassed = results.every((r) => r.passed);
  for (const result of results) {
    const status = result.passed ? "PASS" : "FAIL";
    console.log(`[${status}] ${result.name}`);
  }

  console.log(
    "\nOverall:",
    allPassed ? "ALL TESTS PASSED" : "SOME TESTS FAILED"
  );
  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
