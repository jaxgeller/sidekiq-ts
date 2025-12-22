/**
 * Concurrency Testing Simulation
 *
 * Tests race conditions with shared state and parallel job enqueueing.
 *
 * Run with: npx tsx examples/simulations/concurrency-test.ts
 */

import { Queue, Stats } from "../../src/api.js";
import { Config } from "../../src/config.js";
import { Sidekiq } from "../../src/sidekiq.js";
import {
  CounterJob,
  NoOpJob,
  registerAllJobs,
  resetCounter,
  sharedCounter,
} from "./shared/jobs.js";
import {
  formatResult,
  MetricsCollector,
  type SimulationResult,
  waitForQueueDrain,
} from "./shared/metrics.js";

async function testRaceConditions(): Promise<SimulationResult> {
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 10,
    queues: ["simulation"],
    timeout: 5,
    pollIntervalAverage: 1,
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    resetCounter();
    metrics.start();

    // Enqueue many jobs that modify shared state
    const jobCount = 100;
    console.log(`Enqueueing ${jobCount} counter jobs...`);
    for (let i = 0; i < jobCount; i++) {
      await CounterJob.performAsync(1);
    }

    console.log("Starting worker...");
    const runner = await Sidekiq.run({ config });

    // Wait for all jobs
    const queue = new Queue("simulation", config);
    await waitForQueueDrain(queue, 30_000);

    // Brief pause for any in-flight jobs
    await new Promise((r) => setTimeout(r, 500));

    await runner.stop();
    const totalTime = metrics.stop();

    const stats = new Stats(config);
    const processed = await stats.processed();

    // Detect lost updates (race condition)
    const expectedValue = jobCount;
    const actualValue = sharedCounter;
    const lostUpdates = expectedValue - actualValue;
    const hasRaceCondition = lostUpdates > 0;

    return {
      name: "Race Condition Detection",
      passed: true, // This test demonstrates race conditions exist, not that they don't
      durationMs: totalTime,
      metrics: {
        "Jobs processed": processed,
        "Expected counter value": expectedValue,
        "Actual counter value": actualValue,
        "Lost updates": lostUpdates,
        "Race condition detected": hasRaceCondition ? "YES (expected)" : "No",
      },
      errors: [],
    };
  } finally {
    await config.close();
  }
}

async function testParallelEnqueue(): Promise<SimulationResult> {
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 1, // Don't process yet
    queues: ["simulation"],
    timeout: 5,
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Parallel enqueue from multiple "clients"
    const enqueuers = 10;
    const jobsPerEnqueuer = 500;
    const totalExpected = enqueuers * jobsPerEnqueuer;

    console.log(
      `Parallel enqueueing: ${enqueuers} clients x ${jobsPerEnqueuer} jobs each...`
    );

    const enqueueStart = Date.now();
    const jidResults = await Promise.all(
      Array.from({ length: enqueuers }, async (_, clientId) => {
        const jids: string[] = [];
        for (let j = 0; j < jobsPerEnqueuer; j++) {
          const jid = await NoOpJob.performAsync(
            clientId * jobsPerEnqueuer + j
          );
          if (jid) {
            jids.push(jid);
          }
        }
        return jids;
      })
    );
    const enqueueTime = Date.now() - enqueueStart;

    const totalTime = metrics.stop();

    // Verify no duplicate JIDs
    const allJids = jidResults.flat();
    const uniqueJids = new Set(allJids);
    const duplicates = allJids.length - uniqueJids.size;

    // Verify queue size matches
    const queue = new Queue("simulation", config);
    const queueSize = await queue.size();

    const passed =
      duplicates === 0 &&
      queueSize === totalExpected &&
      allJids.length === totalExpected;

    return {
      name: "Parallel Enqueue Test",
      passed,
      durationMs: totalTime,
      metrics: {
        "Total clients": enqueuers,
        "Jobs per client": jobsPerEnqueuer,
        "Total JIDs collected": allJids.length,
        "Unique JIDs": uniqueJids.size,
        "Duplicate JIDs": duplicates,
        "Queue size": queueSize,
        "Enqueue time (ms)": enqueueTime,
        "Enqueues/second": Math.round(totalExpected / (enqueueTime / 1000)),
      },
      errors: passed
        ? []
        : [
            duplicates > 0 ? `Found ${duplicates} duplicate JIDs` : "",
            queueSize !== totalExpected
              ? `Queue size ${queueSize} != expected ${totalExpected}`
              : "",
          ].filter(Boolean),
    };
  } finally {
    await config.close();
  }
}

async function testBulkEnqueue(): Promise<SimulationResult> {
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 1,
    queues: ["simulation"],
    timeout: 5,
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Test performBulk with concurrent callers
    const callers = 5;
    const jobsPerCaller = 1000;

    console.log(
      `Concurrent bulk enqueue: ${callers} callers x ${jobsPerCaller} jobs...`
    );

    const enqueueStart = Date.now();
    await Promise.all(
      Array.from({ length: callers }, async (_, callerId) => {
        const args: [number][] = Array.from(
          { length: jobsPerCaller },
          (_, j) => [callerId * jobsPerCaller + j]
        );
        await NoOpJob.performBulk(args);
      })
    );
    const enqueueTime = Date.now() - enqueueStart;

    const totalTime = metrics.stop();

    const queue = new Queue("simulation", config);
    const queueSize = await queue.size();
    const expectedTotal = callers * jobsPerCaller;

    const passed = queueSize === expectedTotal;

    return {
      name: "Concurrent Bulk Enqueue Test",
      passed,
      durationMs: totalTime,
      metrics: {
        "Total callers": callers,
        "Jobs per caller": jobsPerCaller,
        "Expected total": expectedTotal,
        "Queue size": queueSize,
        "Enqueue time (ms)": enqueueTime,
        "Jobs/second": Math.round(expectedTotal / (enqueueTime / 1000)),
      },
      errors: passed
        ? []
        : [`Queue size ${queueSize} != expected ${expectedTotal}`],
    };
  } finally {
    await config.close();
  }
}

async function main() {
  console.log("=== CONCURRENCY TESTING SIMULATION ===\n");

  registerAllJobs();

  const results: SimulationResult[] = [];

  console.log("\n--- Running: Race Condition Detection ---");
  results.push(await testRaceConditions());
  console.log(formatResult(results.at(-1)));

  await new Promise((r) => setTimeout(r, 1000));

  console.log("\n--- Running: Parallel Enqueue Test ---");
  results.push(await testParallelEnqueue());
  console.log(formatResult(results.at(-1)));

  await new Promise((r) => setTimeout(r, 1000));

  console.log("\n--- Running: Concurrent Bulk Enqueue Test ---");
  results.push(await testBulkEnqueue());
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
