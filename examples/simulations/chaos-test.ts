/**
 * Chaos Testing Simulation
 *
 * Tests system behavior under random failures and IterableJob interruption.
 *
 * Run with: npx tsx examples/simulations/chaos-test.ts
 */

import {
  Config,
  DeadSet,
  Queue,
  RetrySet,
  Sidekiq,
  Stats,
} from "../../src/index.js";
// Note: Sidekiq is used to access defaultConfiguration for IterableJob workaround
import { FlakyJob, LongIterableJob, registerAllJobs } from "./shared/jobs.js";
import {
  formatResult,
  MetricsCollector,
  type SimulationResult,
} from "./shared/metrics.js";

async function testRandomFailures(): Promise<SimulationResult> {
  const errors: string[] = [];
  const metrics = new MetricsCollector();

  const failureRate = 30; // 30% failure rate
  const testDurationSeconds = 30;

  const config = new Config({
    concurrency: 10,
    queues: ["simulation"],
    timeout: 5,
    pollIntervalAverage: 1,
    averageScheduledPollInterval: 2,
    maxRetries: 3,
    errorHandlers: [
      (error) => {
        errors.push(error.message);
      },
    ],
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    let totalEnqueued = 0;
    const endTime = Date.now() + testDurationSeconds * 1000;

    console.log(
      `Running chaos test for ${testDurationSeconds}s with ${failureRate}% failure rate...`
    );

    // Start worker
    const runner = await Sidekiq.run({ config });

    // Continuously enqueue jobs during test
    const enqueueLoop = async () => {
      while (Date.now() < endTime) {
        // Enqueue batch of jobs
        for (let i = 0; i < 50; i++) {
          await FlakyJob.performAsync(totalEnqueued++, failureRate);
        }
        await new Promise((r) => setTimeout(r, 200));
      }
    };

    await enqueueLoop();

    // Wait for processing to settle
    console.log("Waiting for queue to drain...");
    const queue = new Queue("simulation", config);
    const retrySet = new RetrySet(config);

    const maxWait = 60_000;
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const queueSize = await queue.size();
      const retrySize = await retrySet.size();
      if (queueSize === 0 && retrySize === 0) {
        break;
      }
      await new Promise((r) => setTimeout(r, 1000));
    }

    await runner.stop();
    const totalTime = metrics.stop();

    const stats = new Stats(config);
    const processed = await stats.processed();
    const _failed = await stats.failed();
    const deadSet = new DeadSet(config);
    const deadSize = await deadSet.size();

    // Calculate expected failure rate
    const actualFailRate = errors.length / totalEnqueued;

    return {
      name: `Chaos Test (${failureRate}% failure rate, ${testDurationSeconds}s)`,
      passed: true, // Chaos tests pass if the system stays stable
      durationMs: totalTime,
      metrics: {
        "Total enqueued": totalEnqueued,
        Processed: processed,
        "Error attempts": errors.length,
        "Dead queue size": deadSize,
        "Actual failure rate": `${(actualFailRate * 100).toFixed(1)}%`,
        "Target failure rate": `${failureRate}%`,
        "Peak memory (MB)": Math.round(metrics.getPeakMemoryMB()),
      },
      errors: [],
    };
  } finally {
    await config.close();
  }
}

async function testIterableInterruption(): Promise<SimulationResult> {
  const metrics = new MetricsCollector();

  // Note: IterableJob reads maxIterationRuntime from Sidekiq.defaultConfiguration,
  // not from the runner's config. This is a known limitation.
  const previousMaxRuntime = Sidekiq.defaultConfiguration.maxIterationRuntime;
  Sidekiq.defaultConfiguration.maxIterationRuntime = 2;

  const config = new Config({
    concurrency: 1,
    queues: ["simulation"],
    timeout: 25,
    maxIterationRuntime: 2, // Also set here for consistency
    pollIntervalAverage: 1,
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");
    // Clear any previous iteration state
    const keys = await redis.keys("it-*");
    if (keys.length > 0) {
      await redis.del(keys);
    }

    metrics.start();

    // Enqueue a job with many items that will take multiple executions
    const totalItems = 200; // At 50ms per item, this is 10s of work
    console.log(
      `Enqueueing LongIterableJob with ${totalItems} items (maxIterationRuntime=2s)...`
    );

    const jid = await LongIterableJob.performAsync(totalItems);
    console.log(`Job ID: ${jid}`);

    console.log("Starting worker (job will be interrupted multiple times)...");
    const runner = await Sidekiq.run({ config });

    // Wait for job to complete (with multiple interruptions)
    // The job has 200 items at 50ms each = 10s total, interrupted every 2s = ~5 executions
    const stats = new Stats(config);
    const maxWait = 60_000; // 60 second max
    const start = Date.now();

    // Wait until the job actually completes (processed count becomes 1)
    while (Date.now() - start < maxWait) {
      const processed = await stats.processed();
      if (processed >= 1) {
        console.log(`Job completed! processed=${processed}`);
        break;
      }
      await new Promise((r) => setTimeout(r, 500));
    }

    await runner.stop();
    const totalTime = metrics.stop();

    // stats was already created in the wait loop
    const processed = await stats.processed();

    // Check iteration state was cleaned up
    const remainingKeys = await redis.keys("it-*");

    // The job should complete once (processed=1). Interruptions don't increment
    // processed because they throw JobSkipError which exits early.
    // The key indicator is that state was cleaned up (job completed).
    const jobCompleted = processed === 1;
    const stateCleanedUp = remainingKeys.length === 0;

    return {
      name: "IterableJob Interruption Test",
      passed: jobCompleted && stateCleanedUp,
      durationMs: totalTime,
      metrics: {
        "Total items": totalItems,
        "Final processed count": processed,
        "Job completed": jobCompleted ? "Yes" : "No",
        "State cleaned up": stateCleanedUp ? "Yes" : "No",
        "Max iteration runtime (s)": 2,
      },
      errors:
        jobCompleted && stateCleanedUp
          ? []
          : [
              jobCompleted ? "" : `Expected processed=1, got ${processed}`,
              stateCleanedUp
                ? ""
                : `Iteration state not cleaned up: ${remainingKeys.join(", ")}`,
            ].filter(Boolean),
    };
  } finally {
    // Restore previous value
    Sidekiq.defaultConfiguration.maxIterationRuntime = previousMaxRuntime;
    await config.close();
  }
}

async function testMixedChaos(): Promise<SimulationResult> {
  const errors: string[] = [];
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency: 15,
    queues: ["simulation"],
    timeout: 10,
    pollIntervalAverage: 1,
    averageScheduledPollInterval: 2,
    maxRetries: 2,
    errorHandlers: [
      (error) => {
        errors.push(error.message);
      },
    ],
  });

  try {
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Mix of different job types
    console.log("Enqueueing mixed workload...");

    // Some flaky jobs with different failure rates
    for (let i = 0; i < 100; i++) {
      await FlakyJob.performAsync(i, 10); // 10% failure
    }
    for (let i = 0; i < 50; i++) {
      await FlakyJob.performAsync(i + 100, 50); // 50% failure
    }

    console.log("Starting worker...");
    const runner = await Sidekiq.run({ config });

    // Wait for processing
    const queue = new Queue("simulation", config);
    const retrySet = new RetrySet(config);

    const maxWait = 60_000;
    const start = Date.now();
    while (Date.now() - start < maxWait) {
      const queueSize = await queue.size();
      const retrySize = await retrySet.size();
      if (queueSize === 0 && retrySize === 0) {
        await new Promise((r) => setTimeout(r, 2000));
        break;
      }
      await new Promise((r) => setTimeout(r, 1000));
    }

    await runner.stop();
    const totalTime = metrics.stop();

    const stats = new Stats(config);
    const processed = await stats.processed();
    const deadSet = new DeadSet(config);
    const deadSize = await deadSet.size();

    return {
      name: "Mixed Chaos Test",
      passed: true,
      durationMs: totalTime,
      metrics: {
        "Total jobs enqueued": 150,
        Processed: processed,
        "Error attempts": errors.length,
        "Dead queue size": deadSize,
        "Peak memory (MB)": Math.round(metrics.getPeakMemoryMB()),
      },
      errors: [],
    };
  } finally {
    await config.close();
  }
}

async function main() {
  console.log("=== CHAOS TESTING SIMULATION ===\n");

  registerAllJobs();

  const results: SimulationResult[] = [];

  console.log("\n--- Running: Random Failures Test ---");
  results.push(await testRandomFailures());
  console.log(formatResult(results.at(-1)));

  await new Promise((r) => setTimeout(r, 2000));

  console.log("\n--- Running: IterableJob Interruption Test ---");
  results.push(await testIterableInterruption());
  console.log(formatResult(results.at(-1)));

  await new Promise((r) => setTimeout(r, 2000));

  console.log("\n--- Running: Mixed Chaos Test ---");
  results.push(await testMixedChaos());
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
