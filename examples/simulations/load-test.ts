/**
 * Load Testing Simulation
 *
 * Tests high-volume job processing to measure throughput, memory usage,
 * and system stability under load.
 *
 * Run with: npx tsx examples/simulations/load-test.ts
 */

import { Queue, Stats } from "../../src/api.js";
import { Config } from "../../src/config.js";
import { Sidekiq } from "../../src/sidekiq.js";
import { NoOpJob, registerAllJobs } from "./shared/jobs.js";
import {
  formatResult,
  MetricsCollector,
  type SimulationResult,
  waitForQueueDrain,
} from "./shared/metrics.js";

interface LoadTestOptions {
  totalJobs: number;
  batchSize: number;
  concurrency: number;
}

async function runLoadTest(
  options: LoadTestOptions
): Promise<SimulationResult> {
  const { totalJobs, batchSize, concurrency } = options;
  const errors: string[] = [];
  const metrics = new MetricsCollector();

  const config = new Config({
    concurrency,
    queues: ["simulation"],
    timeout: 10,
    pollIntervalAverage: 1,
    errorHandlers: [
      (error) => {
        errors.push(error.message);
      },
    ],
  });

  try {
    // Clear any existing jobs and state
    const redis = await config.getRedisClient();
    await redis.del("queue:simulation");
    await redis.del("retry");
    await redis.del("dead");

    // Reset stats
    await redis.set("stat:processed", "0");
    await redis.set("stat:failed", "0");

    metrics.start();

    // Enqueue jobs in batches
    console.log(`Enqueueing ${totalJobs} jobs in batches of ${batchSize}...`);
    const enqueueStart = Date.now();

    for (let i = 0; i < totalJobs; i += batchSize) {
      const batch = Math.min(batchSize, totalJobs - i);
      const args: [number][] = Array.from({ length: batch }, (_, j) => [i + j]);
      await NoOpJob.performBulk(args);
    }

    const enqueueTime = Date.now() - enqueueStart;
    console.log(`Enqueued in ${enqueueTime}ms`);

    // Start worker
    console.log(`Starting worker with concurrency=${concurrency}...`);
    const processStart = Date.now();
    const runner = await Sidekiq.run({ config });

    // Wait for queue to drain
    const queue = new Queue("simulation", config);
    const drained = await waitForQueueDrain(queue, 120_000); // 2 min timeout

    const processTime = Date.now() - processStart;
    await runner.stop();
    const totalTime = metrics.stop();

    // Collect final stats
    const stats = new Stats(config);
    const processed = await stats.processed();
    const failed = await stats.failed();

    const jobsPerSecond = processed / (processTime / 1000);

    return {
      name: `Load Test (${totalJobs} jobs, concurrency=${concurrency})`,
      passed: drained && processed === totalJobs && failed === 0,
      durationMs: totalTime,
      metrics: {
        "Total jobs": totalJobs,
        Concurrency: concurrency,
        Processed: processed,
        Failed: failed,
        "Enqueue time (ms)": enqueueTime,
        "Process time (ms)": processTime,
        "Jobs/second": Math.round(jobsPerSecond),
        "Peak memory (MB)": Math.round(metrics.getPeakMemoryMB()),
        "Queue drained": drained ? "Yes" : "No",
      },
      errors,
    };
  } finally {
    await config.close();
  }
}

async function runThroughputComparison(): Promise<SimulationResult[]> {
  const results: SimulationResult[] = [];
  const configs = [
    { totalJobs: 5000, batchSize: 500, concurrency: 5 },
    { totalJobs: 5000, batchSize: 500, concurrency: 10 },
    { totalJobs: 5000, batchSize: 500, concurrency: 25 },
  ];

  for (const options of configs) {
    console.log(
      `\n--- Running load test: concurrency=${options.concurrency} ---`
    );
    const result = await runLoadTest(options);
    results.push(result);
    console.log(formatResult(result));

    // Brief pause between tests
    await new Promise((r) => setTimeout(r, 1000));
  }

  return results;
}

async function main() {
  console.log("=== LOAD TESTING SIMULATION ===\n");

  registerAllJobs();

  const results = await runThroughputComparison();

  // Summary
  console.log(`\n${"=".repeat(60)}`);
  console.log("SUMMARY");
  console.log("=".repeat(60));

  const allPassed = results.every((r) => r.passed);
  for (const result of results) {
    const status = result.passed ? "PASS" : "FAIL";
    const rate = result.metrics["Jobs/second"];
    console.log(`[${status}] ${result.name}: ${rate} jobs/sec`);
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
