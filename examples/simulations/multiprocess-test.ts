/**
 * Multiprocess Workload Simulation
 *
 * Tests:
 * 1. Multi-Consumer Competition - 10 processes competing for 5000 jobs
 * 2. Signal Handling - SIGTERM, SIGINT, remote quiet/stop via Redis API
 * 3. Process Churn - Random spawn/kill over 60s (Kubernetes-like)
 * 4. Graceful Requeue - Jobs requeued when processes killed mid-work
 *
 * Run with: npx tsx examples/simulations/multiprocess-test.ts
 */

import { type ChildProcess, spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Process, ProcessSet, Queue, Stats } from "../../src/api.js";
import { Config } from "../../src/config.js";
import { registerAllJobs, TrackableJob } from "./shared/jobs.js";
import {
  formatResult,
  MetricsCollector,
  type SimulationResult,
  waitForQueueDrain,
} from "./shared/metrics.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface WorkerState {
  child: ChildProcess;
  pid: number;
  spawnedAt: number;
  lifecycleEvents: string[];
  signalsSent: string[];
}

class ProcessManager {
  private readonly workers = new Map<number, WorkerState>();
  private readonly config: Config;
  private readonly metrics = {
    spawned: 0,
    exited: 0,
    signals: { SIGTERM: 0, SIGINT: 0, remoteQuiet: 0, remoteStop: 0 },
  };

  constructor(config: Config) {
    this.config = config;
  }

  spawnWorker(
    options: { concurrency?: number; timeout?: number } = {}
  ): number {
    const workerScript = join(__dirname, "shared", "multiprocess-worker.ts");
    const child = spawn("npx", ["tsx", workerScript], {
      cwd: join(__dirname, "../.."),
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        CONCURRENCY: String(options.concurrency ?? 5),
        TIMEOUT: String(options.timeout ?? 10),
        HEARTBEAT_INTERVAL: "0.5",
      },
    });

    const pid = child.pid ?? 0;
    if (!pid) {
      return 0;
    }
    const state: WorkerState = {
      child,
      pid,
      spawnedAt: Date.now(),
      lifecycleEvents: [],
      signalsSent: [],
    };

    this.workers.set(pid, state);
    this.metrics.spawned++;

    // Parse JSON messages from stdout
    child.stdout?.on("data", (data: Buffer) => {
      const lines = data.toString().split("\n").filter(Boolean);
      for (const line of lines) {
        try {
          const msg = JSON.parse(line);
          if (msg.type === "lifecycle") {
            state.lifecycleEvents.push(msg.event);
          }
        } catch {
          // Ignore non-JSON output
        }
      }
    });

    child.stderr?.on("data", (data: Buffer) => {
      // Suppress stderr noise during normal operation
      const text = data.toString();
      if (text.includes("Error") || text.includes("error")) {
        process.stderr.write(`[worker ${pid}] ${text}`);
      }
    });

    child.on("exit", () => {
      this.workers.delete(pid);
      this.metrics.exited++;
    });

    return pid;
  }

  async killWorker(
    pid: number,
    method: "SIGTERM" | "SIGINT" | "remoteQuiet" | "remoteStop"
  ): Promise<void> {
    const worker = this.workers.get(pid);
    if (!worker) {
      return;
    }

    worker.signalsSent.push(method);

    switch (method) {
      case "SIGTERM":
        worker.child.kill("SIGTERM");
        this.metrics.signals.SIGTERM++;
        break;
      case "SIGINT":
        worker.child.kill("SIGINT");
        this.metrics.signals.SIGINT++;
        break;
      case "remoteQuiet":
      case "remoteStop":
        await this.sendRemoteSignal(pid, method);
        break;
      default:
        break;
    }
  }

  private async sendRemoteSignal(
    pid: number,
    method: "remoteQuiet" | "remoteStop"
  ): Promise<void> {
    const processSet = new ProcessSet(this.config);
    const entries = await processSet.entries();

    // Find the process entry matching this pid
    const entry = entries.find((e) => e.info.pid === pid);
    if (!entry) {
      // Process may not have registered yet, try direct kill
      const worker = this.workers.get(pid);
      if (worker) {
        worker.child.kill("SIGTERM");
        this.metrics.signals.SIGTERM++;
      }
      return;
    }

    const proc = new Process(entry, this.config);
    if (method === "remoteQuiet") {
      await proc.quietProcess();
      this.metrics.signals.remoteQuiet++;
    } else {
      await proc.stopProcess();
      this.metrics.signals.remoteStop++;
    }
  }

  getActiveCount(): number {
    return this.workers.size;
  }

  getAllPids(): number[] {
    return Array.from(this.workers.keys());
  }

  getWorker(pid: number): WorkerState | undefined {
    return this.workers.get(pid);
  }

  async stopAll(): Promise<void> {
    const pids = this.getAllPids();
    for (const pid of pids) {
      const worker = this.workers.get(pid);
      worker?.child.kill("SIGTERM");
    }
    await this.waitForAllExit(15_000);
  }

  private async waitForAllExit(timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.workers.size > 0 && Date.now() < deadline) {
      await sleep(100);
    }
  }

  getMetrics() {
    return { ...this.metrics, activeWorkers: this.workers.size };
  }
}

async function clearRedisState(config: Config): Promise<void> {
  const redis = await config.getRedisClient();
  await redis.del("queue:simulation");
  await redis.del("retry");
  await redis.del("dead");
  await redis.set("stat:processed", "0");
  await redis.set("stat:failed", "0");
  await redis.del("stat:duplicates");
  await redis.del("job:completions");
  // Clear tracked keys
  const trackKeys = await redis.keys("tracked:*");
  if (trackKeys.length > 0) {
    await redis.del(trackKeys);
  }
}

// Test 1: Multi-Consumer Competition
async function testMultiConsumer(config: Config): Promise<SimulationResult> {
  const metrics = new MetricsCollector();
  const manager = new ProcessManager(config);

  await clearRedisState(config);
  metrics.start();

  // Spawn 10 processes simultaneously
  console.log("Spawning 10 worker processes...");
  for (let i = 0; i < 10; i++) {
    manager.spawnWorker({ concurrency: 5 });
  }

  // Wait for processes to register
  await sleep(3000);

  // Verify registration
  const processSet = new ProcessSet(config);
  const entries = await processSet.entries();
  console.log(`Registered processes: ${entries.length}`);

  // Enqueue large batch of quick jobs
  const totalJobs = 5000;
  console.log(`Enqueueing ${totalJobs} jobs for concurrent processing...`);

  const batchSize = 500;
  for (let i = 0; i < totalJobs; i += batchSize) {
    const args: [string, number][] = Array.from(
      { length: Math.min(batchSize, totalJobs - i) },
      (_, j) => [`multi-${i + j}`, 10] // 10ms jobs
    );
    await TrackableJob.performBulk(args);
  }

  // Wait for queue to drain
  console.log("Waiting for queue to drain...");
  const queue = new Queue("simulation", config);
  await waitForQueueDrain(queue, 120_000);

  // Stop all workers
  await manager.stopAll();
  const totalTime = metrics.stop();

  // Verify results
  const redis = await config.getRedisClient();
  const duplicates = Number.parseInt(
    (await redis.get("stat:duplicates")) ?? "0",
    10
  );
  const completions = await redis.hLen("job:completions");
  const stats = new Stats(config);
  const processed = await stats.processed();

  const managerMetrics = manager.getMetrics();

  return {
    name: "Multi-Consumer Competition Test",
    passed: duplicates === 0 && completions === totalJobs,
    durationMs: totalTime,
    metrics: {
      "Total jobs": totalJobs,
      "Jobs completed": completions,
      "Duplicate processing": duplicates,
      "Processed stat": processed,
      "Peak concurrent processes": entries.length,
      "Jobs/second": Math.round(processed / (totalTime / 1000)),
      "Processes spawned": managerMetrics.spawned,
    },
    errors: buildErrors(duplicates, completions, totalJobs),
  };
}

function buildErrors(
  duplicates: number,
  completions: number,
  expected: number
): string[] {
  if (duplicates > 0) {
    return [`${duplicates} duplicate job executions detected`];
  }
  if (completions !== expected) {
    return [`Expected ${expected} completions, got ${completions}`];
  }
  return [];
}

// Test 2: Signal Handling
async function testSignalHandling(config: Config): Promise<SimulationResult> {
  const results: {
    method: string;
    success: boolean;
    gracefulExit: boolean;
  }[] = [];

  const signalMethods: ("SIGTERM" | "SIGINT" | "remoteQuiet" | "remoteStop")[] =
    ["SIGTERM", "SIGINT", "remoteQuiet", "remoteStop"];

  for (const method of signalMethods) {
    console.log(`Testing ${method}...`);
    await clearRedisState(config);

    const manager = new ProcessManager(config);
    const pid = manager.spawnWorker({ concurrency: 2, timeout: 5 });

    // Wait for process to register with Redis
    await sleep(2000);

    // Enqueue some jobs
    for (let i = 0; i < 10; i++) {
      await TrackableJob.performAsync(`signal-test-${method}-${i}`, 500);
    }

    // Wait for jobs to start
    await sleep(1000);

    // Send signal
    await manager.killWorker(pid, method);

    // Wait for exit
    const startWait = Date.now();
    while (manager.getActiveCount() > 0 && Date.now() - startWait < 15_000) {
      await sleep(100);
    }

    const exited = manager.getActiveCount() === 0;
    const worker = manager.getWorker(pid);
    const hasShutdownEvent =
      worker?.lifecycleEvents.includes("shutdown") ?? false;

    results.push({
      method,
      success: exited,
      gracefulExit: hasShutdownEvent,
    });

    // Cleanup if still running
    if (!exited) {
      await manager.stopAll();
    }

    await sleep(500);
  }

  const allPassed = results.every((r) => r.success);

  return {
    name: "Signal Handling Test",
    passed: allPassed,
    durationMs: 0,
    metrics: Object.fromEntries(
      results.map((r) => [r.method, r.success ? "PASS" : "FAIL"])
    ),
    errors: results
      .filter((r) => !r.success)
      .map((r) => `${r.method} did not terminate correctly`),
  };
}

// Test 3: Process Churn (Kubernetes-like pod scaling)
async function testProcessChurn(config: Config): Promise<SimulationResult> {
  const metrics = new MetricsCollector();
  const manager = new ProcessManager(config);

  await clearRedisState(config);
  metrics.start();

  // Enqueue workload
  const totalJobs = 1000;
  console.log(`Enqueueing ${totalJobs} trackable jobs...`);
  for (let i = 0; i < totalJobs; i++) {
    await TrackableJob.performAsync(`churn-${i}`, Math.random() * 100 + 50);
  }

  // Spawn initial processes
  console.log("Spawning initial worker processes...");
  for (let i = 0; i < 7; i++) {
    manager.spawnWorker();
    await sleep(200); // Stagger startup
  }

  // Run chaos loop for 60 seconds
  const testDuration = 60_000;
  const endTime = Date.now() + testDuration;
  const churnEvents: string[] = [];

  console.log(`Running process churn for ${testDuration / 1000}s...`);

  while (Date.now() < endTime) {
    const activeCount = manager.getActiveCount();

    // Random action: spawn, kill, or wait
    const action = Math.random();

    if (action < 0.2 && activeCount < 10) {
      // Spawn new process (20% chance)
      const pid = manager.spawnWorker();
      churnEvents.push(`spawn:${pid}`);
    } else if (action < 0.4 && activeCount > 5) {
      // Kill random process (20% chance)
      const pids = manager.getAllPids();
      const targetPid = pids[Math.floor(Math.random() * pids.length)];
      const killMethods: (
        | "SIGTERM"
        | "SIGINT"
        | "remoteQuiet"
        | "remoteStop"
      )[] = ["SIGTERM", "SIGINT", "remoteQuiet", "remoteStop"];
      const killMethod = killMethods[Math.floor(Math.random() * 4)];
      await manager.killWorker(targetPid, killMethod);
      churnEvents.push(`kill:${targetPid}:${killMethod}`);
    }

    await sleep(2000 + Math.random() * 3000); // 2-5 second intervals
  }

  // Wait for queue to drain
  console.log("Waiting for queue to drain...");
  const queue = new Queue("simulation", config);
  await waitForQueueDrain(queue, 60_000);

  // Stop all workers
  await manager.stopAll();
  const totalTime = metrics.stop();

  // Verify results
  const redis = await config.getRedisClient();
  const duplicates = Number.parseInt(
    (await redis.get("stat:duplicates")) ?? "0",
    10
  );
  const completions = await redis.hLen("job:completions");
  const stats = new Stats(config);
  const processed = await stats.processed();

  const managerMetrics = manager.getMetrics();

  return {
    name: `Process Churn Test (${testDuration / 1000}s)`,
    passed: duplicates === 0 && completions === totalJobs,
    durationMs: totalTime,
    metrics: {
      "Total jobs": totalJobs,
      "Jobs completed": completions,
      "Duplicate processing": duplicates,
      "Processed stat": processed,
      "Processes spawned": managerMetrics.spawned,
      "Processes exited": managerMetrics.exited,
      "Churn events": churnEvents.length,
      "SIGTERM signals": managerMetrics.signals.SIGTERM,
      "SIGINT signals": managerMetrics.signals.SIGINT,
      "Remote quiet signals": managerMetrics.signals.remoteQuiet,
      "Remote stop signals": managerMetrics.signals.remoteStop,
    },
    errors: buildErrors(duplicates, completions, totalJobs),
  };
}

// Test 4: Graceful Shutdown with Requeue
async function testGracefulRequeue(config: Config): Promise<SimulationResult> {
  const manager = new ProcessManager(config);

  await clearRedisState(config);

  // Spawn workers
  console.log("Spawning 5 workers...");
  for (let i = 0; i < 5; i++) {
    manager.spawnWorker({ concurrency: 3, timeout: 5 });
  }
  await sleep(2000);

  // Enqueue long-running jobs
  const totalJobs = 50;
  console.log(`Enqueueing ${totalJobs} long-running jobs (5s each)...`);
  for (let i = 0; i < totalJobs; i++) {
    await TrackableJob.performAsync(`requeue-${i}`, 5000);
  }

  // Wait for jobs to start processing
  await sleep(3000);

  // Kill all processes at once
  console.log("Killing all processes simultaneously...");
  for (const pid of manager.getAllPids()) {
    manager.getWorker(pid)?.child.kill("SIGTERM");
  }

  // Wait for exit
  await sleep(10_000);

  // Check queue for requeued jobs
  const queue = new Queue("simulation", config);
  const requeuedCount = await queue.size();
  console.log(`Jobs requeued: ${requeuedCount}`);

  // Spawn new workers to process remaining
  console.log("Spawning new workers to complete remaining jobs...");
  for (let i = 0; i < 5; i++) {
    manager.spawnWorker({ concurrency: 5, timeout: 10 });
  }
  await sleep(2000);

  // Wait for completion
  await waitForQueueDrain(queue, 120_000);
  await manager.stopAll();

  // Verify all jobs completed
  const redis = await config.getRedisClient();
  const completions = await redis.hLen("job:completions");
  const duplicates = Number.parseInt(
    (await redis.get("stat:duplicates")) ?? "0",
    10
  );

  return {
    name: "Graceful Requeue Test",
    passed: completions === totalJobs && duplicates === 0,
    durationMs: 0,
    metrics: {
      "Total jobs": totalJobs,
      "Jobs requeued": requeuedCount,
      "Jobs completed": completions,
      "Duplicate processing": duplicates,
    },
    errors: buildRequeueErrors(completions, duplicates, totalJobs),
  };
}

function buildRequeueErrors(
  completions: number,
  duplicates: number,
  expected: number
): string[] {
  if (completions !== expected) {
    return [`Expected ${expected} completions, got ${completions}`];
  }
  if (duplicates > 0) {
    return [`${duplicates} duplicate job executions`];
  }
  return [];
}

async function main() {
  console.log(
    "================================================================"
  );
  console.log(
    "       MULTIPROCESS WORKLOAD SIMULATION                        "
  );
  console.log(
    "================================================================"
  );
  console.log();

  registerAllJobs();

  const config = new Config({
    queues: ["simulation"],
    timeout: 10,
    pollIntervalAverage: 0.5,
    heartbeatInterval: 0.5,
  });

  const results: SimulationResult[] = [];

  try {
    console.log("\n--- Test 1: Multi-Consumer Competition ---");
    const result1 = await testMultiConsumer(config);
    results.push(result1);
    console.log(formatResult(result1));

    await sleep(2000);

    console.log("\n--- Test 2: Signal Handling ---");
    const result2 = await testSignalHandling(config);
    results.push(result2);
    console.log(formatResult(result2));

    await sleep(2000);

    console.log("\n--- Test 3: Process Churn ---");
    const result3 = await testProcessChurn(config);
    results.push(result3);
    console.log(formatResult(result3));

    await sleep(2000);

    console.log("\n--- Test 4: Graceful Requeue ---");
    const result4 = await testGracefulRequeue(config);
    results.push(result4);
    console.log(formatResult(result4));
  } finally {
    await config.close();
  }

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
