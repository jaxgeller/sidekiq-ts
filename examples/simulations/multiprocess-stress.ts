/**
 * Multiprocess Stress Test
 *
 * Runs for 5 minutes with high load:
 * - 15-20 concurrent processes
 * - Continuous job enqueueing
 * - Aggressive process churn
 * - All signal types
 *
 * Run with: npx tsx examples/simulations/multiprocess-stress.ts
 */

import { type ChildProcess, spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Process, ProcessSet, Queue, Stats } from "../../src/api.js";
import { Config } from "../../src/config.js";
import { registerAllJobs, TrackableJob } from "./shared/jobs.js";
import { formatResult, type SimulationResult } from "./shared/metrics.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface WorkerState {
  child: ChildProcess;
  pid: number;
  spawnedAt: number;
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
        CONCURRENCY: String(options.concurrency ?? 10),
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
    };

    this.workers.set(pid, state);
    this.metrics.spawned++;

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
    const entry = entries.find((e) => e.info.pid === pid);
    if (!entry) {
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

  async stopAll(): Promise<void> {
    const pids = this.getAllPids();
    for (const pid of pids) {
      const worker = this.workers.get(pid);
      worker?.child.kill("SIGTERM");
    }
    const deadline = Date.now() + 15_000;
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
  const trackKeys = await redis.keys("tracked:*");
  if (trackKeys.length > 0) {
    await redis.del(trackKeys);
  }
}

async function printStatus(
  config: Config,
  elapsed: number,
  activeCount: number,
  totalEnqueued: number,
  churnEvents: number
): Promise<void> {
  const stats = new Stats(config);
  const processed = await stats.processed();
  const queue = new Queue("simulation", config);
  const queueSize = await queue.size();
  const redis = await config.getRedisClient();
  const duplicates = Number.parseInt(
    (await redis.get("stat:duplicates")) ?? "0",
    10
  );

  console.log(
    `[${Math.floor(elapsed / 1000)}s] ` +
      `Processes: ${activeCount} | ` +
      `Enqueued: ${totalEnqueued} | ` +
      `Processed: ${processed} | ` +
      `Queue: ${queueSize} | ` +
      `Duplicates: ${duplicates} | ` +
      `Churn: ${churnEvents}`
  );
}

async function performChurn(
  manager: ProcessManager,
  activeCount: number,
  minProcesses: number,
  maxProcesses: number
): Promise<boolean> {
  const action = Math.random();

  if (action < 0.15 && activeCount < maxProcesses) {
    manager.spawnWorker({ concurrency: 10, timeout: 10 });
    return true;
  }
  if (action < 0.25 && activeCount > minProcesses) {
    const pids = manager.getAllPids();
    const targetPid = pids[Math.floor(Math.random() * pids.length)];
    const killMethods: ("SIGTERM" | "SIGINT" | "remoteQuiet" | "remoteStop")[] =
      ["SIGTERM", "SIGINT", "remoteQuiet", "remoteStop"];
    const killMethod = killMethods[Math.floor(Math.random() * 4)];
    await manager.killWorker(targetPid, killMethod);
    return true;
  }
  return false;
}

async function waitForDrain(config: Config, timeoutMs: number): Promise<void> {
  const queue = new Queue("simulation", config);
  const drainStart = Date.now();

  while (Date.now() - drainStart < timeoutMs) {
    const queueSize = await queue.size();
    if (queueSize === 0) {
      break;
    }
    if ((Date.now() - drainStart) % 10_000 < 500) {
      console.log(`Queue size: ${queueSize}, waiting...`);
    }
    await sleep(500);
  }
}

async function runStressTest(config: Config): Promise<SimulationResult> {
  const manager = new ProcessManager(config);
  const testDuration = 5 * 60 * 1000; // 5 minutes
  const minProcesses = 15;
  const maxProcesses = 20;
  const startTime = Date.now();

  await clearRedisState(config);

  console.log(`Starting 5-minute stress test at ${new Date().toISOString()}`);
  console.log(
    `Target: ${minProcesses}-${maxProcesses} processes, continuous job enqueueing\n`
  );

  // Spawn initial processes
  console.log(`Spawning ${minProcesses} initial worker processes...`);
  for (let i = 0; i < minProcesses; i++) {
    manager.spawnWorker({ concurrency: 10, timeout: 10 });
  }
  await sleep(3000);

  let totalEnqueued = 0;
  let churnEvents = 0;
  let lastStatusTime = Date.now();
  const statusInterval = 30_000;

  // Main stress loop
  while (Date.now() - startTime < testDuration) {
    const elapsed = Date.now() - startTime;
    const activeCount = manager.getActiveCount();

    if (Date.now() - lastStatusTime >= statusInterval) {
      await printStatus(
        config,
        elapsed,
        activeCount,
        totalEnqueued,
        churnEvents
      );
      lastStatusTime = Date.now();
    }

    // Enqueue batch of jobs
    const batchSize = 100;
    const args: [string, number][] = Array.from(
      { length: batchSize },
      (_, j) => [
        `stress-${totalEnqueued + j}`,
        20 + Math.floor(Math.random() * 80),
      ]
    );
    await TrackableJob.performBulk(args);
    totalEnqueued += batchSize;

    // Random process churn
    const didChurn = await performChurn(
      manager,
      activeCount,
      minProcesses,
      maxProcesses
    );
    if (didChurn) {
      churnEvents++;
    }

    await sleep(200 + Math.random() * 300);
  }

  console.log("\nStress phase complete. Waiting for queue to drain...");
  await waitForDrain(config, 120_000);

  // Stop all workers
  console.log("Stopping all workers...");
  await manager.stopAll();

  const totalTime = Date.now() - startTime;

  // Verify results
  const redis = await config.getRedisClient();
  const duplicates = Number.parseInt(
    (await redis.get("stat:duplicates")) ?? "0",
    10
  );
  const completions = await redis.hLen("job:completions");
  const stats = new Stats(config);
  const processed = await stats.processed();
  const queue = new Queue("simulation", config);
  const finalQueueSize = await queue.size();

  const managerMetrics = manager.getMetrics();

  const passed =
    duplicates === 0 && completions === totalEnqueued && finalQueueSize === 0;

  return {
    name: "5-Minute Stress Test",
    passed,
    durationMs: totalTime,
    metrics: {
      Duration: `${Math.floor(totalTime / 1000)}s`,
      "Total jobs enqueued": totalEnqueued,
      "Jobs completed": completions,
      "Processed stat": processed,
      "Final queue size": finalQueueSize,
      "Duplicate processing": duplicates,
      "Processes spawned": managerMetrics.spawned,
      "Processes exited": managerMetrics.exited,
      "Churn events": churnEvents,
      "SIGTERM signals": managerMetrics.signals.SIGTERM,
      "SIGINT signals": managerMetrics.signals.SIGINT,
      "Remote quiet signals": managerMetrics.signals.remoteQuiet,
      "Remote stop signals": managerMetrics.signals.remoteStop,
      "Jobs/second": Math.round(processed / (totalTime / 1000)),
    },
    errors: buildStressErrors(
      duplicates,
      completions,
      totalEnqueued,
      finalQueueSize
    ),
  };
}

function buildStressErrors(
  duplicates: number,
  completions: number,
  expected: number,
  queueSize: number
): string[] {
  const errors: string[] = [];
  if (duplicates > 0) {
    errors.push(`${duplicates} duplicate job executions detected`);
  }
  if (completions !== expected) {
    errors.push(`Expected ${expected} completions, got ${completions}`);
  }
  if (queueSize > 0) {
    errors.push(`Queue not fully drained: ${queueSize} jobs remaining`);
  }
  return errors;
}

async function main() {
  console.log(
    "================================================================"
  );
  console.log(
    "       MULTIPROCESS 5-MINUTE STRESS TEST                       "
  );
  console.log(
    "================================================================\n"
  );

  registerAllJobs();

  const config = new Config({
    queues: ["simulation"],
    timeout: 10,
    pollIntervalAverage: 0.5,
    heartbeatInterval: 0.5,
  });

  try {
    const result = await runStressTest(config);
    console.log(formatResult(result));

    console.log(`\n${"=".repeat(60)}`);
    console.log(result.passed ? "STRESS TEST PASSED" : "STRESS TEST FAILED");
    console.log("=".repeat(60));

    process.exit(result.passed ? 0 : 1);
  } finally {
    await config.close();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
