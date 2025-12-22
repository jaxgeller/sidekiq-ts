/**
 * Leader Election Partition Simulation
 *
 * Scenario:
 * 1. Elect a leader among multiple processes
 * 2. Force a Redis disconnect on the leader (simulated partition)
 * 3. Verify a new leader is elected without split brain
 * 4. Reconnect the partitioned leader and ensure stability
 *
 * Run with: npx tsx examples/simulations/leader-partition-test.ts
 */

import { type ChildProcess, execSync, spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Config } from "../../src/config.js";
import { registerAllJobs } from "./shared/jobs.js";
import { formatResult, type SimulationResult } from "./shared/metrics.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface LeaderWorkerState {
  child: ChildProcess;
  pid: number;
  isLeader: boolean;
  lastStatusAt: number;
  leaderTransitions: number;
}

interface LeaderMetrics {
  spawned: number;
  splitBrainEvents: number;
  maxConcurrentLeaders: number;
  leaderTransitions: number;
  redisDisconnects: number;
  redisReconnects: number;
}

class LeaderProcessManager {
  private readonly workers = new Map<number, LeaderWorkerState>();
  private readonly metrics: LeaderMetrics = {
    spawned: 0,
    splitBrainEvents: 0,
    maxConcurrentLeaders: 0,
    leaderTransitions: 0,
    redisDisconnects: 0,
    redisReconnects: 0,
  };

  spawnWorker(): number {
    const workerScript = join(__dirname, "shared", "leader-worker.ts");
    const child = spawn("npx", ["tsx", workerScript], {
      cwd: join(__dirname, "../.."),
      stdio: ["ignore", "pipe", "pipe"],
      detached: true,
      env: {
        ...process.env,
        CONCURRENCY: "1",
        TIMEOUT: "5",
        STATUS_INTERVAL: "200",
        LEADER_REFRESH_INTERVAL: "1000",
        LEADER_CHECK_INTERVAL: "2000",
        LEADER_TTL: "3",
      },
    });

    const pid = child.pid ?? 0;
    if (!pid) {
      return 0;
    }

    const state: LeaderWorkerState = {
      child,
      pid,
      isLeader: false,
      lastStatusAt: 0,
      leaderTransitions: 0,
    };

    this.workers.set(pid, state);
    this.metrics.spawned++;

    let actualPidSet = false;

    // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: message parsing requires multiple conditions
    child.stdout?.on("data", (data: Buffer) => {
      const lines = data.toString().split("\n").filter(Boolean);
      for (const line of lines) {
        try {
          const msg = JSON.parse(line);

          if (msg.pid && !actualPidSet) {
            const actualPid = msg.pid as number;
            if (actualPid !== pid) {
              this.workers.delete(pid);
              state.pid = actualPid;
              this.workers.set(actualPid, state);
            }
            actualPidSet = true;
          }

          if (msg.type === "lifecycle") {
            if (msg.event === "became_leader") {
              const wasLeader = state.isLeader;
              state.isLeader = true;
              if (!wasLeader) {
                state.leaderTransitions++;
                this.metrics.leaderTransitions++;
              }
            } else if (msg.event === "became_follower") {
              state.isLeader = false;
            } else if (msg.event === "redis_disconnected") {
              this.metrics.redisDisconnects++;
            } else if (msg.event === "redis_reconnected") {
              this.metrics.redisReconnects++;
            }
          } else if (msg.type === "status") {
            const wasLeader = state.isLeader;
            state.isLeader = msg.isLeader;
            state.lastStatusAt = Date.now();

            if (msg.isLeader && !wasLeader) {
              state.leaderTransitions++;
              this.metrics.leaderTransitions++;
            }
          }
        } catch {
          // Ignore non-JSON output
        }
      }
    });

    child.stderr?.on("data", (data: Buffer) => {
      const text = data.toString();
      if (
        text.includes("Error") ||
        (text.includes("error") && !text.includes("Redis error"))
      ) {
        process.stderr.write(`[worker ${pid}] ${text}`);
      }
    });

    child.on("exit", () => {
      this.workers.delete(state.pid);
    });

    return pid;
  }

  signalWorker(pid: number, signal: NodeJS.Signals): void {
    const worker = this.workers.get(pid);
    if (!worker) {
      return;
    }
    const targetPids = new Set<number>();
    if (worker.child.pid) {
      targetPids.add(worker.child.pid);
    }
    targetPids.add(worker.pid);

    for (const targetPid of targetPids) {
      try {
        process.kill(targetPid, signal);
      } catch {
        // Process already dead or not permitted
      }
    }
  }

  countLeaders(): number {
    let count = 0;
    const now = Date.now();
    for (const worker of this.workers.values()) {
      if (worker.isLeader && now - worker.lastStatusAt < 2000) {
        count++;
      }
    }
    return count;
  }

  getLeaderPids(): number[] {
    const now = Date.now();
    const leaders: number[] = [];
    for (const worker of this.workers.values()) {
      if (worker.isLeader && now - worker.lastStatusAt < 2000) {
        leaders.push(worker.pid);
      }
    }
    return leaders;
  }

  checkSplitBrain(): boolean {
    const leaderCount = this.countLeaders();
    if (leaderCount > this.metrics.maxConcurrentLeaders) {
      this.metrics.maxConcurrentLeaders = leaderCount;
    }
    if (leaderCount > 1) {
      this.metrics.splitBrainEvents++;
      return true;
    }
    return false;
  }

  async waitForLeader(timeoutMs: number): Promise<number | null> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const leaders = this.getLeaderPids();
      if (leaders.length === 1) {
        return leaders[0];
      }
      await sleep(100);
    }
    return null;
  }

  async stopAll(): Promise<void> {
    const allPids = new Set<number>();
    for (const worker of this.workers.values()) {
      allPids.add(worker.pid);
      if (worker.child.pid) {
        allPids.add(worker.child.pid);
      }
    }

    for (const worker of this.workers.values()) {
      worker.child.kill("SIGTERM");
    }

    await this.waitForAllExit(3000);

    for (const pid of allPids) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // Process already dead
      }
    }

    for (const worker of this.workers.values()) {
      if (worker.child.pid) {
        try {
          process.kill(-worker.child.pid, "SIGKILL");
        } catch {
          // Process group already dead or doesn't exist
        }
      }
    }

    await sleep(2000);
  }

  private async waitForAllExit(timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.workers.size > 0 && Date.now() < deadline) {
      await sleep(100);
    }
    for (const worker of this.workers.values()) {
      worker.child.kill("SIGKILL");
      try {
        process.kill(worker.pid, "SIGKILL");
      } catch {
        // Process already dead
      }
    }
    await sleep(500);
  }

  getMetrics(): LeaderMetrics {
    return { ...this.metrics };
  }
}

async function clearLeaderKey(config: Config): Promise<void> {
  const redis = await config.getRedisClient();
  await redis.del("leader");
}

async function killAllOrphanWorkers(): Promise<void> {
  try {
    execSync("pkill -9 -f leader-worker.ts 2>/dev/null || true", {
      stdio: "ignore",
    });
  } catch {
    // pkill may not find anything, that's ok
  }
  await sleep(2000);
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: simulation test requires multiple conditions
async function testLeaderPartition(config: Config): Promise<SimulationResult> {
  console.log("Spawning 5 processes...");
  const manager = new LeaderProcessManager();
  await clearLeaderKey(config);

  for (let i = 0; i < 5; i++) {
    manager.spawnWorker();
  }

  await sleep(3000);
  const leaderPid = await manager.waitForLeader(5000);
  console.log(`Initial leader: ${leaderPid}`);

  const errors: string[] = [];
  if (!leaderPid) {
    errors.push("No leader elected before partition");
  }

  let newLeader: number | null = null;
  let failoverMs = 0;
  let leaderlessMs = 0;

  if (leaderPid) {
    console.log(`Disconnecting Redis for leader ${leaderPid}...`);
    manager.signalWorker(leaderPid, "SIGUSR1");

    const pollInterval = 200;
    const deadline = Date.now() + 15_000;
    const start = Date.now();

    while (Date.now() < deadline) {
      const leaders = manager.getLeaderPids();
      if (leaders.length === 0) {
        leaderlessMs += pollInterval;
      }
      if (leaders.length === 1 && leaders[0] !== leaderPid) {
        newLeader = leaders[0];
        failoverMs = Date.now() - start;
        break;
      }
      await sleep(pollInterval);
    }

    console.log(`New leader after partition: ${newLeader}`);

    if (!newLeader) {
      errors.push("No new leader elected after partition");
    } else if (newLeader === leaderPid) {
      errors.push("Leader did not change after partition");
    }

    console.log(`Reconnecting Redis for leader ${leaderPid}...`);
    manager.signalWorker(leaderPid, "SIGUSR2");
  }

  const monitorDuration = 8000;
  const monitorEnd = Date.now() + monitorDuration;
  let splitBrainDetected = false;
  while (Date.now() < monitorEnd) {
    if (manager.checkSplitBrain()) {
      splitBrainDetected = true;
      break;
    }
    await sleep(200);
  }

  const finalLeaderCount = manager.countLeaders();
  if (splitBrainDetected) {
    errors.push("Split brain detected after reconnection");
  }
  if (finalLeaderCount !== 1) {
    errors.push(
      `Expected 1 leader after reconnection, got ${finalLeaderCount}`
    );
  }

  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Leader Partition Recovery",
    passed: errors.length === 0,
    durationMs: 0,
    metrics: {
      "Processes spawned": metrics.spawned,
      "Initial leader pid": leaderPid ?? "none",
      "New leader pid": newLeader ?? "none",
      "Failover latency (ms)": failoverMs,
      "Leaderless time (ms)": leaderlessMs,
      "Split brain events": metrics.splitBrainEvents,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
      "Leader transitions": metrics.leaderTransitions,
      "Redis disconnects": metrics.redisDisconnects,
      "Redis reconnects": metrics.redisReconnects,
    },
    errors,
  };
}

async function main() {
  console.log(
    "================================================================"
  );
  console.log(
    "       LEADER PARTITION SIMULATION                             "
  );
  console.log(
    "================================================================"
  );
  console.log();

  registerAllJobs();

  const config = new Config({
    queues: ["simulation"],
    timeout: 5,
    pollIntervalAverage: 1,
    heartbeatInterval: 1,
  });

  await killAllOrphanWorkers();

  const results: SimulationResult[] = [];
  try {
    const result = await testLeaderPartition(config);
    results.push(result);
    console.log(formatResult(result));
  } finally {
    await config.close();
  }

  console.log(`\n${"=".repeat(64)}`);
  console.log("SUMMARY");
  console.log("=".repeat(64));

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
