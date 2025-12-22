/**
 * Leader Election Stress Test Simulation
 *
 * Tests:
 * 1. Basic Leader Election - Verify single leader with multiple processes
 * 2. Mass Spawn Race - 20 processes simultaneously competing for leadership
 * 3. Rapid Churn - Continuous spawn/kill chaos for 60 seconds
 * 4. Leader Assassination - SIGKILL leader repeatedly (ungraceful shutdown)
 * 5. Thundering Herd - Kill leader + spawn many new processes at once
 * 6. Extended Stress - 30 processes, 2 minutes, random chaos
 *
 * Run with: npx tsx examples/simulations/leader-election-test.ts
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
  spawnedAt: number;
  isLeader: boolean;
  lastStatusAt: number;
  lifecycleEvents: string[];
  leaderTransitions: number;
}

interface LeaderMetrics {
  spawned: number;
  exited: number;
  killed: number;
  splitBrainEvents: number;
  maxConcurrentLeaders: number;
  leaderTransitions: number;
  leadershipGaps: number; // Times when no leader existed
}

class LeaderProcessManager {
  private readonly workers = new Map<number, LeaderWorkerState>();
  private readonly metrics: LeaderMetrics = {
    spawned: 0,
    exited: 0,
    killed: 0,
    splitBrainEvents: 0,
    maxConcurrentLeaders: 0,
    leaderTransitions: 0,
    leadershipGaps: 0,
  };
  private lastLeaderPid: number | null = null;

  spawnWorker(): number {
    const workerScript = join(__dirname, "shared", "leader-worker.ts");
    const child = spawn("npx", ["tsx", workerScript], {
      cwd: join(__dirname, "../.."),
      stdio: ["ignore", "pipe", "pipe"],
      detached: true, // Create new process group for easier cleanup
      env: {
        ...process.env,
        CONCURRENCY: "1",
        TIMEOUT: "5",
        STATUS_INTERVAL: "200", // Report status every 200ms
        // Fast leader election for testing
        LEADER_REFRESH_INTERVAL: "1000", // Leader refreshes every 1s
        LEADER_CHECK_INTERVAL: "2000", // Followers check every 2s
        LEADER_TTL: "3", // Leader key TTL 3 seconds
      },
    });

    const pid = child.pid ?? 0;
    if (!pid) {
      return 0;
    }

    const state: LeaderWorkerState = {
      child,
      pid, // Will be updated to actual worker PID when first message arrives
      spawnedAt: Date.now(),
      isLeader: false,
      lastStatusAt: 0,
      lifecycleEvents: [],
      leaderTransitions: 0,
    };

    // Store initially by npx process PID
    this.workers.set(pid, state);
    this.metrics.spawned++;

    // Track whether we've remapped to actual worker PID
    let actualPidSet = false;

    // Parse JSON messages from stdout
    // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: message parsing requires multiple conditions
    child.stdout?.on("data", (data: Buffer) => {
      const lines = data.toString().split("\n").filter(Boolean);
      for (const line of lines) {
        try {
          const msg = JSON.parse(line);

          // Update PID mapping when we get the actual worker PID
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
            state.lifecycleEvents.push(msg.event);
            if (msg.event === "became_leader") {
              const wasLeader = state.isLeader;
              state.isLeader = true;
              if (!wasLeader) {
                state.leaderTransitions++;
                this.metrics.leaderTransitions++;
              }
            } else if (msg.event === "became_follower") {
              state.isLeader = false;
            }
          } else if (msg.type === "status") {
            const wasLeader = state.isLeader;
            state.isLeader = msg.isLeader;
            state.lastStatusAt = Date.now();

            // Track leader transitions
            if (msg.isLeader && !wasLeader) {
              state.leaderTransitions++;
              this.metrics.leaderTransitions++;
              if (
                this.lastLeaderPid !== null &&
                this.lastLeaderPid !== state.pid
              ) {
                // Leadership changed hands
              }
              this.lastLeaderPid = state.pid;
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
      // Use state.pid since it may have been remapped to actual worker PID
      this.workers.delete(state.pid);
      this.metrics.exited++;
    });

    // Return the npx process PID (will be remapped when first message arrives)
    return pid;
  }

  killWorker(pid: number, signal: "SIGTERM" | "SIGKILL" = "SIGTERM"): void {
    const worker = this.workers.get(pid);
    if (!worker) {
      return;
    }
    // Clear leader status immediately to avoid counting killed process
    worker.isLeader = false;
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

    if (worker.child.pid) {
      try {
        process.kill(-worker.child.pid, signal);
      } catch {
        // Process group already dead or doesn't exist
      }
    }
    this.metrics.killed++;
  }

  countLeaders(): number {
    let count = 0;
    const now = Date.now();
    for (const worker of this.workers.values()) {
      // Only count workers with recent status updates as leaders
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

  debugWorkerStates(): void {
    const now = Date.now();
    console.log("  Worker states:");
    for (const worker of this.workers.values()) {
      const statusAge = now - worker.lastStatusAt;
      console.log(
        `    PID ${worker.pid}: isLeader=${worker.isLeader}, lastStatus=${statusAge}ms ago`
      );
    }
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

  async waitForLeaderCount(
    expectedCount: number,
    timeoutMs: number
  ): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (this.countLeaders() === expectedCount) {
        return true;
      }
      await sleep(100);
    }
    return false;
  }

  getActiveCount(): number {
    return this.workers.size;
  }

  getAllPids(): number[] {
    return Array.from(this.workers.keys());
  }

  getRandomPid(): number | null {
    const pids = this.getAllPids();
    if (pids.length === 0) {
      return null;
    }
    return pids[Math.floor(Math.random() * pids.length)];
  }

  async stopAll(): Promise<void> {
    // Collect all known PIDs before killing (both npx and actual worker PIDs)
    const allPids = new Set<number>();
    for (const worker of this.workers.values()) {
      allPids.add(worker.pid);
      if (worker.child.pid) {
        allPids.add(worker.child.pid);
      }
    }

    // Send SIGTERM first for graceful shutdown
    for (const worker of this.workers.values()) {
      worker.child.kill("SIGTERM");
    }

    // Wait for graceful exit
    await this.waitForAllExit(3000);

    // Force kill any remaining processes by all known PIDs
    for (const pid of allPids) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // Process already dead
      }
    }

    // Also try to kill the entire process group
    for (const worker of this.workers.values()) {
      if (worker.child.pid) {
        try {
          process.kill(-worker.child.pid, "SIGKILL");
        } catch {
          // Process group already dead or doesn't exist
        }
      }
    }

    // Wait for cleanup
    await sleep(2000);
  }

  private async waitForAllExit(timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.workers.size > 0 && Date.now() < deadline) {
      await sleep(100);
    }
    // Force kill any remaining (both the npx wrapper and actual worker)
    for (const worker of this.workers.values()) {
      worker.child.kill("SIGKILL");
      // Also kill the actual worker process (which may have different PID from npx)
      try {
        process.kill(worker.pid, "SIGKILL");
      } catch {
        // Process already dead
      }
    }
    // Small wait for cleanup
    await sleep(500);
  }

  getMetrics(): LeaderMetrics {
    return { ...this.metrics };
  }

  resetMetrics(): void {
    this.metrics.splitBrainEvents = 0;
    this.metrics.maxConcurrentLeaders = 0;
    this.metrics.leaderTransitions = 0;
    this.metrics.leadershipGaps = 0;
  }
}

async function clearLeaderKey(config: Config): Promise<void> {
  const redis = await config.getRedisClient();
  await redis.del("leader");
}

async function killAllOrphanWorkers(): Promise<void> {
  // Kill any orphaned leader-worker processes from previous tests
  try {
    execSync("pkill -9 -f leader-worker.ts 2>/dev/null || true", {
      stdio: "ignore",
    });
  } catch {
    // pkill may not find anything, that's ok
  }
  // Wait for processes to fully exit
  await sleep(2000);
}

async function checkLeaderKey(config: Config): Promise<string | null> {
  const redis = await config.getRedisClient();
  return redis.get("leader");
}

// Test 1: Basic Leader Election
async function testBasicElection(config: Config): Promise<SimulationResult> {
  console.log("Spawning 5 processes...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn 5 processes
  for (let i = 0; i < 5; i++) {
    manager.spawnWorker();
  }

  // Wait for leader to be elected
  await sleep(3000);

  // Check for exactly one leader
  const leaderCount = manager.countLeaders();
  const splitBrain = manager.checkSplitBrain();
  const leaderPid = await manager.waitForLeader(5000);

  console.log(`Leader count: ${leaderCount}, Leader PID: ${leaderPid}`);

  // Kill the leader and verify new election
  let newLeader: number | null = null;
  if (leaderPid) {
    console.log(`Killing leader ${leaderPid}...`);
    manager.killWorker(leaderPid, "SIGTERM");

    // Wait for new leader (TTL 3s + follower check 2s = ~5s max)
    await sleep(2000);
    newLeader = await manager.waitForLeader(15_000);
    console.log(`New leader: ${newLeader}`);
  }

  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Basic Leader Election",
    passed:
      leaderCount === 1 &&
      !splitBrain &&
      metrics.splitBrainEvents === 0 &&
      leaderPid !== null &&
      newLeader !== null &&
      newLeader !== leaderPid,
    durationMs: 0,
    metrics: {
      "Initial leader count": leaderCount,
      "Split brain events": metrics.splitBrainEvents,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
      "Leader transitions": metrics.leaderTransitions,
    },
    errors: buildBasicElectionErrors(
      leaderCount,
      splitBrain,
      leaderPid,
      newLeader
    ),
  };
}

function buildBasicElectionErrors(
  leaderCount: number,
  splitBrain: boolean,
  leaderPid: number | null,
  newLeader: number | null
): string[] {
  const errors: string[] = [];
  if (leaderCount !== 1) {
    errors.push(`Expected 1 leader, got ${leaderCount}`);
  }
  if (splitBrain) {
    errors.push("Split brain detected!");
  }
  if (leaderPid === null) {
    errors.push("No leader detected during initial election");
  }
  if (leaderPid !== null && (newLeader === null || newLeader === leaderPid)) {
    errors.push("Leader did not change after termination");
  }
  return errors;
}

// Test 2: Mass Spawn Race
async function testMassSpawnRace(config: Config): Promise<SimulationResult> {
  console.log("Spawning 20 processes simultaneously...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn 20 processes at once
  for (let i = 0; i < 20; i++) {
    manager.spawnWorker();
  }

  // Wait for processes to start up and report status
  await sleep(5000);

  // Monitor for split-brain over 10 seconds
  let splitBrainDetected = false;
  const checkDuration = 10_000;
  const endTime = Date.now() + checkDuration;

  while (Date.now() < endTime) {
    if (manager.checkSplitBrain()) {
      splitBrainDetected = true;
      console.log(
        `SPLIT BRAIN! Leaders: ${manager.getLeaderPids().join(", ")}`
      );
    }
    await sleep(200);
  }

  const finalLeaderCount = manager.countLeaders();
  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Mass Spawn Race (20 processes)",
    passed: !splitBrainDetected && finalLeaderCount <= 1,
    durationMs: checkDuration,
    metrics: {
      "Processes spawned": 20,
      "Final leader count": finalLeaderCount,
      "Split brain events": metrics.splitBrainEvents,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
    },
    errors: splitBrainDetected
      ? [
          `Split brain detected! Max concurrent leaders: ${metrics.maxConcurrentLeaders}`,
        ]
      : [],
  };
}

// Test 3: Rapid Churn
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: chaos test requires multiple random branches
async function testRapidChurn(config: Config): Promise<SimulationResult> {
  console.log("Starting rapid churn test (60s)...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn initial processes
  for (let i = 0; i < 10; i++) {
    manager.spawnWorker();
  }
  await sleep(3000);

  const testDuration = 60_000;
  const endTime = Date.now() + testDuration;
  let splitBrainCount = 0;
  let churnEvents = 0;

  while (Date.now() < endTime) {
    // Random action
    const action = Math.random();
    const activeCount = manager.getActiveCount();

    if (action < 0.3 && activeCount < 15) {
      // Spawn
      manager.spawnWorker();
      churnEvents++;
    } else if (action < 0.6 && activeCount > 5) {
      // Kill random
      const pid = manager.getRandomPid();
      if (pid) {
        const signal = Math.random() < 0.5 ? "SIGTERM" : "SIGKILL";
        manager.killWorker(pid, signal);
        churnEvents++;
      }
    }

    // Check for split brain
    if (manager.checkSplitBrain()) {
      splitBrainCount++;
      console.log(
        `SPLIT BRAIN at ${Date.now()}! Leaders: ${manager.getLeaderPids().join(", ")}`
      );
    }

    await sleep(500 + Math.random() * 1000);
  }

  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Rapid Churn (60s)",
    passed: splitBrainCount === 0,
    durationMs: testDuration,
    metrics: {
      "Churn events": churnEvents,
      "Processes spawned": metrics.spawned,
      "Processes killed": metrics.killed,
      "Split brain events": splitBrainCount,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
      "Leader transitions": metrics.leaderTransitions,
    },
    errors:
      splitBrainCount > 0 ? [`${splitBrainCount} split brain events!`] : [],
  };
}

// Test 4: Leader Assassination (SIGKILL)
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: stress test with debug logging
async function testLeaderAssassination(
  config: Config
): Promise<SimulationResult> {
  console.log("Starting leader assassination test (10 rounds)...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn initial processes
  for (let i = 0; i < 10; i++) {
    manager.spawnWorker();
  }
  // Wait for processes to start up and elect a leader
  await sleep(5000);

  // Debug: check what the leader key says
  const leaderKeyValue = await checkLeaderKey(config);
  console.log(`  Redis leader key: ${leaderKeyValue}`);

  const rounds = 10;
  let successfulRecoveries = 0;
  let splitBrainEvents = 0;

  for (let round = 0; round < rounds; round++) {
    const leaders = manager.getLeaderPids();
    const activeCount = manager.getActiveCount();
    if (leaders.length === 0) {
      console.log(
        `Round ${round + 1}: No leader found (${activeCount} active processes), waiting...`
      );
      if (round === 0) {
        manager.debugWorkerStates();
      }
      const found = await manager.waitForLeader(15_000);
      if (!found) {
        console.log("  Still no leader after 15s, continuing...");
      }
      continue;
    }

    const leaderPid = leaders[0];
    console.log(
      `Round ${round + 1}: Assassinating leader ${leaderPid} (${activeCount} active)...`
    );

    // Kill with SIGKILL (no graceful shutdown)
    manager.killWorker(leaderPid, "SIGKILL");

    // Wait for new leader (TTL 3s + follower check 2s = ~5s max)
    const newLeader = await manager.waitForLeader(15_000);
    if (newLeader && newLeader !== leaderPid) {
      successfulRecoveries++;
      console.log(`  New leader elected: ${newLeader}`);
    }

    // Check for split brain during recovery
    if (manager.checkSplitBrain()) {
      splitBrainEvents++;
    }

    // Spawn replacement process
    manager.spawnWorker();
    await sleep(1000);
  }

  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Leader Assassination (10 rounds)",
    passed: splitBrainEvents === 0 && successfulRecoveries >= rounds - 1,
    durationMs: 0,
    metrics: {
      Rounds: rounds,
      "Successful recoveries": successfulRecoveries,
      "Split brain events": splitBrainEvents,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
    },
    errors: buildAssassinationErrors(
      splitBrainEvents,
      successfulRecoveries,
      rounds
    ),
  };
}

function buildAssassinationErrors(
  splitBrainEvents: number,
  successfulRecoveries: number,
  rounds: number
): string[] {
  if (splitBrainEvents > 0) {
    return [`${splitBrainEvents} split brain events during recovery`];
  }
  if (successfulRecoveries < rounds - 1) {
    return [`Only ${successfulRecoveries}/${rounds} successful recoveries`];
  }
  return [];
}

// Test 5: Thundering Herd
async function testThunderingHerd(config: Config): Promise<SimulationResult> {
  console.log("Starting thundering herd test...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn initial leader
  manager.spawnWorker();
  await sleep(3000);

  const initialLeader = await manager.waitForLeader(10_000);
  console.log(`Initial leader: ${initialLeader}`);

  // Kill leader and spawn 10 new processes simultaneously
  if (initialLeader) {
    console.log("Killing leader and spawning 10 new processes...");
    manager.killWorker(initialLeader, "SIGKILL");

    // Immediately spawn 10 new processes
    for (let i = 0; i < 10; i++) {
      manager.spawnWorker();
    }
  }

  // Monitor for split brain
  let splitBrainDetected = false;
  const monitorDuration = 15_000;
  const endTime = Date.now() + monitorDuration;

  while (Date.now() < endTime) {
    if (manager.checkSplitBrain()) {
      splitBrainDetected = true;
      console.log(
        `SPLIT BRAIN! Leaders: ${manager.getLeaderPids().join(", ")}`
      );
    }
    await sleep(200);
  }

  const finalLeaders = manager.getLeaderPids();
  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Thundering Herd",
    passed: !splitBrainDetected && finalLeaders.length === 1,
    durationMs: monitorDuration,
    metrics: {
      "Processes spawned": metrics.spawned,
      "Final leader count": finalLeaders.length,
      "Split brain events": metrics.splitBrainEvents,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
    },
    errors: buildThunderingHerdErrors(splitBrainDetected, finalLeaders.length),
  };
}

function buildThunderingHerdErrors(
  splitBrainDetected: boolean,
  finalLeaderCount: number
): string[] {
  if (splitBrainDetected) {
    return ["Split brain detected during thundering herd!"];
  }
  if (finalLeaderCount !== 1) {
    return [`Expected 1 final leader, got ${finalLeaderCount}`];
  }
  return [];
}

// Test 6: Extended Stress Test
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: stress test with many branches
async function testExtendedStress(config: Config): Promise<SimulationResult> {
  console.log("Starting extended stress test (2 minutes, 30 processes)...");
  const manager = new LeaderProcessManager(config);
  await clearLeaderKey(config);

  // Spawn 30 processes
  console.log("Spawning 30 processes...");
  for (let i = 0; i < 30; i++) {
    manager.spawnWorker();
    if (i % 5 === 0) {
      await sleep(100); // Slight stagger
    }
  }
  await sleep(5000);

  const testDuration = 120_000; // 2 minutes
  const endTime = Date.now() + testDuration;
  let splitBrainCount = 0;
  let noLeaderCount = 0;
  let checkCount = 0;

  console.log("Running chaos for 2 minutes...");

  while (Date.now() < endTime) {
    checkCount++;

    // Random chaos action
    const action = Math.random();
    const activeCount = manager.getActiveCount();

    if (action < 0.15 && activeCount < 35) {
      // Spawn
      manager.spawnWorker();
    } else if (action < 0.35 && activeCount > 20) {
      // Kill random (mix of graceful and ungraceful)
      const pid = manager.getRandomPid();
      if (pid) {
        const signal = Math.random() < 0.3 ? "SIGKILL" : "SIGTERM";
        manager.killWorker(pid, signal);
      }
    } else if (action < 0.4 && activeCount > 20) {
      // Kill leader specifically
      const leaders = manager.getLeaderPids();
      if (leaders.length > 0) {
        manager.killWorker(leaders[0], "SIGKILL");
      }
    }

    // Check for anomalies
    const leaderCount = manager.countLeaders();
    if (leaderCount > 1) {
      splitBrainCount++;
      console.log(
        `[${Math.floor((Date.now() - endTime + testDuration) / 1000)}s] SPLIT BRAIN: ${leaderCount} leaders`
      );
    }
    if (leaderCount === 0 && activeCount > 0) {
      noLeaderCount++;
    }

    manager.checkSplitBrain();

    await sleep(2000 + Math.random() * 3000); // 2-5 second intervals
  }

  console.log("Stopping all processes...");
  await manager.stopAll();
  const metrics = manager.getMetrics();

  return {
    name: "Extended Stress Test (2 min, 30 processes)",
    passed: splitBrainCount === 0,
    durationMs: testDuration,
    metrics: {
      "Test duration": "2 minutes",
      "Target processes": 30,
      "Total spawned": metrics.spawned,
      "Total killed": metrics.killed,
      Checks: checkCount,
      "Split brain events": splitBrainCount,
      "No leader moments": noLeaderCount,
      "Max concurrent leaders": metrics.maxConcurrentLeaders,
      "Leader transitions": metrics.leaderTransitions,
    },
    errors:
      splitBrainCount > 0 ? [`${splitBrainCount} split brain events!`] : [],
  };
}

async function main() {
  console.log(
    "================================================================"
  );
  console.log(
    "       LEADER ELECTION STRESS TEST                             "
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

  const results: SimulationResult[] = [];

  try {
    let result: SimulationResult;

    console.log("\n--- Test 1: Basic Leader Election ---");
    result = await testBasicElection(config);
    results.push(result);
    console.log(formatResult(result));
    await sleep(2000);

    console.log("\n--- Test 2: Mass Spawn Race ---");
    result = await testMassSpawnRace(config);
    results.push(result);
    console.log(formatResult(result));
    await sleep(2000);

    console.log("\n--- Test 3: Rapid Churn ---");
    result = await testRapidChurn(config);
    results.push(result);
    console.log(formatResult(result));

    // Heavy cleanup before assassination tests
    console.log("\n  Cleaning up orphan processes...");
    await killAllOrphanWorkers();
    await clearLeaderKey(config);

    console.log("\n--- Test 4: Leader Assassination ---");
    result = await testLeaderAssassination(config);
    results.push(result);
    console.log(formatResult(result));

    // Cleanup before next test
    console.log("\n  Cleaning up orphan processes...");
    await killAllOrphanWorkers();
    await clearLeaderKey(config);

    console.log("\n--- Test 5: Thundering Herd ---");
    result = await testThunderingHerd(config);
    results.push(result);
    console.log(formatResult(result));

    // Cleanup before next test
    console.log("\n  Cleaning up orphan processes...");
    await killAllOrphanWorkers();
    await clearLeaderKey(config);

    console.log("\n--- Test 6: Extended Stress Test ---");
    result = await testExtendedStress(config);
    results.push(result);
    console.log(formatResult(result));
  } finally {
    await config.close();
  }

  // Summary
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

  // Total split brain events across all tests
  const totalSplitBrain = results.reduce(
    (sum, r) => sum + (Number(r.metrics["Split brain events"]) || 0),
    0
  );
  console.log(`Total split brain events: ${totalSplitBrain}`);

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
