/**
 * Master Script - Run All Production Simulations
 *
 * Runs all simulation tests sequentially and provides a summary report.
 *
 * Run with: npx tsx examples/simulations/run-all.ts
 *
 * Or run individual tests:
 *   npx tsx examples/simulations/load-test.ts
 *   npx tsx examples/simulations/failure-modes.ts
 *   npx tsx examples/simulations/concurrency-test.ts
 *   npx tsx examples/simulations/chaos-test.ts
 */

import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

interface TestResult {
  name: string;
  passed: boolean;
  durationMs: number;
  output: string;
}

function runTest(scriptPath: string, name: string): Promise<TestResult> {
  return new Promise((resolve) => {
    const startTime = Date.now();
    let output = "";

    const child = spawn("npx", ["tsx", scriptPath], {
      cwd: join(__dirname, "../.."),
      stdio: ["inherit", "pipe", "pipe"],
    });

    child.stdout?.on("data", (data: Buffer) => {
      const text = data.toString();
      output += text;
      process.stdout.write(text);
    });

    child.stderr?.on("data", (data: Buffer) => {
      const text = data.toString();
      output += text;
      process.stderr.write(text);
    });

    child.on("close", (code) => {
      resolve({
        name,
        passed: code === 0,
        durationMs: Date.now() - startTime,
        output,
      });
    });

    child.on("error", (err) => {
      resolve({
        name,
        passed: false,
        durationMs: Date.now() - startTime,
        output: `Error: ${err.message}`,
      });
    });
  });
}

async function main() {
  console.log("╔════════════════════════════════════════════════════════════╗");
  console.log("║         SIDEKIQ-TS PRODUCTION SIMULATION SUITE             ║");
  console.log("╚════════════════════════════════════════════════════════════╝");
  console.log();

  const tests = [
    { script: "load-test.ts", name: "Load Testing" },
    { script: "failure-modes.ts", name: "Failure Modes" },
    { script: "concurrency-test.ts", name: "Concurrency Testing" },
    { script: "chaos-test.ts", name: "Chaos Testing" },
  ];

  const results: TestResult[] = [];
  const totalStart = Date.now();

  for (const test of tests) {
    console.log();
    console.log(`┌${"─".repeat(58)}┐`);
    console.log(`│ Running: ${test.name.padEnd(47)} │`);
    console.log(`└${"─".repeat(58)}┘`);
    console.log();

    const scriptPath = join(__dirname, test.script);
    const result = await runTest(scriptPath, test.name);
    results.push(result);

    // Brief pause between tests
    await new Promise((r) => setTimeout(r, 2000));
  }

  const totalDuration = Date.now() - totalStart;

  // Final summary
  console.log();
  console.log("╔════════════════════════════════════════════════════════════╗");
  console.log("║                    FINAL SUMMARY                           ║");
  console.log("╠════════════════════════════════════════════════════════════╣");

  let passCount = 0;
  let failCount = 0;

  for (const result of results) {
    const status = result.passed ? "✓ PASS" : "✗ FAIL";
    const duration = `${(result.durationMs / 1000).toFixed(1)}s`;
    const line = `║ ${status} │ ${result.name.padEnd(35)} │ ${duration.padStart(8)} ║`;
    console.log(line);

    if (result.passed) {
      passCount++;
    } else {
      failCount++;
    }
  }

  console.log("╠════════════════════════════════════════════════════════════╣");
  const totalDurationStr = `${(totalDuration / 1000).toFixed(1)}s`;
  console.log(
    `║ Total: ${passCount} passed, ${failCount} failed`.padEnd(49) +
      `${totalDurationStr.padStart(8)} ║`
  );
  console.log("╚════════════════════════════════════════════════════════════╝");

  const allPassed = failCount === 0;
  console.log();
  console.log(
    allPassed
      ? "All simulations completed successfully!"
      : "Some simulations failed."
  );

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
