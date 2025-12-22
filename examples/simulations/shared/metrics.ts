/**
 * Metrics collection utilities for production simulations
 */

export interface SimulationResult {
  name: string;
  passed: boolean;
  durationMs: number;
  metrics: Record<string, number | string>;
  errors: string[];
}

export class MetricsCollector {
  private latencies: number[] = [];
  private memorySnapshots: number[] = [];
  private interval?: ReturnType<typeof setInterval>;
  private startTime = 0;

  start(): void {
    this.startTime = Date.now();
    this.latencies = [];
    this.memorySnapshots = [];
    this.interval = setInterval(() => {
      this.memorySnapshots.push(process.memoryUsage().heapUsed);
    }, 100);
  }

  stop(): number {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = undefined;
    }
    return Date.now() - this.startTime;
  }

  recordLatency(ms: number): void {
    this.latencies.push(ms);
  }

  getPercentile(p: number): number {
    if (this.latencies.length === 0) return 0;
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const idx = Math.min(
      Math.floor((sorted.length * p) / 100),
      sorted.length - 1
    );
    return sorted[idx] ?? 0;
  }

  getAverageLatency(): number {
    if (this.latencies.length === 0) return 0;
    return this.latencies.reduce((a, b) => a + b, 0) / this.latencies.length;
  }

  getPeakMemoryMB(): number {
    if (this.memorySnapshots.length === 0) return 0;
    return Math.max(...this.memorySnapshots) / 1024 / 1024;
  }

  getCurrentMemoryMB(): number {
    return process.memoryUsage().heapUsed / 1024 / 1024;
  }

  getLatencyCount(): number {
    return this.latencies.length;
  }
}

export function formatResult(result: SimulationResult): string {
  const status = result.passed ? "PASS" : "FAIL";
  const lines = [
    `\n${"=".repeat(60)}`,
    `[${status}] ${result.name}`,
    `${"=".repeat(60)}`,
    `Duration: ${(result.durationMs / 1000).toFixed(2)}s`,
    "",
    "Metrics:",
  ];

  for (const [key, value] of Object.entries(result.metrics)) {
    const formattedValue =
      typeof value === "number" ? value.toLocaleString() : value;
    lines.push(`  ${key}: ${formattedValue}`);
  }

  if (result.errors.length > 0) {
    lines.push("", "Errors:");
    for (const error of result.errors.slice(0, 10)) {
      lines.push(`  - ${error}`);
    }
    if (result.errors.length > 10) {
      lines.push(`  ... and ${result.errors.length - 10} more`);
    }
  }

  lines.push("");
  return lines.join("\n");
}

export async function waitForQueueDrain(
  queue: { size(): Promise<number> },
  timeoutMs = 60_000,
  pollInterval = 100
): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const size = await queue.size();
    if (size === 0) return true;
    await new Promise((r) => setTimeout(r, pollInterval));
  }
  return false;
}

export async function waitForRetryDrain(
  retrySet: { size(): Promise<number> },
  timeoutMs = 60_000,
  pollInterval = 500
): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const size = await retrySet.size();
    if (size === 0) return true;
    await new Promise((r) => setTimeout(r, pollInterval));
  }
  return false;
}
