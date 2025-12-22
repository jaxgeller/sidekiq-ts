/**
 * Common job classes for production simulations
 */

import { IterableJob, Job, Sidekiq } from "../../../src/index.js";

// No-op job for pure throughput testing
export class NoOpJob extends Job<[number]> {
  static sidekiqOptions = { retry: false, queue: "simulation" };
  async perform(_id: number) {
    // Intentionally empty for throughput measurement
  }
}

// Configurable delay job for load testing
export class DelayJob extends Job<[number, number]> {
  static sidekiqOptions = { retry: false, queue: "simulation" };
  async perform(_id: number, delayMs: number) {
    await new Promise((r) => setTimeout(r, delayMs));
  }
}

// Job that always fails - for retry testing
export class AlwaysFailJob extends Job<[string]> {
  static sidekiqOptions = { retry: 3, queue: "simulation", backtrace: true };
  perform(message: string): never {
    throw new Error(message);
  }
}

// Configure fast retries for AlwaysFailJob
AlwaysFailJob.retryIn(() => 1); // 1 second retry delay

// Job with configurable failure rate
export class FlakyJob extends Job<[number, number]> {
  static sidekiqOptions = { retry: 5, queue: "simulation" };
  perform(_id: number, failPercent: number) {
    if (Math.random() * 100 < failPercent) {
      throw new Error(`Random failure (${failPercent}% chance)`);
    }
  }
}

// Shared mutable state for race condition testing
export let sharedCounter = 0;
export function resetCounter() {
  sharedCounter = 0;
}

// Job that modifies shared state - for race detection
export class CounterJob extends Job<[number]> {
  static sidekiqOptions = { retry: false, queue: "simulation" };
  async perform(increment: number) {
    const before = sharedCounter;
    // Introduce artificial delay to increase race window
    await new Promise((r) => setTimeout(r, Math.random() * 5));
    sharedCounter = before + increment;
  }
}

// Long-running iterable job for interruption testing
export class LongIterableJob extends IterableJob<[number], number, number> {
  static sidekiqOptions = { retry: 0, queue: "simulation" };

  private readonly lifecycleEvents: string[] = [];

  buildEnumerator(totalItems: number, opts: { cursor: number | null }) {
    return this.arrayEnumerator(
      Array.from({ length: totalItems }, (_, i) => i),
      opts.cursor
    );
  }

  async eachIteration(item: number) {
    // Simulate slow processing
    await new Promise((r) => setTimeout(r, 50));
    if (item % 100 === 0) {
      this.logger().debug(() => `Processed item ${item}`);
    }
  }

  onStart() {
    this.lifecycleEvents.push("start");
    this.logger().info(() => "Starting fresh iteration");
  }

  onResume() {
    this.lifecycleEvents.push("resume");
    this.logger().info(() => `Resuming from cursor: ${this.cursor()}`);
  }

  onStop() {
    this.lifecycleEvents.push("stop");
    this.logger().info(() => `Stopping at cursor: ${this.cursor()}`);
  }

  onComplete() {
    this.lifecycleEvents.push("complete");
    this.logger().info(() => "Completed iteration");
  }
}

// Register all jobs
export function registerAllJobs() {
  Sidekiq.registerJob(NoOpJob);
  Sidekiq.registerJob(DelayJob);
  Sidekiq.registerJob(AlwaysFailJob);
  Sidekiq.registerJob(FlakyJob);
  Sidekiq.registerJob(CounterJob);
  Sidekiq.registerJob(LongIterableJob);
}
