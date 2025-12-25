import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import { IterableJob } from "../src/iterable.js";
import { IterableInterrupted } from "../src/iterable-errors.js";
import { generateJid } from "../src/job-util.js";
import { Sidekiq } from "../src/sidekiq.js";

class MissingBuildEnumeratorJob extends IterableJob<[]> {
  eachIteration(): void {
    // noop for test
  }
}

class MissingEachIterationJob extends IterableJob<[]> {
  buildEnumerator({ cursor }: { cursor: number | null }) {
    return this.arrayEnumerator([1], cursor);
  }
}

class NilEnumeratorJob extends IterableJob<[]> {
  buildEnumerator(_opts: { cursor: number | null }) {
    return null;
  }

  eachIteration(): void {
    // noop for test
  }
}

class ArrayIterableJob extends IterableJob<[], number, number> {
  static iterated: number[] = [];
  static stopAfter: number | null = null;
  static onStartCalled = 0;
  static onResumeCalled = 0;
  static onStopCalled = 0;
  static onCancelCalled = 0;
  static onCompleteCalled = 0;

  private runIterations = 0;

  buildEnumerator({ cursor }: { cursor: number | null }) {
    this.runIterations = 0;
    return this.arrayEnumerator([10, 11, 12, 13, 14], cursor);
  }

  eachIteration(value: number): void {
    ArrayIterableJob.iterated.push(value);
    this.runIterations += 1;
  }

  interrupted(): boolean {
    return (
      ArrayIterableJob.stopAfter !== null &&
      this.runIterations === ArrayIterableJob.stopAfter
    );
  }

  onStart(): void {
    ArrayIterableJob.onStartCalled += 1;
  }

  onResume(): void {
    ArrayIterableJob.onResumeCalled += 1;
  }

  onStop(): void {
    ArrayIterableJob.onStopCalled += 1;
  }

  onCancel(): void {
    ArrayIterableJob.onCancelCalled += 1;
  }

  onComplete(): void {
    ArrayIterableJob.onCompleteCalled += 1;
  }
}

const redisUrl = "redis://localhost:6379/0";

const runIterable = async (jobClass: typeof ArrayIterableJob, jid?: string) => {
  const job = new jobClass();
  job.jid = jid ?? generateJid();
  try {
    await job.perform();
  } catch (error) {
    if (!(error instanceof IterableInterrupted)) {
      throw error;
    }
  }
  return job.jid as string;
};

beforeAll(() => {
  Sidekiq.defaultConfiguration.redis = { url: redisUrl };
});

beforeEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
  ArrayIterableJob.iterated = [];
  ArrayIterableJob.stopAfter = null;
  ArrayIterableJob.onStartCalled = 0;
  ArrayIterableJob.onResumeCalled = 0;
  ArrayIterableJob.onStopCalled = 0;
  ArrayIterableJob.onCancelCalled = 0;
  ArrayIterableJob.onCompleteCalled = 0;
  Sidekiq.defaultConfiguration.maxIterationRuntime = null;
});

afterEach(async () => {
  const redis = await Sidekiq.defaultConfiguration.getRedisClient();
  await redis.flushDb();
});

afterAll(async () => {
  await Sidekiq.defaultConfiguration.close();
});

describe("Iterable jobs", () => {
  it("throws when buildEnumerator is missing", async () => {
    const job = new MissingBuildEnumeratorJob();
    job.jid = generateJid();
    await expect(job.perform()).rejects.toThrow(
      "MissingBuildEnumeratorJob must implement a 'buildEnumerator' method"
    );
  });

  it("throws when eachIteration is missing", async () => {
    const job = new MissingEachIterationJob();
    job.jid = generateJid();
    await expect(job.perform()).rejects.toThrow(
      "MissingEachIterationJob must implement an 'eachIteration' method"
    );
  });

  it("skips when buildEnumerator returns null", async () => {
    const job = new NilEnumeratorJob();
    job.jid = generateJid();
    await job.perform();
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    expect(await redis.exists(`it-${job.jid}`)).toBe(0);
  });

  it("persists cursor state and resumes", async () => {
    ArrayIterableJob.stopAfter = 2;
    const jid = await runIterable(ArrayIterableJob);
    expect(ArrayIterableJob.iterated).toEqual([10, 11]);

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const state = await redis.hGetAll(`it-${jid}`);
    expect(Number(state.ex)).toBe(1);
    expect(JSON.parse(state.c)).toBe(2);

    ArrayIterableJob.stopAfter = 2;
    await runIterable(ArrayIterableJob, jid);
    expect(ArrayIterableJob.iterated).toEqual([10, 11, 12, 13]);

    const state2 = await redis.hGetAll(`it-${jid}`);
    expect(Number(state2.ex)).toBe(2);
    expect(JSON.parse(state2.c)).toBe(4);

    ArrayIterableJob.stopAfter = null;
    await runIterable(ArrayIterableJob, jid);
    expect(ArrayIterableJob.iterated).toEqual([10, 11, 12, 13, 14]);
    expect(await redis.exists(`it-${jid}`)).toBe(0);
  });

  it("cancels and completes without further iteration", async () => {
    ArrayIterableJob.stopAfter = 2;
    const jid = await runIterable(ArrayIterableJob);
    expect(ArrayIterableJob.iterated).toEqual([10, 11]);

    const job = new ArrayIterableJob();
    job.jid = jid;
    expect(await job.cancel()).toBe(true);
    expect(job.cancelled()).toBe(true);

    ArrayIterableJob.stopAfter = null;
    await runIterable(ArrayIterableJob, jid);
    expect(ArrayIterableJob.iterated).toEqual([10, 11]);
    expect(ArrayIterableJob.onCancelCalled).toBe(1);
    expect(ArrayIterableJob.onCompleteCalled).toBe(1);
  });

  it("calls lifecycle hooks in correct order", async () => {
    // First run - should call onStart and onStop
    ArrayIterableJob.stopAfter = 2;
    const jid = await runIterable(ArrayIterableJob);

    expect(ArrayIterableJob.onStartCalled).toBe(1);
    expect(ArrayIterableJob.onResumeCalled).toBe(0);
    expect(ArrayIterableJob.onStopCalled).toBe(1);
    expect(ArrayIterableJob.onCompleteCalled).toBe(0);

    // Resume - should call onResume and onStop
    ArrayIterableJob.stopAfter = 2;
    await runIterable(ArrayIterableJob, jid);

    expect(ArrayIterableJob.onStartCalled).toBe(1);
    expect(ArrayIterableJob.onResumeCalled).toBe(1);
    expect(ArrayIterableJob.onStopCalled).toBe(2);
    expect(ArrayIterableJob.onCompleteCalled).toBe(0);

    // Complete - should call onResume, onStop, and onComplete
    ArrayIterableJob.stopAfter = null;
    await runIterable(ArrayIterableJob, jid);

    expect(ArrayIterableJob.onStartCalled).toBe(1);
    expect(ArrayIterableJob.onResumeCalled).toBe(2);
    expect(ArrayIterableJob.onStopCalled).toBe(3);
    expect(ArrayIterableJob.onCompleteCalled).toBe(1);
  });

  it("flushes state and allows resume after error during iteration", async () => {
    class FailingIterableJob extends IterableJob<[], number, number> {
      static iterated: number[] = [];
      static failOnValue: number | null = null;

      buildEnumerator({ cursor }: { cursor: number | null }) {
        return this.arrayEnumerator([1, 2, 3, 4, 5], cursor);
      }

      eachIteration(value: number): void {
        FailingIterableJob.iterated.push(value);
        if (value === FailingIterableJob.failOnValue) {
          throw new Error("Intentional failure");
        }
      }
    }

    FailingIterableJob.iterated = [];
    FailingIterableJob.failOnValue = 3;

    const job = new FailingIterableJob();
    job.jid = generateJid();

    // First run fails on value 3
    await expect(job.perform()).rejects.toThrow("Intentional failure");
    expect(FailingIterableJob.iterated).toEqual([1, 2, 3]);

    // State should be persisted (cursor is the index of the failed item)
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const state = await redis.hGetAll(`it-${job.jid}`);
    expect(Number(state.ex)).toBe(1);
    expect(JSON.parse(state.c)).toBe(2); // cursor is index of failed item (value 3)

    // Resume after fixing the issue - retries from the failed item
    FailingIterableJob.failOnValue = null;
    const resumeJob = new FailingIterableJob();
    resumeJob.jid = job.jid;
    await resumeJob.perform();

    // Note: sidekiq-ts retries from the failed item (index 2 = value 3),
    // so value 3 appears twice in the total iteration
    expect(FailingIterableJob.iterated).toEqual([1, 2, 3, 3, 4, 5]);
  });

  it("respects maxIterationRuntime limit", async () => {
    class SlowIterableJob extends IterableJob<[], number, number> {
      static iterated: number[] = [];

      buildEnumerator({ cursor }: { cursor: number | null }) {
        return this.arrayEnumerator([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], cursor);
      }

      async eachIteration(value: number): Promise<void> {
        SlowIterableJob.iterated.push(value);
        // Sleep for 20ms each iteration
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }

    SlowIterableJob.iterated = [];
    Sidekiq.defaultConfiguration.maxIterationRuntime = 0.05; // 50ms limit

    const job = new SlowIterableJob();
    job.jid = generateJid();

    // Should be interrupted before completing all iterations
    try {
      await job.perform();
    } catch (error) {
      if (!(error instanceof IterableInterrupted)) {
        throw error;
      }
    }

    // Should have processed some but not all items
    expect(SlowIterableJob.iterated.length).toBeGreaterThan(0);
    expect(SlowIterableJob.iterated.length).toBeLessThan(10);
  });

  it("handles empty enumerator gracefully", async () => {
    class EmptyIterableJob extends IterableJob<[], never, number> {
      static onCompleteCalled = 0;

      buildEnumerator({ cursor }: { cursor: number | null }) {
        return this.arrayEnumerator([], cursor);
      }

      eachIteration(_value: never): void {
        throw new Error("Should not be called");
      }

      onComplete(): void {
        EmptyIterableJob.onCompleteCalled += 1;
      }
    }

    EmptyIterableJob.onCompleteCalled = 0;

    const job = new EmptyIterableJob();
    job.jid = generateJid();
    await job.perform();

    // onComplete should still be called even with empty enumerator
    expect(EmptyIterableJob.onCompleteCalled).toBe(1);

    // No state should be persisted
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    expect(await redis.exists(`it-${job.jid}`)).toBe(0);
  });

  it("sets TTL on iteration state keys", async () => {
    // This test verifies the fix for the TTL race condition.
    // Previously, expire was called with "NX" flag which could fail
    // to set TTL if the key was just created by hSetNX.
    ArrayIterableJob.stopAfter = 2;
    const jid = await runIterable(ArrayIterableJob);

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const key = `it-${jid}`;

    // Key should exist with state
    expect(await redis.exists(key)).toBe(1);

    // Key should have a TTL set (not -1 which means no expiry)
    const ttl = await redis.ttl(key);
    expect(ttl).toBeGreaterThan(0);
    // TTL should be approximately 30 days (2592000 seconds)
    expect(ttl).toBeLessThanOrEqual(30 * 24 * 60 * 60);
    expect(ttl).toBeGreaterThan(30 * 24 * 60 * 60 - 60); // Within 60 seconds of 30 days
  });

  it("sets TTL on cancelled iteration state", async () => {
    ArrayIterableJob.stopAfter = 2;
    const jid = await runIterable(ArrayIterableJob);

    // Cancel the job
    const job = new ArrayIterableJob();
    job.jid = jid;
    await job.cancel();

    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const key = `it-${jid}`;

    // Key should exist with cancelled field
    const cancelled = await redis.hGet(key, "cancelled");
    expect(cancelled).toBeTruthy();

    // Key should have a TTL set
    const ttl = await redis.ttl(key);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(30 * 24 * 60 * 60);
  });
});
