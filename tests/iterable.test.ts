import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { IterableJob, IterableInterrupted } from "../src/index.js";
import { generateJid } from "../src/job_util.js";
import { Sidekiq } from "../src/sidekiq.js";

class MissingBuildEnumeratorJob extends IterableJob<[]> {
  async eachIteration() {}
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

  async eachIteration() {}
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

  async eachIteration(value: number) {
    ArrayIterableJob.iterated.push(value);
    this.runIterations += 1;
  }

  interrupted(): boolean {
    return (
      ArrayIterableJob.stopAfter !== null &&
      this.runIterations === ArrayIterableJob.stopAfter
    );
  }

  async onStart() {
    ArrayIterableJob.onStartCalled += 1;
  }

  async onResume() {
    ArrayIterableJob.onResumeCalled += 1;
  }

  async onStop() {
    ArrayIterableJob.onStopCalled += 1;
  }

  async onCancel() {
    ArrayIterableJob.onCancelCalled += 1;
  }

  async onComplete() {
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
});
