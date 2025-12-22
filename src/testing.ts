import { dumpJson, loadJson } from "./json.js";
import { nowInMillis } from "./job_util.js";
import { resolveJob } from "./registry.js";
import type { Config } from "./config.js";
import type { JobPayload } from "./types.js";
import { ensureInterruptHandler } from "./interrupt_handler.js";

export type TestMode = "disable" | "fake" | "inline";

export class EmptyQueueError extends Error {
  constructor() {
    super("performOne called with empty job queue");
  }
}

export class Testing {
  private static globalMode: TestMode = "disable";
  private static localMode: TestMode | null = null;

  static mode(): TestMode {
    return this.localMode ?? this.globalMode;
  }

  static setMode(mode: TestMode, fn?: () => void | Promise<void>) {
    if (!fn) {
      this.globalMode = mode;
      return;
    }
    if (this.localMode) {
      throw new Error("Nested testing modes are not supported");
    }
    this.localMode = mode;
    const result = fn();
    if (result && typeof result.then === "function") {
      return result.finally(() => {
        this.localMode = null;
      });
    }
    this.localMode = null;
  }

  static disable(fn?: () => void | Promise<void>) {
    return this.setMode("disable", fn);
  }

  static fake(fn?: () => void | Promise<void>) {
    return this.setMode("fake", fn);
  }

  static inline(fn?: () => void | Promise<void>) {
    return this.setMode("inline", fn);
  }

  static enabled(): boolean {
    return this.mode() !== "disable";
  }

  static disabled(): boolean {
    return this.mode() === "disable";
  }

  static enqueue(payload: JobPayload): void {
    const job = loadJson(dumpJson(payload)) as JobPayload;
    if (!job.queue) {
      job.queue = "default";
    }
    if (!job.at) {
      job.enqueued_at = nowInMillis();
    }
    Queues.push(job.queue, String(job.class), job);
  }

  static async performInline(payload: JobPayload, config: Config): Promise<void> {
    ensureInterruptHandler(config);
    const job = loadJson(dumpJson(payload)) as JobPayload;
    const className = String(job.class);
    const klass = resolveJob(className);
    if (!klass) {
      throw new Error(`Unknown job class ${className}`);
    }
    const instance = new klass();
    instance.jid = job.jid;
    instance._context = { stopping: () => false };

    await config.serverMiddleware.invoke(instance, job, job.queue ?? "default", async () => {
      await instance.perform(...(job.args ?? []));
    });
  }
}

export class Queues {
  private static jobsByQueue = new Map<string, JobPayload[]>();
  private static jobsByClass = new Map<string, JobPayload[]>();

  static get(queue: string): JobPayload[] {
    return this.ensure(this.jobsByQueue, queue);
  }

  static jobs(): JobPayload[] {
    return Array.from(this.jobsByQueue.values()).flat();
  }

  static jobsForClass(klass: string): JobPayload[] {
    return this.ensure(this.jobsByClass, klass);
  }

  static push(queue: string, klass: string, job: JobPayload): void {
    this.ensure(this.jobsByQueue, queue).push(job);
    this.ensure(this.jobsByClass, klass).push(job);
  }

  static deleteFor(jid: string, queue: string, klass: string): void {
    const queueJobs = this.ensure(this.jobsByQueue, queue);
    const classJobs = this.ensure(this.jobsByClass, klass);
    this.jobsByQueue.set(
      queue,
      queueJobs.filter((job) => job.jid !== jid)
    );
    this.jobsByClass.set(
      klass,
      classJobs.filter((job) => job.jid !== jid)
    );
  }

  static clearFor(queue: string, klass: string): void {
    this.jobsByQueue.set(queue, []);
    this.jobsByClass.set(klass, []);
  }

  static clearAll(): void {
    this.jobsByQueue.clear();
    this.jobsByClass.clear();
  }

  private static ensure(map: Map<string, JobPayload[]>, key: string): JobPayload[] {
    if (!map.has(key)) {
      map.set(key, []);
    }
    return map.get(key) as JobPayload[];
  }
}
