import {
  ITERATION_STATE_FLUSH_INTERVAL_SECONDS,
  ITERATION_STATE_TTL_SECONDS,
} from "./iterable-constants.js";
import { IterableAbort, IterableInterrupted } from "./iterable-errors.js";
import { Job } from "./job.js";
import { dumpJson, loadJson } from "./json.js";
import { Sidekiq } from "./sidekiq.js";

type IterableEntry<TItem, TCursor> = [TItem, TCursor];

type IterableSource<TItem, TCursor> =
  | Iterator<IterableEntry<TItem, TCursor>>
  | Iterable<IterableEntry<TItem, TCursor>>;

const freezeValue = <T>(value: T): T => {
  if (value && typeof value === "object") {
    return Object.freeze(value);
  }
  return value;
};

export class IterableJob<
  TArgs extends unknown[] = unknown[],
  TItem = unknown,
  TCursor = unknown,
> extends Job<TArgs> {
  private executions = 0;
  private cursorValue: TCursor | null = null;
  private startTime = 0;
  private runtimeSeconds = 0;
  private argsValue: Readonly<TArgs> | null = null;
  private cancelledValue: number | null = null;
  private currentObjectValue: TItem | null = null;

  static abort(): never {
    throw new IterableAbort();
  }

  get currentObject(): TItem | null {
    return this.currentObjectValue;
  }

  arguments(): Readonly<TArgs> | null {
    return this.argsValue;
  }

  cursor(): TCursor | null {
    return freezeValue(this.cursorValue);
  }

  async cancel(): Promise<boolean> {
    if (await this.isCancelled()) {
      return true;
    }
    if (!this.jid) {
      return false;
    }
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const key = this.iterationKey();
    const now = Math.floor(Date.now() / 1000);
    const pipeline = redis.multi();
    pipeline.hSetNX(key, "cancelled", String(now));
    pipeline.hGet(key, "cancelled");
    pipeline.expire(key, ITERATION_STATE_TTL_SECONDS, "NX");
    const result = await pipeline.exec();
    const cancelled =
      (result?.[1] as unknown as string | null | undefined) ?? null;
    this.cancelledValue = cancelled ? Number(cancelled) : null;
    return Boolean(this.cancelledValue);
  }

  cancelled(): boolean {
    return Boolean(this.cancelledValue);
  }

  // biome-ignore lint/suspicious/useAwait: lifecycle hook may be overridden with async implementation
  async onStart(): Promise<void> {
    return undefined;
  }
  // biome-ignore lint/suspicious/useAwait: lifecycle hook may be overridden with async implementation
  async onResume(): Promise<void> {
    return undefined;
  }
  // biome-ignore lint/suspicious/useAwait: lifecycle hook may be overridden with async implementation
  async onStop(): Promise<void> {
    return undefined;
  }
  // biome-ignore lint/suspicious/useAwait: lifecycle hook may be overridden with async implementation
  async onCancel(): Promise<void> {
    return undefined;
  }
  // biome-ignore lint/suspicious/useAwait: lifecycle hook may be overridden with async implementation
  async onComplete(): Promise<void> {
    return undefined;
  }

  async aroundIteration(fn: () => Promise<void> | void): Promise<void> {
    await fn();
  }

  buildEnumerator(
    ..._args: [...TArgs, { cursor: TCursor | null }]
  ): IterableSource<TItem, TCursor> | null {
    throw new Error(
      `${this.constructor.name} must implement a 'buildEnumerator' method`
    );
  }

  eachIteration(_item: TItem, ..._args: TArgs): Promise<void> {
    throw new Error(
      `${this.constructor.name} must implement an 'eachIteration' method`
    );
  }

  protected arrayEnumerator(
    array: TItem[],
    cursor: number | null | undefined
  ): IterableSource<TItem, number> {
    if (!Array.isArray(array)) {
      throw new Error("array must be an Array");
    }
    let index = cursor ?? 0;
    const iterator: Iterator<IterableEntry<TItem, number>> &
      Iterable<IterableEntry<TItem, number>> = {
      [Symbol.iterator]() {
        return this;
      },
      next(): IteratorResult<IterableEntry<TItem, number>> {
        if (index >= array.length) {
          return { done: true, value: undefined as never };
        }
        const value = array[index];
        const cursor = index;
        index += 1;
        return { done: false, value: [value, cursor] };
      },
    };
    return iterator;
  }

  protected iterationKey(): string {
    return `it-${this.jid ?? ""}`;
  }

  async perform(...args: TArgs): Promise<void> {
    this.argsValue = Object.freeze([...args]) as Readonly<TArgs>;
    await this.fetchPreviousIterationState();
    this.executions += 1;
    this.startTime = this.monoNow();

    const enumerator = this.buildEnumerator(...args, {
      cursor: this.cursorValue,
    });
    if (!enumerator) {
      this.logger().info(
        () => "'buildEnumerator' returned nil, skipping the job."
      );
      return;
    }
    const iterator = this.assertEnumerator(enumerator);

    if (this.executions === 1) {
      await this.onStart();
    } else {
      await this.onResume();
    }

    let completed: boolean | null = null;
    try {
      completed = await this.iterateWithEnumerator(iterator, args);
    } catch (error) {
      if (error instanceof IterableAbort) {
        completed = null;
      } else {
        throw error;
      }
    } finally {
      await this.onStop();
    }

    const finished = this.handleCompleted(completed);
    if (finished) {
      await this.onComplete();
      await this.cleanup();
    } else {
      await this.reenqueueIterationJob();
    }
  }

  private async isCancelled(): Promise<boolean> {
    if (!this.jid) {
      return false;
    }
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const cancelled = await redis.hGet(this.iterationKey(), "cancelled");
    this.cancelledValue = cancelled ? Number(cancelled) : null;
    return Boolean(this.cancelledValue);
  }

  private async fetchPreviousIterationState(): Promise<void> {
    if (!this.jid) {
      return;
    }
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const state = await redis.hGetAll(this.iterationKey());
    if (Object.keys(state).length === 0) {
      return;
    }
    this.executions = Number(state.ex ?? 0);
    this.cursorValue = state.c ? (loadJson(state.c) as TCursor) : null;
    this.runtimeSeconds = Number(state.rt ?? 0);
  }

  private async iterateWithEnumerator(
    iterator: Iterator<IterableEntry<TItem, TCursor>>,
    args: TArgs
  ): Promise<boolean> {
    if (await this.isCancelled()) {
      await this.onCancel();
      this.logger().info(() => "Job cancelled");
      return true;
    }

    const timeLimit = Sidekiq.defaultConfiguration.timeout;
    let foundRecord = false;
    let stateFlushedAt = this.monoNow();

    try {
      for (let next = iterator.next(); !next.done; next = iterator.next()) {
        const [object, cursor] = next.value;
        foundRecord = true;
        this.cursorValue = cursor;
        this.currentObjectValue = object;

        const interruptJob = this.interrupted() || this.shouldInterrupt();
        if (
          this.monoNow() - stateFlushedAt >=
            ITERATION_STATE_FLUSH_INTERVAL_SECONDS ||
          interruptJob
        ) {
          const cancelled = await this.flushState();
          stateFlushedAt = this.monoNow();
          if (cancelled) {
            this.cancelledValue = cancelled;
            await this.onCancel();
            this.logger().info(() => "Job cancelled");
            return true;
          }
        }

        if (interruptJob) {
          return false;
        }

        await this.verifyIterationTime(timeLimit, async () => {
          await this.aroundIteration(async () => {
            try {
              await this.eachIteration(object, ...(args as TArgs));
            } catch (error) {
              await this.flushState();
              throw error;
            }
          });
        });
      }

      if (!foundRecord) {
        this.logger().debug("Enumerator found nothing to iterate!");
      }
      return true;
    } finally {
      this.runtimeSeconds += this.monoNow() - this.startTime;
    }
  }

  private async verifyIterationTime(
    timeLimitSeconds: number,
    fn: () => Promise<void>
  ): Promise<void> {
    const start = this.monoNow();
    await fn();
    const total = this.monoNow() - start;
    if (timeLimitSeconds > 0 && total > timeLimitSeconds) {
      this.logger().warn(
        () =>
          `Iteration took longer (${total.toFixed(
            2
          )}) than Sidekiq's shutdown timeout (${timeLimitSeconds}).`
      );
    }
  }

  private async reenqueueIterationJob(): Promise<void> {
    await this.flushState();
    this.logger().debug(
      () => `Interrupting job (cursor=${String(this.cursorValue)})`
    );
    throw new IterableInterrupted();
  }

  private assertEnumerator(
    source: IterableSource<TItem, TCursor>
  ): Iterator<IterableEntry<TItem, TCursor>> {
    if (Array.isArray(source)) {
      throw new Error(
        `buildEnumerator must return an Iterator, but returned ${source.constructor.name}.`
      );
    }
    if (
      typeof (source as Iterator<IterableEntry<TItem, TCursor>>).next ===
      "function"
    ) {
      return source as Iterator<IterableEntry<TItem, TCursor>>;
    }
    const iterable = source as Iterable<IterableEntry<TItem, TCursor>>;
    if (iterable && typeof iterable[Symbol.iterator] === "function") {
      return iterable[Symbol.iterator]();
    }
    const typeName =
      source && typeof source === "object"
        ? ((source as { constructor?: { name?: string } }).constructor?.name ??
          "Object")
        : typeof source;
    throw new Error(
      `buildEnumerator must return an Iterator, but returned ${typeName}.`
    );
  }

  private shouldInterrupt(): boolean {
    const maxRuntime = Sidekiq.defaultConfiguration.maxIterationRuntime;
    return Boolean(maxRuntime && this.monoNow() - this.startTime > maxRuntime);
  }

  private async flushState(): Promise<number | null> {
    if (!this.jid) {
      return null;
    }
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    const key = this.iterationKey();
    const state = {
      ex: String(this.executions),
      c: dumpJson(this.cursorValue),
      rt: String(this.runtimeSeconds),
    };
    const pipeline = redis.multi();
    pipeline.hSet(key, state);
    pipeline.expire(key, ITERATION_STATE_TTL_SECONDS, "NX");
    pipeline.hGet(key, "cancelled");
    const result = await pipeline.exec();
    const cancelled =
      (result?.[2] as unknown as string | null | undefined) ?? null;
    return cancelled ? Number(cancelled) : null;
  }

  private async cleanup(): Promise<void> {
    this.logger().debug(
      () =>
        `Completed iteration. executions=${this.executions} runtime=${this.runtimeSeconds.toFixed(
          3
        )}`
    );
    if (!this.jid) {
      return;
    }
    const redis = await Sidekiq.defaultConfiguration.getRedisClient();
    await redis.unlink(this.iterationKey());
  }

  private handleCompleted(completed: boolean | null): boolean {
    if (completed === null || completed === true) {
      return true;
    }
    if (completed === false) {
      return false;
    }
    throw new Error(`Unexpected completion value: ${String(completed)}`);
  }

  private monoNow(): number {
    return Number(process.hrtime.bigint()) / 1_000_000_000;
  }
}
