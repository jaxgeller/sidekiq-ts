import type { Config } from "./config.js";

export type MiddlewareNext<TResult> = () => Promise<TResult> | TResult;

export type MiddlewareCall<TArgs extends unknown[], TResult> = (
  ...args: [...TArgs, MiddlewareNext<TResult>]
) => Promise<TResult> | TResult;

export type MiddlewareConstructor<TArgs extends unknown[], TResult> = new (
  ...args: unknown[]
) => {
  call: MiddlewareCall<TArgs, TResult>;
  config?: Config;
  setConfig?: (config: Config) => void;
};

class MiddlewareEntry<TArgs extends unknown[], TResult> {
  private config?: Config;
  readonly klass: MiddlewareConstructor<TArgs, TResult>;
  readonly args: unknown[];

  constructor(
    config: Config | undefined,
    klass: MiddlewareConstructor<TArgs, TResult>,
    args: unknown[]
  ) {
    this.config = config;
    this.klass = klass;
    this.args = args;
  }

  makeNew() {
    const instance = new this.klass(...this.args);
    if (this.config) {
      if (typeof instance.setConfig === "function") {
        instance.setConfig(this.config);
      } else if ("config" in instance) {
        instance.config = this.config;
      }
    }
    return instance;
  }
}

export class MiddlewareChain<TArgs extends unknown[], TResult> {
  private config?: Config;
  private entries: Array<MiddlewareEntry<TArgs, TResult>> = [];

  constructor(config?: Config) {
    this.config = config;
  }

  add(klass: MiddlewareConstructor<TArgs, TResult>, ...args: unknown[]): void {
    this.remove(klass);
    this.entries.push(new MiddlewareEntry(this.config, klass, args));
  }

  use(klass: MiddlewareConstructor<TArgs, TResult>, ...args: unknown[]): void {
    this.add(klass, ...args);
  }

  prepend(
    klass: MiddlewareConstructor<TArgs, TResult>,
    ...args: unknown[]
  ): void {
    this.remove(klass);
    this.entries.unshift(new MiddlewareEntry(this.config, klass, args));
  }

  insertBefore(
    oldklass: MiddlewareConstructor<TArgs, TResult>,
    newklass: MiddlewareConstructor<TArgs, TResult>,
    ...args: unknown[]
  ): void {
    const existingIndex = this.entries.findIndex(
      (entry) => entry.klass === newklass
    );
    const entry =
      existingIndex === -1
        ? new MiddlewareEntry(this.config, newklass, args)
        : this.entries.splice(existingIndex, 1)[0];
    const index = this.entries.findIndex((e) => e.klass === oldklass);
    if (index === -1) {
      this.entries.unshift(entry);
    } else {
      this.entries.splice(index, 0, entry);
    }
  }

  insertAfter(
    oldklass: MiddlewareConstructor<TArgs, TResult>,
    newklass: MiddlewareConstructor<TArgs, TResult>,
    ...args: unknown[]
  ): void {
    const existingIndex = this.entries.findIndex(
      (entry) => entry.klass === newklass
    );
    const entry =
      existingIndex === -1
        ? new MiddlewareEntry(this.config, newklass, args)
        : this.entries.splice(existingIndex, 1)[0];
    const index = this.entries.findIndex((e) => e.klass === oldklass);
    if (index === -1) {
      this.entries.push(entry);
    } else {
      this.entries.splice(index + 1, 0, entry);
    }
  }

  remove(klass: MiddlewareConstructor<TArgs, TResult>): void {
    this.entries = this.entries.filter((entry) => entry.klass !== klass);
  }

  exists(klass: MiddlewareConstructor<TArgs, TResult>): boolean {
    return this.entries.some((entry) => entry.klass === klass);
  }

  clear(): void {
    this.entries = [];
  }

  async invoke(...args: [...TArgs, MiddlewareNext<TResult>]): Promise<TResult> {
    if (this.entries.length === 0) {
      const last = args[args.length - 1] as MiddlewareNext<TResult>;
      return last();
    }

    const chain = this.entries.map((entry) => entry.makeNew());
    const callNext = async (index: number): Promise<TResult> => {
      if (index >= chain.length) {
        const last = args[args.length - 1] as MiddlewareNext<TResult>;
        return last();
      }
      const middleware = chain[index];
      const next: MiddlewareNext<TResult> = () => callNext(index + 1);
      return middleware.call(...(args.slice(0, -1) as TArgs), next);
    };

    return callNext(0);
  }
}
