import { AsyncLocalStorage } from "node:async_hooks";

export type ContextValue = unknown;

export type ContextMap = Record<string, ContextValue>;

export class Context {
  private static storage = new AsyncLocalStorage<ContextMap>();

  static current(): ContextMap {
    const store = this.storage.getStore();
    if (store) {
      return store;
    }
    const initial: ContextMap = {};
    this.storage.enterWith(initial);
    return initial;
  }

  static peek(): ContextMap | undefined {
    return this.storage.getStore();
  }

  static with<T>(values: ContextMap, fn: () => Promise<T> | T): Promise<T> | T {
    const merged = { ...this.current(), ...values };
    return this.storage.run(merged, fn);
  }

  static add(key: string, value: ContextValue): void {
    const store = this.storage.getStore();
    if (store) {
      store[key] = value;
      return;
    }
    this.storage.enterWith({ [key]: value });
  }
}
