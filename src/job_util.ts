import { randomBytes } from "node:crypto";
import type {
  JobClassLike,
  JobOptions,
  JobPayload,
  StrictArgsMode,
} from "./types.js";
import { registerJob } from "./registry.js";

export const TRANSIENT_ATTRIBUTES: string[] = [];

const MAX_RETRY_FOR_SECONDS = 1_000_000_000;

export const nowInMillis = (): number =>
  Date.now();

export const generateJid = (): string => randomBytes(12).toString("hex");

const className = (klass: string | JobClassLike): string => {
  if (typeof klass === "string") {
    return klass;
  }
  const name = klass?.name;
  if (!name) {
    throw new Error("Job class must have a name");
  }
  return name;
};

const isJobClassLike = (value: unknown): value is JobClassLike =>
  typeof value === "function" ||
  (typeof value === "object" && value !== null && "name" in value);

export const validateItem = (item: JobPayload): void => {
  if (!item || typeof item !== "object") {
    throw new Error(`Job must be an object with 'class' and 'args' keys: ${item}`);
  }
  if (!("class" in item) || !("args" in item)) {
    throw new Error(`Job must include 'class' and 'args': ${JSON.stringify(item)}`);
  }
  if (!Array.isArray(item.args)) {
    throw new Error(`Job args must be an Array: ${JSON.stringify(item)}`);
  }
  if (!isJobClassLike(item.class) && typeof item.class !== "string") {
    throw new Error(
      `Job class must be a class or string name: ${JSON.stringify(item)}`
    );
  }
  if (item.at !== undefined && typeof item.at !== "number") {
    throw new Error(`Job 'at' must be a number timestamp: ${JSON.stringify(item)}`);
  }
  if (item.tags && !Array.isArray(item.tags)) {
    throw new Error(`Job tags must be an Array: ${JSON.stringify(item)}`);
  }
  if (typeof item.retry_for === "number" && item.retry_for > MAX_RETRY_FOR_SECONDS) {
    throw new Error(
      `retry_for must be a relative amount of time: ${JSON.stringify(item)}`
    );
  }
};

const normalizedHash = (
  itemClass: JobPayload["class"],
  defaultOptions: JobOptions
): JobOptions => {
  if (typeof itemClass === "function") {
    const klass = itemClass as JobClassLike;
    if (klass.getSidekiqOptions) {
      return klass.getSidekiqOptions();
    }
  }
  return defaultOptions;
};

export const normalizeItem = (
  item: JobPayload,
  defaultOptions: JobOptions
): JobPayload => {
  validateItem(item);

  let defaults = normalizedHash(item.class, defaultOptions);
  if (item.wrapped && typeof item.wrapped !== "string") {
    const wrapped = item.wrapped as JobClassLike;
    if (!wrapped.getSidekiqOptions) {
      // fall through without overriding defaults
    } else {
    defaults = {
      ...defaults,
      ...wrapped.getSidekiqOptions(),
    };
    }
  }

  const merged: JobPayload = {
    ...defaults,
    ...item,
  };

  if (!merged.queue || merged.queue === "") {
    throw new Error("Job must include a valid queue name");
  }

  TRANSIENT_ATTRIBUTES.forEach((key) => {
    delete merged[key];
  });

  merged.jid = merged.jid ?? generateJid();
  if (typeof merged.class === "function") {
    registerJob(merged.class);
  }
  merged.class = className(merged.class);
  if (merged.wrapped) {
    merged.wrapped = className(merged.wrapped);
  }
  merged.queue = String(merged.queue);
  if (typeof merged.retry_for === "number") {
    merged.retry_for = Math.trunc(merged.retry_for);
  }
  merged.created_at = merged.created_at ?? nowInMillis();
  return merged;
};

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

const jsonUnsafe = (value: unknown): unknown => {
  if (value === null) {
    return null;
  }
  const valueType = typeof value;
  if (valueType === "string" || valueType === "boolean") {
    return null;
  }
  if (valueType === "number") {
    return Number.isFinite(value) ? null : value;
  }
  if (valueType === "undefined" || valueType === "bigint" || valueType === "symbol") {
    return value;
  }
  if (valueType === "function") {
    return value;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const unsafe = jsonUnsafe(entry);
      if (unsafe !== null) {
        return unsafe;
      }
    }
    return null;
  }
  if (isPlainObject(value)) {
    const keys = Reflect.ownKeys(value);
    for (const key of keys) {
      if (typeof key !== "string") {
        return key;
      }
      const unsafe = jsonUnsafe(value[key]);
      if (unsafe !== null) {
        return unsafe;
      }
    }
    return null;
  }
  return value;
};

export const verifyJson = (args: unknown[], mode: StrictArgsMode): void => {
  if (mode === "none") {
    return;
  }
  const unsafe = jsonUnsafe(args);
  if (unsafe === null) {
    return;
  }
  const message =
    `Job arguments must be native JSON types, ` +
    `but ${String(unsafe)} is a ${typeof unsafe}. ` +
    "See https://github.com/sidekiq/sidekiq/wiki/Best-Practices";
  if (mode === "raise") {
    throw new Error(message);
  }
  console.warn(message);
};
