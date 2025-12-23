export type RegisteredJobClass = new () => {
  // biome-ignore lint/suspicious/noExplicitAny: allows typed job args via contravariance
  perform: (...args: any[]) => Promise<void> | void;
  jid?: string;
  _context?: { stopping: () => boolean };
};

const registry = new Map<string, RegisteredJobClass>();

export const registerJob = (klass: RegisteredJobClass): void => {
  if (klass?.name) {
    registry.set(klass.name, klass);
  }
};

export const resolveJob = (name: string): RegisteredJobClass | undefined =>
  registry.get(name);

export const registeredJob = (name: string): RegisteredJobClass | undefined =>
  registry.get(name);
