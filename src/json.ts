export const loadJson = (value: string): unknown => JSON.parse(value);

export const dumpJson = (value: unknown): string => JSON.stringify(value);
