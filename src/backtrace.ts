import { deflateSync } from "node:zlib";

export const extractBacktrace = (error: Error): string[] => {
  if (!error.stack) {
    return [];
  }
  const lines = error.stack.split("\n").map((line) => line.trim());
  if (lines.length > 0 && lines[0].startsWith(error.name)) {
    return lines.slice(1);
  }
  return lines;
};

export const compressBacktrace = (lines: string[]): string => {
  const serialized = JSON.stringify(lines);
  const compressed = deflateSync(serialized);
  return compressed.toString("base64");
};
