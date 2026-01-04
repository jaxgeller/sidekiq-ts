/**
 * Utilities for AbortController-based cancellation.
 */

/**
 * Sleep that can be interrupted by an AbortSignal.
 * Resolves immediately if signal is already aborted or when aborted during sleep.
 *
 * @param ms - Time to sleep in milliseconds
 * @param signal - Optional AbortSignal to allow early cancellation
 */
export async function sleepWithAbort(
  ms: number,
  signal: AbortSignal | undefined
): Promise<void> {
  if (!signal || signal.aborted) {
    if (signal?.aborted) {
      return;
    }
    // No signal provided, fall back to regular sleep
    await new Promise((resolve) => setTimeout(resolve, ms));
    return;
  }
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timeout);
      signal.removeEventListener("abort", onAbort);
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
