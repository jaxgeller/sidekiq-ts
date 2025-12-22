export class IterableInterrupted extends Error {
  constructor(message = "Iterable job interrupted") {
    super(message);
    this.name = "IterableInterrupted";
  }
}

export class JobSkipError extends Error {
  constructor(message = "Job skipped") {
    super(message);
    this.name = "JobSkipError";
  }
}

export class IterableAbort extends Error {
  constructor() {
    super("Iterable job aborted");
    this.name = "IterableAbort";
  }
}
