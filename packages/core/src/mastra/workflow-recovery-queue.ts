import { normalizeConcurrency } from './run-with-concurrency';

interface QueuedRecovery {
  workflowName: string;
  runId: string;
  /** Settles once the run's restart settled, or once the entry was dropped before it started. */
  done: Promise<void>;
  settle: () => void;
}

/**
 * The workflow runs this process is about to restart or is restarting. At
 * most `limit` restarts run at a time; the others wait in the order they were
 * queued. A run is queued once until its restart settles, however many
 * recovery passes find it meanwhile, so one process never recovers a run
 * twice at the same time.
 */
export class WorkflowRecoveryQueue {
  readonly #limit: number;
  readonly #restart: (workflowName: string, runId: string) => Promise<void>;
  readonly #onError: (error: unknown, run: { workflowName: string; runId: string }) => void;
  readonly #pending = new Map<string, QueuedRecovery>();
  readonly #running = new Map<string, QueuedRecovery>();
  #closed = false;

  /**
   * `restart` restarts one run. A restart that throws or rejects is reported to `onError` and
   * frees its slot, so it never rejects `enqueue()` for that run or the runs around it.
   */
  constructor(
    limit: number | undefined,
    restart: (workflowName: string, runId: string) => Promise<void>,
    onError: (error: unknown, run: { workflowName: string; runId: string }) => void,
  ) {
    this.#limit = normalizeConcurrency(limit);
    this.#restart = restart;
    this.#onError = onError;
  }

  /** Whether the run is queued or being restarted. */
  has(workflowName: string, runId: string): boolean {
    const key = JSON.stringify([workflowName, runId]);
    return this.#pending.has(key) || this.#running.has(key);
  }

  /**
   * Queues a restart of the run unless it is already queued or running.
   * Returns a promise that settles once that restart settles. Once the queue
   * is closed nothing is queued and the promise is already settled.
   */
  enqueue(workflowName: string, runId: string): Promise<void> {
    const key = JSON.stringify([workflowName, runId]);
    const existing = this.#pending.get(key) ?? this.#running.get(key);
    if (existing) return existing.done;
    if (this.#closed) return Promise.resolve();
    let settle!: () => void;
    const done = new Promise<void>(resolve => (settle = resolve));
    this.#pending.set(key, { workflowName, runId, done, settle });
    this.#startNext();
    return done;
  }

  /** Drops the runs still waiting for a slot and queues nothing more. Restarts already running go on. */
  close(): void {
    this.#closed = true;
    for (const entry of this.#pending.values()) entry.settle();
    this.#pending.clear();
  }

  #startNext(): void {
    while (this.#running.size < this.#limit) {
      const next = this.#pending.entries().next();
      if (next.done) return;
      const [key, entry] = next.value;
      this.#pending.delete(key);
      this.#running.set(key, entry);
      const run = { workflowName: entry.workflowName, runId: entry.runId };
      void Promise.resolve()
        .then(() => this.#restart(run.workflowName, run.runId))
        .catch(error => this.#onError(error, run))
        .finally(() => {
          this.#running.delete(key);
          entry.settle();
          this.#startNext();
        });
    }
  }
}
