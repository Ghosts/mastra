import { normalizeConcurrency } from './run-with-concurrency';

interface QueuedRecovery {
  workflowName: string;
  runId: string;
  /** Checked when the run gets its slot; a run it finds no longer orphaned is not restarted. */
  stillOrphaned?: () => Promise<boolean>;
  /** Settles once the run's restart settled, or once the entry was dropped before it started. */
  done: Promise<void>;
  settle: () => void;
}

/**
 * The workflow runs this process is about to restart or is restarting. At
 * most `limit` restarts run at a time; the others wait in the order they were
 * queued. A run is queued once until its restart settles, however many
 * recovery passes find it meanwhile, so one process never recovers a run
 * twice at the same time, and the queue never holds more entries than there
 * are active runs.
 */
export class WorkflowRecoveryQueue {
  readonly #limit: number;
  readonly #restart: (workflowName: string, runId: string) => Promise<void>;
  readonly #onError: (error: unknown, run: { workflowName: string; runId: string }) => void;
  readonly #pending = new Map<string, QueuedRecovery>();
  readonly #running = new Map<string, QueuedRecovery>();
  #closed = false;

  /**
   * `restart` restarts one run. A restart, or a `stillOrphaned` check, that throws or rejects is
   * reported to `onError` and frees its slot, so it never rejects `enqueue()` for that run or the
   * runs around it.
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
   *
   * `stillOrphaned` runs when the run gets its slot, since a run can wait for
   * one long enough to recover, move into a sleep or finish. A run queued
   * again without it while it waits is restarted without the check.
   */
  enqueue(workflowName: string, runId: string, stillOrphaned?: () => Promise<boolean>): Promise<void> {
    const key = JSON.stringify([workflowName, runId]);
    const pending = this.#pending.get(key);
    if (pending) {
      if (!stillOrphaned) pending.stillOrphaned = undefined;
      return pending.done;
    }
    const running = this.#running.get(key);
    if (running) return running.done;
    if (this.#closed) return Promise.resolve();
    let settle!: () => void;
    const done = new Promise<void>(resolve => (settle = resolve));
    this.#pending.set(key, { workflowName, runId, stillOrphaned, done, settle });
    this.#startNext();
    return done;
  }

  /** Drops the runs still waiting for a slot and queues nothing more. Restarts already running go on. */
  close(): void {
    this.#closed = true;
    for (const entry of this.#pending.values()) entry.settle();
    this.#pending.clear();
  }

  /** Accepts runs again after {@link close}. */
  open(): void {
    this.#closed = false;
  }

  /**
   * Starts the oldest waiting runs while slots are free. Each frees its slot
   * and starts the next once its restart settles, however it settles.
   */
  #startNext(): void {
    while (this.#running.size < this.#limit) {
      const next = this.#pending.entries().next();
      if (next.done) return;
      const [key, entry] = next.value;
      this.#pending.delete(key);
      this.#running.set(key, entry);
      const run = { workflowName: entry.workflowName, runId: entry.runId };
      void Promise.resolve()
        .then(async () => {
          if (entry.stillOrphaned && !(await entry.stillOrphaned())) return;
          await this.#restart(run.workflowName, run.runId);
        })
        .catch(error => {
          try {
            this.#onError(error, run);
          } catch {
            // A failing error report must not keep the slot or stop the queue.
          }
        })
        .finally(() => {
          this.#running.delete(key);
          entry.settle();
          this.#startNext();
        });
    }
  }
}
