/**
 * Per-process liveness of evented runs: the `::live` key a process keeps fresh
 * while it runs any step of a run. Separate from step leases because one key
 * covers every step of a run that the process runs.
 */
import type { IMastraLogger } from '../../../logger';
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import { runOutsideRunFenceScope } from '../../../storage/run-fencing';
import type { RunFence } from '../../../storage/run-fencing';
import {
  WORKFLOW_RUN_LEASE_RENEW_INTERVAL_MS,
  WORKFLOW_RUN_LEASE_TTL_MS,
  repeatAfterSettled,
} from '../../run-ownership';
import { forEachBounded } from './bounded';
import { runLivenessKey } from './keys';

/** The process a step runs in, as seen by the liveness bookkeeping. */
export interface LivenessScope {
  /** True once the process started shutting down; it stops waiting for leases then. */
  readonly __shuttingDown: boolean;
  /** Whether the process's pubsub delivers events within this process only. */
  readonly __inProcessPubsub?: boolean;
}

// Liveness writes one renewal tick sends at once, so a process driving many runs does not flood the store.
export const LIVENESS_RENEW_CONCURRENCY = 8;
// A renew interval configured close to the TTL would leave no margin for a missed tick.
export const LIVENESS_RENEW_INTERVAL_MS = Math.max(
  1,
  Math.min(WORKFLOW_RUN_LEASE_RENEW_INTERVAL_MS, Math.floor(WORKFLOW_RUN_LEASE_TTL_MS / 3)),
);

/**
 * Keeps the liveness key of every run with a leaf step running in one process
 * fresh, writing each key about once per renew interval however many of the
 * run's steps the process runs. A single unref'd timer renews all of them.
 */
export class ProcessRunLiveness {
  readonly #ownerId = globalThis.crypto.randomUUID();
  /** Leaf steps running in this process per run, and the store the run's leaves last used. */
  readonly #active = new Map<string, { count: number; store: WorkflowsStorage }>();
  /**
   * When each run's last successful write started; the key holds a full TTL
   * from about then. Kept for a renew interval after the run's last leaf, so
   * the quick steps that follow it do not write again.
   */
  readonly #lastWriteAt = new Map<string, number>();
  /** The claim of each run's last write, kept while it may still be live so {@link end} can release it. */
  readonly #claims = new Map<string, { store: WorkflowsStorage; fence: RunFence; writtenAt: number }>();
  readonly #writing = new Map<string, Promise<void>>();
  /** False once {@link end} ran: this process writes no liveness key after that. */
  #vouching = true;
  /** Stops the renewal timer; set while it runs. */
  #stopTimer?: () => void;

  constructor(
    readonly scope: LivenessScope,
    readonly logger?: IMastraLogger,
  ) {}

  /**
   * Marks `runId` live and waits for the write, unless this process wrote it
   * within a renew interval. The run's first step event is published after
   * this, so a run is never `running` in storage without a live key.
   */
  async markLive(runId: string, store: WorkflowsStorage): Promise<void> {
    this.#ensureTimer();

    if (this.#age(runId) >= LIVENESS_RENEW_INTERVAL_MS) {
      await this.#write(runId, store);
    }
  }

  /**
   * Counts a leaf step of `runId` as running here and returns the function
   * that counts it out again. Writes the run's liveness key unless this
   * process wrote it within a renew interval.
   *
   * The leaf waits only for this process's first write of the run, or for a
   * write already in flight. A leaf that found the key fresh would otherwise
   * overtake one waiting on a later write, changing the order in which
   * concurrent steps (foreach items, parallel branches) start. Later writes
   * need no wait: the key is still live, and the step lease keeps a second
   * delivery of the step from running.
   */
  async enter(runId: string, store: WorkflowsStorage): Promise<() => void> {
    const active = this.#active.get(runId);
    this.#active.set(runId, { count: (active?.count ?? 0) + 1, store });
    this.#ensureTimer();

    const inFlight = this.#writing.get(runId);
    if (inFlight) {
      // Another leaf of this run is writing: wait so this one does not start ahead of it.
      await inFlight;
    } else if (!this.#lastWriteAt.has(runId)) {
      // This process never vouched for the run: the step must not start before the key is written.
      await this.#write(runId, store);
    } else if (this.#age(runId) >= LIVENESS_RENEW_INTERVAL_MS) {
      // The key is still live from the last write, so the step need not wait for this one.
      // `#write` logs its own failure and never rejects.
      void this.#write(runId, store);
    }

    let left = false;
    return () => {
      // Leaving twice must not count out another leaf of the run.
      if (left) {
        return;
      }

      left = true;
      const current = this.#active.get(runId);
      if (current && current.count > 1) {
        current.count--;
      } else {
        this.#active.delete(runId);
      }
    };
  }

  /** Milliseconds since this process last wrote the run's liveness key; infinite when it never did. */
  #age(runId: string): number {
    return Date.now() - (this.#lastWriteAt.get(runId) ?? -Infinity);
  }

  /** Starts the renewal timer unless it runs. */
  #ensureTimer() {
    if (this.#stopTimer) {
      return;
    }

    // Not setInterval: it fires on schedule while the previous tick's writes are still pending, so a
    // slow store would get a new round of writes on top of the old ones. The next tick starts one
    // interval after this one settles.
    // Created outside the triggering step's fence scope, which the timer would otherwise carry into every tick.
    this.#stopTimer = runOutsideRunFenceScope(() =>
      repeatAfterSettled(() => this.#renew(), LIVENESS_RENEW_INTERVAL_MS),
    );
  }

  /**
   * Marks the run live for one more lease TTL, or joins the write already in
   * flight for it. Every process force-claims the key, and only a process that
   * shut down releases it, so its generation only orders the writes: no write
   * is fenced on it and losing it to another process's write changes nothing.
   *
   * Never rejects: a failed write is logged here and leaves the last write
   * time alone, so the next leaf start or tick tries again. Skipped once the
   * process stopped vouching for its runs (see {@link end}), so they can be
   * recovered once their keys lapse.
   */
  #write(runId: string, store: WorkflowsStorage): Promise<void> {
    if (!this.#vouching) {
      return Promise.resolve();
    }

    const inFlight = this.#writing.get(runId);
    if (inFlight) {
      return inFlight;
    }

    const startedAt = Date.now();
    const write = (async () => {
      try {
        const { record } = await store.claimRunOwnership({
          runId: runLivenessKey(runId),
          ownerId: this.#ownerId,
          leaseMs: WORKFLOW_RUN_LEASE_TTL_MS,
          force: true,
        });
        this.#lastWriteAt.set(runId, startedAt);

        if (record) {
          const fence = { runId: record.runId, ownerId: record.ownerId, generation: record.generation };
          // Dated when the write returned: a slow write still holds a full TTL from about then,
          // which release() must not skip.
          this.#claims.set(runId, { store, fence, writtenAt: Date.now() });
        }
      } catch (error) {
        this.logger?.warn('Failed to mark a workflow run live', { runId, error });
      } finally {
        this.#writing.delete(runId);
      }
    })();

    this.#writing.set(runId, write);
    return write;
  }

  /**
   * Whether a step that finishes now keeps its step lease instead of releasing it. Once this
   * process stopped vouching for its runs, or while an in-process pubsub shuts down, the step's
   * end event may be lost without an error: a released lease would then make a delivery waiting
   * on it in another instance drop the occurrence as handled, while a lapsed one lets that
   * delivery claim it and continue the run from the saved result.
   */
  keepsStepLeases(): boolean {
    return !this.#vouching || (this.scope.__shuttingDown && !!this.scope.__inProcessPubsub);
  }

  /**
   * Stops vouching for this process's runs once its shutdown drained them: no liveness key is
   * written after this, so a step still running past the drain lets its run be recovered once
   * the key lapses. Until then the keys stay fresh, so no instance restarts a run this process
   * still drives.
   *
   * With `release`, also releases the key of every run this process wrote within a lease TTL.
   * With an in-process pubsub nothing else can deliver those runs' messages, and a released key
   * lets the instance that replaces this one restart them without waiting for the keys to lapse.
   * A key another process wrote since is left alone by the release's fence. Two instances in one
   * process that share one in-process pubsub are the exception: the other one may restart the
   * released runs while the shared pubsub still delivers their messages, and step leases and the
   * finished-step check keep each step from running twice.
   *
   * Without `release` it returns at once. With it, it waits at most one renew interval, so a slow
   * store cannot hold up the rest of shutdown; a key not released by then lapses as it would have
   * without the release. Runs once; later calls do nothing.
   */
  async end(release: boolean): Promise<void> {
    if (!this.#vouching) {
      return;
    }

    this.#vouching = false;
    this.#active.clear();
    this.#lastWriteAt.clear();

    if (!release) {
      // Nothing to release, so nothing to wait for: a write still in flight only keeps its key
      // live for one more lease TTL.
      this.#claims.clear();
      return;
    }

    // Never rejects: each release logs its own failure.
    const releasing = (async () => {
      // A write in flight lands after this, so its claim must be known before releasing.
      await Promise.allSettled([...this.#writing.values()]);

      const claims = [...this.#claims.values()].filter(
        claim => Date.now() - claim.writtenAt < WORKFLOW_RUN_LEASE_TTL_MS,
      );
      this.#claims.clear();
      await forEachBounded(claims, LIVENESS_RENEW_CONCURRENCY, async ({ store, fence }) => {
        try {
          await store.releaseRunOwnership(fence);
        } catch (error) {
          this.logger?.warn('Failed to release the liveness of a workflow run', { runId: fence.runId, error });
        }
      });

      return false;
    })();

    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<boolean>(resolve => {
      timer = setTimeout(() => resolve(true), LIVENESS_RENEW_INTERVAL_MS);
      timer.unref?.();
    });

    try {
      if (await Promise.race([releasing, timedOut])) {
        this.logger?.warn('Stopped waiting for the liveness of workflow runs to be released during shutdown', {
          timeoutMs: LIVENESS_RENEW_INTERVAL_MS,
        });
      }
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * One renewal tick: forgets runs with no running leaf whose last write is a
   * renew interval old, rewrites the key of every active run that is half an
   * interval old, and stops the timer once nothing is left to track.
   */
  async #renew() {
    // A finished run's last write time is kept for one interval so its next quick step skips the write.
    for (const runId of this.#lastWriteAt.keys()) {
      const idle = !this.#active.has(runId);
      if (idle && this.#age(runId) >= LIVENESS_RENEW_INTERVAL_MS) {
        this.#lastWriteAt.delete(runId);
      }
    }

    // A claim a lease TTL old has lapsed, so there is nothing left for end() to release.
    for (const [runId, claim] of this.#claims) {
      if (Date.now() - claim.writtenAt >= WORKFLOW_RUN_LEASE_TTL_MS) {
        this.#claims.delete(runId);
      }
    }

    if (this.#active.size === 0 && this.#lastWriteAt.size === 0) {
      // `#ensureTimer` starts a new chain if a leaf of any run starts later.
      this.#stopTimer?.();
      this.#stopTimer = undefined;
      return;
    }

    // Half an interval old is due: waiting a full interval could let the key lapse between ticks.
    const due = [...this.#active].filter(([runId]) => this.#age(runId) >= LIVENESS_RENEW_INTERVAL_MS / 2);
    await forEachBounded(due, LIVENESS_RENEW_CONCURRENCY, ([runId, { store }]) => this.#write(runId, store));
  }
}

/**
 * Liveness bookkeeping per Mastra instance, which stands for one process: it
 * sees every leaf the process runs, whether the in-process push subscription
 * or an orchestration worker delivered it, while each of those has its own
 * event processor.
 */
const livenessByProcess = new WeakMap<LivenessScope, ProcessRunLiveness>();

/** The liveness bookkeeping of the process `scope` stands for. */
export function processLiveness(scope: LivenessScope, logger?: IMastraLogger): ProcessRunLiveness {
  let liveness = livenessByProcess.get(scope);
  if (!liveness) {
    liveness = new ProcessRunLiveness(scope, logger);
    livenessByProcess.set(scope, liveness);
  }

  return liveness;
}

/**
 * Marks `runId` live before its first step event is published, so a booting
 * instance does not restart a run that is just starting. See
 * {@link ProcessRunLiveness.markLive}.
 */
export function markRunLive({
  scope,
  store,
  runId,
  logger,
}: {
  scope: LivenessScope;
  store: WorkflowsStorage;
  runId: string;
  logger?: IMastraLogger;
}): Promise<void> {
  return processLiveness(scope, logger).markLive(runId, store);
}

/**
 * Stops the process `scope` stands for from vouching for its runs once its shutdown drained
 * them, and with `release` releases the liveness keys it wrote. See {@link ProcessRunLiveness.end}.
 */
export async function endRunLiveness(scope: LivenessScope, { release }: { release: boolean }): Promise<void> {
  await livenessByProcess.get(scope)?.end(release);
}
