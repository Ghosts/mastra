/**
 * Leases and stored state of `sleep` and `sleepUntil` steps: the lease that
 * sets a sleep up once, the lease that wakes it once, and how recovery reads a
 * stored run's sleeps.
 */
import type { IMastraLogger } from '../../../logger';
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import type { RunFence, RunOwnershipRecord } from '../../../storage/run-fencing';
import { WORKFLOW_RUN_LEASE_TTL_MS, renewWorkflowLease } from '../../run-ownership';
import type { WorkflowRunState } from '../../types';
import { allNestedRuns, stepResultsOf } from './recovery-state';
import type { StoredNestedRun } from './recovery-state';

/** A sleep lease this process holds, renewed until it is released or abandoned. */
export interface SleepWakeLease {
  /** The work it guarded is done: releases the lease. On a wake lease this marks the occurrence woken. */
  release(): Promise<void>;
  /** Stops renewing and leaves the lease to lapse, so another process can claim it later. */
  abandon(): void;
  /**
   * Stops renewing and lets the lease run out now, without marking anything
   * done, once the work it guarded stopped before writing anything that lasts.
   */
  lapse(): Promise<void>;
  /**
   * Whether this process still holds the lease, checked by renewing it in the
   * store. `false` once it was abandoned or another process took it over.
   */
  held(): Promise<boolean>;
}

/** Renews the lease `record` grants until it is released or abandoned. */
function holdSleepLease(
  store: WorkflowsStorage,
  record: RunOwnershipRecord,
  logger: IMastraLogger | undefined,
): SleepWakeLease {
  const fence: RunFence = { runId: record.runId, generation: record.generation, ownerId: record.ownerId };
  const key = record.runId;
  let abandoned = false;
  const stopRenewing = renewWorkflowLease({
    store,
    fence,
    onLost: () => logger?.warn('Lost the lease of a workflow sleep', { key }),
    onError: error => logger?.warn('Failed to renew the lease of a workflow sleep', { key, error }),
  });

  return {
    async release() {
      stopRenewing();
      await store
        .releaseRunOwnership(fence)
        .catch(error => logger?.warn('Failed to release the lease of a workflow sleep', { key, error }));
    },

    abandon() {
      abandoned = true;
      stopRenewing();
    },

    async lapse() {
      abandoned = true;
      stopRenewing();
      // The fence fails once another process took the lease over, which leaves its lease alone.
      await store
        .renewRunOwnership({ ...fence, leaseMs: 1 })
        .catch(error => logger?.warn('Failed to let the lease of a workflow sleep lapse', { key, error }));
    },

    async held() {
      if (abandoned) {
        return false;
      }

      // A failed renewal reads as a lost lease: the caller then stops before writing or publishing.
      const renewal = await store.renewRunOwnership({ ...fence, leaseMs: WORKFLOW_RUN_LEASE_TTL_MS }).catch(error => {
        logger?.debug('Failed to check the lease of a workflow sleep', { key, error });
        return undefined;
      });
      // An abandon() while the renewal was on its way counts too.
      return !abandoned && !!renewal?.renewed;
    },
  };
}

/**
 * Claims the lease that serializes starting and resuming one sleep (see
 * `sleepStartKey`), without waiting: `null` while another delivery of the
 * sleep's event holds it. That delivery sets the occurrence up, and the broker
 * redelivers its event if its process stops first.
 */
export async function claimSleepStart({
  store,
  key,
  logger,
}: {
  store: WorkflowsStorage;
  key: string;
  logger?: IMastraLogger;
}): Promise<SleepWakeLease | null> {
  const claim = await store.claimRunOwnership({
    runId: key,
    ownerId: globalThis.crypto.randomUUID(),
    leaseMs: WORKFLOW_RUN_LEASE_TTL_MS,
    force: false,
  });

  return claim.acquired ? holdSleepLease(store, claim.record, logger) : null;
}

/**
 * Claims the lease of a sleep occurrence (see `sleepOccurrenceKey`) to wake it,
 * without waiting. `'woken'` once the lease was released: only a wake that
 * published the next step releases it, so the marker survives a restart that
 * writes an older snapshot back over the run. `'busy'` while another process
 * holds it, since that process is waking the same sleep. A lease that lapsed
 * unreleased is claimed again.
 */
export async function claimSleepWake({
  store,
  key,
  logger,
}: {
  store: WorkflowsStorage;
  key: string;
  logger?: IMastraLogger;
}): Promise<SleepWakeLease | 'busy' | 'woken'> {
  const current = await store.getRunOwnership({ runId: key });
  // Released wins over live: a released lease has no expiry, and nothing claims it again.
  if (current && !current.leaseExpiresAt) {
    return 'woken';
  }

  if (current?.live) {
    return 'busy';
  }

  // The claim expects the generation just read, so of two wakes racing only one claims it, even
  // when the first already released it by the time the second claims.
  const claim = await store.claimRunOwnership({
    runId: key,
    ownerId: globalThis.crypto.randomUUID(),
    leaseMs: WORKFLOW_RUN_LEASE_TTL_MS,
    force: false,
    expectedGeneration: current?.generation ?? 0,
  });
  if (!claim.acquired) {
    return claim.record && !claim.record.leaseExpiresAt ? 'woken' : 'busy';
  }

  return holdSleepLease(store, claim.record, logger);
}

/**
 * How long past its wake time a sleep must still be `waiting` before the
 * recovery loop wakes it. The process that started the sleep wakes it on time
 * with its own timer; the margin keeps the loop from racing a timer that is
 * still alive.
 */
export const SLEEP_WAKE_GRACE_MS = WORKFLOW_RUN_LEASE_TTL_MS;

/**
 * The wake time, in epoch milliseconds, that a sleep's `waiting` result
 * stores in `metadata.wakeAt`. `undefined` for a result written before wake
 * times were stored.
 */
export function storedWakeAt(result: { metadata?: unknown } | undefined): number | undefined {
  const wakeAt = (result?.metadata as { wakeAt?: unknown } | undefined)?.wakeAt;
  return typeof wakeAt === 'number' && Number.isFinite(wakeAt) ? wakeAt : undefined;
}

/**
 * Whether `path` is within the run's current `activePaths`. A sleep id can
 * linger in `activeStepsPath` after the run moved on; only a sleep on the
 * run's current path counts.
 */
function onCurrentPath(snapshot: WorkflowRunState, path: number[] | undefined): boolean {
  const activePaths = snapshot.activePaths ?? [];
  return Array.isArray(path) && activePaths.length > 0 && activePaths.every((index, i) => path[i] === index);
}

/**
 * How recovery treats an evented run because of its `sleep` and `sleepUntil`
 * steps, its own or those of nested runs its running steps drive:
 *
 * - `'awake'`: the run is in no sleep.
 * - `'overdue'`: a sleep is {@link SLEEP_WAKE_GRACE_MS} past its stored wake
 *   time, so the timer that should have woken it most likely died with its
 *   process.
 * - `'untimed'`: otherwise, a sleep started before wake times were stored,
 *   which only its own timer or a restart wakes. It wins over a sibling sleep
 *   that stored its wake time, which a restart only resumes.
 * - `'asleep'`: every sleep stored its wake time, and none is overdue.
 *
 * A sleep holds no lease: the process that started it wakes the run with a
 * timer, so a sleeping run looks orphaned to anything that only reads leases.
 * Only a `waiting` sleep the run lists in `activeStepsPath` at a path within
 * its current `activePaths` counts. `nested` holds the nested runs
 * `loadNestedRuns` read.
 */
export function sleepRecoveryState(
  snapshot: WorkflowRunState,
  nested: StoredNestedRun[],
): 'awake' | 'asleep' | 'overdue' | 'untimed' {
  let state: 'awake' | 'asleep' | 'overdue' | 'untimed' = 'awake';
  const now = Date.now();

  for (const run of [{ snapshot }, ...allNestedRuns(nested)]) {
    const active = run.snapshot.activeStepsPath ?? {};
    for (const [stepId, result] of stepResultsOf(run.snapshot.context)) {
      if (result?.status !== 'waiting' || !onCurrentPath(run.snapshot, active[stepId])) {
        continue;
      }

      const wakeAt = storedWakeAt(result);
      // One overdue sleep decides: its restart wakes it, and resumes the other sleeps.
      if (wakeAt !== undefined && now >= wakeAt + SLEEP_WAKE_GRACE_MS) {
        return 'overdue';
      }

      if (wakeAt === undefined) {
        state = 'untimed';
      } else if (state === 'awake') {
        state = 'asleep';
      }
    }
  }

  return state;
}
