/**
 * The `restart()` side of evented leases: whether a run, or a nested run it
 * drives, is still live, and the short lease that lets one concurrent restart
 * publish.
 */
import type { IMastraLogger } from '../../../logger';
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import type { RunOwnershipRecord } from '../../../storage/run-fencing';
import { WORKFLOW_RUN_LEASE_TTL_MS } from '../../run-ownership';
import type { WorkflowRunOwnership } from '../../run-ownership';
import type { WorkflowRunState } from '../../types';
import { holdRestartLease } from './held-leases';
import { restartLeaseKey, runLivenessKey } from './keys';
import type { LivenessScope } from './liveness';
import { allNestedRuns, loadNestedRuns } from './recovery-state';

/**
 * The liveness lease of the run, or of a nested run one of its running steps
 * drives, that is live and expires first. `null` when none is live. Nested
 * runs keep their own liveness key, and a parent step that only dispatched a
 * nested run holds no step lease while the nested run works.
 */
async function findLiveRun(
  store: WorkflowsStorage,
  runId: string,
  snapshot: WorkflowRunState,
): Promise<RunOwnershipRecord | null> {
  const nested = await loadNestedRuns({ store, snapshot });
  const keys = [runId, ...allNestedRuns(nested).map(run => run.runId)].map(runLivenessKey);
  const leases = await store.getRunOwnerships({ runIds: keys });

  // The earliest expiry is reported, so a refused restart is retried as soon as any key could lapse.
  let earliest: RunOwnershipRecord | null = null;
  for (const record of leases.values()) {
    if (record.live && (!earliest || record.leaseExpiresAt! < earliest.leaseExpiresAt!)) {
      earliest = record;
    }
  }

  return earliest;
}

/**
 * Claims an evented run for `restart()`. Returns the live liveness lease
 * instead while a worker runs a step of the run or of a nested run it drives,
 * so the restart is refused until the run could be orphaned.
 *
 * Otherwise claims the short `restart()` lease, so that of several concurrent
 * restarts only one publishes. Liveness is read first so a refused restart
 * takes no lease that would block a later one. The lease is kept until a
 * worker marks the run live (see {@link holdRestartLease}); after that the
 * restarted steps hold their own leases and it is left to lapse.
 */
export async function claimEventedRestart({
  scope,
  store,
  runId,
  snapshot,
  logger,
}: {
  scope?: LivenessScope;
  store: WorkflowsStorage | undefined;
  runId: string;
  snapshot: WorkflowRunState;
  logger?: IMastraLogger;
}): Promise<WorkflowRunOwnership | { owner: RunOwnershipRecord | null }> {
  const unfenced: WorkflowRunOwnership = { release: async () => {} };
  if (!store || !(await store.supportsRunFencing())) {
    return unfenced;
  }

  const live = await findLiveRun(store, runId, snapshot);
  if (live) {
    return { owner: live };
  }

  const claim = await store.claimRunOwnership({
    runId: restartLeaseKey(runId),
    ownerId: globalThis.crypto.randomUUID(),
    leaseMs: WORKFLOW_RUN_LEASE_TTL_MS,
    force: false,
  });
  return claim.acquired
    ? holdRestartLease({ scope, store, runId, record: claim.record, logger })
    : { owner: claim.record };
}
