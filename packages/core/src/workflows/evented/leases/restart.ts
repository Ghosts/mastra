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
import { forEachBounded } from './bounded';
import { holdRestartLease } from './held-leases';
import { restartLeaseKey, runLivenessKey } from './keys';
import type { LivenessScope } from './liveness';
import { MAX_NESTED_RUN_DEPTH, NESTED_RUN_READ_CONCURRENCY, runningNestedRunIds } from './recovery-state';

/**
 * The liveness lease of the run, or of a nested run one of its running steps
 * drives, that is live and expires first. `null` when none is live. Nested
 * runs keep their own liveness key, and a parent step that only dispatched a
 * nested run holds no step lease while the nested run works.
 */
async function findLiveRun(
  store: WorkflowsStorage,
  runId: string,
  context: WorkflowRunState['context'] | undefined,
): Promise<RunOwnershipRecord | null> {
  let earliest: RunOwnershipRecord | null = null;

  const visit = async (runId: string, context: WorkflowRunState['context'] | undefined, depth: number) => {
    // The earliest expiry is reported, so a refused restart is retried as soon as any key could lapse.
    const record = await store.getRunOwnership({ runId: runLivenessKey(runId) });
    if (record?.live && (!earliest || record.leaseExpiresAt! < earliest.leaseExpiresAt!)) {
      earliest = record;
    }

    if (depth >= MAX_NESTED_RUN_DEPTH) {
      return;
    }

    const nested = Object.entries(context ?? {}).flatMap(([stepId, result]) =>
      runningNestedRunIds(result).map(nestedRunId => ({ stepId, nestedRunId })),
    );
    // Nested runs are stored under the id of the step that runs them.
    await forEachBounded(nested, NESTED_RUN_READ_CONCURRENCY, async ({ stepId, nestedRunId }) => {
      const snapshot = await store.loadWorkflowSnapshot({ workflowName: stepId, runId: nestedRunId });
      await visit(nestedRunId, snapshot?.context, depth + 1);
    });
  };

  await visit(runId, context, 0);
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

  const live = await findLiveRun(store, runId, snapshot.context);
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
