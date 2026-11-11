/**
 * What the recovery loop reads from a stored evented run to tell whether its process stopped: the
 * steps it shows running, the nested runs it drives and whether it sleeps.
 */
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import type { RunOwnershipRecord } from '../../../storage/run-fencing';
import type { SerializedSingleStepEntry, WorkflowRunState } from '../../types';
import { isQueuedForeachIteration } from '../workflow-event-processor/loop';
import { runLivenessKey, stepOccurrenceKey } from './keys';

// Nested workflows nest a few levels at most; the bound only stops a corrupt snapshot from looping.
export const MAX_NESTED_RUN_DEPTH = 16;

/**
 * The nested runs a running step result still drives: its own nested run, or
 * the nested runs of the foreach items that have not finished.
 */
export function runningNestedRunIds(
  result: { status?: unknown; metadata?: unknown; output?: unknown } | undefined,
): string[] {
  if (result?.status !== 'running') {
    return [];
  }

  const metadata = result.metadata as
    | { nestedRunId?: string; foreachNestedRunIds?: Record<string, string> }
    | undefined;
  const items = metadata?.foreachNestedRunIds;
  if (!items) {
    return metadata?.nestedRunId ? [metadata.nestedRunId] : [];
  }

  // A foreach item whose output slot is filled finished; an empty or queued slot still runs.
  const output = result.output;
  return Object.entries(items)
    .filter(([index]) => {
      const slot = Array.isArray(output) ? output[Number(index)] : undefined;
      return slot === null || slot === undefined || isQueuedForeachIteration(slot);
    })
    .map(([, nestedRunId]) => nestedRunId);
}

/** The step results of a run's context, without the run input and state stored next to them. */
function stepResultsOf(context: WorkflowRunState['context'] | undefined) {
  return Object.entries(context ?? {}).filter(([stepId]) => stepId !== 'input' && stepId !== '__state') as [
    string,
    { status?: unknown; metadata?: unknown; output?: unknown } | undefined,
  ][];
}

/** The id of a serialized leaf entry, the key its result is stored under. */
function serializedEntryId(entry: SerializedSingleStepEntry): string {
  return entry.type === 'step' ? entry.step.id : entry.id;
}

/**
 * The lease keys of the leaf step occurrences the stored run shows `running`,
 * found from its serialized step graph without reading anything else. A step
 * at the top level is at `[i]`, a parallel or conditional branch at `[i, j]`,
 * a foreach item at `[i, item]` and a loop body at `[i]` for its current
 * iteration.
 *
 * Records that only dispatch other work hold no step lease and are left out:
 * a step driving a nested run (`nestedRunId`, `foreachNestedRunIds`) and a
 * record a restart marked `__restartPending`. A key whose path this misses
 * (a graph the run no longer matches) is simply not read.
 */
export function runningStepOccurrenceKeys(runId: string, snapshot: WorkflowRunState): string[] {
  const context = snapshot.context as Record<
    string,
    { status?: unknown; metadata?: any; output?: unknown } | undefined
  >;
  const keys: string[] = [];

  const leaf = (entry: SerializedSingleStepEntry) => {
    const result = context?.[serializedEntryId(entry)];
    const metadata = result?.metadata;
    if (result?.status !== 'running' || metadata?.nestedRunId || metadata?.foreachNestedRunIds) {
      return undefined;
    }

    return metadata?.__restartPending ? undefined : result;
  };

  (snapshot.serializedStepGraph ?? []).forEach((entry, i) => {
    switch (entry.type) {
      case 'parallel':
      case 'conditional':
        entry.steps.forEach((branch, j) => {
          if (leaf(branch)) {
            keys.push(stepOccurrenceKey(runId, [i, j]));
          }
        });
        break;
      case 'loop': {
        const result = leaf(entry.step);
        if (result) {
          keys.push(stepOccurrenceKey(runId, [i], result.metadata?.iterationCount ?? 0));
        }
        break;
      }
      case 'foreach': {
        const output = leaf(entry.step)?.output;
        if (!Array.isArray(output)) {
          break;
        }

        // A filled slot is a finished item; an empty or queued one still holds a lease.
        output.forEach((slot, item) => {
          if (slot === null || slot === undefined || isQueuedForeachIteration(slot)) {
            keys.push(stepOccurrenceKey(runId, [i, item]));
          }
        });
        break;
      }
      // Sleeps hold no step lease.
      case 'sleep':
      case 'sleepUntil':
        break;
      default:
        if (leaf(entry)) {
          keys.push(stepOccurrenceKey(runId, [i]));
        }
    }
  });

  return keys;
}

/**
 * Whether the worker running a step of the run stopped mid-step: one of the
 * run's {@link runningStepOccurrenceKeys} has a lease that was never released
 * and has lapsed. A released lease means the step finished and its next event
 * is still on its way; a missing one means the step never started.
 */
export function stoppedMidStep(
  runId: string,
  snapshot: WorkflowRunState,
  leases: Map<string, RunOwnershipRecord>,
): boolean {
  return runningStepOccurrenceKeys(runId, snapshot).some(key => {
    const lease = leases.get(key);
    return !!lease && lease.leaseExpiresAt !== null && !lease.live;
  });
}

/**
 * Whether a nested run one of the run's running steps drives, or a run nested
 * deeper, lost the worker running one of its steps (see {@link stoppedMidStep}).
 * A nested run a worker keeps live did not. Reads each nested run's stored
 * state and, in one batch per nested run, its liveness key and step leases.
 */
export async function nestedRunStoppedMidStep({
  store,
  snapshot,
}: {
  store: WorkflowsStorage;
  snapshot: WorkflowRunState;
}): Promise<boolean> {
  const visit = async (snapshot: WorkflowRunState, depth: number): Promise<boolean> => {
    if (depth >= MAX_NESTED_RUN_DEPTH) {
      return false;
    }

    for (const [stepId, result] of stepResultsOf(snapshot.context)) {
      for (const nestedRunId of runningNestedRunIds(result)) {
        const nested = await store.loadWorkflowSnapshot({ workflowName: stepId, runId: nestedRunId });
        if (!nested) {
          continue;
        }

        const keys = runningStepOccurrenceKeys(nestedRunId, nested);
        const leases = await store.getRunOwnerships({ runIds: [runLivenessKey(nestedRunId), ...keys] });
        if (leases.get(runLivenessKey(nestedRunId))?.live) {
          continue;
        }

        if (stoppedMidStep(nestedRunId, nested, leases) || (await visit(nested, depth + 1))) {
          return true;
        }
      }
    }

    return false;
  };

  return visit(snapshot, 0);
}

/**
 * Whether the run, or a nested run one of its running steps drives, is in a
 * `sleep` or `sleepUntil` step. A sleep holds no lease: the process that
 * started it wakes the run with a timer, so a sleeping run looks orphaned to
 * anything that only reads leases. A sleep's result stays `waiting` after the
 * run moves on, so only a sleep the run lists in `activeStepsPath` at a path
 * within its current `activePaths` counts. Nested runs are read from storage,
 * at most one level per running nested step.
 */
export async function isRunAsleep({
  store,
  snapshot,
}: {
  store: WorkflowsStorage;
  snapshot: WorkflowRunState;
}): Promise<boolean> {
  const visit = async (snapshot: WorkflowRunState | null, depth: number): Promise<boolean> => {
    if (!snapshot) {
      return false;
    }

    const steps = stepResultsOf(snapshot.context);
    const active = snapshot.activeStepsPath ?? {};
    // A sleep id can linger in `activeStepsPath` after the run moved on; only a sleep on the
    // run's current path counts.
    const activePaths = snapshot.activePaths ?? [];
    const current = (path: number[] | undefined) =>
      Array.isArray(path) && activePaths.length > 0 && activePaths.every((index, i) => path[i] === index);
    if (steps.some(([stepId, result]) => result?.status === 'waiting' && current(active[stepId]))) {
      return true;
    }

    if (depth >= MAX_NESTED_RUN_DEPTH) {
      return false;
    }

    // Nested runs are stored under the id of the step that runs them.
    for (const [stepId, result] of steps) {
      for (const nestedRunId of runningNestedRunIds(result)) {
        if (await visit(await store.loadWorkflowSnapshot({ workflowName: stepId, runId: nestedRunId }), depth + 1)) {
          return true;
        }
      }
    }

    return false;
  };

  return visit(snapshot, 0);
}
