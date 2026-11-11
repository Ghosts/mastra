/**
 * What the recovery loop reads from a stored evented run to tell whether its process stopped: the
 * steps it shows running, the nested runs it drives and whether it sleeps.
 */
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import type { WorkflowRunState } from '../../types';
import { isQueuedForeachIteration } from '../workflow-event-processor/loop';

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

/**
 * Whether a step of the run was running when its snapshot was stored. A run
 * whose liveness lapsed with a step still running lost the worker running it;
 * one with none is between steps, waiting for the message of its next step.
 */
export function hasRunningStep(snapshot: WorkflowRunState): boolean {
  return stepResultsOf(snapshot.context).some(([, result]) => result?.status === 'running');
}

/**
 * Whether the run, or a nested run one of its running steps drives, is in a
 * `sleep` or `sleepUntil` step. A sleep holds no lease: the process that
 * started it wakes the run with a timer, so a sleeping run looks orphaned to
 * anything that only reads leases. A sleep's result stays `waiting` after the
 * run moves on, so only a sleep the run still lists in `activeStepsPath`
 * counts. Nested runs are read from storage, at most one level per running
 * nested step.
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
    if (steps.some(([stepId, result]) => result?.status === 'waiting' && stepId in active)) {
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
