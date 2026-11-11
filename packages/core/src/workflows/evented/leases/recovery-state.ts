/**
 * What the recovery loop reads from a stored evented run to tell whether its process stopped: the
 * steps it shows running, the nested runs it drives and whether it sleeps.
 */
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import type { RunOwnershipRecord } from '../../../storage/run-fencing';
import type { SerializedSingleStepEntry, WorkflowRunState } from '../../types';
import { isQueuedForeachIteration } from '../workflow-event-processor/loop';
import { forEachBounded } from './bounded';
import { runLivenessKey, stepOccurrenceKey } from './keys';

// Nested workflows nest a few levels at most; the bound only stops a corrupt snapshot from looping.
const MAX_NESTED_RUN_DEPTH = 16;

// Reads one recovery check or restart check sends at once while it walks a run's nested runs.
const NESTED_RUN_READ_CONCURRENCY = 8;

/**
 * The nested runs a running or suspended step result may still drive: its own
 * nested run, or the nested runs of the foreach items that have not finished.
 *
 * A suspended result counts because a nested run resumed directly runs while
 * the parent's step result, and a foreach item's output slot, stay `suspended`
 * until it finishes. A nested run that is still suspended is read for nothing:
 * it holds no lease and runs no step.
 */
function runningNestedRunIds(result: { status?: unknown; metadata?: unknown; output?: unknown } | undefined): string[] {
  if (result?.status !== 'running' && result?.status !== 'suspended') {
    return [];
  }

  const metadata = result.metadata as
    | { nestedRunId?: string; foreachNestedRunIds?: Record<string, string> }
    | undefined;
  const items = metadata?.foreachNestedRunIds;
  if (!items) {
    return metadata?.nestedRunId ? [metadata.nestedRunId] : [];
  }

  // A foreach item whose output slot holds a result finished; an empty, queued or suspended slot
  // may still run.
  const output = result.output;
  return Object.entries(items)
    .filter(([index]) => {
      const slot = Array.isArray(output) ? output[Number(index)] : undefined;
      return (
        slot === null ||
        slot === undefined ||
        isQueuedForeachIteration(slot) ||
        (slot as { status?: unknown }).status === 'suspended'
      );
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

/** A nested run a running step drives, as stored, with the nested runs it drives in turn. */
export interface StoredNestedRun {
  runId: string;
  snapshot: WorkflowRunState;
  /** When the nested run was last written, in milliseconds; `NaN` when unreadable. */
  updatedAt: number;
  nested: StoredNestedRun[];
}

/**
 * The nested runs the run's running steps drive, and the runs nested deeper,
 * each read from storage once, a few at a time. Empty for a run that drives
 * none, which costs no read.
 */
export async function loadNestedRuns({
  store,
  snapshot,
}: {
  store: WorkflowsStorage;
  snapshot: WorkflowRunState;
}): Promise<StoredNestedRun[]> {
  // The nested runs `snapshot` drives, each to be read into `into`, the list of its parent.
  const drivenBy = (snapshot: WorkflowRunState, into: StoredNestedRun[]) =>
    // Nested runs are stored under the id of the step that runs them.
    stepResultsOf(snapshot.context).flatMap(([stepId, result]) =>
      runningNestedRunIds(result).map(runId => ({ stepId, runId, into })),
    );

  const root: StoredNestedRun[] = [];
  let level = drivenBy(snapshot, root);
  // One level at a time, so the read limit holds for the whole tree rather than per parent.
  for (let depth = 0; depth < MAX_NESTED_RUN_DEPTH && level.length > 0; depth++) {
    const next: typeof level = [];
    await forEachBounded(level, NESTED_RUN_READ_CONCURRENCY, async ({ stepId, runId, into }) => {
      const run = await store.getWorkflowRunById({ runId, workflowName: stepId });
      if (!run) {
        return;
      }

      const nestedSnapshot: WorkflowRunState =
        typeof run.snapshot === 'string' ? JSON.parse(run.snapshot) : run.snapshot;
      const loaded: StoredNestedRun = {
        runId,
        snapshot: nestedSnapshot,
        updatedAt: new Date(run.updatedAt).getTime(),
        nested: [],
      };
      into.push(loaded);
      next.push(...drivenBy(nestedSnapshot, loaded.nested));
    });

    level = next;
  }

  return root;
}

/** Every nested run of `nested`, the deeper ones included. */
export function allNestedRuns(nested: StoredNestedRun[]): StoredNestedRun[] {
  return nested.flatMap(run => [run, ...allNestedRuns(run.nested)]);
}

/** The lease keys {@link nestedRunStoppedMidStep} reads: each nested run's liveness key and running step leases. */
export function nestedRunLeaseKeys(nested: StoredNestedRun[]): string[] {
  return allNestedRuns(nested).flatMap(run => [
    runLivenessKey(run.runId),
    ...runningStepOccurrenceKeys(run.runId, run.snapshot),
  ]);
}

/**
 * Whether one of the `nested` runs, or a run nested deeper, lost the worker
 * running one of its steps (see {@link stoppedMidStep}). A nested run a worker
 * keeps live did not, and neither did the runs it drives. `leases` holds the
 * keys {@link nestedRunLeaseKeys} lists.
 */
export function nestedRunStoppedMidStep(nested: StoredNestedRun[], leases: Map<string, RunOwnershipRecord>): boolean {
  return nested.some(
    run =>
      !leases.get(runLivenessKey(run.runId))?.live &&
      (stoppedMidStep(run.runId, run.snapshot, leases) || nestedRunStoppedMidStep(run.nested, leases)),
  );
}

/** Whether a worker keeps one of the `nested` runs, or a run nested deeper, live. */
export function nestedRunLive(nested: StoredNestedRun[], leases: Map<string, RunOwnershipRecord>): boolean {
  return allNestedRuns(nested).some(run => leases.get(runLivenessKey(run.runId))?.live);
}

/** When the run or one of the `nested` runs it drives was last written, in milliseconds. */
export function lastWriteOf(updatedAt: number, nested: StoredNestedRun[]): number {
  // An unreadable nested `updatedAt` must not make the whole run look unwritten.
  return Math.max(
    updatedAt,
    ...allNestedRuns(nested).map(run => (Number.isNaN(run.updatedAt) ? -Infinity : run.updatedAt)),
  );
}

/**
 * Whether the run, or one of the `nested` runs it drives, is in a `sleep` or
 * `sleepUntil` step. A sleep holds no lease: the process that started it wakes
 * the run with a timer, so a sleeping run looks orphaned to anything that only
 * reads leases. A sleep's result stays `waiting` after the run moves on, so
 * only a sleep the run lists in `activeStepsPath` at a path within its current
 * `activePaths` counts.
 */
export function isRunAsleep(snapshot: WorkflowRunState, nested: StoredNestedRun[]): boolean {
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

  return nested.some(run => isRunAsleep(run.snapshot, run.nested));
}
