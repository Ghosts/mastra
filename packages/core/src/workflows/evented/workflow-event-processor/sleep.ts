import type { StepFlowEntry, WorkflowRunState } from '../..';
import { RequestContext } from '../../../di';
import type { PubSub } from '../../../events';
import type { IMastraLogger } from '../../../logger';
import type { WorkflowsStorage } from '../../../storage/domains/workflows';
import { withoutAuthToken } from '../helpers';
import {
  claimSleepStart,
  claimSleepWake,
  markRunLive,
  sleepOccurrenceKey,
  sleepStartKey,
  storedWakeAt,
} from '../leases';
import type { LivenessScope, SleepWakeLease } from '../leases';
import type { StepExecutor } from '../step-executor';
import { getStepId } from './utils';
import type { ProcessorArgs } from '.';

export async function processWorkflowWaitForEvent(
  workflowData: ProcessorArgs,
  {
    pubsub,
    eventName,
    currentState,
  }: {
    pubsub: PubSub;
    eventName: string;
    currentState: WorkflowRunState;
  },
) {
  const executionPath = currentState?.waitingPaths[eventName];
  if (!executionPath) {
    return;
  }

  const currentStepId = getStepId(workflowData.workflow, executionPath);
  const prevResult = {
    status: 'success',
    output: currentState?.context[currentStepId ?? 'input']?.payload,
  };

  await pubsub.publish('workflows', {
    type: 'workflow.step.run',
    runId: workflowData.runId,
    data: {
      workflowId: workflowData.workflowId,
      runId: workflowData.runId,
      executionPath,
      resumeSteps: [],
      resumeData: workflowData.resumeData,
      parentWorkflow: workflowData.parentWorkflow,
      stepResults: currentState?.context,
      prevResult,
      activeStepsPath: {},
      requestContext: withoutAuthToken(currentState?.requestContext),
      // Known gap (deliberately deferred — PR #24569 review, Superagent P2):
      // the actor signal is not persisted in the workflow snapshot, so a run
      // continued from a waitForEvent only keeps the actor if the resuming
      // event carried one. requestContext survives via the snapshot; actor
      // does not. Consequence: an FGA-gated tool with `requireActor` fails
      // closed after the wait even though the originating caller was
      // authorized, and permissive paths run unattributed. Intended fix:
      // persist the ActorSignal (a plain identity claim, no secret material)
      // in the snapshot alongside requestContext and restore it here with an
      // event-carried actor taking precedence: `workflowData.actor ?? snapshot`.
      actor: workflowData.actor,
      perStep: workflowData.perStep,
    },
  });
}

/** What the sleep handlers need from the event processor running them. */
interface SleepDeps {
  pubsub: PubSub;
  stepExecutor: StepExecutor;
  workflowsStore?: WorkflowsStorage;
  /** The store holding the run's step leases; `undefined` when the run's steps run unleased. */
  leaseStore?: WorkflowsStorage;
  /** The process running the sleep. */
  scope: LivenessScope;
  logger?: IMastraLogger;
}

/** One sleep occurrence as its `waiting` result stores it. Times are epoch milliseconds. */
interface SleepOccurrence {
  /** The id the result is stored under, which can differ from the sleep's id in this process. */
  stepId: string;
  /** The sleep's input, which it passes on to the next step. */
  payload: unknown;
  startedAt: number;
  wakeAt: number;
}

type SleepEntry = Extract<StepFlowEntry, { type: 'sleep' | 'sleepUntil' }>;

/** A wake this process set a timer for, or put aside while its workers are stopped. */
interface PendingWake {
  args: ProcessorArgs;
  deps: SleepDeps;
  occurrence: SleepOccurrence;
  timer?: ReturnType<typeof setTimeout>;
}

/** The pending wakes of the sleeps each process started or resumed. */
const pendingWakes = new WeakMap<LivenessScope, Set<PendingWake>>();
/** The wakes each process put aside when its workers stopped, set again when they start. */
const pausedWakes = new WeakMap<LivenessScope, PendingWake[]>();
/** The processes whose workers stopped, from {@link pauseSleepWakes} until {@link resumeSleepWakes}. */
const pausedScopes = new WeakSet<LivenessScope>();
/** The leases of the wakes each process is running. */
const activeWakes = new WeakMap<LivenessScope, Set<SleepWakeLease>>();

const MAX_TIMER_DELAY_MS = 2 ** 31 - 1;

const FINISHED_RUN_STATUSES = new Set(['success', 'failed', 'canceled', 'tripwire', 'bailed']);

const samePath = (a: number[] | undefined, b: number[]) =>
  Array.isArray(a) && a.length === b.length && a.every((index, i) => index === b[i]);

/**
 * Stops the wakes of the sleeps `scope` started or resumed. Called when its
 * workers stop, before their consumers go, since an event published from then
 * on might reach no worker.
 * Pending wake timers are cleared and kept for {@link resumeSleepWakes}. A
 * wake already running stops renewing its lease, so another process can wake
 * the sleep once the lease lapses, and stops before writing or publishing
 * unless it already published; once stopped, it lets its lease lapse and is
 * kept the same way. The sleeps stay `waiting`: a restart resumes
 * them, and the recovery loop of an instance still running wakes them once
 * they are overdue. A sleep set up or a wake lease claimed until the workers
 * start again, by work the drain gave up on, is put aside or abandoned the
 * same way.
 */
export function pauseSleepWakes(scope: LivenessScope): void {
  pausedScopes.add(scope);
  const pending = [...(pendingWakes.get(scope) ?? [])];
  pendingWakes.delete(scope);
  pending.forEach(wake => clearTimeout(wake.timer));
  if (pending.length > 0) pausedWakes.set(scope, [...(pausedWakes.get(scope) ?? []), ...pending]);
  activeWakes.get(scope)?.forEach(lease => lease.abandon());
}

/**
 * Sets the timers {@link pauseSleepWakes} cleared again, once the workers of
 * `scope` started again. Each fires at its sleep's wake time, or at once when
 * that passed.
 */
export function resumeSleepWakes(scope: LivenessScope): void {
  pausedScopes.delete(scope);
  const paused = pausedWakes.get(scope) ?? [];
  pausedWakes.delete(scope);
  paused.forEach(wake => scheduleWake(wake.args, wake.deps, wake.occurrence));
}

/** Starts a `sleep` step, or resumes it when the run is already in it. See {@link startSleep}. */
export async function processWorkflowSleep(
  args: ProcessorArgs,
  deps: SleepDeps & { step: Extract<StepFlowEntry, { type: 'sleep' }> },
) {
  return startSleep(args, deps, requestContext =>
    deps.stepExecutor.resolveSleep({
      workflowId: args.workflowId,
      step: deps.step,
      runId: args.runId,
      stepResults: args.stepResults,
      requestContext,
      input: args.prevResult?.status === 'success' ? args.prevResult.output : undefined,
      resumeData: args.resumeData,
      actor: args.actor,
    }),
  );
}

/** Starts a `sleepUntil` step, or resumes it when the run is already in it. See {@link startSleep}. */
export async function processWorkflowSleepUntil(
  args: ProcessorArgs,
  deps: SleepDeps & { step: Extract<StepFlowEntry, { type: 'sleepUntil' }> },
) {
  return startSleep(args, deps, requestContext =>
    deps.stepExecutor.resolveSleepUntil({
      workflowId: args.workflowId,
      step: deps.step,
      runId: args.runId,
      stepResults: args.stepResults,
      requestContext,
      input: args.prevResult?.status === 'success' ? args.prevResult.output : undefined,
      resumeData: args.resumeData,
      actor: args.actor,
    }),
  );
}

/**
 * Records the sleep as `waiting` with its wake time in `metadata.wakeAt`, and
 * sets a timer that wakes the run then (see {@link wakeSleep}).
 *
 * When the stored run is already in this sleep (a restart, or a redelivery of
 * the sleep's event), the stored wake time is kept and only the remaining wait
 * is timed, so the sleep is not replayed from the start and a wake time that
 * passed wakes the run at once. Time travel always starts the sleep afresh.
 * `resolveDuration` runs only for a fresh sleep.
 *
 * A delivery that arrives after the run left the sleep (it finished, moved
 * past the sleep, or the sleep was woken and the next step is on its way) is
 * dropped; see {@link leftSleep} for what counts under time travel.
 *
 * With step leases, deliveries of the sleep's event set it up one at a time
 * (see {@link claimSleepStart}): one that finds another delivery doing so is
 * dropped, and the stored run is read only once the lease is held.
 */
async function startSleep(
  args: ProcessorArgs,
  deps: SleepDeps & { step: SleepEntry },
  resolveDuration: (requestContext: RequestContext) => Promise<number>,
) {
  const { workflowId, runId, executionPath } = args;
  const start = deps.leaseStore
    ? await claimSleepStart({ store: deps.leaseStore, key: sleepStartKey(runId, executionPath), logger: deps.logger })
    : undefined;
  if (start === null) {
    deps.logger?.debug('Dropping a delivery of a sleep another delivery is starting', {
      workflowId,
      runId,
      executionPath,
    });
    return;
  }
  try {
    await setUpSleep(args, deps, resolveDuration);
  } finally {
    await start?.release();
  }
}

/** The part of {@link startSleep} that runs under the sleep's start lease. */
async function setUpSleep(
  args: ProcessorArgs,
  deps: SleepDeps & { step: SleepEntry },
  resolveDuration: (requestContext: RequestContext) => Promise<number>,
) {
  const { workflow, workflowId, runId, executionPath, activeStepsPath, prevResult, requestContext, timeTravel } = args;
  const { pubsub, workflowsStore, leaseStore, step } = deps;
  // Step-lifecycle watch events honor `emitStepEvents: false` (#21529). The
  // `workflows` routing publish in wakeSleep is never gated: it drives execution.
  const emitStepEvents = workflow.options.emitStepEvents !== false;
  const snapshot = await workflowsStore?.loadWorkflowSnapshot({ workflowName: workflowId, runId });
  // A time-travelled run whose `running` snapshots are left out stored nothing yet: the stored
  // run is still the one it travels from, so it cannot show this run past the sleep.
  const storesProgress =
    !timeTravel ||
    (workflow.options.shouldPersistSnapshot?.({ stepResults: args.stepResults ?? {}, workflowStatus: 'running' }) ??
      true);
  if (snapshot && storesProgress && leftSleep(snapshot, executionPath, !!timeTravel)) {
    deps.logger?.debug('Dropping a delivery of a sleep the workflow run already left', {
      workflowId,
      runId,
      executionPath,
    });
    return;
  }
  const resumed = snapshot && !timeTravel ? sleepInProgress(snapshot, executionPath, step.id) : undefined;
  // A sleep woken since the stored run was written (a restart or a redelivery wrote back an older
  // read) must not be set up again: that would put the run back at the sleep.
  if (resumed && leaseStore) {
    const wake = await leaseStore.getRunOwnership({
      runId: sleepOccurrenceKey(runId, executionPath, resumed.startedAt),
    });
    if (wake && !wake.leaseExpiresAt) {
      deps.logger?.debug('Dropping a delivery of a sleep that was already woken', { workflowId, runId, executionPath });
      return markWokenSleepFinished(args, deps, resumed);
    }
  }
  const stepId = resumed?.stepId ?? step.id;
  const payload = resumed ? resumed.payload : prevResult.status === 'success' ? prevResult.output : undefined;
  const startedAt = resumed?.startedAt ?? Date.now();

  if (emitStepEvents) {
    await pubsub.publish(`workflow.events.v2.${runId}`, {
      type: 'watch',
      runId,
      data: {
        type: 'workflow-step-waiting',
        payload: { id: stepId, status: 'waiting', payload, startedAt },
      },
    });
  }

  let wakeAt = resumed?.wakeAt;
  if (wakeAt === undefined) {
    // Create a proper RequestContext from the plain object passed in ProcessorArgs
    const duration = await resolveDuration(new RequestContext(Object.entries(requestContext ?? {}) as any));
    wakeAt = Date.now() + (Number.isFinite(duration) ? Math.max(0, duration) : 0);
  }

  await workflowsStore?.updateWorkflowResults({
    workflowName: workflowId,
    runId,
    stepId,
    result: { status: 'waiting', payload, startedAt, metadata: { wakeAt } },
    requestContext: withoutAuthToken(requestContext ?? {}),
  });
  await workflowsStore?.updateWorkflowState({
    workflowName: workflowId,
    runId,
    opts: {
      status: 'running',
      activePaths: executionPath,
      activeStepsPath: { ...activeStepsPath, [stepId]: executionPath },
    },
  });

  // A restart derives the input from the sleep's id in this process, which may not be the stored one.
  const wakeArgs: ProcessorArgs = resumed
    ? { ...args, prevResult: { ...prevResult, status: 'success', output: payload } as ProcessorArgs['prevResult'] }
    : args;
  scheduleWake(wakeArgs, deps, { stepId, payload, startedAt, wakeAt });
}

/**
 * Sets the timer that calls {@link wakeSleep} at the occurrence's wake time,
 * or puts the wake aside for {@link resumeSleepWakes} while the workers of the
 * process are stopped.
 */
function scheduleWake(args: ProcessorArgs, deps: SleepDeps, occurrence: SleepOccurrence) {
  if (pausedScopes.has(deps.scope)) {
    pausedWakes.set(deps.scope, [...(pausedWakes.get(deps.scope) ?? []), { args, deps, occurrence }]);
    return;
  }
  let pending = pendingWakes.get(deps.scope);
  if (!pending) pendingWakes.set(deps.scope, (pending = new Set()));
  const wake: PendingWake = { args, deps, occurrence, timer: undefined! };
  wake.timer = setTimeout(
    () => {
      pending.delete(wake);
      void wakeSleep(args, deps, occurrence).catch(error =>
        deps.logger?.error('Failed to wake a sleeping workflow run', {
          workflowId: args.workflowId,
          runId: args.runId,
          stepId: occurrence.stepId,
          error,
        }),
      );
    },
    // A longer delay overflows and fires at once; wakeSleep sets the timer again until the wake time.
    Math.min(Math.max(0, occurrence.wakeAt - Date.now()), MAX_TIMER_DELAY_MS),
  );
  pending.add(wake);
}

/**
 * Whether the stored run already left the sleep at `executionPath`: its
 * `activePaths` moved past the sleep, it finished, or it lists a finished
 * sleep at that path, which a wake marks before publishing the next step.
 *
 * Under `timeTravel` only the first counts. A time-travelled run can still
 * show the status and results of the run it travels from, for example when
 * `shouldPersistSnapshot` leaves out `running` snapshots, and its steps move
 * `activePaths` as they run. When `running` snapshots are left out, the
 * caller skips this check under `timeTravel`, as the step handler skips its
 * finished-step check: a late delivery of the sleep's event then starts it
 * again.
 */
function leftSleep(snapshot: WorkflowRunState, executionPath: number[], timeTravel: boolean): boolean {
  if ((snapshot.activePaths?.[0] ?? -1) > executionPath[0]!) return true;
  if (timeTravel) return false;
  if (FINISHED_RUN_STATUSES.has(snapshot.status)) return true;
  if (!samePath(snapshot.activePaths, executionPath)) return false;
  return Object.entries(snapshot.activeStepsPath ?? {}).some(
    ([stepId, path]) => samePath(path, executionPath) && snapshot.context?.[stepId]?.status === 'success',
  );
}

/**
 * The sleep occurrence the stored run is in at `executionPath`: a `waiting`
 * result with a stored wake time that the run lists as active at that path.
 * The result stored under `stepId` is preferred; otherwise one is matched by
 * path, because a sleep declared without an `id` gets a new generated id in
 * every process that builds the workflow. A sleep recorded before wake times
 * were stored is not resumed.
 */
function sleepInProgress(
  snapshot: WorkflowRunState,
  executionPath: number[],
  stepId: string,
): SleepOccurrence | undefined {
  const active = snapshot.activeStepsPath ?? {};
  const ids = [stepId, ...Object.keys(active).filter(id => id !== stepId)];
  for (const id of ids) {
    const stored = snapshot.context?.[id];
    const wakeAt = storedWakeAt(stored);
    if (!samePath(active[id], executionPath) || stored?.status !== 'waiting' || wakeAt === undefined) continue;
    return { stepId: id, payload: stored.payload, startedAt: stored.startedAt, wakeAt };
  }
}

/**
 * Ends the sleep `occurrence` and publishes the step after it. The sleep's
 * timer calls it. The recovery loop reaches it by restarting a run whose
 * sleep is overdue: the restart resumes the sleep in whichever process takes
 * it, and the timer it sets fires at once.
 *
 * Waking more than once is a no-op. The wake claims the occurrence's lease
 * (see {@link claimSleepWake}) and gives up while another process holds it
 * or once a wake released it. It then wakes only while the stored run is
 * still active and in this occurrence. It marks the run live and the sleep
 * `success` before publishing, so a restart is refused until the next step
 * takes its own lease, and releases the lease only once the next step was
 * published. When publishing fails, the sleep is put back to `waiting` and
 * the lease left to lapse, so an overdue check wakes it again.
 *
 * Leases are advisory, so the wake checks that it still holds its lease
 * before marking the sleep `success` and again before publishing, and stops
 * (putting the sleep back to `waiting` if it already marked it) when the
 * lease lapsed or was abandoned meanwhile. A wake stalled past the lease TTL
 * between that check and the publish can still publish twice.
 *
 * A call before the wake time sets the timer again; a process shutting down
 * does not wake, and the recovery loop of another instance wakes the sleep
 * once it is overdue.
 */
async function wakeSleep(args: ProcessorArgs, deps: SleepDeps, occurrence: SleepOccurrence) {
  const { workflow, workflowId, runId, executionPath, prevResult, requestContext } = args;
  const { pubsub, workflowsStore, leaseStore, scope, logger } = deps;
  const { stepId, payload, startedAt } = occurrence;
  if (scope.__shuttingDown) return;
  if (Date.now() < occurrence.wakeAt) return scheduleWake(args, deps, occurrence);
  const key = sleepOccurrenceKey(runId, executionPath, startedAt);
  const lease = leaseStore ? await claimSleepWake({ store: leaseStore, key, logger }) : undefined;
  if (lease === 'busy') {
    logger?.debug('Another process is waking this workflow sleep', { workflowId, runId, stepId });
    return;
  }
  if (lease === 'woken') {
    logger?.debug('This workflow sleep was already woken', { workflowId, runId, stepId });
    return markWokenSleepFinished(args, deps, occurrence);
  }
  // Stopping the workers abandons it (see pauseSleepWakes).
  let active = activeWakes.get(scope);
  if (lease) {
    if (!active) activeWakes.set(scope, (active = new Set()));
    active.add(lease);
    // The workers stopped while the claim was on its way.
    if (pausedScopes.has(scope)) lease.abandon();
  }
  // Set when stopping the workers stopped this wake, which then runs again once they start.
  let interrupted = false;
  // A lease that lapsed while this wake waited on the store may already be another wake's.
  const lost = async () => {
    if (!lease || (await lease.held())) return false;
    interrupted = pausedScopes.has(scope);
    logger?.debug('Lost the lease of a workflow sleep while waking it', { workflowId, runId, stepId });
    return true;
  };
  let published = false;
  try {
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({ workflowName: workflowId, runId });
    // A run with no stored state has nothing to check against, and its timer wakes it.
    if (snapshot) {
      const stored = snapshot.context?.[stepId];
      const sameSleep =
        (snapshot.status === 'running' || snapshot.status === 'waiting') &&
        stored?.status === 'waiting' &&
        stored.startedAt === startedAt &&
        storedWakeAt(stored) === occurrence.wakeAt;
      if (!sameSleep) {
        logger?.debug('Not waking a workflow sleep the run already left', { workflowId, runId, stepId });
        return;
      }
    }
    if (leaseStore) await markRunLive({ scope, store: leaseStore, runId, logger });
    if (await lost()) return;

    const endedAt = Date.now();
    await workflowsStore?.updateWorkflowResults({
      workflowName: workflowId,
      runId,
      stepId,
      result: { status: 'success', payload, output: payload, startedAt, endedAt },
      requestContext: withoutAuthToken(requestContext ?? {}),
    });
    // The run leaves the sleep; a step after it must not count it as active.
    const { [stepId]: _woken, ...activeStepsPath } = args.activeStepsPath ?? {};
    if (await lost()) {
      await revertWake(args, deps, occurrence, endedAt);
      return;
    }

    try {
      if (workflow.options.emitStepEvents !== false) {
        await pubsub.publish(`workflow.events.v2.${runId}`, {
          type: 'watch',
          runId,
          data: {
            type: 'workflow-step-result',
            payload: {
              id: stepId,
              status: 'success',
              payload,
              output: payload,
              startedAt,
              endedAt,
            },
          },
        });
        await pubsub.publish(`workflow.events.v2.${runId}`, {
          type: 'watch',
          runId,
          data: { type: 'workflow-step-finish', payload: { id: stepId, metadata: {} } },
        });
      }

      await pubsub.publish('workflows', {
        type: 'workflow.step.run',
        runId,
        data: {
          workflowId,
          runId,
          executionPath: executionPath.slice(0, -1).concat([executionPath[executionPath.length - 1]! + 1]),
          resumeSteps: args.resumeSteps,
          timeTravel: args.timeTravel,
          restart: args.restart,
          stepResults: args.stepResults,
          prevResult,
          resumeData: args.resumeData,
          parentWorkflow: args.parentWorkflow,
          activeStepsPath,
          requestContext,
          actor: args.actor,
          perStep: args.perStep,
        },
      });
      published = true;
    } catch (error) {
      // Back in `waiting`, the sleep is woken by the next overdue check rather than the much later stall check.
      await revertWake(args, deps, occurrence, endedAt).catch(revertError =>
        logger?.error('Failed to put a workflow sleep back to waiting after its wake failed', {
          workflowId,
          runId,
          stepId,
          error: revertError,
        }),
      );
      throw error;
    }
  } finally {
    if (lease) active?.delete(lease);
    if (published) await lease?.release();
    else if (lease && interrupted) {
      // The timer that started this wake is gone, so the wake is kept for resumeSleepWakes. Its own
      // lease would hold the next attempt off for a lease TTL.
      await lease.lapse();
      scheduleWake(args, deps, occurrence);
    } else lease?.abandon();
  }
}

/**
 * Puts the sleep back to `waiting` after its wake wrote `success` but failed
 * to publish the next step, unless the stored result changed since.
 */
async function revertWake(args: ProcessorArgs, deps: SleepDeps, occurrence: SleepOccurrence, endedAt: number) {
  const { workflowId, runId, requestContext } = args;
  const snapshot = await deps.workflowsStore?.loadWorkflowSnapshot({ workflowName: workflowId, runId });
  const stored = snapshot?.context?.[occurrence.stepId];
  if (stored?.status !== 'success' || stored.startedAt !== occurrence.startedAt || stored.endedAt !== endedAt) return;
  await deps.workflowsStore?.updateWorkflowResults({
    workflowName: workflowId,
    runId,
    stepId: occurrence.stepId,
    result: {
      status: 'waiting',
      payload: occurrence.payload,
      startedAt: occurrence.startedAt,
      metadata: { wakeAt: occurrence.wakeAt },
    },
    requestContext: withoutAuthToken(requestContext ?? {}),
  });
}

/**
 * Marks the sleep `success` when another wake already published the next step
 * but the stored result shows the occurrence `waiting` again, as it does after
 * a restart or a redelivery wrote back what it read before that wake.
 */
async function markWokenSleepFinished(args: ProcessorArgs, deps: SleepDeps, occurrence: SleepOccurrence) {
  const { workflowId, runId, requestContext } = args;
  const snapshot = await deps.workflowsStore?.loadWorkflowSnapshot({ workflowName: workflowId, runId });
  const stored = snapshot?.context?.[occurrence.stepId];
  if (stored?.status !== 'waiting' || stored.startedAt !== occurrence.startedAt) return;
  await deps.workflowsStore?.updateWorkflowResults({
    workflowName: workflowId,
    runId,
    stepId: occurrence.stepId,
    result: {
      status: 'success',
      payload: occurrence.payload,
      output: occurrence.payload,
      startedAt: occurrence.startedAt,
      endedAt: Date.now(),
    },
    requestContext: withoutAuthToken(requestContext ?? {}),
  });
}
