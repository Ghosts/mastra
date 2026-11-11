/**
 * A sleep is woken by a timer in the process that started it, so it used to be
 * lost when that process stopped (#30012). Its wake time is stored now: the
 * recovery loop of another instance restarts a run whose sleep is overdue,
 * `restart()` waits only for what's left of a sleep, and
 * `restartAllActiveWorkflowRuns()` leaves a sleep that isn't due to the loop
 * when this instance runs one.
 *
 * Replicas are separate Mastra instances, each with its own pubsub, sharing
 * one store the way pods share one database. Leases use a short TTL here, read
 * when core loads.
 */
vi.hoisted(() => {
  process.env.MASTRA_AGENT_THREAD_LEASE_TTL_MS = '600';
});

import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventEmitterPubSub } from '../../events/event-emitter';
import type { MastraRecoveryConfig } from '../../mastra';
import { InMemoryStore } from '../../storage/mock';
import { runLivenessKey, SLEEP_WAKE_GRACE_MS } from './leases';
import { forgetWakeTime } from './test-utils/forget-wake-time';
import {
  Num,
  crash,
  interceptWorkflowEvents,
  isStepRunAt,
  observe,
  recordRestarts,
  runStatus,
  scanning,
  sleep,
  sleepStatuses,
  sleepingFor,
  sleepingRunId,
  sleepingWorkflows,
  startInBackground,
  startReplica,
  stopReplicas,
  work,
} from './test-utils/lease-replicas';
import type { Built, Report } from './test-utils/lease-replicas';
import { createWorkflow } from '.';

afterEach(stopReplicas);

describe('recovery of evented runs in a sleep (#30012)', () => {
  it.each(Object.keys(sleepingWorkflows) as (keyof typeof sleepingWorkflows)[])(
    'a replica still running wakes a run in a %s once its replica stopped, and not before the sleep is overdue',
    async name => {
      const storage = new InMemoryStore();
      const seen = observe(name === 'sleep of a nested workflow' ? 'child' : 'leased');
      const replica = async (replicaName: string, recovery?: MastraRecoveryConfig) => {
        const { workflow, nested } = sleepingWorkflows[name](seen.reporter(replicaName));
        return startReplica(storage, workflow as unknown as Built, {
          pubsub: seen.pubsub(replicaName),
          nested: nested as any,
          recovery,
        });
      };
      const a = await replica('A');
      await replica('B', scanning);
      const run = await a.workflow.createRun();
      startInBackground(run, { n: 1 });
      const sleeping = await sleepingRunId(storage, run.runId);
      const restarts = await recordRestarts(storage, run.runId);

      await crash(a, storage, [runLivenessKey(run.runId), runLivenessKey(sleeping)]);

      await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 10_000 });
      expect(seen.executions).toEqual(['A:before', 'B:after']);
      expect(seen.continuations).toEqual(['B']);
      expect(restarts).toEqual([true]);
      // B left the run alone until the two-second sleep was due and the grace for A's timer passed.
      expect(seen.at['B:after']).toBeGreaterThanOrEqual(seen.at['A:before']! + 2_000 + SLEEP_WAKE_GRACE_MS);
    },
    20_000,
  );

  it.each([
    { sleep: 'sleep', restartAfterMs: 700, until: false },
    { sleep: 'sleepUntil whose date is still ahead', restartAfterMs: 700, until: true },
    { sleep: 'sleepUntil whose date passed', restartAfterMs: 2_000, until: true },
  ])(
    'restart() of a run whose replica stopped in a $sleep waits only for what is left of it',
    async ({ restartAfterMs, until }) => {
      const storage = new InMemoryStore();
      const seen = observe('leased');
      // The sleepUntil date is computed when the sleep starts, so a restart that started the sleep
      // again would wait the full 1.5 seconds again.
      const build = (report: Report) => {
        const before = createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num }).then(
          work('before', report),
        );
        const slept = until ? before.sleepUntil(async () => new Date(Date.now() + 1_500)) : before.sleep(1_500);
        return slept.then(work('after', report)).commit() as unknown as Built;
      };
      const a = await startReplica(storage, build(seen.reporter('A')));
      const b = await startReplica(storage, build(seen.reporter('B')), { recovery: { workflows: 'off' } });
      const run = await a.workflow.createRun();
      startInBackground(run, { n: 1 });
      await sleepingRunId(storage, run.runId);
      await crash(a, storage, [runLivenessKey(run.runId)]);
      const due = seen.at['A:before']! + 1_500;

      await sleep(Math.max(0, seen.at['A:before']! + restartAfterMs - Date.now()));
      const restartedAt = Date.now();
      const result = await (await b.workflow.createRun({ runId: run.runId })).restart();

      expect(result.status).toBe('success');
      expect(seen.executions).toEqual(['A:before', 'B:after']);
      expect(seen.at['B:after']).toBeGreaterThanOrEqual(due);
      expect(seen.at['B:after']! - Math.max(due, restartedAt)).toBeLessThan(500);
      // One sleep, finished, although B's graph gave the sleep a different id than A's.
      expect(sleepStatuses(result.steps)).toEqual(['success']);
    },
    20_000,
  );

  it('leaves a sleep stored without a wake time to the timer that started it', async () => {
    const storage = new InMemoryStore();
    const store = (await storage.getStore('workflows'))!;
    const seen = observe('leased');
    const a = await startReplica(storage, sleepingFor(300)(seen.reporter('A')));
    await startReplica(storage, sleepingFor(300)(seen.reporter('B')), { recovery: scanning });
    const run = await a.workflow.createRun();
    startInBackground(run, { n: 1 });
    await sleepingRunId(storage, run.runId);
    await crash(a, storage, [runLivenessKey(run.runId)]);

    await forgetWakeTime(storage, 'leased', run.runId);
    const restarts = await recordRestarts(storage, run.runId);

    // Wait for two of B's ticks to read the run after the sleep would have been overdue: the first
    // may have started before, and the second starts once the first decided. A restart ends the wait.
    const overdueAt = seen.at['A:before']! + 300 + SLEEP_WAKE_GRACE_MS;
    const readMany = store.getRunOwnerships.bind(store);
    let reads = 0;
    vi.spyOn(store, 'getRunOwnerships').mockImplementation(async args => {
      if (Date.now() >= overdueAt && args.runIds.includes(runLivenessKey(run.runId))) {
        reads++;
      }

      return readMany(args);
    });
    await vi.waitFor(() => expect(reads >= 2 || restarts.length > 0).toBe(true), { timeout: 10_000 });

    expect(restarts).toEqual([]);
    expect(await runStatus(storage, run.runId)).toBe('running');
    expect(seen.executions).toEqual(['A:before']);
  }, 20_000);

  it('restart() continues a run whose sleep woke but whose next step event was lost, under any sleep id', async () => {
    const storage = new InMemoryStore();
    const seen = observe('leased');
    const lost = Promise.withResolvers<void>();
    const pubsubA = interceptWorkflowEvents(new EventEmitterPubSub(), event =>
      isStepRunAt(event, [2]) ? { divert: () => lost.resolve() } : undefined,
    );
    const a = await startReplica(storage, sleepingFor(300)(seen.reporter('A')), { pubsub: pubsubA });
    const b = await startReplica(storage, sleepingFor(300)(seen.reporter('B')), { recovery: { workflows: 'off' } });
    const run = await a.workflow.createRun();
    startInBackground(run, { n: 1 });
    await lost.promise;
    await crash(a, storage, [runLivenessKey(run.runId)]);

    const result = await (await b.workflow.createRun({ runId: run.runId })).restart();

    expect(result.status).toBe('success');
    expect(seen.executions).toEqual(['A:before', 'B:after']);
    expect(sleepStatuses(result.steps)).toEqual(['success']);
  }, 20_000);

  it('wakes a sleep again from another instance when publishing the step after it failed', async () => {
    const storage = new InMemoryStore();
    const seen = observe('leased');
    let rejected = false;
    const pubsubA = interceptWorkflowEvents(new EventEmitterPubSub(), event => {
      if (rejected || !isStepRunAt(event, [2])) {
        return undefined;
      }

      rejected = true;
      return { reject: new Error('broker unavailable') };
    });
    const a = await startReplica(storage, sleepingFor(300)(seen.reporter('A')), { pubsub: pubsubA });
    await startReplica(storage, sleepingFor(300)(seen.reporter('B')), { recovery: scanning });
    const run = await a.workflow.createRun();
    startInBackground(run, { n: 1 });

    // Default stall threshold: only an overdue sleep gets the run restarted within the wait.
    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 10_000 });
    expect(rejected).toBe(true);
    expect(seen.executions).toEqual(['A:before', 'B:after']);
  }, 20_000);

  it.each([
    {
      title:
        'on an instance that runs the recovery loop leaves alone a run in a sleep whose stored wake time is not due',
      legacy: false,
      loop: true,
      restarts: [],
    },
    {
      title: 'on an instance without the recovery loop restarts a run in a sleep whose stored wake time is not due',
      legacy: false,
      loop: false,
      restarts: [true],
    },
    { title: 'restarts a run in a sleep stored without a wake time', legacy: true, loop: false, restarts: [true] },
  ])(
    "restartAllActiveWorkflowRuns({ mode: 'all' }) $title",
    async ({ legacy, loop, restarts: expected }) => {
      const storage = new InMemoryStore();
      const seen = observe('leased');
      const a = await startReplica(storage, sleepingFor(5_000)(seen.reporter('A')));
      // The loop's first pass runs before the run starts, and the next one only after the test.
      const b = await startReplica(storage, sleepingFor(5_000)(seen.reporter('B')), {
        recovery: loop ? { workflowScanIntervalMs: 60_000 } : { workflows: 'off' },
      });
      const run = await a.workflow.createRun();
      startInBackground(run, { n: 1 });
      await sleepingRunId(storage, run.runId);
      await crash(a, storage, [runLivenessKey(run.runId)]);
      if (legacy) {
        await forgetWakeTime(storage, 'leased', run.runId);
      }

      const restarts = await recordRestarts(storage, run.runId);

      await b.mastra.restartAllActiveWorkflowRuns({ mode: 'all' });

      expect(restarts).toEqual(expected);
    },
    20_000,
  );

  it('restartAllActiveWorkflowRuns() on an instance without the recovery loop wakes a sleep whose replica stopped once it is due', async () => {
    const storage = new InMemoryStore();
    const seen = observe('leased');
    const a = await startReplica(storage, sleepingFor(500)(seen.reporter('A')));
    const b = await startReplica(storage, sleepingFor(500)(seen.reporter('B')), { recovery: { workflows: 'off' } });
    const run = await a.workflow.createRun();
    startInBackground(run, { n: 1 });
    await sleepingRunId(storage, run.runId);
    await crash(a, storage, [runLivenessKey(run.runId)]);

    // Nothing checks the run again later, so these calls must not leave it to an overdue check.
    await b.mastra.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    await b.mastra.restartAllActiveWorkflowRuns({ mode: 'all' });

    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 4_000 });
    expect(seen.executions).toEqual(['A:before', 'B:after']);
    // The restart still waited until the sleep was due.
    expect(seen.at['B:after']).toBeGreaterThanOrEqual(seen.at['A:before']! + 500);
  }, 20_000);
});
