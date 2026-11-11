/**
 * The recovery loop each instance that runs workers starts restarts evented runs whose process
 * stopped (#28712): a run whose worker crashed mid-step, and one no process keeps alive that went
 * unwritten for the stall threshold. Replicas are separate Mastra instances, each with its own
 * pubsub, sharing one store the way pods share one database. Leases use a short TTL here, read
 * when core loads.
 */
vi.hoisted(() => {
  process.env.MASTRA_AGENT_THREAD_LEASE_TTL_MS = '600';
});

import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { InMemoryStore } from '../../storage/mock';
import { restartLeaseKey, runLivenessKey, stepOccurrenceKey } from './leases';
import {
  Num,
  crash,
  interceptWorkflowEvents,
  isStepRunAt,
  runStatus,
  scanning,
  shapes,
  startReplica,
  stopReplicas,
  track,
  work,
} from './test-utils/lease-replicas';
import type { Built, Report, Store } from './test-utils/lease-replicas';
import { createWorkflow } from '.';

afterEach(stopReplicas);

/**
 * Holds the next `count` liveness reads of `runId` until all of them arrived,
 * then lets them read the store at once. Recovery ticks wait for their read,
 * so the held reads come from the ticks of `count` different replicas.
 */
async function readLivenessTogether(storage: Store, runId: string, count: number) {
  const store = (await storage.getStore('workflows'))!;
  const readMany = store.getRunOwnerships.bind(store);
  const together = Promise.withResolvers<void>();
  let arrived = 0;
  vi.spyOn(store, 'getRunOwnerships').mockImplementation(async args => {
    if (arrived < count && args.runIds.includes(runLivenessKey(runId))) {
      arrived++;
      if (arrived === count) {
        together.resolve();
      }

      await together.promise;
    }

    return readMany(args);
  });
  return together.promise;
}

/** Records every `restart()` of `runId` that got past the liveness check: each claims the restart lease. */
async function recordRestarts(storage: Store, runId: string) {
  const store = (await storage.getStore('workflows'))!;
  const claim = store.claimRunOwnership.bind(store);
  const restarts: boolean[] = [];
  vi.spyOn(store, 'claimRunOwnership').mockImplementation(async args => {
    const result = await claim(args);
    if (args.runId === restartLeaseKey(runId)) {
      restarts.push(result.acquired);
    }

    return result;
  });
  return restarts;
}

describe('evented recovery loop (#28712)', () => {
  it.each(Object.keys(shapes) as (keyof typeof shapes)[])(
    '%s: the replicas still running restart a run whose replica crashed mid-step once, even when they find it at the same time',
    async name => {
      const shape = shapes[name];
      // The crashed loop was in its second iteration, so the restart runs that iteration and the third.
      const iteration = name === 'loop' ? 1 : undefined;
      const recoveredLabels = name === 'loop' ? ['body:2', 'body:3'] : shape.labels;
      const storage = new InMemoryStore();
      const tracked = track(shape, { hangForever: true });
      const a = await startReplica(storage, shape.build(tracked.reporter('A')));
      // B and C run all along; no replica boots after A crashes.
      await startReplica(storage, shape.build(tracked.reporter('B')), { recovery: scanning });
      await startReplica(storage, shape.build(tracked.reporter('C')), { recovery: scanning });
      const run = await a.workflow.createRun();
      void run.start({ inputData: shape.input as any });
      await tracked.allBlocked;

      await crash(a, storage, [
        ...shape.running.map(path => stepOccurrenceKey(run.runId, path, iteration)),
        runLivenessKey(run.runId),
      ]);
      const restarts = await recordRestarts(storage, run.runId);
      await readLivenessTogether(storage, run.runId, 2);

      await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 10_000 });
      // Both replicas found the run orphaned; the restart lease let one of them restart it.
      expect(restarts.sort()).toEqual([false, true]);
      const recovered = tracked.executions.filter(e => !e.startsWith('A:')).map(e => e.slice(2));
      expect(recovered.sort()).toEqual([...recoveredLabels].sort());
    },
    20_000,
  );

  it('restarts a run that slept before the replica running its next step crashed', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: ['a'] }, { hangForever: true });
    const build = (report: Report) =>
      createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
        .sleep(10)
        .then(work('a', report))
        .commit() as unknown as Built;
    const a = await startReplica(storage, build(tracked.reporter('A')));
    await startReplica(storage, build(tracked.reporter('B')), { recovery: scanning });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await tracked.allBlocked;

    // The finished sleep's result stays `waiting`; only the step A was running counts.
    await crash(a, storage, [stepOccurrenceKey(run.runId, [1]), runLivenessKey(run.runId)]);

    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 10_000 });
    expect(tracked.executions).toEqual(['A:a', 'B:a']);
  }, 20_000);

  it('restarts a run whose next step message was lost once the run went unwritten for the stall threshold', async () => {
    const storage = new InMemoryStore();
    const store = (await storage.getStore('workflows'))!;
    const stallMs = 1_500;
    const tracked = track({ blocks: [] });
    const lost = Promise.withResolvers<void>();
    let delivered = false;
    // The first message starting the second step never reaches a worker.
    const pubsub = interceptWorkflowEvents(new EventEmitterPubSub(), event => {
      if (delivered || !isStepRunAt(event, [1])) {
        return undefined;
      }

      delivered = true;
      return { divert: () => lost.resolve() };
    });
    const workflow = createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
      .then(work('first', tracked.reporter('A')))
      .then(work('second', tracked.reporter('A')))
      .commit();
    const a = await startReplica(storage, workflow as unknown as Built, {
      pubsub,
      recovery: { ...scanning, workflowStallThresholdMs: stallMs },
    });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await lost.promise;
    const { runs } = await store.listWorkflowRuns({ workflowName: 'leased', status: 'running' });
    const lastWrite = new Date(runs[0]!.updatedAt).getTime();

    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 10_000 });
    expect(tracked.executions).toEqual(['A:first', 'A:second']);
    // The run's liveness lapsed a lease TTL after the first step, long before the threshold.
    expect(tracked.at['A:second']).toBeGreaterThanOrEqual(lastWrite + stallMs);
  }, 20_000);

  /** Steps `before` and `after` around a two-second sleep, and the nested workflows to register. */
  const sleepingWorkflows = {
    sleep: (report: Report) => ({
      workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
        .then(work('before', report))
        .sleep(2_000)
        .then(work('after', report))
        .commit(),
      nested: {},
    }),
    sleepUntil: (report: Report) => ({
      workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
        .then(work('before', report))
        .sleepUntil(new Date(Date.now() + 2_000))
        .then(work('after', report))
        .commit(),
      nested: {},
    }),
    'sleep of a nested workflow': (report: Report) => {
      const child = createWorkflow({ id: 'child', inputSchema: Num, outputSchema: Num })
        .then(work('before', report))
        .sleep(2_000)
        .then(work('after', report))
        .commit();
      return {
        workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: z.any() }).then(child).commit(),
        nested: { child },
      };
    },
  };

  it.each(Object.keys(sleepingWorkflows) as (keyof typeof sleepingWorkflows)[])(
    'leaves a run in a %s to the replica that wakes it',
    async name => {
      const storage = new InMemoryStore();
      const executions: string[] = [];
      const reporter = (replica: string) => async (label: string) => void executions.push(`${replica}:${label}`);
      // Stalled long before the sleep ends, if a sleeping run counted as stalled.
      const recovery = { ...scanning, workflowStallThresholdMs: 300 };
      const replica = async (replicaName: string) => {
        const { workflow, nested } = sleepingWorkflows[name](reporter(replicaName));
        return startReplica(storage, workflow as unknown as Built, { nested: nested as any, recovery });
      };
      const a = await replica('A');
      await replica('B');
      const run = await a.workflow.createRun();
      const restarts = await recordRestarts(storage, run.runId);

      expect((await run.start({ inputData: { n: 1 } })).status).toBe('success');
      expect(executions).toEqual(['A:before', 'A:after']);
      expect(restarts).toEqual([]);
    },
    20_000,
  );
});
