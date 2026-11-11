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
import type { PubSub } from '../../events/pubsub';
import type { Event } from '../../events/types';
import { Mastra } from '../../mastra';
import { InMemoryStore } from '../../storage/mock';
import { WORKFLOW_RUN_LEASE_TTL_MS } from '../run-ownership';
import { runLivenessKey, stepOccurrenceKey } from './leases';
import {
  Num,
  crash,
  interceptWorkflowEvents,
  isStepRunAt,
  recordRestarts,
  replicas,
  runStatus,
  scanning,
  shapes,
  sleep,
  startReplica,
  stopReplicas,
  track,
  work,
} from './test-utils/lease-replicas';
import type { Built, Report, Store } from './test-utils/lease-replicas';
import { createStep, createWorkflow } from '.';

afterEach(stopReplicas);

/**
 * Holds the next `count` liveness reads of `runId` until all of them arrived and `until`
 * settled, then lets them read the store at once. Recovery ticks wait for their read, so the held
 * reads come from the ticks of `count` different replicas, and all of them see the store as
 * `until` left it.
 */
async function readLivenessTogether(storage: Store, runId: string, count: number, until: Promise<unknown>) {
  const store = (await storage.getStore('workflows'))!;
  const readMany = store.getRunOwnerships.bind(store);
  const together = Promise.withResolvers<void>();
  const released = Promise.all([together.promise, until]);
  let arrived = 0;
  vi.spyOn(store, 'getRunOwnerships').mockImplementation(async args => {
    if (arrived < count && args.runIds.includes(runLivenessKey(runId))) {
      arrived++;
      if (arrived === count) {
        together.resolve();
      }

      await released;
    }

    return readMany(args);
  });
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

      // Installed before the crash, so no tick of B or C restarts the run before both read it.
      const restarts = await recordRestarts(storage, run.runId);
      const crashed = Promise.withResolvers<void>();
      await readLivenessTogether(storage, run.runId, 2, crashed.promise);
      await crash(a, storage, [
        ...shape.running.map(path => stepOccurrenceKey(run.runId, path, iteration)),
        runLivenessKey(run.runId),
      ]);
      crashed.resolve();

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

  /**
   * A broker that delivers every `workflows` event `pick` matches `ms` late, the way a busy one
   * does. The publisher goes on meanwhile.
   */
  const delayDeliveries = (pick: (event: Event) => boolean, ms: number): PubSub =>
    new Proxy(new EventEmitterPubSub(), {
      get(target, prop) {
        if (prop === 'subscribe') {
          return (topic: string, cb: (event: Event, ...rest: any[]) => unknown, ...options: any[]) =>
            target.subscribe(
              topic,
              async (event: Event, ...rest: any[]) => {
                if (topic === 'workflows' && pick(event)) {
                  await sleep(ms);
                }

                return cb(event, ...rest);
              },
              ...options,
            );
        }

        const value = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

  /** Like {@link delayDeliveries}, for the first matching event only. */
  const delayFirst = (pick: (event: Event) => boolean, ms: number): PubSub => {
    let delayed = false;
    return delayDeliveries(event => {
      if (delayed || !pick(event)) {
        return false;
      }

      delayed = true;
      return true;
    }, ms);
  };
  const carriesHandoff = (event: Event) => !!(event.data as { stepLeaseHandoff?: unknown })?.stepLeaseHandoff;
  // Long enough for the run's liveness to lapse, far below the stall threshold.
  const backlog = WORKFLOW_RUN_LEASE_TTL_MS * 4;

  const delayedEvents = {
    "a finished step's step end event": {
      pick: (event: Event) => event.type === 'workflow.step.end',
      build: (report: Report) => ({
        workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
          .then(work('a', report))
          .then(work('b', report))
          .commit(),
        nested: {},
      }),
      input: { n: 1 } as unknown,
      labels: ['a', 'b'],
    },
    "a nested run's next step event": {
      pick: (event: Event) => isStepRunAt(event, [1]) && (event.data as { workflowId?: string }).workflowId === 'child',
      build: (report: Report) => {
        const child = createWorkflow({ id: 'child', inputSchema: Num, outputSchema: Num })
          .then(work('c1', report))
          .then(work('c2', report))
          .commit();
        return {
          workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: z.any() }).then(child).commit(),
          nested: { child },
        };
      },
      input: { n: 1 } as unknown,
      labels: ['c1', 'c2'],
    },
    "a foreach item's step event": {
      pick: (event: Event) =>
        isStepRunAt(event, [0, 1]) && (event.data as { workflowId?: string }).workflowId === 'leased',
      build: (report: Report) => ({
        workflow: createWorkflow({ id: 'leased', inputSchema: z.array(Num), outputSchema: z.any() })
          .foreach(
            createStep({
              id: 'item',
              inputSchema: Num,
              outputSchema: Num,
              execute: async ({ inputData }) => {
                await report(`item:${inputData.n}`);
                return inputData;
              },
            }),
            { concurrency: 1 },
          )
          .commit(),
        nested: {},
      }),
      input: [{ n: 1 }, { n: 2 }, { n: 3 }] as unknown,
      labels: ['item:1', 'item:2', 'item:3'],
    },
    "a failed step's retry event": {
      pick: (event: Event) => event.type === 'workflow.step.run' && carriesHandoff(event),
      build: (report: Report) => {
        let attempts = 0;
        return {
          workflow: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num, retryConfig: { attempts: 1 } })
            .then(
              createStep({
                id: 'flaky',
                inputSchema: Num,
                outputSchema: Num,
                execute: async ({ inputData }) => {
                  attempts++;
                  await report(`flaky:${attempts}`);
                  if (attempts === 1) {
                    throw new Error('transient');
                  }

                  return inputData;
                },
              }),
            )
            .then(work('after', report))
            .commit(),
          nested: {},
        };
      },
      input: { n: 1 } as unknown,
      labels: ['flaky:1', 'flaky:2', 'after'],
    },
  };

  it.each([
    // The step lease handed to the retry is renewed meanwhile, so nothing restarts the run.
    ['two lease TTLs', 2, false],
    // Past the hold limit the lease lapses and the loop restarts the run, which continues from the retry.
    ['six lease TTLs', 6, true],
  ] as const)(
    'fails a run whose step keeps failing once its retries run out, while the broker holds each retry back %s',
    async (_, ttls, restarted) => {
      const storage = new InMemoryStore();
      let attempts = 0;
      const workflow = createWorkflow({
        id: 'leased',
        inputSchema: Num,
        outputSchema: Num,
        retryConfig: { attempts: 2 },
      })
        .then(
          createStep({
            id: 'broken',
            inputSchema: Num,
            outputSchema: Num,
            execute: async () => {
              attempts++;
              throw new Error('always fails');
            },
          }),
        )
        .commit();
      // Every retry event reaches its worker late.
      const a = await startReplica(storage, workflow as unknown as Built, {
        pubsub: delayDeliveries(carriesHandoff, WORKFLOW_RUN_LEASE_TTL_MS * ttls),
        recovery: scanning,
      });
      const run = await a.workflow.createRun();
      const restarts = await recordRestarts(storage, run.runId);

      const result = run.start({ inputData: { n: 1 } });
      await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('failed'), { timeout: 25_000 });
      // Once a restart took the run over, only storage shows how it ended.
      if (!restarted) {
        expect((await result).status).toBe('failed');
      }

      expect(attempts).toBe(3);
      expect(restarts.length > 0).toBe(restarted);
    },
    30_000,
  );

  it('restarts at boot a run whose next step event died with an in-process pubsub', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: [] });
    const lost = Promise.withResolvers<void>();
    // A's process stops right after the first step: the event of the second dies with it.
    const pubsub = interceptWorkflowEvents(new EventEmitterPubSub(), event =>
      isStepRunAt(event, [1]) && !(event.data as { restart?: unknown }).restart
        ? { divert: () => lost.resolve() }
        : undefined,
    );
    const build = (report: Report) =>
      createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
        .then(work('first', report))
        .then(work('second', report))
        .commit() as unknown as Built;
    const a = await startReplica(storage, build(tracked.reporter('A')), { pubsub });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await lost.promise;
    await crash(a, storage, [runLivenessKey(run.runId)]);

    // B boots with the default stall threshold: its first pass restarts the run at once.
    await startReplica(storage, build(tracked.reporter('B')), { recovery: scanning });
    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 5_000 });
    expect(tracked.executions).toEqual(['A:first', 'B:second']);
  }, 20_000);

  it("leaves a run alone while the nested run doing its work wrote recently, though the run's own row is old", async () => {
    const storage = new InMemoryStore();
    const stallMs = 1_000;
    const executions: string[] = [];
    const child = createWorkflow({ id: 'child', inputSchema: Num, outputSchema: Num })
      .then(
        createStep({
          id: 'c1',
          inputSchema: Num,
          outputSchema: Num,
          execute: async ({ inputData }) => {
            executions.push('c1');
            // Longer than the stall threshold; the parent's row isn't written meanwhile.
            await sleep(stallMs * 1.2);
            return inputData;
          },
        }),
      )
      .then(work('c2', async label => void executions.push(label)))
      .commit();
    const workflow = createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: z.any() }).then(child).commit();
    // The child's second step event arrives late enough for the child's liveness to lapse.
    const a = await startReplica(storage, workflow as unknown as Built, {
      pubsub: delayFirst(
        event => isStepRunAt(event, [1]) && (event.data as { workflowId?: string }).workflowId === 'child',
        WORKFLOW_RUN_LEASE_TTL_MS * 1.5,
      ),
      nested: { child } as unknown as Record<string, Built>,
      recovery: { ...scanning, workflowStallThresholdMs: stallMs },
    });
    const run = await a.workflow.createRun();
    const restarts = await recordRestarts(storage, run.runId);

    expect((await run.start({ inputData: { n: 1 } })).status).toBe('success');
    expect(restarts).toEqual([]);
    expect(executions).toEqual(['c1', 'c2']);
  }, 20_000);

  it("continues a failing step's attempt count when its worker crashed during a retry", async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const retrying = Promise.withResolvers<void>();
    const build = (replica: string) =>
      createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num, retryConfig: { attempts: 2 } })
        .then(
          createStep({
            id: 'broken',
            inputSchema: Num,
            outputSchema: Num,
            execute: async ({ retryCount }) => {
              executions.push(`${replica}:${retryCount}`);
              // A crashes during its first retry.
              if (replica === 'A' && retryCount === 1) {
                retrying.resolve();
                await new Promise<never>(() => {});
              }

              throw new Error('always fails');
            },
          }),
        )
        .commit() as unknown as Built;
    const a = await startReplica(storage, build('A'));
    await startReplica(storage, build('B'), { recovery: scanning });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await retrying.promise;
    await crash(a, storage, [stepOccurrenceKey(run.runId, [0]), runLivenessKey(run.runId)]);

    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('failed'), { timeout: 10_000 });
    // B runs the interrupted retry again and the one retry left, not every attempt anew.
    expect(executions).toEqual(['A:0', 'A:1', 'B:1', 'B:2']);
  }, 20_000);

  it.each(Object.keys(delayedEvents) as (keyof typeof delayedEvents)[])(
    'runs each step once when the broker holds %s back for longer than the lease TTL',
    async name => {
      const { pick, build, input, labels } = delayedEvents[name];
      const storage = new InMemoryStore();
      const executions: string[] = [];
      const { workflow, nested } = build(async label => void executions.push(label));
      const a = await startReplica(storage, workflow as unknown as Built, {
        pubsub: delayFirst(pick, backlog),
        nested: nested as Record<string, Built>,
        recovery: scanning,
      });
      const listings = vi.spyOn((await storage.getStore('workflows'))!, 'listWorkflowRuns');
      const run = await a.workflow.createRun();
      const restarts = await recordRestarts(storage, run.runId);

      expect((await run.start({ inputData: input as any })).status).toBe('success');
      // The loop ticked through the backlog while the run's liveness had lapsed, and left it alone.
      expect(listings.mock.calls.length).toBeGreaterThanOrEqual(backlog / scanning.workflowScanIntervalMs / 2);
      expect(restarts).toEqual([]);
      expect(executions.sort()).toEqual([...labels].sort());
    },
    20_000,
  );

  it('restarts a parallel run whose branch crashed mid-step while its other branch sleeps for a day', async () => {
    const storage = new InMemoryStore();
    const store = (await storage.getStore('workflows'))!;
    const tracked = track({ blocks: ['work'] }, { hangForever: true });
    const build = (report: Report) => {
      const sleeper = createWorkflow({ id: 'sleeper', inputSchema: Num, outputSchema: Num })
        .sleep(24 * 60 * 60 * 1000)
        .commit();
      const worker = createWorkflow({ id: 'worker', inputSchema: Num, outputSchema: Num })
        .then(work('work', report))
        .commit();
      return {
        parent: createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: z.any() })
          .parallel([sleeper, worker])
          .commit() as unknown as Built,
        nested: { sleeper, worker } as unknown as Record<string, Built>,
      };
    };
    const onA = build(tracked.reporter('A'));
    const onB = build(tracked.reporter('B'));
    const a = await startReplica(storage, onA.parent, { nested: onA.nested });
    await startReplica(storage, onB.parent, { nested: onB.nested, recovery: scanning });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await tracked.allBlocked;
    const nestedRunId = async (stepId: string) =>
      ((await store.loadWorkflowSnapshot({ workflowName: 'leased', runId: run.runId }))?.context[stepId] as any)
        ?.metadata?.nestedRunId as string;
    await vi.waitFor(async () => expect(await nestedRunId('sleeper')).toBeTruthy());
    const workerRunId = await nestedRunId('worker');
    const sleeperRunId = await nestedRunId('sleeper');

    // A crashes while its worker branch runs a step and its sleeper branch sleeps.
    await crash(a, storage, [
      stepOccurrenceKey(workerRunId, [0]),
      runLivenessKey(workerRunId),
      runLivenessKey(sleeperRunId),
      runLivenessKey(run.runId),
    ]);

    // B's loop restarts the run for the crashed branch, without waiting for the sleep.
    await vi.waitFor(() => expect(tracked.executions).toEqual(['A:work', 'B:work']), { timeout: 10_000 });
  }, 20_000);

  it('restarts no other run when a restart() starts the workers of an instance that never called startWorkers()', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: ['a'] }, { hangForever: true });
    const a = await startReplica(storage, shapes.step.build(tracked.reporter('A')));
    const left = await a.workflow.createRun();
    const restarted = await a.workflow.createRun();
    void left.start({ inputData: { n: 1 } });
    void restarted.start({ inputData: { n: 2 } });
    await vi.waitFor(() => expect(tracked.executions).toEqual(['A:a', 'A:a']));
    await crash(a, storage, [
      stepOccurrenceKey(left.runId, [0]),
      runLivenessKey(left.runId),
      stepOccurrenceKey(restarted.runId, [0]),
      runLivenessKey(restarted.runId),
    ]);

    // A script restarts one run without starting workers; the restart starts them lazily.
    const workflow = shapes.step.build(tracked.reporter('B'));
    const script = new Mastra({ logger: false, storage, recovery: scanning, workflows: { leased: workflow } });
    replicas.push(script);
    const restarts = await recordRestarts(storage, left.runId);
    expect((await (await workflow.createRun({ runId: restarted.runId })).restart()).status).toBe('success');

    // A loop started with those workers would have ticked several times within this window.
    const listings = vi.spyOn((await storage.getStore('workflows'))!, 'listWorkflowRuns');
    await sleep(scanning.workflowScanIntervalMs * 3);
    expect(listings).not.toHaveBeenCalled();
    expect(tracked.executions).toEqual(['A:a', 'A:a', 'B:a']);
    expect(restarts).toEqual([]);
    expect(await runStatus(storage, left.runId)).toBe('running');
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
