/**
 * The run liveness key a process keeps fresh while it runs steps of an evented
 * run (#28712): how often it is written, that a slow store does not get
 * overlapping renewals, and what shutdown does with it. Leases use a short TTL
 * here, read when core loads.
 */
vi.hoisted(() => {
  process.env.MASTRA_AGENT_THREAD_LEASE_TTL_MS = '600';
});

import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { createWorkflow } from '..';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { InMemoryStore } from '../../../storage/mock';
import { WORKFLOW_RUN_LEASE_RENEW_INTERVAL_MS, WORKFLOW_RUN_LEASE_TTL_MS } from '../../run-ownership';
import {
  Num,
  interceptWorkflowEvents,
  isStepRunAt,
  recordLeases,
  recordRestarts,
  runIsLive,
  runStatus,
  scanning,
  sleep,
  startReplica,
  stopReplicas,
  track,
  work,
} from '../test-utils/lease-replicas';
import type { Built, Report } from '../test-utils/lease-replicas';
import { acquireStepLease, restartLeaseKey, runLivenessKey, stepOccurrenceKey } from '.';
import type { StepLease } from '.';

afterEach(stopReplicas);

describe('evented run liveness (#28712)', () => {
  it('writes the liveness of a run about once per renew interval however many of its steps run at once', async () => {
    const storage = new InMemoryStore();
    const leases = await recordLeases(storage);
    const workflow = createWorkflow({ id: 'leased', inputSchema: z.array(Num), outputSchema: z.any() })
      .foreach(
        work('item', async () => {}),
        { concurrency: 10 },
      )
      .commit();
    const a = await startReplica(storage, workflow as unknown as Built);
    const startedAt = Date.now();
    const result = await (
      await a.workflow.createRun()
    ).start({
      inputData: Array.from({ length: 10 }, (_, n) => ({ n })) as any,
    });
    expect(result.status).toBe('success');

    const writes = leases.claims.filter(c => c.key.endsWith('::live')).length;
    // The liveness timer ticks at the renew interval, or at a third of the TTL when that is shorter.
    const tick = Math.min(WORKFLOW_RUN_LEASE_RENEW_INTERVAL_MS, Math.floor(WORKFLOW_RUN_LEASE_TTL_MS / 3));
    expect(writes).toBeGreaterThanOrEqual(1);
    expect(writes).toBeLessThanOrEqual(1 + Math.ceil((Date.now() - startedAt) / tick));
  }, 20_000);

  it('writes no more run liveness while an earlier liveness write is still pending', async () => {
    vi.useFakeTimers();
    try {
      const store = (await new InMemoryStore().getStore('workflows'))!;
      const scope = { __shuttingDown: false };
      const claim = store.claimRunOwnership.bind(store);
      const stalled = Promise.withResolvers<void>();
      const livenessWrites: Record<string, number> = {};
      // The store stalls on the second liveness write of run `slow`.
      vi.spyOn(store, 'claimRunOwnership').mockImplementation(async args => {
        if (args.runId.endsWith('::live')) {
          livenessWrites[args.runId] = (livenessWrites[args.runId] ?? 0) + 1;
          if (args.runId === runLivenessKey('slow') && livenessWrites[args.runId] === 2) {
            await stalled.promise;
          }
        }
        return claim(args);
      });
      const hold = (runId: string) =>
        acquireStepLease({ scope, store, runId, key: stepOccurrenceKey(runId, [0]), claimAfterRelease: false });
      const leases = [await hold('slow'), await hold('fast')] as StepLease[];

      await vi.advanceTimersByTimeAsync(WORKFLOW_RUN_LEASE_TTL_MS * 4);

      // The tick waiting on `slow` holds the next one back, so `fast` is not written again meanwhile.
      expect(livenessWrites[runLivenessKey('slow')]).toBe(2);
      expect(livenessWrites[runLivenessKey('fast')]).toBe(2);

      stalled.resolve();
      await Promise.all(leases.map(lease => lease.release()));
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('with an in-process pubsub, a run whose next step event was lost while its process still keeps it live (#28712)', () => {
  const twoSteps = (report: Report) =>
    createWorkflow({ id: 'leased', inputSchema: Num, outputSchema: Num })
      .then(work('first', report))
      .then(work('second', report))
      .commit() as unknown as Built;

  /** A replica whose second step event of the `leased` run is lost, and the run it lost it for. */
  async function loseSecondStep(storage: InMemoryStore, tracked: ReturnType<typeof track>) {
    const lost = Promise.withResolvers<void>();
    const pubsub = interceptWorkflowEvents(new EventEmitterPubSub(), event =>
      isStepRunAt(event, [1]) && !(event.data as { restart?: unknown }).restart
        ? { divert: () => lost.resolve() }
        : undefined,
    );
    const a = await startReplica(storage, twoSteps(tracked.reporter('A')), { pubsub });
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await lost.promise;
    expect(await runIsLive(storage, run.runId)).toBe(true);

    return { a, run };
  }

  it.each([
    ['A shut down before B booted', true],
    ['B booted before A shut down', false],
  ] as const)(
    'is restarted once its process shut down: %s',
    async (_, shutDownFirst) => {
      const storage = new InMemoryStore();
      const tracked = track({ blocks: [] });
      const { a, run } = await loseSecondStep(storage, tracked);
      const store = (await storage.getStore('workflows'))!;
      const claim = store.claimRunOwnership.bind(store);
      let restartedAt: number | undefined;
      vi.spyOn(store, 'claimRunOwnership').mockImplementation(async args => {
        if (args.runId === restartLeaseKey(run.runId)) {
          restartedAt ??= Date.now();
        }

        return claim(args);
      });
      const shutDown = async () => {
        // A's key would stay live until then had A not released it.
        const liveUntil = (await store.getRunOwnership({ runId: runLivenessKey(run.runId) }))!.leaseExpiresAt!;
        await a.mastra.shutdown({ drainTimeout: 0 });
        return liveUntil.getTime();
      };

      let liveUntil: number | undefined;
      if (shutDownFirst) {
        liveUntil = await shutDown();
      }

      await startReplica(storage, twoSteps(tracked.reporter('B')), { recovery: scanning });
      if (!shutDownFirst) {
        await sleep(scanning.workflowScanIntervalMs * 3);
        expect(await runStatus(storage, run.runId)).toBe('running');
        liveUntil = await shutDown();
      }

      await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), { timeout: 5_000 });
      expect(tracked.executions).toEqual(['A:first', 'B:second']);
      expect(restartedAt).toBeLessThan(liveUntil!);
    },
    20_000,
  );

  it('continues a run whose step outlasted the drain and finished after its process stopped its workers', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: ['first'] });
    const a = await startReplica(storage, twoSteps(tracked.reporter('A')));
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await tracked.allBlocked;

    // The drain gives up on the step and the run is released. B restarts it, and B's delivery of
    // the step waits on the lease A's step still holds.
    await a.mastra.shutdown({ drainTimeout: 50 });
    const leases = await recordLeases(storage);
    const stepKey = stepOccurrenceKey(run.runId, [0]);
    await startReplica(storage, twoSteps(tracked.reporter('B')), { recovery: scanning });
    await leases.until(() => leases.claims.some(claim => claim.key === stepKey && !claim.acquired));

    // A's step finishes now, and its step end goes nowhere. B's delivery takes the step over once
    // A's lease lapses, well before B's restart lease would let another restart through.
    tracked.release();
    await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), {
      timeout: WORKFLOW_RUN_LEASE_TTL_MS * 2.5,
      interval: 20,
    });
    // This store saves a step's result only when its end is handled, so B runs the step again.
    // A store with step guards saves it first, and B continues from it.
    expect(tracked.executions).toEqual(['A:first', 'B:first', 'B:second']);
  }, 20_000);

  it('stops vouching for its runs when its shutdown fails', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: ['first'] });
    const a = await startReplica(storage, twoSteps(tracked.reporter('A')));
    const run = await a.workflow.createRun();
    void run.start({ inputData: { n: 1 } });
    await tracked.allBlocked;
    vi.spyOn(a.mastra, 'stopWorkers').mockRejectedValue(new Error('transport stuck'));

    await expect(a.mastra.shutdown({ drainTimeout: 50 })).rejects.toThrow('transport stuck');
    const store = (await storage.getStore('workflows'))!;
    expect(await store.getRunOwnership({ runId: runLivenessKey(run.runId) })).toMatchObject({
      leaseExpiresAt: null,
    });
    tracked.release();
  }, 10_000);

  it('keeps a run live while its process drains it on shutdown, so a booting instance leaves it alone', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: ['first'] });
    const a = await startReplica(storage, twoSteps(tracked.reporter('A')));
    const run = await a.workflow.createRun();
    const result = run.start({ inputData: { n: 1 } });
    await tracked.allBlocked;
    const restarts = await recordRestarts(storage, run.runId);

    // The drain outlasts a lease TTL, and B boots meanwhile.
    const shutdown = a.mastra.shutdown({ drainTimeout: WORKFLOW_RUN_LEASE_TTL_MS * 6 });
    await sleep(WORKFLOW_RUN_LEASE_TTL_MS * 1.5);
    await startReplica(storage, twoSteps(tracked.reporter('B')), { recovery: scanning });
    await sleep(scanning.workflowScanIntervalMs * 3);
    expect(restarts).toEqual([]);

    tracked.release();
    expect((await result).status).toBe('success');
    await shutdown;
    expect(tracked.executions).toEqual(['A:first', 'A:second']);
  }, 20_000);

  it('releases a liveness key whose write took longer than a lease TTL', async () => {
    const storage = new InMemoryStore();
    const store = (await storage.getStore('workflows'))!;
    const claim = store.claimRunOwnership.bind(store);
    // The store applies each liveness write only after a lease TTL, so it holds a full TTL from then.
    vi.spyOn(store, 'claimRunOwnership').mockImplementation(async args => {
      if (args.runId.endsWith('::live')) {
        await sleep(WORKFLOW_RUN_LEASE_TTL_MS * 1.1);
      }

      return claim(args);
    });
    const releases = vi.spyOn(store, 'releaseRunOwnership');
    const { a, run } = await loseSecondStep(storage, track({ blocks: [] }));
    // Let every liveness write in flight return.
    await sleep(WORKFLOW_RUN_LEASE_TTL_MS * 1.3);
    expect(await runIsLive(storage, run.runId)).toBe(true);

    await a.mastra.shutdown({ drainTimeout: 0 });
    expect(releases.mock.calls.map(([fence]) => fence.runId)).toContain(runLivenessKey(run.runId));
  }, 10_000);

  it('does not hold up shutdown while the store does not answer the release', async () => {
    const storage = new InMemoryStore();
    const { a, run } = await loseSecondStep(storage, track({ blocks: [] }));
    const store = (await storage.getStore('workflows'))!;
    const release = store.releaseRunOwnership.bind(store);
    vi.spyOn(store, 'releaseRunOwnership').mockImplementation(fence =>
      fence.runId === runLivenessKey(run.runId) ? new Promise<boolean>(() => {}) : release(fence),
    );

    const started = Date.now();
    await a.mastra.shutdown({ drainTimeout: 0 });
    expect(Date.now() - started).toBeLessThan(WORKFLOW_RUN_LEASE_TTL_MS * 2);
    expect(vi.mocked(store.releaseRunOwnership).mock.calls.map(([fence]) => fence.runId)).toContain(
      runLivenessKey(run.runId),
    );
  }, 10_000);

  it('is left alone while that process runs, which may be a replica driving its own runs', async () => {
    const storage = new InMemoryStore();
    const tracked = track({ blocks: [] });
    const { run } = await loseSecondStep(storage, tracked);
    const restarts = await recordRestarts(storage, run.runId);

    await startReplica(storage, twoSteps(tracked.reporter('B')), { recovery: scanning });
    await sleep(WORKFLOW_RUN_LEASE_TTL_MS * 2);
    expect(restarts).toEqual([]);
    expect(tracked.executions).toEqual(['A:first']);
  }, 20_000);
});
