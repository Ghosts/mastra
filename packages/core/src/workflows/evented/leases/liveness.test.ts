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
import { acquireStepLease, runLivenessKey, stepOccurrenceKey } from '.';
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

      // Well within one lease TTL, so A's key would still be live had A not released it.
      if (shutDownFirst) {
        await a.mastra.shutdown({ drainTimeout: 0 });
      }

      await startReplica(storage, twoSteps(tracked.reporter('B')), { recovery: scanning });
      if (!shutDownFirst) {
        await sleep(scanning.workflowScanIntervalMs * 3);
        expect(await runStatus(storage, run.runId)).toBe('running');
        await a.mastra.shutdown({ drainTimeout: 0 });
      }

      await vi.waitFor(async () => expect(await runStatus(storage, run.runId)).toBe('success'), {
        timeout: WORKFLOW_RUN_LEASE_TTL_MS / 2,
        interval: 20,
      });
      expect(tracked.executions).toEqual(['A:first', 'B:second']);
    },
    20_000,
  );

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
