/**
 * Boot-time workflow recovery must not re-drive a run another live replica is
 * executing (issue #25579). Two Mastra instances share one store, the way two
 * replicas share one database.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryStore } from '../storage/mock';
import { createStep, createWorkflow } from '../workflows';
import { WORKFLOW_RUN_LEASE_TTL_MS } from '../workflows/run-ownership';
import { Mastra } from './index';
import type { MastraRecoveryConfig } from './index';

/** Recovery loop settings that let a tick find an orphaned run within a test. */
const scanning: MastraRecoveryConfig = { workflowScanIntervalMs: 100 };

function buildReplica(
  storage: InMemoryStore,
  onExecute: (runId: string) => Promise<void>,
  recovery?: MastraRecoveryConfig,
) {
  const step = createStep({
    id: 'side-effect',
    inputSchema: z.object({}),
    outputSchema: z.object({ done: z.boolean() }),
    execute: async ({ runId }) => {
      await onExecute(runId);
      return { done: true };
    },
  });
  const workflow = createWorkflow({
    id: 'recoverable-wf',
    inputSchema: z.object({}),
    outputSchema: z.object({ done: z.boolean() }),
  })
    .then(step)
    .commit();
  return new Mastra({ logger: false, storage, recovery, workflows: { workflow } });
}

const replicas: Mastra[] = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  await Promise.all(replicas.splice(0).map(mastra => mastra.shutdown()));
});

async function persistOrphanedRun(storage: InMemoryStore, mastra: Mastra, leaseMs: number) {
  const workflow = mastra.getWorkflow('workflow');
  const run = await workflow.createRun();
  const workflowsStore = (await storage.getStore('workflows'))!;
  const snapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
  await workflowsStore.persistWorkflowSnapshot({
    workflowName: workflow.id,
    runId: run.runId,
    snapshot: { ...snapshot!, status: 'running', activePaths: [0], context: { input: {} } as any },
  });
  const { record } = await workflowsStore.claimRunOwnership({ runId: run.runId, ownerId: 'crashed-replica', leaseMs });
  const status = async () =>
    (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }))?.status;
  return { workflow, run, workflowsStore, status, leaseExpiresAt: record!.leaseExpiresAt!.getTime() };
}

describe('boot-time workflow recovery and run ownership (#25579)', () => {
  it('does not re-execute a run that another live replica is executing', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    let finishFirstExecution!: () => void;
    const firstExecutionBlocked = new Promise<void>(resolve => (finishFirstExecution = resolve));
    let signalStarted!: () => void;
    const started = new Promise<void>(resolve => (signalStarted = resolve));

    const replicaA = buildReplica(storage, async runId => {
      executions.push(`A:${runId}`);
      signalStarted();
      await firstExecutionBlocked;
    });
    const replicaB = buildReplica(storage, async runId => {
      executions.push(`B:${runId}`);
    });
    replicas.push(replicaA, replicaB);

    const run = await replicaA.getWorkflow('workflow').createRun();
    const resultPromise = run.start({ inputData: {} });
    await started;

    const active = await replicaB.listActiveWorkflowRuns();
    expect(active.runs.map(r => r.runId)).toEqual([run.runId]);

    // Replica B boots while A is still executing the run.
    await replicaB.restartAllActiveWorkflowRuns();
    expect(executions).toEqual([`A:${run.runId}`]);

    finishFirstExecution();
    const result = await resultPromise;
    expect(result.status).toBe('success');
    expect(executions).toEqual([`A:${run.runId}`]);

    // A released its lease when the run settled.
    const workflowsStore = await storage.getStore('workflows');
    expect((await workflowsStore!.getRunOwnership({ runId: run.runId }))?.live).toBe(false);
  });

  it("recovers a run whose owner crashed once its lease expires, on a running instance's recovery loop", async () => {
    const storage = new InMemoryStore();
    const executions: { runId: string; at: number }[] = [];
    const replica = buildReplica(storage, async runId => void executions.push({ runId, at: Date.now() }), scanning);
    replicas.push(replica);
    await replica.startWorkers();
    await replica.restartAllActiveWorkflowRuns();

    // Then another replica crashed, leaving a running snapshot and a lease it can no longer renew.
    // No replica boots after it.
    const { run, status, leaseExpiresAt } = await persistOrphanedRun(storage, replica, 300);

    await expect.poll(status, { timeout: 5_000 }).toBe('success');
    expect(executions.map(e => e.runId)).toEqual([run.runId]);
    expect(executions[0]!.at).toBeGreaterThanOrEqual(leaseExpiresAt);
  }, 15_000);

  it('keeps recovering after a tick of the recovery loop fails to read storage', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const replica = buildReplica(storage, async runId => void executions.push(runId), scanning);
    replicas.push(replica);
    await replica.startWorkers();
    await replica.restartAllActiveWorkflowRuns();
    const { run, workflowsStore, status } = await persistOrphanedRun(storage, replica, 300);

    // The first listing after the lease lapsed hits a storage error.
    const list = workflowsStore.listWorkflowRuns.bind(workflowsStore);
    let failures = 0;
    vi.spyOn(workflowsStore, 'listWorkflowRuns').mockImplementation(async args => {
      const ownership = await workflowsStore.getRunOwnership({ runId: run.runId });
      if (!ownership?.live && failures === 0) {
        failures++;
        throw new Error('connection reset');
      }
      return list(args);
    });

    await expect.poll(status, { timeout: 5_000 }).toBe('success');
    expect(failures).toBe(1);
    expect(executions).toEqual([run.runId]);
  }, 15_000);

  it('waits on shutdown for a run its recovery loop restarted, and ticks no more once shutdown starts', async () => {
    const storage = new InMemoryStore();
    const finish = Promise.withResolvers<void>();
    const started = Promise.withResolvers<void>();
    const events: string[] = [];
    const replica = buildReplica(
      storage,
      async () => {
        started.resolve();
        await finish.promise;
        events.push('step finished');
      },
      scanning,
    );
    const { workflowsStore, status } = await persistOrphanedRun(storage, replica, 100);
    await replica.startWorkers();
    await started.promise;

    const listings = vi.spyOn(workflowsStore, 'listWorkflowRuns');
    const shutdown = replica.shutdown({ drainTimeout: 10_000 }).then(() => events.push('shut down'));
    const listedBeforeShutdown = listings.mock.calls.length;
    setTimeout(finish.resolve, 300);
    await shutdown;

    expect(events).toEqual(['step finished', 'shut down']);
    expect(await status()).toBe('success');
    // The drain took several scan intervals, and no tick listed runs meanwhile.
    expect(listings.mock.calls.length).toBe(listedBeforeShutdown);
  }, 15_000);

  it("runs no recovery loop with recovery.workflows 'off'", async () => {
    const runOn = async (recovery: MastraRecoveryConfig) => {
      const storage = new InMemoryStore();
      const replica = buildReplica(storage, async () => {}, recovery);
      replicas.push(replica);
      const orphaned = await persistOrphanedRun(storage, replica, 300);
      const listings = vi.spyOn(orphaned.workflowsStore, 'listWorkflowRuns');
      await replica.startWorkers();
      return { ...orphaned, listings };
    };
    const auto = await runOn(scanning);
    const off = await runOn({ ...scanning, workflows: 'off' });

    // By the time the loop of the other instance recovered its run on a later tick, this one listed nothing.
    await expect.poll(auto.status, { timeout: 5_000 }).toBe('success');
    expect(off.listings).not.toHaveBeenCalled();
    expect(await off.status()).toBe('running');
  }, 15_000);

  it('does not keep the process alive while the recovery loop waits for its next tick', async () => {
    const replica = buildReplica(new InMemoryStore(), async () => {}, { workflowScanIntervalMs: 60_000 });
    replicas.push(replica);
    // Unref'd timers are not active resources.
    const timers = () => process.getActiveResourcesInfo().filter(resource => resource === 'Timeout').length;
    const before = timers();

    await replica.startWorkers();
    // Joins the loop's first pass, after which the loop schedules its next tick.
    await replica.restartAllActiveWorkflowRuns();
    await new Promise(resolve => setImmediate(resolve));

    expect(timers()).toBe(before);
  });

  it('keeps renewing the lease of a run that outlives the lease TTL', async () => {
    // The lease renews on a setTimeout chain.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] });
    const storage = new InMemoryStore();
    const executions: string[] = [];
    let finish!: () => void;
    const gate = new Promise<void>(resolve => (finish = resolve));
    let signalStarted!: () => void;
    const started = new Promise<void>(resolve => (signalStarted = resolve));
    const replicaA = buildReplica(storage, async runId => {
      executions.push(`A:${runId}`);
      signalStarted();
      await gate;
    });
    const replicaB = buildReplica(storage, async runId => {
      executions.push(`B:${runId}`);
    });
    replicas.push(replicaA, replicaB);

    const run = await replicaA.getWorkflow('workflow').createRun();
    const resultPromise = run.start({ inputData: {} });
    await started;

    // A is still executing well past the TTL of the lease it first claimed.
    await vi.advanceTimersByTimeAsync(WORKFLOW_RUN_LEASE_TTL_MS * 3);
    const workflowsStore = (await storage.getStore('workflows'))!;
    expect((await workflowsStore.getRunOwnership({ runId: run.runId }))?.live).toBe(true);

    await replicaB.restartAllActiveWorkflowRuns();
    expect(executions).toEqual([`A:${run.runId}`]);
    const runOnB = await replicaB.getWorkflow('workflow').createRun({ runId: run.runId });
    await expect(runOnB.restart()).rejects.toMatchObject({ id: 'WORKFLOW_RESTART_RUN_OWNED' });
    expect(executions).toEqual([`A:${run.runId}`]);

    finish();
    expect((await resultPromise).status).toBe('success');
    expect(executions).toEqual([`A:${run.runId}`]);
  });
});
