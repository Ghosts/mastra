/**
 * Boot-time workflow recovery must not re-drive a run another live replica is
 * executing (issue #25579). Two Mastra instances share one store, the way two
 * replicas share one database. Leases use a short TTL here, read when core
 * loads, so the recovery loop's grace after a lapsed lease passes within a test.
 */
vi.hoisted(() => {
  process.env.MASTRA_AGENT_THREAD_LEASE_TTL_MS = '600';
});

import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryStore } from '../storage/mock';
import { createStep, createWorkflow } from '../workflows';
import { WORKFLOW_RUN_LEASE_TTL_MS } from '../workflows/run-ownership';
import { Mastra } from './index';
import type { MastraRecoveryConfig } from './index';

/** Recovery loop settings that let a tick find an orphaned run within a test. */
const scanning: MastraRecoveryConfig = { workflowScanIntervalMs: 100 };

/**
 * A replica whose `workflow` (`recoverable-wf`) runs one default-engine step that calls
 * `onExecute` with the run id.
 */
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

/**
 * Stores a `running` run of `mastra`'s workflow with a run lease that lapses after `leaseMs`, as a
 * replica that crashed mid-run leaves it, and returns a reader of the run's stored status.
 */
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
    // One lease TTL of grace after the lease lapsed, for an owner that only paused.
    expect(executions[0]!.at).toBeGreaterThanOrEqual(leaseExpiresAt + WORKFLOW_RUN_LEASE_TTL_MS);
  }, 15_000);

  it("leaves a run to its owner while the owner's renewals pause past the lease TTL, as when its event loop blocks", async () => {
    const storage = new InMemoryStore();
    const workflowsStore = (await storage.getStore('workflows'))!;
    const executions: string[] = [];
    const resume = Promise.withResolvers<void>();
    const started = Promise.withResolvers<void>();
    const owner = buildReplica(storage, async runId => {
      executions.push(`A:${runId}`);
      started.resolve();
      await resume.promise;
    });
    const other = buildReplica(storage, async runId => void executions.push(`B:${runId}`), scanning);
    replicas.push(owner, other);
    await other.startWorkers();
    const run = await owner.getWorkflow('workflow').createRun();
    const result = run.start({ inputData: {} });
    await started.promise;

    // A renews nothing for 1.5 TTLs: its lease lapses half a TTL before it renews again.
    const renew = workflowsStore.renewRunOwnership.bind(workflowsStore);
    let paused = true;
    vi.spyOn(workflowsStore, 'renewRunOwnership').mockImplementation(async args => {
      if (paused && args.runId === run.runId) throw new Error('renewal did not run');
      return renew(args);
    });
    const listings = vi.spyOn(workflowsStore, 'listWorkflowRuns');
    await new Promise(resolve => setTimeout(resolve, WORKFLOW_RUN_LEASE_TTL_MS * 1.5));
    paused = false;
    await vi.waitFor(async () => expect((await workflowsStore.getRunOwnership({ runId: run.runId }))?.live).toBe(true));

    // B ticked through the lapse without restarting the run.
    expect(listings.mock.calls.length).toBeGreaterThanOrEqual(3);
    resume.resolve();
    expect((await result).status).toBe('success');
    expect(executions).toEqual([`A:${run.runId}`]);
  }, 15_000);

  it.each([
    ['', scanning],
    // The parent's row isn't written while the nested run works.
    [' for longer than the stall threshold', { ...scanning, workflowStallThresholdMs: WORKFLOW_RUN_LEASE_TTL_MS }],
  ] as const)(
    'leaves a parent run alone while one of its nested runs is resumed directly%s',
    async (_, recovery) => {
      const storage = new InMemoryStore();
      const executions: string[] = [];
      const resumed = Promise.withResolvers<void>();
      const finish = Promise.withResolvers<void>();
      const build = (replica: string) => {
        const child = createWorkflow({ id: 'child', inputSchema: z.object({}), outputSchema: z.object({}) })
          .then(
            createStep({
              id: 'approve',
              inputSchema: z.object({}),
              outputSchema: z.object({}),
              resumeSchema: z.object({ approved: z.boolean() }),
              execute: async ({ resumeData, suspend }) => {
                executions.push(`${replica}:approve:${resumeData ? 'resumed' : 'suspended'}`);
                if (!resumeData) return suspend({});
                resumed.resolve();
                await finish.promise;
                return {};
              },
            }),
          )
          .commit();
        const parent = createWorkflow({ id: 'parent', inputSchema: z.object({}), outputSchema: z.any() })
          .then(child)
          .then(
            createStep({
              id: 'after',
              inputSchema: z.any(),
              outputSchema: z.object({}),
              execute: async () => {
                executions.push(`${replica}:after`);
                return {};
              },
            }),
          )
          .commit();
        return { parent, child };
      };
      const onA = build('A');
      const onB = build('B');
      const a = new Mastra({ logger: false, storage, workflows: onA });
      const b = new Mastra({ logger: false, storage, workflows: onB, recovery });
      replicas.push(a, b);
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await onA.parent.createRun();
      expect((await run.start({ inputData: {} })).status).toBe('suspended');
      const parentSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: 'parent', runId: run.runId });
      const nestedRunId = (parentSnapshot?.context.child as { suspendPayload?: any }).suspendPayload?.__workflow_meta
        ?.runId as string;
      await b.startWorkers();

      // Resuming the nested run sets the parent `running` and holds the parent's lease until the parent resumes.
      const listings = vi.spyOn(workflowsStore, 'listWorkflowRuns');
      const resume = (await onA.child.createRun({ runId: nestedRunId })).resume({
        step: 'approve',
        resumeData: { approved: true },
      });
      await resumed.promise;
      expect(await workflowsStore.loadWorkflowRunStatus({ workflowName: 'parent', runId: run.runId })).toBe('running');
      const listedAtResume = listings.mock.calls.length;
      await new Promise(resolve => setTimeout(resolve, WORKFLOW_RUN_LEASE_TTL_MS * 3));
      expect(listings.mock.calls.length).toBeGreaterThanOrEqual(listedAtResume + 3);

      finish.resolve();
      expect((await resume).status).toBe('success');
      await vi.waitFor(async () =>
        expect(await workflowsStore.loadWorkflowRunStatus({ workflowName: 'parent', runId: run.runId })).toBe(
          'success',
        ),
      );
      expect(executions).toEqual(['A:approve:suspended', 'A:approve:resumed', 'A:after']);
    },
    15_000,
  );

  /** A parent run suspended in its nested `child` workflow, and the nested run's id. */
  async function suspendedInChild(storage: InMemoryStore, executions: string[]) {
    const child = createWorkflow({ id: 'child', inputSchema: z.object({}), outputSchema: z.object({}) })
      .then(
        createStep({
          id: 'approve',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          resumeSchema: z.object({ approved: z.boolean() }),
          execute: async ({ resumeData, suspend }) => {
            executions.push(`approve:${resumeData ? 'resumed' : 'suspended'}`);
            if (!resumeData) return suspend({});
            return {};
          },
        }),
      )
      .commit();
    const parent = createWorkflow({ id: 'parent', inputSchema: z.object({}), outputSchema: z.any() })
      .then(child)
      .then(
        createStep({
          id: 'after',
          inputSchema: z.any(),
          outputSchema: z.object({}),
          execute: async () => {
            executions.push('after');
            return {};
          },
        }),
      )
      .commit();
    const mastra = new Mastra({ logger: false, storage, workflows: { parent, child } });
    replicas.push(mastra);
    const run = await parent.createRun();
    expect((await run.start({ inputData: {} })).status).toBe('suspended');
    const workflowsStore = (await storage.getStore('workflows'))!;
    const parentSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: 'parent', runId: run.runId });
    const nestedRunId = (parentSnapshot?.context.child as { suspendPayload?: any }).suspendPayload?.__workflow_meta
      ?.runId as string;
    const resume = async () =>
      (await child.createRun({ runId: nestedRunId })).resume({ step: 'approve', resumeData: { approved: true } });
    return { run, workflowsStore, resume };
  }

  it('hands the parent lease a directly resumed nested run held over to the parent, without releasing it in between', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const { run, workflowsStore, resume } = await suspendedInChild(storage, executions);
    const claims = vi.spyOn(workflowsStore, 'claimRunOwnership');

    expect((await resume()).status).toBe('success');
    await vi.waitFor(async () =>
      expect(await workflowsStore.loadWorkflowRunStatus({ workflowName: 'parent', runId: run.runId })).toBe('success'),
    );
    expect(executions).toEqual(['approve:suspended', 'approve:resumed', 'after']);
    // The parent's lease was claimed once, by the nested run's resume, and the parent ran under it.
    expect(claims.mock.calls.filter(([args]) => args.runId === run.runId)).toHaveLength(1);
  });

  it('fails a direct resume of a nested run whose parent lease cannot be claimed, and leaves the parent suspended', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const { run, workflowsStore, resume } = await suspendedInChild(storage, executions);
    const claim = workflowsStore.claimRunOwnership.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'claimRunOwnership').mockImplementation(async args => {
      if (args.runId === run.runId) throw new Error('store unavailable');
      return claim(args);
    });

    await expect(resume()).rejects.toThrow('store unavailable');
    expect(executions).toEqual(['approve:suspended']);
    expect(await workflowsStore.loadWorkflowRunStatus({ workflowName: 'parent', runId: run.runId })).toBe('suspended');
  });

  it('does not restart a queued run that its owner took up again while it waited for a recovery slot', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const first = Promise.withResolvers<void>();
    const finishFirst = Promise.withResolvers<void>();
    const replica = buildReplica(
      storage,
      async runId => {
        executions.push(runId);
        first.resolve();
        await finishFirst.promise;
      },
      { workflowConcurrency: 1 },
    );
    replicas.push(replica);
    // Two runs whose process stopped long ago.
    const runs = [
      await persistOrphanedRun(storage, replica, -2 * WORKFLOW_RUN_LEASE_TTL_MS),
      await persistOrphanedRun(storage, replica, -2 * WORKFLOW_RUN_LEASE_TTL_MS),
    ];

    const pass = replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    await first.promise;
    // A later pass, as the next tick of the recovery loop runs, finds the run still queued.
    const later = replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    const restarted = runs.find(orphaned => orphaned.run.runId === executions[0])!;
    const waiting = runs.find(orphaned => orphaned !== restarted)!;
    // While the first restart holds the only slot, the second run's owner resumes it: it writes
    // the run and takes and releases its lease, as a resume does before the engine claims it.
    const { workflowsStore } = waiting;
    const { record } = await workflowsStore.claimRunOwnership({
      runId: waiting.run.runId,
      ownerId: 'resuming-process',
      leaseMs: WORKFLOW_RUN_LEASE_TTL_MS,
      force: true,
    });
    await workflowsStore.releaseRunOwnership({
      runId: waiting.run.runId,
      generation: record!.generation,
      ownerId: 'resuming-process',
    });
    await workflowsStore.updateWorkflowState({
      workflowName: waiting.workflow.id,
      runId: waiting.run.runId,
      opts: { status: 'running' },
    });

    finishFirst.resolve();
    await Promise.all([pass, later]);
    expect(executions).toEqual([restarted.run.runId]);
    expect(await waiting.status()).toBe('running');
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

  it('keeps recovering after a tick of the recovery loop fails to get the workflows store', async () => {
    const storage = new InMemoryStore();
    const executions: string[] = [];
    const replica = buildReplica(storage, async runId => void executions.push(runId), scanning);
    replicas.push(replica);
    await replica.startWorkers();
    const { run, status } = await persistOrphanedRun(storage, replica, 300);

    // The store fails to come up for a few ticks, while the run's lease lapses.
    const mastraStorage = replica.getStorage()!;
    const getStore = mastraStorage.getStore.bind(mastraStorage);
    let failing = true;
    let failures = 0;
    vi.spyOn(mastraStorage, 'getStore').mockImplementation((async (domain: 'workflows') => {
      if (failing) {
        failures++;
        throw new Error('store unavailable');
      }
      return getStore(domain);
    }) as typeof mastraStorage.getStore);
    await vi.waitFor(() => expect(failures).toBeGreaterThan(0), { timeout: 5_000 });
    failing = false;

    await expect.poll(status, { timeout: 5_000 }).toBe('success');
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
    replicas.push(replica);
    const { workflowsStore, status } = await persistOrphanedRun(storage, replica, 100);
    const listings = vi.spyOn(workflowsStore, 'listWorkflowRuns');
    await replica.startWorkers();
    await started.promise;

    // The loop keeps ticking while the run it restarted is still running.
    const listedAtRestart = listings.mock.calls.length;
    await vi.waitFor(() => expect(listings.mock.calls.length).toBeGreaterThanOrEqual(listedAtRestart + 2));
    expect(events).toEqual([]);
    const shutdown = replica.shutdown({ drainTimeout: 10_000 }).then(() => events.push('shut down'));
    const listedBeforeShutdown = listings.mock.calls.length;
    setTimeout(finish.resolve, 300);
    await shutdown;

    expect(events).toEqual(['step finished', 'shut down']);
    expect(await status()).toBe('success');
    // The drain took several scan intervals, and no tick listed runs meanwhile.
    expect(listings.mock.calls.length).toBe(listedBeforeShutdown);
  }, 15_000);

  it.each([
    ["recovery.workflows 'off'", { ...scanning, workflows: 'off' } as MastraRecoveryConfig, false],
    ['a store without run ownership support', scanning, true],
  ] as const)(
    'runs no recovery loop with %s',
    async (_, recovery, unfenced) => {
      const runOn = async (recovery: MastraRecoveryConfig, unfenced: boolean) => {
        const storage = new InMemoryStore();
        const replica = buildReplica(storage, async () => {}, recovery);
        replicas.push(replica);
        const orphaned = await persistOrphanedRun(storage, replica, 300);
        if (unfenced) vi.spyOn(orphaned.workflowsStore, 'supportsRunFencing').mockReturnValue(false);
        const listings = vi.spyOn(orphaned.workflowsStore, 'listWorkflowRuns');
        await replica.startWorkers();
        return { ...orphaned, replica, listings };
      };
      const auto = await runOn(scanning, false);
      const off = await runOn(recovery, unfenced);

      // By the time the loop of the other instance recovered its run on a later tick, this one listed nothing.
      await expect.poll(auto.status, { timeout: 5_000 }).toBe('success');
      expect(off.listings).not.toHaveBeenCalled();
      expect(await off.status()).toBe('running');
      if (!unfenced) return;
      // The server's boot-time call still sweeps a store without run ownership support once.
      await off.replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
      await expect.poll(off.status, { timeout: 5_000 }).toBe('success');
    },
    15_000,
  );

  it('does not keep the process alive while the recovery loop waits for its next tick', async () => {
    const replica = buildReplica(new InMemoryStore(), async () => {}, { workflowScanIntervalMs: 60_000 });
    replicas.push(replica);
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout');

    await replica.startWorkers();
    // Joins the loop's first pass, after which the loop schedules its next tick.
    await replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    await new Promise(resolve => setImmediate(resolve));

    // The next tick, 60s give or take a fifth.
    const tick = setTimeoutSpy.mock.calls.findIndex(
      ([, delay]) => delay !== undefined && delay >= 48_000 && delay <= 72_000,
    );
    expect(tick).toBeGreaterThanOrEqual(0);
    expect((setTimeoutSpy.mock.results[tick]!.value as NodeJS.Timeout).hasRef()).toBe(false);
  });

  it("sweeps every active run once on an 'orphaned' call whose run fencing probe failed", async () => {
    const storage = new InMemoryStore();
    const replica = buildReplica(storage, async () => {});
    replicas.push(replica);
    const orphaned = await persistOrphanedRun(storage, replica, 50);
    await new Promise(resolve => setTimeout(resolve, 100));
    // Only the first probe fails; the sweep it falls back to probes again.
    vi.spyOn(orphaned.workflowsStore, 'supportsRunFencing').mockRejectedValueOnce(new Error('probe failed'));

    // The lease lapsed less than a TTL ago, which an `orphaned` pass that could probe would wait out.
    await replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    expect(await orphaned.status()).toBe('success');
  });

  it('waits for the next tick when the scan interval exceeds the longest timer delay', async () => {
    const storage = new InMemoryStore();
    const replica = buildReplica(storage, async () => {}, { workflowScanIntervalMs: 2 ** 31 });
    replicas.push(replica);
    const listings = vi.spyOn((await storage.getStore('workflows'))!, 'listWorkflowRuns');
    // The latest jitter, a fifth past the interval.
    vi.spyOn(Math, 'random').mockReturnValue(0.999);

    await replica.startWorkers();
    await replica.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
    const afterFirstPass = listings.mock.calls.length;
    expect(afterFirstPass).toBeGreaterThan(0);

    // Node fires a timer whose delay is past 2^31-1 after 1 ms, which would start a pass per tick.
    await new Promise(resolve => setTimeout(resolve, 200));
    expect(listings).toHaveBeenCalledTimes(afterFirstPass);
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
