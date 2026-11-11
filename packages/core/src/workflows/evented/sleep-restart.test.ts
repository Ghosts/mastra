import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { PubSub } from '../../events/pubsub';
import type { EventCallback, SubscribeOptions } from '../../events/types';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { runLivenessKey } from './leases';
import { abandonLease } from './test-utils/abandon-lease';
import { createStep, createWorkflow } from '.';

/**
 * Delivers through an in-process emitter, but doesn't count as one: recovery treats it like a
 * broker whose messages outlive the process that published them.
 */
class BrokerLikePubSub extends PubSub {
  readonly #inner = new EventEmitterPubSub();
  publish(...args: Parameters<PubSub['publish']>) {
    return this.#inner.publish(...args);
  }
  subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions) {
    return this.#inner.subscribe(topic, cb, options);
  }
  unsubscribe(topic: string, cb: EventCallback) {
    return this.#inner.unsubscribe(topic, cb);
  }
  flush() {
    return this.#inner.flush();
  }
  override get supportedModes() {
    return this.#inner.supportedModes;
  }
}

function createSleepingWorkflow({
  id,
  duration,
  beforeSleep,
  afterSleep,
  autoRestartActiveRuns,
  until = false,
}: {
  id: string;
  duration: number;
  beforeSleep: () => Promise<{ value: string }>;
  afterSleep: (args: { inputData: { value: string } }) => Promise<{ value: string }>;
  autoRestartActiveRuns?: boolean;
  until?: boolean;
}) {
  const workflow = createWorkflow({
    id,
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    options: { autoRestartActiveRuns },
  }).then(
    createStep({
      id: `before-${id}`,
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: beforeSleep,
    }),
  );

  const sleepingWorkflow = until ? workflow.sleepUntil(new Date(Date.now() + duration)) : workflow.sleep(duration);
  return sleepingWorkflow
    .then(
      createStep({
        id: `after-${id}`,
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ value: z.string() }),
        execute: afterSleep,
      }),
    )
    .commit();
}

async function startSleepingRun({
  storage,
  workflow,
  runId,
}: {
  storage: InstanceType<typeof MockStore>;
  workflow: ReturnType<ReturnType<typeof createWorkflow>['commit']>;
  runId: string;
}) {
  const pubsub = new EventEmitterPubSub();
  const runtime = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
  await runtime.startWorkers();

  const run = await workflow.createRun({ runId });
  void run.start({ inputData: { value: 'persist me' } });
  const workflowsStore = await storage.getStore('workflows');
  await vi.waitFor(async () => {
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({ workflowName: workflow.id, runId });
    expect(snapshot?.status).toBe('running');
    expect(Object.values(snapshot?.context ?? {}).some(result => result.status === 'waiting')).toBe(true);
  });

  await runtime.stopWorkers();
  // The stopped runtime stands for a process that went away; the liveness its last step wrote lapses.
  await abandonLease(storage, runLivenessKey(runId));
  return workflowsStore;
}

// With an in-process pubsub the boot pass restarts a run whose liveness lapsed before it checks for a
// sleep, so only the broker-like pubsub exercises the sleep rule.
const pubsubs = [
  ['an in-process pubsub', () => new EventEmitterPubSub()],
  ['a pubsub shared across processes', () => new BrokerLikePubSub()],
] as const;

describe('evented workflow durable sleep recovery', () => {
  it.each(pubsubs)(
    'replays a sleeping path after restart without rerunning completed work, with %s',
    async (_, pubsub) => {
      const storage = new MockStore();
      const beforeSleep = vi.fn(async () => ({ value: 'persist me' }));
      const afterSleep = vi.fn(async ({ inputData }: { inputData: { value: string } }) => inputData);
      const workflow = createSleepingWorkflow({
        id: 'sleep-restart-workflow',
        duration: 5_000,
        beforeSleep,
        afterSleep,
      });
      const workflowsStore = await startSleepingRun({ storage, workflow, runId: 'sleep-restart-run' });
      const replacementRuntime = new Mastra({
        logger: false,
        storage,
        pubsub: pubsub(),
        workflows: { [workflow.id]: workflow },
      });
      // Boots like a server: the boot pass restarts a sleeping run whose process stopped.
      await replacementRuntime.startWorkers();
      await replacementRuntime.restartAllActiveWorkflowRuns({ mode: 'orphaned' });

      // The sweep returns once the restart is handed to the workers; the replayed sleep still runs out.
      await vi.waitFor(
        async () => {
          const snapshot = await workflowsStore?.loadWorkflowSnapshot({
            workflowName: workflow.id,
            runId: 'sleep-restart-run',
          });
          expect(snapshot?.status).toBe('success');
        },
        { timeout: 10_000 },
      );
      expect(beforeSleep).toHaveBeenCalledTimes(1);
      expect(afterSleep).toHaveBeenCalledTimes(1);
      await replacementRuntime.stopWorkers();
    },
  );

  it.each(['listWorkflowRuns', 'getRunOwnerships'] as const)(
    'replays a sleeping path on the next pass when %s failed during the boot pass',
    async method => {
      const storage = new MockStore();
      const afterSleep = vi.fn(async ({ inputData }: { inputData: { value: string } }) => inputData);
      const workflow = createSleepingWorkflow({
        id: 'sleep-boot-retry-workflow',
        duration: 5_000,
        beforeSleep: async () => ({ value: 'persist me' }),
        afterSleep,
      });
      const workflowsStore = (await startSleepingRun({ storage, workflow, runId: 'sleep-boot-retry-run' }))!;
      const status = async () =>
        (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: 'sleep-boot-retry-run' }))
          ?.status;
      // Only the recovery pass's first read of the run fails.
      const read = workflowsStore[method].bind(workflowsStore) as (args: unknown) => Promise<unknown>;
      let failed = false;
      vi.spyOn(workflowsStore, method).mockImplementation((async (args: Record<string, unknown>) => {
        const ofRun =
          args.workflowName === workflow.id ||
          (args.runIds as string[] | undefined)?.includes(runLivenessKey('sleep-boot-retry-run'));
        if (ofRun && !failed) {
          failed = true;
          throw new Error('store unavailable');
        }
        return read(args);
      }) as any);
      // The sleep rule only decides on a pubsub whose messages outlive their process.
      const replacementRuntime = new Mastra({
        logger: false,
        storage,
        pubsub: new BrokerLikePubSub(),
        workflows: { [workflow.id]: workflow },
      });
      await replacementRuntime.startWorkers();
      await replacementRuntime.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
      expect(await status()).toBe('running');

      await replacementRuntime.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
      await vi.waitFor(async () => expect(await status()).toBe('success'), { timeout: 10_000 });
      expect(afterSleep).toHaveBeenCalledTimes(1);
      await replacementRuntime.stopWorkers();
    },
  );

  it.each(pubsubs)('replays a sleepUntil path after restart, with %s', async (_, pubsub) => {
    const storage = new MockStore();
    const beforeSleep = vi.fn(async () => ({ value: 'persist me' }));
    const afterSleep = vi.fn(async ({ inputData }: { inputData: { value: string } }) => inputData);
    const workflow = createSleepingWorkflow({
      id: 'sleep-until-restart-workflow',
      duration: 5_000,
      beforeSleep,
      afterSleep,
      until: true,
    });
    const workflowsStore = await startSleepingRun({ storage, workflow, runId: 'sleep-until-restart-run' });

    const replacementRuntime = new Mastra({
      logger: false,
      storage,
      pubsub: pubsub(),
      workflows: { [workflow.id]: workflow },
    });
    await replacementRuntime.startWorkers();
    await replacementRuntime.restartAllActiveWorkflowRuns({ mode: 'orphaned' });

    await vi.waitFor(
      async () => {
        const snapshot = await workflowsStore?.loadWorkflowSnapshot({
          workflowName: workflow.id,
          runId: 'sleep-until-restart-run',
        });
        expect(snapshot?.status).toBe('success');
      },
      { timeout: 10_000 },
    );
    expect(beforeSleep).toHaveBeenCalledTimes(1);
    expect(afterSleep).toHaveBeenCalledTimes(1);
    await replacementRuntime.stopWorkers();
  });

  it.each(pubsubs)('does not restart evented runs when autoRestartActiveRuns is false, with %s', async (_, pubsub) => {
    const storage = new MockStore();
    const beforeSleep = vi.fn(async () => ({ value: 'persist me' }));
    const afterSleep = vi.fn(async ({ inputData }: { inputData: { value: string } }) => inputData);
    const workflow = createSleepingWorkflow({
      id: 'disabled-sleep-restart-workflow',
      duration: 5_000,
      beforeSleep,
      afterSleep,
      autoRestartActiveRuns: false,
    });
    const workflowsStore = await startSleepingRun({ storage, workflow, runId: 'disabled-sleep-restart-run' });
    const createRun = vi.spyOn(workflow, 'createRun');

    const replacementRuntime = new Mastra({
      logger: false,
      storage,
      pubsub: pubsub(),
      workflows: { [workflow.id]: workflow },
    });
    await replacementRuntime.startWorkers();
    await replacementRuntime.restartAllActiveWorkflowRuns({ mode: 'orphaned' });

    expect(createRun).not.toHaveBeenCalled();
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'disabled-sleep-restart-run',
    });
    expect(snapshot?.status).toBe('running');
    expect(beforeSleep).toHaveBeenCalledTimes(1);
    expect(afterSleep).not.toHaveBeenCalled();
    await replacementRuntime.stopWorkers();
  });
});
