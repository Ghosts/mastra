/**
 * Helpers for the tests of `restartAllActiveWorkflowRuns()` (#28712). Each
 * replica is a Mastra instance sharing one store, the way pods share one
 * database.
 */
import { afterEach, vi } from 'vitest';
import { z } from 'zod/v4';
import type { PubSub } from '../../events/pubsub';
import type { InMemoryStore } from '../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../storage/workflow-snapshot';
import { createStep, createWorkflow } from '../../workflows';
import type { WorkflowRunStatus } from '../../workflows';
import { createStep as createEventedStep, createWorkflow as createEventedWorkflow } from '../../workflows/evented';
import { Mastra } from '../index';
import type { MastraRecoveryConfig } from '../index';

export type Engine = 'default' | 'evented';

/**
 * Shuts the replicas a test file creates down after each test, and returns the
 * function that creates one: a replica whose `recovered-wf` workflow runs one
 * step that calls `execute` with the run id. Its workers are not started, so
 * no recovery loop restarts runs on its own.
 */
export function useRecoveredReplicas() {
  const replicas: Mastra[] = [];
  afterEach(async () => {
    vi.restoreAllMocks();
    await Promise.all(replicas.splice(0).map(mastra => mastra.shutdown({ drainTimeout: 0 })));
  });

  return function createReplica(
    storage: InMemoryStore,
    engine: Engine,
    execute: (runId: string) => void | Promise<void>,
    { pubsub, recovery }: { pubsub?: PubSub; recovery?: MastraRecoveryConfig } = {},
  ) {
    const [create, step] =
      engine === 'evented'
        ? ([createEventedWorkflow, createEventedStep] as const)
        : ([createWorkflow, createStep] as const);
    const workflow = (create as typeof createWorkflow)({
      id: 'recovered-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(
        (step as typeof createStep)({
          id: 'work',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async ({ runId }) => {
            await execute(runId);
            return {};
          },
        }),
      )
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { 'recovered-wf': workflow }, recovery });
    replicas.push(mastra);
    return mastra;
  };
}

/** Stores a run of `recovered-wf` its process left `status` before its first step finished, as a crash leaves it. */
export async function persistActiveRun(
  storage: InMemoryStore,
  runId: string,
  { updatedAt, status = 'running' }: { updatedAt?: Date; status?: WorkflowRunStatus } = {},
) {
  const store = (await storage.getStore('workflows'))!;
  await store.persistWorkflowSnapshot({
    workflowName: 'recovered-wf',
    runId,
    updatedAt,
    snapshot: { ...createEmptyWorkflowSnapshot(runId), status, activePaths: [], context: { input: {} } as any },
  });
}
