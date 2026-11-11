/**
 * How the recovery checks read the nested runs a stored evented run drives (#28712).
 */
import { describe, expect, it, vi } from 'vitest';
import { InMemoryStore } from '../../../storage/mock';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import type { WorkflowRunState } from '../../types';
import { allNestedRuns, loadNestedRuns } from './recovery-state';

/** A stored run whose step `step` is running and drives the nested runs `children`. */
function driving(runId: string, step: string, children: string[]): WorkflowRunState {
  return {
    ...createEmptyWorkflowSnapshot(runId),
    status: 'running',
    context: {
      [step]: {
        status: 'running',
        output: children.map(() => null),
        metadata: { foreachNestedRunIds: Object.fromEntries(children.map((child, i) => [i, child])) },
      },
    } as any,
  };
}

describe('loadNestedRuns', () => {
  it('reads at most a few nested runs at a time across every level of nesting', async () => {
    const store = (await new InMemoryStore().getStore('workflows'))!;
    // Eight children, each driving eight grandchildren.
    const children = Array.from({ length: 8 }, (_, i) => `child-${i}`);
    for (const child of children) {
      const grandchildren = Array.from({ length: 8 }, (_, i) => `${child}-${i}`);
      await store.persistWorkflowSnapshot({
        workflowName: 'items',
        runId: child,
        snapshot: driving(child, 'inner', grandchildren),
      });
      for (const grandchild of grandchildren) {
        await store.persistWorkflowSnapshot({
          workflowName: 'inner',
          runId: grandchild,
          snapshot: { ...createEmptyWorkflowSnapshot(grandchild), status: 'running' },
        });
      }
    }

    const read = store.getWorkflowRunById.bind(store);
    let reading = 0;
    let most = 0;
    vi.spyOn(store, 'getWorkflowRunById').mockImplementation(async args => {
      reading++;
      most = Math.max(most, reading);
      try {
        // Slow enough that reads of every level would overlap if they could.
        await new Promise(resolve => setTimeout(resolve, 5));
        return await read(args);
      } finally {
        reading--;
      }
    });

    const nested = await loadNestedRuns({ store, snapshot: driving('parent', 'items', children) });

    expect(allNestedRuns(nested)).toHaveLength(8 + 64);
    expect(most).toBeLessThanOrEqual(8);
  });
});
