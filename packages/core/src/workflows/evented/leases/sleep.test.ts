import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../types';
import type { StoredNestedRun } from './recovery-state';
import { SLEEP_WAKE_GRACE_MS, sleepRecoveryState } from './sleep';

/** A nested run whose one step `nap` sleeps, with the wake time `wakeAt` or none stored. */
const sleeping = (runId: string, wakeAt?: number): StoredNestedRun => ({
  runId,
  updatedAt: Date.now(),
  nested: [],
  snapshot: {
    activePaths: [0],
    activeStepsPath: { nap: [0] },
    context: { nap: { status: 'waiting', startedAt: Date.now(), metadata: wakeAt ? { wakeAt } : {} } },
  } as unknown as WorkflowRunState,
});

const parent = { activePaths: [], activeStepsPath: {}, context: {} } as unknown as WorkflowRunState;

describe('sleepRecoveryState', () => {
  it.each([
    ['a sleep stored without a wake time first', true],
    ['a sleep whose wake time is not due first', false],
  ])(
    'counts a run with nested sleeps as untimed when one has no wake time and another is not due, %s',
    (_, untimedFirst) => {
      const untimed = sleeping('untimed');
      const notDue = sleeping('not-due', Date.now() + 60_000);

      // The not-due sleep's timer may wake it on time, which leaves only the untimed sleep for recovery.
      expect(sleepRecoveryState(parent, untimedFirst ? [untimed, notDue] : [notDue, untimed])).toBe('untimed');
    },
  );

  it('counts a run as overdue once one of its sleeps is overdue, whatever its other sleeps', () => {
    const overdue = sleeping('overdue', Date.now() - SLEEP_WAKE_GRACE_MS);

    expect(sleepRecoveryState(parent, [sleeping('untimed'), overdue])).toBe('overdue');
  });
});
