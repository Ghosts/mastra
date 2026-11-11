import { describe, expect, it, vi } from 'vitest';
import { WorkflowRecoveryQueue } from './workflow-recovery-queue';

describe('WorkflowRecoveryQueue', () => {
  it('restarts a run once when a restart without a check joins it while its slot check runs and finds it no longer orphaned', async () => {
    const restart = vi.fn(async () => {});
    const queue = new WorkflowRecoveryQueue(1, restart, () => {});
    const checking = Promise.withResolvers<boolean>();
    const checked = vi.fn(() => checking.promise);

    const orphaned = queue.enqueue('wf', 'run', checked);
    await vi.waitFor(() => expect(checked).toHaveBeenCalled());
    // An `all` pass finds the run while its check is pending: it wants the run restarted regardless.
    const all = queue.enqueue('wf', 'run');
    checking.resolve(false);
    await Promise.all([orphaned, all]);

    expect(restart).toHaveBeenCalledTimes(1);
  });

  it('leaves a run alone when its slot check finds it no longer orphaned and nothing overrode it', async () => {
    const restart = vi.fn(async () => {});
    const queue = new WorkflowRecoveryQueue(1, restart, () => {});

    await queue.enqueue('wf', 'run', async () => false);

    expect(restart).not.toHaveBeenCalled();
  });
});
