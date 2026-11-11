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

  it.each([
    ['closed', (_queue: WorkflowRecoveryQueue) => {}],
    ['closed and opened again', (queue: WorkflowRecoveryQueue) => queue.open()],
  ])('restarts no run whose slot check was still running when the queue was %s', async (_, after) => {
    const restart = vi.fn(async () => {});
    const queue = new WorkflowRecoveryQueue(1, restart, () => {});
    const checking = Promise.withResolvers<boolean>();
    const checked = vi.fn(() => checking.promise);

    const queued = queue.enqueue('wf', 'run', checked);
    await vi.waitFor(() => expect(checked).toHaveBeenCalled());
    // The workers stop while the check reads storage.
    queue.close();
    after(queue);
    checking.resolve(true);
    await queued;

    expect(restart).not.toHaveBeenCalled();
    // The slot is free again, so a run queued after reopening is restarted.
    queue.open();
    await queue.enqueue('wf', 'run', async () => true);
    expect(restart).toHaveBeenCalledTimes(1);
  });

  it('leaves a run alone when its slot check finds it no longer orphaned and nothing overrode it', async () => {
    const restart = vi.fn(async () => {});
    const queue = new WorkflowRecoveryQueue(1, restart, () => {});

    await queue.enqueue('wf', 'run', async () => false);

    expect(restart).not.toHaveBeenCalled();
  });
});
