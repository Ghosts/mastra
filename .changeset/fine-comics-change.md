---
'@mastra/core': minor
---

Added a workflow recovery loop (#28712). An instance that runs workers now keeps restarting orphaned workflow runs while it runs, instead of only once at boot. Before, a run whose instance crashed after the other instances booted stayed stuck until the next boot.

`startWorkers()` now starts the loop, and its first pass is the boot-time restart. This reaches every process that calls `startWorkers()`, including standalone `mastra worker` processes and scripts. On storage without run ownership support (no `supportsRunFencing()`), that first pass restarts every active run, as the boot-time restart always did there, and the loop stops after it. A process whose workers only start on their own, for example when it restarts a run or dispatches a background task, runs no loop. Set `recovery.workflows` to `'off'` to turn off both the loop and the boot-time restart.

The loop checks storage every `recovery.workflowScanIntervalMs` (30 seconds by default, 2 minutes on storage that reads every run of a workflow to list its active runs). It restarts:

- an evented run whose worker stopped in the middle of a step, once that step's lease expires
- a default-engine run one lease TTL after its lease expired
- a run that no instance keeps alive and that wasn't written for `recovery.workflowStallThresholdMs` (5 minutes by default), such as an evented run whose next step message was lost

It leaves evented runs in a `sleep()` or `sleepUntil()` step alone, restarts each run once even when several instances find it at the same time, and stops on `stopWorkers()` and `shutdown()`.

```ts
export const mastra = new Mastra({
  recovery: {
    workflowScanIntervalMs: 60_000,
    workflowStallThresholdMs: 10 * 60_000,
  },
});
```

`mastra.restartAllActiveWorkflowRuns()` now applies the same rules on storage with run ownership support. It no longer restarts a sleeping evented run or a run that may only be waiting for its next step. Pass `{ mode: 'all' }` to restart every active run no instance keeps alive, as before. It also no longer schedules its own check of a run another instance still holds. An instance that runs workers checks that run again on its recovery loop. An instance without workers doesn't check it again.

The in-memory store now moves a run's `updatedAt` on every write to the run, as the database stores do. The recovery loop reads it to find stalled runs.
