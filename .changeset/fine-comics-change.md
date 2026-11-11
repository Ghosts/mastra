---
'@mastra/core': minor
---

Added a workflow recovery loop (#28712). An instance that runs workers now keeps restarting orphaned workflow runs while it runs, instead of only once at boot. Before, a run whose instance crashed after the other instances booted stayed stuck until the next boot.

`startWorkers()` now starts the loop, and its first pass is the boot-time restart. This reaches every process that calls `startWorkers()`, including standalone `mastra worker` processes and scripts. A process whose workers only start on their own, for example when it restarts a run or dispatches a background task, runs no loop. On storage without run ownership support (no `supportsRunFencing()`, for example MongoDB without a replica set), the loop does nothing, and the server's boot-time call restarts every active run once, as before. Set `recovery.workflows` to `'off'` to turn off both the loop and the boot-time restart.

The loop checks storage every `recovery.workflowScanIntervalMs` (30 seconds by default, 2 minutes on storage that reads every run of a workflow to list its active runs). It restarts:

- an evented run whose worker stopped in the middle of a step, once that step's lease expires
- a default-engine run one lease TTL after its lease expired
- a run that no instance keeps alive and that wasn't written for `recovery.workflowStallThresholdMs` (5 minutes by default), such as an evented run whose next step message was lost

The first pass of an instance also restarts sleeping evented runs whose process stopped, as the boot-time restart did before. With the default in-process pubsub it also restarts evented runs waiting for their next step message, since those messages died with their process. An instance on that pubsub marks its evented runs as no longer driven when `shutdown()` runs, so the next pass of any instance restarts them at once, even within one lease TTL of the shutdown. Later passes leave sleeping runs alone. After an outage of every instance, an evented run waiting for its next step message on a broker that keeps messages is restarted only once `recovery.workflowStallThresholdMs` passed, not at boot. The loop restarts each run once even when several instances find it at the same time, and stops on `stopWorkers()` and `shutdown()`.

```ts
export const mastra = new Mastra({
  recovery: {
    workflowScanIntervalMs: 60_000,
    workflowStallThresholdMs: 10 * 60_000,
  },
});
```

`mastra.restartAllActiveWorkflowRuns()` keeps restarting every active run no instance keeps alive. Pass `{ mode: 'orphaned' }` to apply the loop's rules instead, as the server now does at boot. A run another instance still holds is no longer checked again by the call itself. An instance that runs workers checks that run again on its recovery loop. An instance without workers doesn't check it again.

```ts
await mastra.restartAllActiveWorkflowRuns({ mode: 'orphaned' });
```

A restart of an evented step that crashed during a retry continues the attempt count instead of starting its attempts over.

The in-memory store now moves a run's `updatedAt` on every write to the run, as the database stores do. The recovery loop reads it to find stalled runs.
