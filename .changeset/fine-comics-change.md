---
'@mastra/core': minor
---

Added a workflow recovery loop (#28712). An instance that runs workers now keeps restarting orphaned workflow runs while it runs, instead of only once at boot. Before, a run whose instance crashed after the other instances booted stayed stuck until the next boot.

The loop checks storage every `recovery.workflowScanIntervalMs` (30 seconds by default). It restarts a run once the lease of the instance running it expires. It also restarts a run that no instance keeps alive and that wasn't written for `recovery.workflowStallThresholdMs` (5 minutes by default), such as an evented run whose next step message was lost. It leaves evented runs in a `sleep()` or `sleepUntil()` step alone, restarts each run once even when several instances find it at the same time, and stops on `shutdown()`. Set `recovery.workflows` to `'off'` to turn it off along with the boot-time restart.

```ts
export const mastra = new Mastra({
  recovery: {
    workflowScanIntervalMs: 60_000,
    workflowStallThresholdMs: 10 * 60_000,
  },
});
```

`mastra.restartAllActiveWorkflowRuns()` no longer schedules its own check of a run another instance still holds. The recovery loop checks that run again instead.
