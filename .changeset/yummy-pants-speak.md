---
'@mastra/core': patch
---

Fixed evented workflow sleeps being lost when the instance that started them stopped (#30012). A `sleep()` or `sleepUntil()` step now stores the time it ends. When that instance stops, the recovery loop of another instance that runs workers wakes the run at its first check more than `MASTRA_AGENT_THREAD_LEASE_TTL_MS` (15 seconds by default) after the sleep ended. A lease on the sleep keeps instances that find it in the same check from each waking it. A wake that stalls for longer than the lease TTL after its last lease check can still publish the next step twice, and that step's lease and the check for finished steps drop the extra delivery. The loop needs a storage adapter with run-ownership support.

On storage adapters with run-ownership support, the boot-time restart and `mastra.restartAllActiveWorkflowRuns()` on an instance that runs the recovery loop now leave a run in such a sleep alone until the sleep is overdue, instead of replaying it. On an instance without the loop, for example with `recovery.workflows` set to `'off'`, they restart the run, and the restart waits only for what's left of the sleep. A run in a sleep started before this release is still restarted at boot, and its sleep starts over.

Restarting a run during a sleep now waits only for the rest of the sleep instead of starting it over, also when the sleep has no `id` and the restart runs on another instance. A finished sleep now reports `success` in the run's step results, as it does on the default engine.

Each sleep adds two `workflows.runOwnership` rows. Keep that policy's `maxAge` well above four lease TTLs.
