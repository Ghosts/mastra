---
'@mastra/core': patch
---

Fixed evented workflow sleeps being lost when the instance that started them stopped (#30012). A `sleep()` or `sleepUntil()` step now stores the time it ends. If that instance stops, the recovery loop of another instance that runs workers wakes the run at its first check more than `MASTRA_AGENT_THREAD_LEASE_TTL_MS` (15 seconds by default) after the sleep ended. This needs a storage adapter with run-ownership support. Instances that find the same overdue sleep don't each wake it.

On an instance that runs the recovery loop, the boot-time restart and `mastra.restartAllActiveWorkflowRuns()` now leave a run in such a sleep alone until the sleep is overdue, instead of replaying it. An instance without the loop, for example one with `recovery.workflows` set to `'off'`, restarts the run, and the restart waits only for what's left of the sleep. A run in a sleep that started before this release is still restarted at boot, and its sleep starts over.

Restarting a run during a sleep now waits only for the rest of the sleep instead of starting it over, also when the sleep has no `id` and the restart runs on another instance. A woken sleep now reports `success` in the run's step results, as it does on the default engine.

Each sleep adds two `workflows.runOwnership` records. If you prune that table, keep its `maxAge` well above four lease TTLs: pruning a sleep's records sooner can run the step after the sleep a second time.
