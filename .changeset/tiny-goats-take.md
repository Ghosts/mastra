---
'@mastra/deployer': patch
---

After a reload, `mastra dev` restarts workflow runs with the recovery loop's rules: the dev server's boot-time restart now calls `restartAllActiveWorkflowRuns({ mode: 'orphaned' })`. With the default in-process pubsub, the previous process marks its evented runs as no longer driven as it shuts down, so a run it left between steps is restarted at once. A step it left running is restarted once that step's lease lapses.
