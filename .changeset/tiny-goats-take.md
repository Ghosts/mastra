---
'@mastra/deployer': patch
---

The `/__restart-active-workflow-runs` boot handler now calls `restartAllActiveWorkflowRuns({ mode: 'orphaned' })`. It joins the recovery loop that `startWorkers()` starts, and leaves alone the runs another instance still drives or that may only be waiting for their next step.
