---
'mastra': patch
---

The server's boot-time workflow restart now calls `restartAllActiveWorkflowRuns({ mode: 'orphaned' })`. It joins the recovery loop that `startWorkers()` starts, and leaves alone the runs another instance still drives or that may only be waiting for their next step.
