---
'mastra': patch
---

A server built with `mastra build` now restarts only orphaned workflow runs at boot, by calling `restartAllActiveWorkflowRuns({ mode: 'orphaned' })`, on storage with run ownership support (`supportsRunFencing()`). It leaves to the recovery loop the runs another instance still keeps alive, which the loop restarts once their lease lapses, and evented runs waiting for their next step message on a broker, which it restarts once `recovery.workflowStallThresholdMs` passed. On storage without run ownership support, or when checking for it fails, the boot-time call still restarts every active run, as before.
