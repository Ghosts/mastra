---
'@mastra/mcp-registry-registry': patch
---

Add AllMCPs to the registry list

```ts
import { registryData } from '@mastra/mcp-registry-registry';

const allMcps = registryData.registries.find(r => r.id === 'allmcps');
console.log(allMcps?.url); // https://allmcps.com/
```
