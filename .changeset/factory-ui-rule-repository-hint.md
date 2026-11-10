---
'@mastra/playground-ui': patch
---

The rule builder now shows a field's JSON Schema `examples` as the placeholder of its value input, so a field can hint the format its value takes:

```ts
const schema = {
  type: 'object',
  properties: {
    repository: { type: 'string', title: 'Repository', examples: ['owner/repo', 'ENG'] },
  },
};
// The value input for "Repository" reads "owner/repo, ENG" until you type.
```
