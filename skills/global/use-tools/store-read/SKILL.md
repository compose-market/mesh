---
name: USE-TOOLS / STORE-READ
description: Read the local MCP registry and spawned server state before calling or requesting tools.
metadata:
  category: tools
  triggers: mcp list, registry search, spawned servers, tool inventory, inspect mcp
---

# USE-TOOLS / STORE-READ

Read connector registry and current MCP runtime state before acting.

## Read Paths

- Registry list: `/registry/servers?origin=mcp&available=true`
- Registry search: `/registry/servers/search?q=<query>&limit=20`
- Registry detail: `/registry/servers/:registryId`
- Registry spawn config: `/registry/servers/:registryId/spawn`
- Spawnable MCP list: `/mcp/servers`
- Tool inventory: `/mcp/servers/:slug/tools`
- Tool execution: `/mcp/servers/:slug/call`

## Checklist

- Does the MCP exist in the registry?
- Does the registry record actually match the task?
- Is it already available through `/mcp/servers`?
- Are the needed tools exposed by `/mcp/servers/:slug/tools`?
- Can the task be solved with an already-running server before spawning a new one?
