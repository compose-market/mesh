---
name: USE-TOOLS / CORE
description: Core rules for MCPs, connectors, accounts, and local Compose tool surfaces. Use before spawning or calling tools from the local runtime.
---

# USE-TOOLS / CORE

Check what already exists before asking the user for anything.

Use these local Compose surfaces:
- central MCP registry list: `/registry/servers?origin=mcp&available=true`
- central MCP registry search: `/registry/servers/search?q=<query>&limit=20`
- central MCP registry detail: `/registry/servers/:registryId`
- connector registry spawn: `/registry/servers/:registryId/spawn`
- MCP servers list: `/mcp/servers`
- MCP tools list: `/mcp/servers/:slug/tools`
- MCP tool call: `/mcp/servers/:slug/call`
- MCP spawn: `/mcp/spawn`
- runtime memory tool bridge: `POST /mesh/tools/execute`
- runtime memory tools: `search_memory`, `save_memory`, `search_all_memory`
- Backpack accounts: `/api/backpack/connections?userAddress=<userAddress>`
- Backpack toolkit actions: `/api/backpack/toolkits?search=<query>&limit=20`, `/api/backpack/toolkits/:toolkit/actions?limit=40`
- WhatsApp pairing socket: `ws://localhost:<PORT>/whatsapp?userAddress=<userAddress>`

Order of work:
1. inspect registry and available MCPs
2. inspect the chosen registry record before spawning
3. inspect connected accounts
4. ask for missing access only when required
5. use runtime memory tools only when memory actually helps
6. execute the tool
