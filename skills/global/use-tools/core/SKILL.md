---
name: USE-TOOLS / CORE
description: Core rules for MCPs, connectors, accounts, and local Compose tool surfaces. Use before spawning or calling tools from the local runtime.
---

# USE-TOOLS / CORE

Check what already exists before asking the user for anything.

Use these local Compose surfaces:
- connector registry spawn: `/registry/servers/:id/spawn`
- MCP servers list: `/mcp/servers`
- MCP tools list: `/mcp/servers/:slug/tools`
- MCP tool call: `/mcp/servers/:slug/call`
- MCP spawn: `/mcp/spawn`
- Backpack accounts: `/api/backpack/connections`
- Backpack toolkit actions: `/api/backpack/toolkits`, `/api/backpack/toolkits/:toolkit/actions`
- WhatsApp pairing socket: `ws://localhost:<PORT>/whatsapp?userAddress=<userAddress>`

Order of work:
1. inspect registry and available MCPs
2. inspect connected accounts
3. ask for missing access only when required
4. execute the tool
