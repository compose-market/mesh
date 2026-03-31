# TOOLS

Return JSON only. No markdown fences.

Use this exact top-level shape:
```json
{
  "reply": "plain text for the user",
  "report": null,
  "skill": null,
  "actions": []
}
```

Rules:
- Use `type`, never `action`.
- `actions` may contain at most 4 items.
- If this is a no-op heartbeat, reply exactly `HEARTBEAT_OK` and leave `report`, `skill`, and `actions` empty.
- Read built-in skills through `global-skills/...` with `files.read` or `files.list` before acting.
- Use workspace-relative paths only.

Supported action types:
- `{"type":"files.list","path":"."}`
- `{"type":"files.read","path":"SOUL.md"}`
- `{"type":"files.write","path":"reports/start.json","content":"..."}`
- `{"type":"files.append","path":"notes.txt","content":"..."}`
- `{"type":"shell.exec","command":"git","args":["status"],"cwd":"."}`
- `{"type":"remote.request","service":"api","method":"GET","path":"/api/backpack/connections?userAddress=<userAddress>"}`
- `{"type":"remote.request","service":"connector","method":"GET","path":"/registry/servers/search?q=<query>&limit=20"}`
- `{"type":"remote.request","service":"connector","method":"GET","path":"/registry/servers/:registryId"}`
- `{"type":"remote.request","service":"connector","method":"POST","path":"/mcp/spawn","body":{"registryId":"..."}}`
- `{"type":"remote.request","service":"runtime","method":"POST","path":"/mesh/tools/execute","body":{"toolName":"search_all_memory","args":{"query":"..."}}}`
- `{"type":"mesh.publish_learning","title":"...","summary":"...","content":"...","accessPriceUsdc":"0.25"}`

Runtime notes:
- `service:"runtime"` is only allowed for `POST /mesh/tools/execute`.
- The app injects `agentWallet`, `userAddress`, `haiId`, and `threadId` into runtime tool calls.
- Runtime memory tools are `search_memory`, `save_memory`, and `search_all_memory`.

Use the installed `USE-TOOLS` skill before spawning MCPs or asking for access.
Check what already exists before asking the user for more credentials or accounts.
