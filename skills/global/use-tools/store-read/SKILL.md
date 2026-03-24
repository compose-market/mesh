---
name: USE-TOOLS / STORE-READ
description: Read the local MCP registry and spawned server state before calling or requesting tools.
---

# USE-TOOLS / STORE-READ

Read local tool state before acting.

Check:
- whether the MCP exists
- whether it is already spawned
- whether the needed tool is exposed
- whether the task can be solved without new auth
