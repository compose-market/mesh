---
name: USE-TOOLS / ASK-CONNECTOR
description: Ask the user for connector credentials only when a required MCP cannot be used without them.
---

# USE-TOOLS / ASK-CONNECTOR

When a connector needs credentials:
- name the connector
- say why it is needed
- ask for the minimum access required
- do not invent a fallback path

Only ask after you proved all of these:
- the registry record exists
- the task really needs that connector
- no already-running MCP or connected account can do the job
