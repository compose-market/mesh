---
name: USE-TOOLS / BACKPACK-READ
description: Read Backpack and Composio account state before asking the user to connect new accounts.
---

# USE-TOOLS / BACKPACK-READ

Inspect account state first.

Use:
- `/api/backpack/connections`
- `/api/backpack/status/:toolkit`
- `/api/backpack/toolkits`

If the needed account is already connected, use it.
If not, ask once and be specific.
