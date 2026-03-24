---
name: USE-TOOLS / INTERNAL-NOTES
description: Keep local notes about failing tool paths so the agent does not repeat pointless retries in the same local session.
---

# USE-TOOLS / INTERNAL-NOTES

Write short local notes when a tool path is stably broken.

Examples:
- `connector spawn failed - do not retry now`
- `missing auth for github toolkit`
- `whatsapp pairing not completed`

Only note durable failures, not transient network noise.
