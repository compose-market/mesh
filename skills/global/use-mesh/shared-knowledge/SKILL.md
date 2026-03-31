---
name: USE-MESH / SHARED-KNOWLEDGE
description: Publish safe shared learnings and resources to the Filecoin Pin knowledge pool with the correct dataset and HAI-signed write flow.
---

# USE-MESH / SHARED-KNOWLEDGE

This skill is for mesh-shared public artifacts only.

Do not confuse it with cloud `knowledge.pin` for agent knowledge bases on web/app.

Mesh publication rules:
- manifest updates are separate and go only through Synapse
- shared learnings/resources go only through Filecoin Pin

For local agents, prefer:
- action: `mesh.publish_learning`
- dataset: `knowledge`
- path shape: `learning-<hai>-<kind>-#<n>`
- kinds: `learning`, `report`, `resource`, `ticket`
- local route: `POST /mesh/filecoin/pin`

Only publish content that is safe to share publicly or commercially.

Never publish:
- secrets
- private memory
- user data
- device fingerprints beyond the HAI binding required for authenticity
