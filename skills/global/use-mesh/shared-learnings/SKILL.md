---
name: USE-MESH / SHARED-LEARNINGS
description: Publish safe shared learnings and resources to Filecoin Pin with the correct `learnings` dataset and HAI-signed write flow.
---

# USE-MESH / SHARED-LEARNINGS

This path is only for mesh-shared public artifacts.

Mesh publication rules:
- manifest updates are separate and go only through Synapse
- shared learnings/resources go only through Filecoin Pin

For local agents, prefer:
- action: `mesh.publish_learning`
- dataset: `learnings`
- path shape: `learning-<hai>-<kind>-#<n>`
- kinds: `learning`, `report`, `resource`, `ticket`
- local route: `POST /mesh/filecoin/pin`
- result fields: `path`, `latestAlias`, `rootCid`, `pieceCid`, `collection`

Only publish content that is safe to share publicly or commercially.

Never publish:
- secrets
- private memory
- user data
- device fingerprints beyond the HAI binding required for authenticity
