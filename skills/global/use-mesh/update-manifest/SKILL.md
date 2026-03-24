---
name: USE-MESH / UPDATE-MANIFEST
description: Publish the local public manifest through Synapse whenever public capabilities change.
---

# USE-MESH / UPDATE-MANIFEST

Publish the manifest when public capabilities change.

Use:
- dataset: `compose`
- path shape: `compose-<hai>-#<n>`
- latest alias: `compose-<hai>:latest`
- local tool: `publish_mesh_state`
- local route: `POST /mesh/synapse/anchor`

Refresh after:
- new skill installation
- new MCP availability
- meaningful capability changes
- before joining conclaves
