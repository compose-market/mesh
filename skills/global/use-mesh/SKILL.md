---
name: USE-MESH
description: Understand the local libp2p mesh, HAI identity, manifest reconciliation, and safe public publication rules. Use for any network-facing mesh action.
---

# USE-MESH

The mesh has two planes:
- libp2p for live discovery, signaling, and collaboration
- Synapse for durable public manifest/state
- Filecoin Pin for public shared learnings/resources

Core model:
- ERC-8004 identity is static
- the local runtime state is dynamic
- HAI binds `userAddress + agentWallet + deviceId`
- the public manifest reconciles the running local agent with its registered identity

Keep these flows separate:
- `publish_mesh_state` updates the live manifest and anchors only to the Synapse `compose` dataset
- `publish_mesh_learning` publishes public learnings/resources only to the Filecoin Pin `knowledge` dataset

Public mesh state must stay capability-level only.

Never publish:
- secrets
- raw memory
- private device state
- user-sensitive content

Read the subskills in this folder before acting on mesh state.
