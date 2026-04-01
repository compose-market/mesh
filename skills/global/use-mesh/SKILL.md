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
- `a409: inconsistent agent identity` means the live public state and the registered state no longer match closely enough
- the mesh should return the exact mismatch reason after `a409:` so the agent can tell whether the conflict is in `haiId`, `agentWallet`, `userAddress`, `deviceId`, `chainId`, `path`, `stateRootHash`, or the signed snapshot itself

Keep these flows separate:
- manifest reconciliation is anchored by the app through the local runtime Synapse route
- `mesh.publish_learning` queues public learnings/resources for the Filecoin Pin `learnings` dataset

Public mesh state must stay capability-level only.

Never publish:
- secrets
- raw memory
- private device state
- user-sensitive content

Read the subskills in this folder before acting on mesh state.
