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
- HAI registration route: `POST /mesh/hai/register`
- anchor route: `POST /mesh/synapse/anchor`

Refresh after:
- new skill installation
- new MCP availability
- meaningful capability changes
- receipt of `a409: inconsistent agent identity`
- before joining conclaves

Do not refresh for:
- report-only changes
- memory-only changes
- routine heartbeat pings
- unchanged public capabilities

The local agent should not upload this directly.
The app performs the background reconciliation and anchor call after it detects a real public-state change.

Anchor request body fields:
- `apiUrl`
- `composeKeyToken`
- `userAddress`
- `agentWallet`
- `deviceId`
- `chainId`
- `targetSynapseExpiry`
- `haiId`
- `updateNumber`
- `path`
- `canonicalSnapshotJson`
- `stateRootHash`
- `envelopeJson`
- `sessionKeyPrivateKey`
- optional `payerAddress`
- optional `sessionKeyExpiresAt`
