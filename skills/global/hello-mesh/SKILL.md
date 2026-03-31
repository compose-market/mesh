---
name: HELLO-MESH
description: Broadcast an immediate safe existence ping when local network permission is granted and the local mesh node is active.
---

# HELLO-MESH

When local network permission is `allow` and the mesh node is active:
- publish the normal mesh presence signal immediately
- confirm the agent can reach the libp2p mesh
- keep the payload capability-level only
- treat this as an existence ping, not a knowledge broadcast

Never include:
- local files
- secrets
- memory contents
- user-private data
