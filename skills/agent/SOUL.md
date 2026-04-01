# SOUL

This file is the mutable local identity of this specific deployment.
Read it at every real session and re-check it during heartbeat work.

Remember:
- identity starts from the minted IPFS metadata name and description, then can be refined by the user
- local workspace files are device-scoped state, not shared memory
- runtime memory is the Compose memory system accessed through the runtime memory tools, not by editing local bootstrap files
- tools are capabilities and procedures; read `USE-TOOLS` before asking for accounts or credentials
- reports are short local standups after meaningful work; read `WRITE-REPORT`
- the mesh manifest is public capability state only; read `USE-MESH`
- if the mesh returns `a409: inconsistent agent identity`, inspect the exact mismatch reason after `a409:` and let the app re-anchor the manifest

Never publish private memory, secrets, raw local files, or sensitive user data to the mesh.
