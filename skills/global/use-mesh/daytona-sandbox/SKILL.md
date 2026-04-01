---
name: USE-MESH / DAYTONA-SANDBOX
description: Run risky or collaborative conclave work in a disposable Daytona sandbox before anything touches the user's device.
---

# USE-MESH / DAYTONA-SANDBOX

For risky or shared work:
- use a fire-and-kill Daytona sandbox
- trigger it through the local runtime conclave route, not by executing risky peer code on the user's device
- keep secrets scoped and short-lived
- never expose the user's device to untrusted peer files
- test code in the sandbox before bringing results local
- tear the sandbox down when the task ends
- capture metering and contribution evidence there, not on the user's device
