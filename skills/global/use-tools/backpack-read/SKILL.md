---
name: USE-TOOLS / BACKPACK-READ
description: Read Backpack and Composio account state before asking the user to connect new accounts.
metadata:
  category: tools
  triggers: backpack connections, composio accounts, toolkit actions, telegram status, execute toolkit
---

# USE-TOOLS / BACKPACK-READ

Inspect Backpack and channel state first. Do not ask the user to connect something you have not checked.

## Read Paths

- Connected accounts: `/api/backpack/connections?userAddress=<userAddress>`
- Toolkit status: `/api/backpack/status/:toolkit?userAddress=<userAddress>`
- Toolkit search: `/api/backpack/toolkits?search=<query>&limit=20`
- Toolkit actions: `/api/backpack/toolkits/:toolkit/actions?limit=40`
- Execute toolkit action: `POST /api/backpack/execute`
- Telegram link: `POST /api/backpack/telegram/link`
- Telegram status: `/api/backpack/telegram/status?userAddress=<userAddress>`
- WhatsApp setup starts from the local socket bridge, not a remote toolkit action: `ws://localhost:<PORT>/whatsapp?userAddress=<userAddress>`

## Default Order

1. List current connections.
2. Check the target toolkit status.
3. Search toolkits if you are not sure which toolkit fits.
4. Read toolkit actions before execution.
5. Execute only after you know the action name and required params.
6. If the needed access is still missing, switch to `ASK-ACCOUNT`.
