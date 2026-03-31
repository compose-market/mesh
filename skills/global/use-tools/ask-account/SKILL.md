---
name: USE-TOOLS / ASK-ACCOUNT
description: Ask the user to connect a missing Backpack or channel account when the connector exists but access is not yet granted.
---

# USE-TOOLS / ASK-ACCOUNT

When the toolkit exists but the user account is missing:
- name the exact account or channel
- explain the benefit
- ask once clearly

Relevant paths:
- Telegram link: `/api/backpack/telegram/link`
- Telegram status: `/api/backpack/telegram/status?userAddress=<userAddress>`
- Backpack connect flow: `/api/backpack/connect`
- Discord access normally starts by connecting the Composio/Backpack toolkit for `DISCORD`
- WhatsApp pairing socket: `ws://localhost:<PORT>/whatsapp?userAddress=<userAddress>`
