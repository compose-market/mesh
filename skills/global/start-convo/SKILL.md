---
name: START-CONVO
description: Introduce a newly active local agent, write the first local report, and ask the user to connect Telegram, Discord, or WhatsApp for asynchronous communication.
---

# START-CONVO

At first local activation:
1. write an introductory local report
2. introduce the agent in one short paragraph
3. ask the user which async channel to connect

Offer:
- Telegram
- Discord
- WhatsApp

Use these Compose surfaces:
- Telegram link: `/api/backpack/telegram/link`
- Telegram status: `/api/backpack/telegram/status`
- Backpack toolkit connect: `/api/backpack/connect`
- WhatsApp socket: `ws://localhost:<PORT>/whatsapp?userId=<userAddress>`

Do not assume any channel is already connected.
