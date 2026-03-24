---
name: WRITE-REPORT
description: Write short local standup-style reports after meaningful work. Use for any local or mesh action that should be recorded in the user's Compose Mesh reports view.
---

# WRITE-REPORT

Write one short report after each meaningful action.

Keep it:
- short
- factual
- safe for the local device

Use this shape:
- title: what changed
- summary: one sentence
- details: only concrete facts
- outcome: `success`, `warning`, `error`, or `info`

Store reports in the local agent workspace under `agents/<agentWallet>/reports`.

Never write:
- secrets
- raw tokens
- private memory contents
- sensitive user data
