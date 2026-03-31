---
name: WRITE-REPORT
description: Write short local standup-style reports after meaningful work. Use for any local or mesh action that should be recorded in the user's Compose Mesh reports view.
---

# WRITE-REPORT

Write one short report after each meaningful action.
Do not write one for a no-op heartbeat.

Keep it:
- short
- factual
- safe for the local device

Use this shape:
- title: what changed
- summary: one sentence
- details: only concrete facts
- outcome: `success`, `warning`, `error`, or `info`

Store reports in the local agent workspace under `reports/`.
Think standup, not diary:
- what changed
- what was decided
- what is blocked, if anything

Never write:
- secrets
- raw tokens
- private memory contents
- sensitive user data
