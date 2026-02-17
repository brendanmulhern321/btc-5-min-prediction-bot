# Model Policy for `btc-5-min-prediction-bot`

Goal: avoid running out of credits while keeping coding quality high.

## 1) Default Model (always start here)

Use a low-cost model for:
- reading files
- simple refactors
- config edits
- log parsing
- writing small scripts/tests

Rule:
- Stay on cheap model unless blocked twice on the same task.

## 2) Escalation Model (use briefly)

Escalate to a mid/high model only for:
- hard debugging with unclear root cause
- architecture changes across multiple files
- risky trading logic changes (entry/exit/sizing logic)
- security-sensitive changes

Rule:
- Escalate for one scoped task, then immediately return to cheap model.

## 3) Task Routing for This Repo

- `fastloop_trader.py` small edits: cheap model
- `config.json` tuning help: cheap model
- adding tests or validation scripts: cheap model
- strategy redesign or major risk controls: mid/high model
- final review before live-trading logic changes: mid/high model, then cheap for implementation polish

## 4) Hard Credit Controls (required)

Set all of these in your AI billing/settings:
- monthly hard cap (cannot exceed)
- daily soft cap alerts (50%, 75%, 90%)
- per-request max output tokens
- disable automatic premium model upgrades

Recommended starting caps (adjust to budget):
- monthly hard cap: `$30`
- daily alert threshold: `$1`
- per-request max output: `400-800` tokens for routine coding

## 5) Prompt Discipline (biggest saver)

Before each request, include:
- exact file path(s)
- exact expected output
- "no broad rewrite"
- "keep response under N lines"

Example:
`Edit fastloop_trader.py only. Fix X bug. No refactor. Show minimal diff.`

## 6) Session Rules

- Use one chat/session per feature, not one long mixed thread.
- Start new session when context drifts.
- Paste only relevant snippets, not whole files, when possible.

## 7) Live-Trading Safety Rule

For any change that affects execution of `--live`:
- do implementation on cheap model
- run one high-model audit pass only
- stop escalation after audit

This keeps quality checks where they matter without paying premium rates for routine edits.
