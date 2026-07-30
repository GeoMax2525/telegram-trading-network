"""Ecconos — the honest, present AI developer teammate for the REVOLT project.

Separate Telegram bot identity (ECCONOS_BOT_TOKEN), sharing the main
project's Postgres Data Hub (read via database/models.py — same functions
the MCP tools use, so nothing it says about the bot's numbers can drift from
what /hub or /sourcestats show). Distinct from:
  - The main trading bot (BOT_TOKEN) — executes trades, owns Callers HQ.
  - Echo/ECCO (ECHO_BOT_TOKEN) — the cross-group signal-consensus oracle,
    a completely different product with its own identity and purpose.

Ecconos's job: be genuinely present in the main Revolt community chat as a
transparent AI developer working on the project — never concealing that it's
an AI, never impersonating a human. It can discuss the bot's real
performance (via the shared Data Hub), propose ideas, and post updates
autonomously. It does NOT execute trades, and any treasury/new-product/
money-moving proposal it raises still needs the operator's explicit sign-off
in HQ, per the operator's own stated model — Ecconos proposes, the operator
decides. See bot/ecconos/app.py for the actual message-handling logic and
bot/ecconos/persona.py for the system prompt establishing that boundary.

Dormant unless ECCONOS_BOT_TOKEN is set — the trading bot is unaffected
either way, same pattern as Echo.
"""

import os

ECCONOS_BOT_TOKEN = os.getenv("ECCONOS_BOT_TOKEN", "").strip()

# Same admin list as the main bot — whoever administers Revolt admins Ecconos.
from bot.config import ADMIN_IDS as ECCONOS_ADMIN_IDS  # noqa: E402

# Cost + spam guardrails. Full autonomy on WHAT it says; these bound HOW
# OFTEN, so "always on, for fun" doesn't become an unbounded API bill or a
# chat nobody can read through. Tunable via env var, no code change needed.
ECCONOS_DAILY_BUDGET_USD = float(os.getenv("ECCONOS_DAILY_BUDGET_USD", "5.0"))
ECCONOS_RATE_LIMIT_PER_HOUR = int(os.getenv("ECCONOS_RATE_LIMIT_PER_HOUR", "30"))


def ecconos_enabled() -> bool:
    return bool(ECCONOS_BOT_TOKEN)
