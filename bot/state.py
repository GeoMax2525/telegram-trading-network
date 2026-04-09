"""
bot/state.py — Shared in-memory state for cross-module communication.

Modules that read/write:
  handlers.py      — reads/writes autotrade_enabled via /hub toggle + /autotrade cmd
  scanner_agent.py — reads autotrade_enabled; writes scanner_* live stats
"""

from datetime import datetime

# ── Autotrade toggle ──────────────────────────────────────────────────────────
autotrade_enabled:        bool            = False

# ── Scanner Agent 4 live stats (updated each tick) ───────────────────────────
scanner_last_run:         datetime | None = None
scanner_candidates_today: int             = 0   # reset at midnight UTC by daily_loss_reset_loop
scanner_status:           str             = "idle"   # "running" | "idle" | "disabled"

# ── Pending candidates queued for Agent 5 ────────────────────────────────────
# Each entry: {mint, name, symbol, source, ai_score, match_score, mcap, liquidity}
pending_candidates:       list[dict]      = []
