"""
bot/state.py — Shared in-memory state for cross-module communication.

Modules that read/write:
  handlers.py      — reads/writes autotrade_enabled via /hub toggle + /autotrade cmd
  scanner_agent.py — writes scanner_* live stats (always runs; autotrade controls execution only)
"""

from datetime import datetime

# ── Trade mode: "off" / "paper" / "live" ─────────────────────────────────────
trade_mode:               str             = "off"
autotrade_enabled:        bool            = False   # legacy compat — True when mode is "live"

# ── Scanner Agent 4 live stats (updated each tick) ───────────────────────────
scanner_last_run:         datetime | None = None
scanner_candidates_today: int             = 0   # reset at midnight UTC by daily_loss_reset_loop
scanner_status:           str             = "idle"   # "running" | "idle"

# ── Pending candidates queued for Agent 5 ────────────────────────────────────
# Each entry: {mint, name, symbol, source, ai_score, match_score, mcap, liquidity}
pending_candidates:       list[dict]      = []

# ── Learning Loop Agent 6 live stats ────────────────────────────────────────
learning_loop_last_run:      datetime | None = None
learning_loop_last_analyzed: int             = 0
learning_loop_total_closed:  int             = 0
learning_loop_weights:       dict            = {}
learning_loop_last_change:   str             = "No changes yet"

# ── Dynamic parameters (adjusted by Agent 6) ───────────────────────────────
confidence_thresholds:       dict            = {"execute_full": 80, "execute_half": 70, "monitor": 60}
market_regime:               str             = "NEUTRAL"   # GOOD / NEUTRAL / BAD
sol_24h_change:              float           = 0.0

# ── Harvester Agent 1 live stats ────────────────────────────────────────────
harvester_ws_connected:      bool            = False
harvester_ws_source:         str             = "none"  # pumpdev / pumpportal / helius / none
harvester_ws_tokens_today:   int             = 0
harvester_poll_tokens_today: int             = 0
harvester_pumpswap_today:    int             = 0
