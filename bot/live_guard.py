"""
live_guard.py — safety rails for REAL-money (live) swaps.

The manual Key Buy / Full Clip buttons execute real Jupiter swaps with real
SOL the moment they're tapped — regardless of trade_mode (which only governs
the paper scanner). Until the strategy proves positive expectancy, live
execution must be gated. This module is that gate.

Every live BUY must pass live_preflight() first:
  1. Master arm switch  — live_trading_armed must be 1 (DEFAULT 0 = locked).
  2. Per-trade size cap  — reject buys above live_max_trade_sol.
  3. Daily spend cap     — reject once today's live spend hits live_daily_spend_cap_sol.
  4. Daily count cap     — reject after live_max_buys_per_day buys.

After a successful buy, call record_live_buy(amount_sol) to update the day's
running totals. Counters reset on UTC date rollover. They're in-memory (a
restart resets them) — acceptable, since the arm switch + per-trade cap still
hard-bound risk on every single trade regardless of restarts.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# {"date": "YYYY-MM-DD", "spent_sol": float, "buys": int}
_today = {"date": "", "spent_sol": 0.0, "buys": 0}


def _roll_day() -> None:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if _today["date"] != today:
        _today["date"] = today
        _today["spent_sol"] = 0.0
        _today["buys"] = 0


async def live_preflight(amount_sol: float) -> tuple[bool, str]:
    """Gate a live BUY. Returns (allowed, human-readable reason)."""
    from database.models import get_params

    _roll_day()
    cfg = await get_params(
        "live_trading_armed",
        "live_max_trade_sol",
        "live_daily_spend_cap_sol",
        "live_max_buys_per_day",
    )

    if float(cfg.get("live_trading_armed") or 0) < 1.0:
        return False, (
            "Live trading is NOT armed (real-money safety lock). "
            "Arm it with /setparam live_trading_armed 1 once the strategy is validated."
        )

    max_trade = float(cfg.get("live_max_trade_sol") or 0.1)
    if amount_sol > max_trade:
        return False, (
            f"Buy {amount_sol:.3f} SOL exceeds the live per-trade cap of "
            f"{max_trade:.3f} SOL (/setparam live_max_trade_sol)."
        )

    spend_cap = float(cfg.get("live_daily_spend_cap_sol") or 0.5)
    if _today["spent_sol"] + amount_sol > spend_cap:
        return False, (
            f"Daily live spend cap hit: {_today['spent_sol']:.3f}+{amount_sol:.3f} "
            f"> {spend_cap:.3f} SOL (/setparam live_daily_spend_cap_sol)."
        )

    max_buys = int(float(cfg.get("live_max_buys_per_day") or 10))
    if _today["buys"] >= max_buys:
        return False, (
            f"Daily live trade count cap hit ({_today['buys']}/{max_buys}) "
            f"(/setparam live_max_buys_per_day)."
        )

    return True, "ok"


def record_live_buy(amount_sol: float) -> None:
    """Record a successful live buy against today's running totals."""
    _roll_day()
    _today["spent_sol"] += float(amount_sol or 0)
    _today["buys"] += 1
    logger.info(
        "live_guard: recorded live buy %.3f SOL — today spent=%.3f buys=%d",
        amount_sol, _today["spent_sol"], _today["buys"],
    )


def live_status() -> dict:
    """Snapshot of today's live exposure (for /state or diagnostics)."""
    _roll_day()
    return dict(_today)
