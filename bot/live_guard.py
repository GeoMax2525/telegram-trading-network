"""
live_guard.py — safety rails for REAL-money (live) swaps.

Every live BUY must pass live_preflight() first:
  1. Master arm switch  — live_trading_armed must be 1 (DEFAULT 0 = locked).
  2. Per-trade size cap  — reject buys above live_max_trade_sol.
  3. Daily LOSS breaker  — halt once today's realized live PnL <= -live_daily_loss_cap_sol.
  4. Daily spend cap     — reject once today's live spend hits live_daily_spend_cap_sol.
  5. Daily count cap     — reject after live_max_buys_per_day buys.

State is PERSISTED in the DailyRiskLedger table (keyed by UTC date) so the caps
and the loss circuit breaker SURVIVE restarts/redeploys. The old in-memory
counters reset on every deploy, which silently defeated the loss breaker — a
bad day could resume trading after a Railway restart. This version cannot.
"""

import logging

logger = logging.getLogger(__name__)


async def live_preflight(amount_sol: float) -> tuple[bool, str]:
    """Gate a live BUY. Returns (allowed, human-readable reason)."""
    from database.models import get_params, ledger_today

    cfg = await get_params(
        "live_trading_armed",
        "live_max_trade_sol",
        "live_daily_spend_cap_sol",
        "live_max_buys_per_day",
        "live_daily_loss_cap_sol",
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

    today = await ledger_today()

    # Daily LOSS circuit breaker — halt new buys once today's realized live PnL
    # breaches the cap. Persistent: survives restarts so a bad day stays halted.
    loss_cap = float(cfg.get("live_daily_loss_cap_sol") or 0.0)
    if loss_cap > 0 and today["realized_pnl"] <= -loss_cap:
        return False, (
            f"🛑 Daily LOSS circuit breaker tripped: today's realized "
            f"{today['realized_pnl']:.3f} SOL <= -{loss_cap:.3f}. Trading halted "
            f"until UTC rollover (/setparam live_daily_loss_cap_sol to adjust)."
        )

    spend_cap = float(cfg.get("live_daily_spend_cap_sol") or 0.5)
    if today["spent_sol"] + amount_sol > spend_cap:
        return False, (
            f"Daily live spend cap hit: {today['spent_sol']:.3f}+{amount_sol:.3f} "
            f"> {spend_cap:.3f} SOL (/setparam live_daily_spend_cap_sol)."
        )

    max_buys = int(float(cfg.get("live_max_buys_per_day") or 10))
    if today["buys"] >= max_buys:
        return False, (
            f"Daily live trade count cap hit ({today['buys']}/{max_buys}) "
            f"(/setparam live_max_buys_per_day)."
        )

    return True, "ok"


async def record_live_buy(amount_sol: float) -> None:
    """Record a successful live buy against today's PERSISTENT totals."""
    from database.models import ledger_record_buy, ledger_today
    await ledger_record_buy(amount_sol)
    t = await ledger_today()
    logger.info("live_guard: recorded live buy %.3f SOL — today spent=%.3f buys=%d",
                amount_sol, t["spent_sol"], t["buys"])


async def record_live_close(pnl_sol: float) -> None:
    """Record the realized PnL of a closed live position against today's
    PERSISTENT running net — drives the daily-loss circuit breaker."""
    from database.models import ledger_record_close, ledger_today
    await ledger_record_close(pnl_sol)
    t = await ledger_today()
    logger.info("live_guard: recorded live close %+.4f SOL — today realized=%+.4f",
                pnl_sol, t["realized_pnl"])


async def live_status() -> dict:
    """Snapshot of today's PERSISTENT live exposure (for /livestatus)."""
    from database.models import ledger_today
    return await ledger_today()
