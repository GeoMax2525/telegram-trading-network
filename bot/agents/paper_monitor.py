"""
paper_monitor.py — Paper Trade Monitor

Two jobs:
  1. Every 5 minutes: check open paper trades for TP/SL hits
  2. Every 10 minutes: track post-close performance for 24 hours
     Records mc at 1h, 6h, 24h after close.
     Flags sold_too_early (price went 50%+ higher) and
     sold_too_late (SL hit but recovered within 1h).
"""

import asyncio
import logging
from datetime import datetime, timedelta

from bot import state
from bot.config import CALLER_GROUP_ID
from bot.scanner import fetch_live_data
from database.models import (
    get_open_paper_trades,
    close_paper_trade,
    update_paper_trade_peak,
    get_recently_closed_paper_trades,
    update_paper_post_close,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL      = 300   # 5 minutes — open trade checks
POST_CLOSE_INTERVAL = 600  # 10 minutes — post-close tracking
STARTUP_DELAY      = 60


# ── Open trade monitoring ────────────────────────────────────────────────────

async def _check_open_trades(bot) -> None:
    trades = await get_open_paper_trades()
    if not trades:
        return

    for pt in trades:
        try:
            live = await fetch_live_data(pt.token_address)
            if not live:
                continue

            current_mc = live.get("market_cap") or 0
            if current_mc <= 0:
                continue

            entry_mc = pt.entry_mc or 1
            current_mult = current_mc / entry_mc if entry_mc > 0 else 1.0
            peak_mc = max(pt.peak_mc or 0, current_mc)
            peak_mult = max(pt.peak_multiple or 1.0, current_mult)

            await update_paper_trade_peak(pt.id, current_mc, peak_mc, peak_mult)

            name = (pt.token_name or "Unknown").replace("_", " ")
            sol = pt.paper_sol_spent

            # Check TP
            if current_mult >= pt.take_profit_x:
                pnl = round(sol * (current_mult - 1), 4)
                await close_paper_trade(pt.id, "tp_hit", pnl, peak_mc, peak_mult)
                state.paper_balance += sol + pnl
                logger.info("Paper: TP hit %s — %.1fx +%.4f SOL bal=%.4f",
                            name, current_mult, pnl, state.paper_balance)
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"✅ PAPER TRADE WIN",
                        f"🪙 {name} | {current_mult:.1f}x | +{pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {state.paper_balance:.2f} SOL",
                    ]))
                except Exception:
                    pass
                continue

            # Check SL
            sl_threshold = 1.0 - (pt.stop_loss_pct / 100.0)
            if current_mult <= sl_threshold:
                pnl = round(-sol * (1.0 - current_mult), 4)
                await close_paper_trade(pt.id, "sl_hit", pnl, peak_mc, peak_mult)
                state.paper_balance += sol + pnl
                logger.info("Paper: SL hit %s — %.2fx %.4f SOL bal=%.4f",
                            name, current_mult, pnl, state.paper_balance)
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"❌ PAPER TRADE LOSS",
                        f"🪙 {name} | SL hit | {pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {state.paper_balance:.2f} SOL",
                    ]))
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Paper monitor error for %s: %s", pt.token_address[:12], exc)


# ── Post-close tracking ─────────────────────────────────────────────────────

async def _track_post_close() -> None:
    """
    For recently closed trades (last 25h), fetch current MC and record
    performance at 1h, 6h, 24h intervals after close.
    """
    trades = await get_recently_closed_paper_trades(hours=25)
    if not trades:
        return

    now = datetime.utcnow()

    for pt in trades:
        try:
            if not pt.closed_at:
                continue

            hours_since_close = (now - pt.closed_at).total_seconds() / 3600
            entry_mc = pt.entry_mc or 1
            close_mc = pt.peak_mc or entry_mc  # approximate close MC

            live = await fetch_live_data(pt.token_address)
            if not live:
                continue

            current_mc = live.get("market_cap") or 0
            if current_mc <= 0:
                continue

            # Update peak after close
            peak_after = max(pt.peak_after_close or 0, current_mc)

            # Fill in time-based snapshots
            mc_1h = pt.mc_1h_after
            mc_6h = pt.mc_6h_after
            mc_24h = pt.mc_24h_after

            if mc_1h is None and hours_since_close >= 1:
                mc_1h = current_mc
            if mc_6h is None and hours_since_close >= 6:
                mc_6h = current_mc
            if mc_24h is None and hours_since_close >= 24:
                mc_24h = current_mc

            # Determine sold_too_early: price went 50%+ higher after close
            close_mult = close_mc / entry_mc if entry_mc > 0 else 1.0
            peak_after_mult = peak_after / entry_mc if entry_mc > 0 else 1.0
            sold_too_early = peak_after_mult > close_mult * 1.5 if close_mult > 0 else False

            # Determine sold_too_late: SL hit but recovered within 1h
            sold_too_late = False
            if pt.close_reason == "sl_hit" and mc_1h is not None:
                # If 1h after SL, MC recovered above entry
                if mc_1h > entry_mc:
                    sold_too_late = True

            await update_paper_post_close(
                trade_id=pt.id, mc_1h=mc_1h, mc_6h=mc_6h, mc_24h=mc_24h,
                peak_after=peak_after,
                sold_too_early=sold_too_early, sold_too_late=sold_too_late,
            )

            name = (pt.token_name or "?")[:15]
            logger.debug(
                "Post-close %s: %.1fh since close, peak_after=%.0f, early=%s late=%s",
                name, hours_since_close, peak_after, sold_too_early, sold_too_late,
            )

        except Exception as exc:
            logger.debug("Post-close tracking error for %s: %s", pt.token_address[:12], exc)


# ── Background loops ─────────────────────────────────────────────────────────

async def paper_monitor_loop(bot) -> None:
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Paper monitor started — open trades every %ds, post-close every %ds",
                POLL_INTERVAL, POST_CLOSE_INTERVAL)

    last_post_close = datetime.utcnow()

    while True:
        try:
            await _check_open_trades(bot)

            # Post-close tracking every 10 minutes
            if (datetime.utcnow() - last_post_close).total_seconds() >= POST_CLOSE_INTERVAL:
                await _track_post_close()
                last_post_close = datetime.utcnow()

        except Exception as exc:
            logger.error("Paper monitor loop error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
