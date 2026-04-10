"""
paper_monitor.py — Paper Trade Monitor

Runs every 5 minutes. For each open paper trade:
  - Fetches current MC from DexScreener
  - Updates peak MC / peak multiple
  - Checks TP hit (current MC >= entry_mc * tp_x)
  - Checks SL hit (current MC <= entry_mc * (1 - sl_pct/100))
  - Closes trade and posts result to Callers HQ
"""

import asyncio
import logging

from bot import state
from bot.config import CALLER_GROUP_ID
from bot.scanner import fetch_live_data
from database.models import (
    get_open_paper_trades,
    close_paper_trade,
    update_paper_trade_peak,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 300   # 5 minutes
STARTUP_DELAY = 60


async def _check_paper_trades(bot) -> None:
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

            # Update peak
            await update_paper_trade_peak(pt.id, current_mc, peak_mc, peak_mult)

            name = (pt.token_name or "Unknown").replace("_", " ")
            sol = pt.paper_sol_spent

            # Check TP
            if current_mult >= pt.take_profit_x:
                pnl = round(sol * (current_mult - 1), 4)
                await close_paper_trade(pt.id, "tp_hit", pnl, peak_mc, peak_mult)
                state.paper_balance += sol + pnl  # return spent + profit
                logger.info("Paper: TP hit %s — %.1fx +%.4f SOL bal=%.4f",
                            name, current_mult, pnl, state.paper_balance)
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"✅ PAPER TRADE WIN",
                        f"🪙 {name} | {current_mult:.1f}x | +{pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {state.paper_balance:.4f} SOL",
                    ]))
                except Exception:
                    pass
                continue

            # Check SL
            sl_threshold = 1.0 - (pt.stop_loss_pct / 100.0)
            if current_mult <= sl_threshold:
                pnl = round(-sol * (1.0 - current_mult), 4)
                await close_paper_trade(pt.id, "sl_hit", pnl, peak_mc, peak_mult)
                state.paper_balance += sol + pnl  # return remaining
                logger.info("Paper: SL hit %s — %.2fx %.4f SOL bal=%.4f",
                            name, current_mult, pnl, state.paper_balance)
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"❌ PAPER TRADE LOSS",
                        f"🪙 {name} | SL hit | {pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {state.paper_balance:.4f} SOL",
                    ]))
                except Exception:
                    pass

        except Exception as exc:
            logger.error("Paper monitor error for %s: %s", pt.token_address[:12], exc)


async def paper_monitor_loop(bot) -> None:
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Paper trade monitor started — checking every %ds", POLL_INTERVAL)
    while True:
        try:
            await _check_paper_trades(bot)
        except Exception as exc:
            logger.error("Paper monitor loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
