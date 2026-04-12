"""
main.py — Entry point for the AI Trading Network Bot.

Startup sequence:
  1. Load config from .env
  2. Initialise the SQLite database
  3. Register all routers/handlers
  4. Start background peak-tracker task
  5. Start polling Telegram for updates
"""

import asyncio
import logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

from bot.config import BOT_TOKEN, DATABASE_URL
from bot.handlers import router
from bot.keybot import router as keybot_router, position_monitor_loop
from bot.scanner import fetch_live_data
from bot.agents.harvester import harvester_loop
from bot.agents.wallet_analyst import wallet_analyst_loop
from bot.agents.pattern_engine import pattern_engine_loop
from bot.agents.scanner_agent import scanner_agent_loop
from bot.agents.learning_loop import learning_loop
from bot.agents.paper_monitor import paper_monitor_loop
from bot.agents.mc_repair import mc_repair_loop
from bot.agents.gmgn_agent import gmgn_agent_loop
from database.models import init_db, init_agent_params, get_param, compute_paper_balance, get_open_scans, update_scan_pnl, close_old_scans, reset_all_daily_losses, seed_ai_trade_params

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Background task ───────────────────────────────────────────────────────────

async def peak_tracker_loop() -> None:
    """
    Every 5 minutes:
    - Fetches current MC for all open scans under 7 days old
    - Updates peak MC if higher
    - Auto-closes scans older than 7 days (locks in peak X forever)
    """
    await asyncio.sleep(30)   # brief startup delay
    while True:
        try:
            # Close expired scans first (7 days)
            closed = await close_old_scans()
            if closed:
                logger.info("Peak tracker: closed %d expired scans", closed)

            # Update peaks for remaining open scans; check rug conditions
            open_scans = await get_open_scans()
            for scan in open_scans:
                live = await fetch_live_data(scan.contract_address)
                if not live:
                    continue

                mc  = live["market_cap"]
                liq = live["liquidity_usd"]

                # Rug: MC collapsed 80%+ below entry
                if mc and scan.entry_price and mc < scan.entry_price * 0.20:
                    await update_scan_pnl(scan.id, mc, close=True, close_reason="rug_mc")
                    logger.info("Rug (MC) detected: %s", scan.token_name)
                # Rug: liquidity drained below $1000
                elif liq < 1000:
                    await update_scan_pnl(scan.id, mc or scan.entry_price, close=True, close_reason="rug_liquidity")
                    logger.info("Rug (liquidity) detected: %s", scan.token_name)
                else:
                    await update_scan_pnl(scan.id, mc)

            if open_scans:
                logger.info("Peak tracker: updated %d open scans", len(open_scans))
        except Exception as e:
            logger.error("Peak tracker error: %s", e)

        await asyncio.sleep(300)   # 5 minutes


# ── Midnight daily-loss reset ─────────────────────────────────────────────────

async def daily_loss_reset_loop() -> None:
    """Sleeps until midnight UTC, then resets daily_loss_today_sol for all users."""
    while True:
        now            = datetime.utcnow()
        next_midnight  = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_secs = (next_midnight - now).total_seconds()
        logger.info("Daily loss reset: sleeping %.0f s until midnight UTC", sleep_secs)
        await asyncio.sleep(sleep_secs)
        try:
            count = await reset_all_daily_losses()
            logger.info("Daily loss reset: cleared losses for %d user(s)", count)
        except Exception as exc:
            logger.error("Daily loss reset failed: %s", exc)


# ── Main coroutine ────────────────────────────────────────────────────────────

async def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set. Check your .env file.")

    db_type = "PostgreSQL" if DATABASE_URL.startswith("postgresql") else "SQLite"
    logger.info("Using %s (%s)", db_type, DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL)
    await init_db()
    added = await init_agent_params()
    if added:
        logger.info("Initialized %d agent params with defaults", added)

    seeded = await seed_ai_trade_params()
    if seeded:
        logger.info("Seeded %d ai_trade_params rows", seeded)

    # Restore trade mode from DB (survives restarts)
    from bot import state as _state
    mode_val = await get_param("trade_mode")
    _state.trade_mode = {0: "off", 1: "paper", 2: "live"}.get(int(mode_val), "off")
    _state.autotrade_enabled = (_state.trade_mode == "live")

    # Restore paper balance from DB (computed, not cached)
    starting = await get_param("paper_starting_balance")
    _state.PAPER_STARTING_BALANCE = starting
    _state.paper_balance = await compute_paper_balance(starting)
    logger.info("Restored: trade_mode=%s paper_balance=%.4f SOL", _state.trade_mode, _state.paper_balance)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(keybot_router)
    dp.include_router(router)

    # Start background tasks
    asyncio.create_task(peak_tracker_loop())
    asyncio.create_task(position_monitor_loop(bot))
    asyncio.create_task(daily_loss_reset_loop())
    asyncio.create_task(harvester_loop())
    asyncio.create_task(wallet_analyst_loop())
    asyncio.create_task(pattern_engine_loop())
    asyncio.create_task(scanner_agent_loop())
    asyncio.create_task(learning_loop(bot))
    asyncio.create_task(paper_monitor_loop(bot))
    asyncio.create_task(gmgn_agent_loop())
    asyncio.create_task(mc_repair_loop())

    logger.info("Bot is starting. Press Ctrl+C to stop.")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logger.info("Bot stopped.")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(main())
