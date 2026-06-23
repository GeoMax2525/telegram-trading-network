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
import os
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
from bot.agents.gmgn_agent import gmgn_agent_loop
from bot.agents.tg_scraper import tg_scraper_loop
from bot.agents.laserstream import laserstream_loop
from bot.agents.pf_stream import pf_stream_loop
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
    """Sleeps until midnight UTC, then resets daily_loss_today_sol for all users
    and Claude's daily spend counter."""
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
        try:
            from bot.agents.claude_strategist import reset_daily_spend
            await reset_daily_spend()
            logger.info("Claude daily spend counter reset to $0")
        except Exception as exc:
            logger.error("Claude spend reset failed: %s", exc)


# ── Main coroutine ────────────────────────────────────────────────────────────

async def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set. Check your .env file.")

    db_type = "PostgreSQL" if DATABASE_URL.startswith("postgresql") else "SQLite"
    logger.info("Using %s (%s)", db_type, DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL)

    # Log Helius config so we know immediately if the API key is missing
    from bot.config import HELIUS_API_KEY, HELIUS_RPC_URL
    key_preview = HELIUS_API_KEY[:8] + "..." if len(HELIUS_API_KEY) > 8 else HELIUS_API_KEY
    is_demo = HELIUS_API_KEY == "demo"
    logger.info(
        "Helius: key=%s rpc=%s %s",
        key_preview,
        HELIUS_RPC_URL[:40] + "..." if len(HELIUS_RPC_URL) > 40 else HELIUS_RPC_URL,
        "WARNING: DEMO KEY -- wallet data will fail!" if is_demo else "OK Business plan",
    )
    if is_demo:
        logger.warning("HELIUS_API_KEY is 'demo' — set HELIUS_API_KEY env var for wallet/transaction data")

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

    from bot.signal_relay import set_relay_bot

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(keybot_router)
    # Echo intelligence + controls, HQ-only (reads the shared Data Hub).
    # MUST be registered before the main `router` — handlers.py has a catch-all
    # @router.message() that would otherwise swallow /ecco + /echo_* commands.
    from bot.echo.hq import router as echo_hq_router
    dp.include_router(echo_hq_router)
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
    # Migration Dip Buyer — own source, gated off by migration_sniper_enabled.
    from bot.agents.migration_sniper import migration_sniper_loop
    asyncio.create_task(migration_sniper_loop())
    logger.info("Migration sniper: queued (gated by migration_sniper_enabled)")

    # Health watchdog — pages admins if any critical loop stalls.
    from bot.health import watchdog_loop
    asyncio.create_task(watchdog_loop())
    logger.info("Health watchdog: queued")
    # mc_repair_loop REMOVED — was burning Helius credits on 66K+ old tokens.
    # MC is fetched fresh when needed: harvester discovery, scanner eval, paper monitor.

    # TG scraper — only starts if TG_SESSION_STRING is set
    import os as _os
    if _os.getenv("TG_SESSION_STRING"):
        asyncio.create_task(tg_scraper_loop())
        logger.info("TG scraper: queued for startup (session string found)")
    else:
        logger.info("TG scraper: skipped (TG_SESSION_STRING not set)")

    # LaserStream — real-time token detection via Helius WebSocket
    asyncio.create_task(laserstream_loop())
    logger.info("LaserStream: queued for startup")

    # PumpPortal stream — early Pump.fun/Bonk launch discovery.
    # Gated off by default (pf_stream_enabled=0); idles until /setparam'd on.
    asyncio.create_task(pf_stream_loop())
    logger.info("pf_stream: queued for startup (gated by pf_stream_enabled)")

    # Live mirror reconcile — retries any failed live sell so a real position
    # is never left holding. No-op unless live_trading_armed.
    from bot.live_mirror import live_mirror_reconcile_loop
    asyncio.create_task(live_mirror_reconcile_loop())
    logger.info("live_mirror: reconcile loop queued")

    # Regime tracker — Solana memecoin market state (HOT/NEUTRAL/COLD)
    # Polls every 5 min, drives downstream probe sizing decisions
    from bot.agents.regime_tracker import regime_tracker_loop
    asyncio.create_task(regime_tracker_loop())
    logger.info("Regime tracker: queued for startup")

    # Phase 5: Claude warm path — per-position reasoning every 5 min
    # Silently no-ops if ANTHROPIC_API_KEY isn't set
    from bot.agents.claude_warm import claude_warm_loop
    asyncio.create_task(claude_warm_loop())
    logger.info("Claude warm path: queued for startup")

    # Phase 5: Claude cold path — daily strategy review at 9 AM UTC
    from bot.agents.claude_cold import claude_cold_loop
    asyncio.create_task(claude_cold_loop())
    logger.info("Claude cold path: queued for startup")

    # Dashboard web server — serves /api/dashboard + static cyberpunk UI.
    # Skipped only if DISABLE_WEB=1 (e.g. local-only bot mode).
    if os.getenv("DISABLE_WEB", "0") != "1":
        from bot.web import run_server as _run_web
        asyncio.create_task(_run_web())
        logger.info("Dashboard web server: queued for startup")

    logger.info("Bot is starting. Press Ctrl+C to stop.")
    # Expose bot reference so background tasks (scanner, tg_scraper,
    # community feed) can post without needing to be passed bot explicitly.
    _state.bot = bot
    # Set bot reference for signal relay
    set_relay_bot(bot)

    # Echo — Trojan Horse signal bot (separate token, shared Data Hub). Runs
    # its own Bot/Dispatcher polling in parallel; no-op unless ECHO_BOT_TOKEN
    # is set, so the trading bot is unaffected.
    from bot.echo.app import start_echo
    asyncio.create_task(start_echo())
    logger.info("Echo: queued for startup (gated by ECHO_BOT_TOKEN)")

    # ECCO Telethon listener — user account that sees bot-posted calls the Bot
    # API can't. No-op unless ECCO_TG_SESSION + TG_API_ID/HASH are set.
    from bot.echo.tg_listener import ecco_tg_listener_loop
    asyncio.create_task(ecco_tg_listener_loop())
    logger.info("ECCO TG listener: queued (gated by ECCO_TG_SESSION)")

    # Register the "/" command menu so commands autocomplete in HQ + DM.
    try:
        from aiogram.types import BotCommand
        await bot.set_my_commands([
            # ── Core ──
            BotCommand(command="commands", description="📋 Full command list"),
            BotCommand(command="hub", description="Control panel"),
            BotCommand(command="status", description="Bot status snapshot"),
            BotCommand(command="dashboard", description="Command center"),
            BotCommand(command="weeklyreport", description="Last 7 days performance"),
            # ── Trade mode toggles ──
            BotCommand(command="autotrade", description="Switch trade mode on/off/paper/live"),
            BotCommand(command="4amonly", description="Only 4am trades (scanner off)"),
            BotCommand(command="scanneronly", description="Only scanner trades (4am off)"),
            BotCommand(command="alltrades", description="Enable both sources"),
            BotCommand(command="migration", description="Migration dip buyer on/off"),
            BotCommand(command="tradesoff", description="Disable all trading"),
            BotCommand(command="aimode", description="AI controls TP/SL/size"),
            BotCommand(command="manualmode", description="KeyBot static values override AI"),
            # ── Params ──
            BotCommand(command="params", description="All tunable params"),
            BotCommand(command="setparam", description="Set a param"),
            BotCommand(command="getparam", description="Read a param"),
            BotCommand(command="tradeparams", description="Per-pattern trade params"),
            # ── Reports & analysis ──
            BotCommand(command="claude_report", description="Full Claude analyst report"),
            BotCommand(command="claude_actions", description="Recent Claude decisions"),
            BotCommand(command="4amreport", description="4am call hit rates"),
            BotCommand(command="report", description="All-time learning report"),
            BotCommand(command="papertrades", description="Recent paper trades"),
            BotCommand(command="audit", description="Pre-live GO/NO-GO audit"),
            # ── Bundles & wallets ──
            BotCommand(command="sourcestats", description="4am vs scanner W/L + PnL"),
            BotCommand(command="4amattribution", description="Per-channel 4am edge"),
            BotCommand(command="dumpmoon", description="Moonbag validation (round-trips)"),
            BotCommand(command="livestatus", description="Live risk ledger + caps today"),
            BotCommand(command="health", description="Loop heartbeats / watchdog"),
            BotCommand(command="shadowstats", description="Exit-engine shadow match rate"),
            BotCommand(command="bundlers", description="Top bundle wallets by avg X"),
            BotCommand(command="wallets", description="Top wallet leaderboard"),
            # ── Token tools ──
            BotCommand(command="scan", description="Scan a token"),
            BotCommand(command="analyze", description="Deep token analysis"),
            BotCommand(command="pnl", description="PnL for a contract"),
            BotCommand(command="sl", description="Signal leaders"),
            BotCommand(command="lb", description="Top calls"),
            BotCommand(command="regime", description="Market regime"),
            # ── Diagnostics ──
            BotCommand(command="healthcheck", description="Agents + DB + wallet status"),
            BotCommand(command="forcecheck", description="Force paper-monitor tick now"),
            BotCommand(command="scannerwhy", description="Why scanner skipped a token"),
            # ── ECCO signal bot ──
            BotCommand(command="ecco", description="🐬 ECCO intelligence dashboard"),
            BotCommand(command="echo_check", description="ECCO sightings + score for a CA"),
            BotCommand(command="echo_health", description="ECCO per-group health"),
            BotCommand(command="echo_groups", description="ECCO groups + chat ids"),
            BotCommand(command="echo_stats", description="ECCO top groups + callers"),
            BotCommand(command="echo_signals", description="ECCO recent signals"),
            BotCommand(command="echo_referrals", description="ECCO referral leaderboard"),
            BotCommand(command="echo_rescore", description="ECCO recompute scores"),
            BotCommand(command="echo_reset_scores", description="ECCO wipe scores"),
        ])
        logger.info("Main bot: command menu registered")
    except Exception as exc:
        logger.warning("set_my_commands failed: %s", exc)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        # Clean up shared Helius session
        from bot.helius import close_session as _close_helius
        await _close_helius()
        logger.info("Bot stopped.")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(main())
