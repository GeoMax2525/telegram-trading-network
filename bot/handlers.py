import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta

import aiohttp

from aiogram import Router, Bot, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.config import MAIN_GROUP_ID, ADMIN_IDS
from bot import state
from bot.scanner import scan_token, fetch_current_market_cap, fetch_live_data, fetch_sol_price_usd, fetch_token_data, parse_token_metrics
from bot.keyboards import trade_card_keyboard, pnl_keyboard, top_calls_keyboard
from bot.wallet import get_wallet_address, get_token_holding
from database.models import (
    log_scan, get_leaderboard, get_break_evens_count, add_caller,
    get_open_scans, update_scan_pnl, get_scan_by_address, close_old_scans,
    get_signal_leaders, get_top_calls, get_top_calls_stats,
    get_any_open_position_by_token, get_hub_stats, get_top_wallets,
    get_candidate_stats_today, get_all_trade_params,
    get_chart_pattern_stats_today, get_chart_pattern_win_rates,
    get_pumpfun_count_today, get_pumpswap_count_today,
    token_exists, save_token, get_token_by_mint,
    get_tier_wallets, get_pattern_by_type, has_caller_scanned,
    upsert_wallet, get_paper_trade_stats,
)
from bot.agents.confidence_engine import score_candidate
from bot.agents.chart_detector import analyze_chart
from bot.agents.wallet_analyst import _get_early_buyers, _score_wallet

logger = logging.getLogger(__name__)
router = Router()

CALLER_GROUP_ID = -1003852140576


# Users waiting to input an address for /analyze (chat_id -> True)
_analyze_waiting: set[int] = set()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _verdict_emoji(verdict: str) -> str:
    return {
        "STRONG BUY": "🟢",
        "PROMISING":  "🟡",
        "RISKY":      "🟠",
        "AVOID":      "🔴",
    }.get(verdict, "⚪")


def _format_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.2f}"


def _format_price(price: float) -> str:
    if price >= 1:
        return f"${price:.2f}"
    if price >= 0.01:
        return f"${price:.4f}"
    # Strip trailing zeros but keep all significant digits
    return f"${price:.10f}".rstrip("0")


def build_trade_card(data: dict) -> str:
    emoji = _verdict_emoji(data["verdict"])
    change_sign = "+" if data["price_change_24h"] >= 0 else ""

    filled = int(data["total"] / 10)
    score_bar = "█" * filled + "░" * (10 - filled)

    lines = [
        f"{'─' * 34}",
        f"🤖 *AI TRADE SIGNAL*",
        f"{'─' * 34}",
        f"",
        f"🪙 *{data['name']}* (${data['symbol']})",
        f"🔗 `{data['address']}`",
        f"",
        f"💵 Price:       `{_format_price(data['price_usd'])}`",
        f"📊 Market Cap:  `{_format_usd(data['market_cap'])}`",
        f"💧 Liquidity:   `{_format_usd(data['liquidity_usd'])}`",
        f"📈 Volume 24h:  `{_format_usd(data['volume_24h'])}`",
        f"🕯 Change 24h:  `{change_sign}{data['price_change_24h']:.1f}%`",
        f"",
        f"{'─' * 34}",
        f"🧠 *AI SCORE*",
        f"",
        f"`[{score_bar}]` *{data['total']}/100*",
        f"",
        f"  • Liquidity:            {data['components']['liquidity']:.1f}/20",
        f"  • Volume:               {data['components']['volume']:.1f}/20",
        f"  • Momentum:             {data['components']['momentum']:.1f}/20",
        f"  • Holder Distribution:  {data['components']['holder_distribution']:.1f}/15",
        f"  • Contract Safety:      {data['components']['contract_safety']:.1f}/15",
        f"  • Deployer Reputation:  {data['components']['deployer_reputation']:.1f}/10",
        f"",
        f"{'─' * 34}",
        f"📋 Verdict: {emoji} *{data['verdict']}*",
        f"{'─' * 34}",
        f"",
        f"_Scanned at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_",
    ]
    return "\n".join(lines)


def _build_position_section(
    holding: dict,
    price_usd: float,
    sol_price_usd: float,
    current_mc: float,
    pos,                 # Position ORM object or None
) -> str:
    """Returns the YOUR POSITION block to append to a Trade Card, or '' if nothing to show."""
    balance     = holding["balance"]
    pct_supply  = holding["pct_supply"]
    value_usd   = balance * price_usd if price_usd else 0.0
    value_sol   = value_usd / sol_price_usd if sol_price_usd else 0.0

    lines = [
        f"{'─' * 34}",
        f"💼 *YOUR POSITION*",
        f"🪙 Hold: `{balance:,.0f} tokens ({pct_supply:.3f}% supply)`",
        f"💵 Value: `{_format_usd(value_usd)}` / `{value_sol:.4f} SOL`",
    ]

    if pos and pos.entry_mc and pos.entry_mc > 0 and current_mc > 0:
        pct_change = (current_mc - pos.entry_mc) / pos.entry_mc * 100
        sign       = "+" if pct_change >= 0 else ""
        lines.append(
            f"📈 Avg Entry: `{_format_usd(pos.entry_mc)} MC`"
            f" | Now: `{_format_usd(current_mc)} MC`"
            f" | `{sign}{pct_change:.1f}%`"
        )

    return "\n".join(lines)


async def _build_card_text(address: str) -> tuple[str | None, dict | None]:
    """
    Fetches live token data + wallet position concurrently.
    Returns (card_text, data) where card_text includes the position block if held,
    or (None, None) if the token could not be fetched.
    """
    data = await scan_token(address)
    if data is None:
        return None, None

    card_text = build_trade_card(data)

    wallet_address = get_wallet_address()
    if wallet_address:
        holding, sol_price_usd, pos = await asyncio.gather(
            get_token_holding(wallet_address, address),
            fetch_sol_price_usd(),
            get_any_open_position_by_token(address),
            return_exceptions=True,
        )
        if isinstance(holding, dict) and holding.get("balance", 0) > 0:
            price_usd  = data.get("price_usd", 0) or 0
            current_mc = data.get("market_cap", 0) or 0
            sol_price  = sol_price_usd if isinstance(sol_price_usd, float) else 0.0
            pos_obj    = pos if not isinstance(pos, Exception) else None
            card_text  = card_text + "\n" + _build_position_section(
                holding, price_usd, sol_price, current_mc, pos_obj
            )

    return card_text, data


async def _do_scan(message: Message, address: str) -> None:
    loading_msg = await message.reply("🔍 Scanning token… please wait.")

    card_text, data = await _build_card_text(address)

    if data is None:
        await loading_msg.edit_text(
            "❌ Could not find token data for that address.\n"
            "Make sure it's a valid Solana token listed on DexScreener."
        )
        return

    keyboard = trade_card_keyboard(
        dex_url=data.get("dex_url", ""),
        contract_address=address,
    )

    await loading_msg.delete()
    await message.answer(card_text, parse_mode="Markdown", reply_markup=keyboard)

    entry_mc  = data["market_cap"]   if data.get("market_cap",   0) > 0 else None
    entry_liq = data["liquidity_usd"] if data.get("liquidity_usd", 0) > 0 else None
    logger.info("SCAN entry_mc=%s entry_liq=%s token=%s", entry_mc, entry_liq, data["name"])

    await log_scan(
        contract_address=address,
        token_name=data["name"],
        ai_score=data["total"],
        scanned_by=message.from_user.username or str(message.from_user.id),
        group_id=message.chat.id,
        entry_price=entry_mc,
        entry_liquidity=entry_liq,
    )


# ── /hub — Live Dashboard ─────────────────────────────────────────────────────


def _mode_label() -> str:
    m = state.trade_mode
    if m == "live":
        return "🟢 LIVE"
    if m == "paper":
        return "📋 PAPER"
    return "🔴 OFF"


def _hub_keyboard(autotrade: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    # Row 1: Paper trading toggle
    if state.trade_mode == "paper":
        paper_label = "📋 Paper Trading: ON ✅"
    else:
        paper_label = "📋 Paper Trading: OFF"
    builder.row(InlineKeyboardButton(text=paper_label, callback_data="hub:toggle_paper"))

    # Row 2: Live trading — locked
    builder.row(InlineKeyboardButton(text="🟢 Live Trading: LOCKED 🔒", callback_data="hub:live_locked"))

    # Row 3: Navigation
    builder.row(
        InlineKeyboardButton(text="🔄 Refresh",       callback_data="hub:refresh"),
        InlineKeyboardButton(text="🤖 Agent Details", callback_data="hub:agents"),
        InlineKeyboardButton(text="👛 Top Wallets",   callback_data="hub:wallets"),
    )

    # Row 4: Tools
    builder.row(
        InlineKeyboardButton(text="🔍 Analyze Token", callback_data="hub:analyze"),
        InlineKeyboardButton(text="📋 Paper Trades",  callback_data="hub:papertrades"),
        InlineKeyboardButton(text="⚙️ Settings",      callback_data="hub:settings"),
    )
    return builder.as_markup()


def _pattern_engine_line(last_run, total: int, winners: int, rugs: int) -> str:
    if last_run is None:
        return "✅ Pattern Engine — waiting for first run..."
    elapsed_min = int((datetime.utcnow() - last_run.run_at).total_seconds() / 60)
    if elapsed_min < 60:
        age = f"{elapsed_min}min ago"
    else:
        age = f"{elapsed_min // 60}h ago"
    return (
        f"✅ Pattern Engine — {total} active patterns "
        f"({winners} winner / {rugs} rug) | last run {age}"
    )


async def _chart_detector_line() -> str:
    cs = await get_chart_pattern_stats_today()
    if cs["detected"] == 0:
        return "✅ Chart Detector — waiting for candidates..."
    return (
        f"✅ Chart Detector — "
        f"{cs['detected']} detected | "
        f"{cs['confirmed']} confirmed | "
        f"{cs['rejected']} rejected"
    )


async def _learning_loop_line() -> str:
    remaining = max(0, 10 - (state.learning_loop_total_closed - state.learning_loop_last_analyzed))
    w = state.learning_loop_weights
    regime = getattr(state, "market_regime", "NEUTRAL")
    regime_icon = {"GOOD": "🟢", "NEUTRAL": "🟡", "BAD": "🔴"}.get(regime, "⚪")
    thresholds = getattr(state, "confidence_thresholds", {})
    t_full = thresholds.get("execute_full", 80)
    t_half = thresholds.get("execute_half", 70)
    sol_chg = getattr(state, "sol_24h_change", 0.0)
    last_change = getattr(state, "learning_loop_last_change", "No changes yet")

    if state.learning_loop_last_run is None and not w:
        return f"✅ Learning Loop — {regime_icon} {regime} | waiting for trades..."

    if state.learning_loop_last_run is None:
        return f"✅ Learning Loop — {regime_icon} {regime} | {remaining} trades to next adjust"

    elapsed_min = int((datetime.utcnow() - state.learning_loop_last_run).total_seconds() / 60)
    age = f"{elapsed_min}min ago" if elapsed_min < 60 else f"{elapsed_min // 60}h ago"

    w_str = " ".join(f"{k[:3]}={v:.0%}" for k, v in sorted(w.items(), key=lambda x: -x[1]))

    params = await get_all_trade_params()
    ai_str = f" | {sum(1 for p in params if p.sample_size >= 10)}/{len(params)} AI" if params else ""

    # Truncate last change for display
    change_short = last_change[:60] + "..." if len(last_change) > 60 else last_change

    return (
        f"✅ Learning Loop — {regime_icon} {regime} | SOL {sol_chg:+.1f}% | last {age}{ai_str}\n"
        f"     Thresh: full={t_full} half={t_half} | {w_str}\n"
        f"     Last: {change_short}"
    )


async def _build_hub_text(autotrade: bool) -> str:
    stats = await get_hub_stats()

    scans_today  = stats["scans_today"]
    trades_today = stats["trades_today"]
    today_pnl    = stats["today_pnl"]
    alltime_pnl  = stats["alltime_pnl"]
    win_rate     = stats["win_rate"]
    total_closed = stats["total_closed"]
    recent       = stats["recent_trades"]
    token_count         = stats["token_count"]
    last_harvest        = stats["last_harvest"]
    wallet_total        = stats["wallet_total"]
    wallet_tier1        = stats["wallet_tier1"]
    wallet_tier2        = stats["wallet_tier2"]
    last_analyst        = stats["last_analyst"]
    pattern_total       = stats["pattern_total"]
    pattern_winners     = stats["pattern_winners"]
    pattern_rugs        = stats["pattern_rugs"]
    last_pattern_engine = stats["last_pattern_engine"]

    at_status = "ON 🟢" if autotrade else "OFF 🔴"

    # Scanner (Agent 4) line — always running
    if state.scanner_last_run is None:
        scanner_line = "✅ Scanner — always on | waiting for first run..."
    else:
        elapsed_s = int((datetime.utcnow() - state.scanner_last_run).total_seconds())
        scanner_line = (
            f"✅ Scanner — always on | {state.scanner_candidates_today} logged today "
            f"| last scan {elapsed_s}s ago"
        )

    # Harvester last-run label
    pump_today = await get_pumpfun_count_today()
    pumpswap_today = await get_pumpswap_count_today()
    ws_connected = state.harvester_ws_connected
    ws_source = state.harvester_ws_source
    ws_icon = "🟢" if ws_connected else "🔴"
    ws_label = ws_source if ws_connected else "Polling"
    ws_count = state.harvester_ws_tokens_today
    ps_count = state.harvester_pumpswap_today
    if last_harvest is None:
        harvest_line = f"✅ Harvester — {ws_icon} {ws_label} | waiting for first run..."
    else:
        elapsed_min = int(
            (datetime.utcnow() - last_harvest.run_at).total_seconds() / 60
        )
        harvest_line = (
            f"✅ Harvester — {ws_icon} {ws_label} | {token_count} tokens | "
            f"pump: {pump_today} | pumpswap: {pumpswap_today} | last {elapsed_min}min ago"
        )

    # Wallet Analyst last-run label
    if last_analyst is None:
        analyst_line = "✅ Wallet Analyst — waiting for first run..."
    else:
        elapsed_min = int(
            (datetime.utcnow() - last_analyst.run_at).total_seconds() / 60
        )
        analyst_line = (
            f"✅ Wallet Analyst — {wallet_total} wallets scored "
            f"({wallet_tier1} T1 / {wallet_tier2} T2) "
            f"| last run {elapsed_min}min ago"
        )

    ce_stats = await get_candidate_stats_today()
    ce_icon = "✅" if autotrade else "🔧"
    ce_line = (
        f"{ce_icon} Confidence Engine — "
        f"{ce_stats['scored_today']} scored | "
        f"{ce_stats['high_conf']} high conf | "
        f"{ce_stats['executed_today']} executed"
    )
    mode_display = _mode_label()
    paper_stats = await get_paper_trade_stats()

    lines = [
        "🔑 *LOWKEY ALPHA HUB*",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        "🤖 *AGENTS*",
        scanner_line,
        harvest_line,
        analyst_line,
        _pattern_engine_line(last_pattern_engine, pattern_total, pattern_winners, pattern_rugs),
        ce_line,
        await _learning_loop_line(),
        await _chart_detector_line(),
        f"⚡ Trade Mode: *{mode_display}*",
        "",
        "📊 *PERFORMANCE*",
        f"Today: `{today_pnl:+.4f} SOL` | All Time: `{alltime_pnl:+.4f} SOL`",
        f"Win Rate: `{win_rate}%` | Closed Trades: `{total_closed}`",
        "",
        f"📋 *PAPER TRADING* ({paper_stats['open_count']} open)",
        f"Today: `{paper_stats['today_count']}` trades | `{paper_stats['today_pnl']:+.4f} SOL`",
        f"All time: `{paper_stats['win_rate']}%` WR | `{paper_stats['total_pnl']:+.4f} SOL`",
        "",
        "🔥 *TOP WALLETS*",
    ]

    top_wallets = await get_top_wallets(limit=3)
    if not top_wallets:
        lines.append("_No wallets scored yet — Agent 2 is analyzing..._")
    else:
        for i, w in enumerate(top_wallets, 1):
            short = f"{w.address[:4]}...{w.address[-4:]}"
            lines.append(
                f"#{i} `{short}` | Score: {w.score:.0f} | "
                f"Win: {w.win_rate * 100:.0f}% | Avg: {w.avg_multiple:.1f}x | Tier {w.tier}"
            )

    lines += [
        "",
        "📈 *RECENT AUTO-TRADES*",
    ]

    if not recent:
        lines.append("_No trades yet_")
    else:
        for pos in recent:
            name = (pos.token_name or "Unknown").replace("_", "\\_")
            if pos.status == "closed" and pos.pnl_sol is not None:
                icon   = "✅" if pos.pnl_sol >= 0 else "❌"
                reason = {
                    "tp_hit": "TP hit",
                    "sl_hit": "SL hit",
                    "manual": "manual close",
                }.get(pos.close_reason or "", pos.close_reason or "closed")
                mc_str  = _format_usd(pos.entry_mc) if pos.entry_mc else "?"
                pnl_str = f"{pos.pnl_sol:+.4f} SOL"
                lines.append(f"{icon} `{name}` — {reason}, `{pnl_str}` @ {mc_str} MC")
            else:
                mc_str = _format_usd(pos.entry_mc) if pos.entry_mc else "?"
                lines.append(f"🟡 `{name}` — open @ {mc_str} MC")

    lines.append("")
    lines.append(f"_Updated: {datetime.utcnow().strftime('%H:%M:%S')} UTC_")

    return "\n".join(lines)


@router.message(Command("hub"))
async def cmd_hub(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/hub` is only available in Callers HQ.")
        return
    text = await _build_hub_text(state.autotrade_enabled)
    await message.reply(text, parse_mode="Markdown", reply_markup=_hub_keyboard(state.autotrade_enabled))


@router.callback_query(lambda c: c.data and c.data.startswith("hub:"))
async def cb_hub(callback: CallbackQuery):
    action = callback.data.split(":", 1)[1]

    if action == "refresh":
        await callback.answer("🔄 Refreshing…")
        text = await _build_hub_text(state.autotrade_enabled)
        try:
            await callback.message.edit_text(
                text, parse_mode="Markdown",
                reply_markup=_hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass  # unchanged

    elif action == "toggle_paper":
        if state.trade_mode == "paper":
            state.trade_mode = "off"
            state.autotrade_enabled = False
            await callback.answer("📋 Paper trading OFF")
        else:
            state.trade_mode = "paper"
            state.autotrade_enabled = False
            await callback.answer("📋 Paper trading ON ✅")
        text = await _build_hub_text(state.autotrade_enabled)
        await callback.message.edit_text(
            text, parse_mode="Markdown",
            reply_markup=_hub_keyboard(state.autotrade_enabled),
        )

    elif action == "live_locked":
        await callback.answer(
            "🔒 Live trading will be enabled after paper trading validation.\n"
            "Use /autotrade live when ready.",
            show_alert=True,
        )

    elif action == "agents":
        await callback.answer(
            "🔧 Agent Details: most agents are still being built.", show_alert=True
        )

    elif action == "wallets":
        wallets = await get_top_wallets(limit=10)
        if not wallets:
            await callback.answer(
                "No wallets scored yet — Agent 2 is still running.", show_alert=True
            )
        else:
            lines = ["👛 *TOP WALLETS*\n"]
            for i, w in enumerate(wallets, 1):
                short = f"{w.address[:4]}...{w.address[-4:]}"
                lines.append(
                    f"#{i} `{short}` | Score: {w.score:.0f} | "
                    f"{w.wins}W {w.losses}L | Tier {w.tier} | Avg: {w.avg_multiple:.1f}x"
                )
            await callback.answer()
            await callback.message.reply("\n".join(lines), parse_mode="Markdown")

    elif action == "history":
        await callback.answer(
            "🔧 Trade History: full history view coming soon.", show_alert=True
        )

    elif action == "settings":
        await callback.answer(
            "⚙️ Use /keybot to manage your trading settings.", show_alert=True
        )

    elif action == "analyze":
        await callback.answer()
        _analyze_waiting.add(callback.message.chat.id)
        await callback.message.reply("🔍 Send the contract address to analyze:", parse_mode=None)

    elif action == "papertrades":
        await callback.answer()
        ps = await get_paper_trade_stats()
        lines = [
            "📋 *PAPER TRADING RESULTS*",
            f"Total: `{ps['total']}` | Win rate: `{ps['win_rate']}%`",
            f"Paper P&L: `{ps['total_pnl']:+.4f} SOL`",
            f"Open: `{ps['open_count']}` | Today: `{ps['today_count']}`",
            "",
        ]
        if ps["recent"]:
            lines.append("📈 *Recent:*")
            for pt in ps["recent"]:
                n = (pt.token_name or "?").replace("_", "\\_")
                if pt.paper_pnl_sol and pt.paper_pnl_sol > 0:
                    lines.append(f"✅ `{n}` — {pt.peak_multiple or 0:.1f}x | +{pt.paper_pnl_sol:.4f} SOL")
                else:
                    lines.append(f"❌ `{n}` — {pt.close_reason} | {pt.paper_pnl_sol or 0:.4f} SOL")
        else:
            lines.append("_No paper trades yet_")
        await callback.message.reply("\n".join(lines), parse_mode="Markdown")

    else:
        await callback.answer()


# ── /wallets — Top Scored Wallets ─────────────────────────────────────────────

@router.message(Command("wallets"))
async def cmd_wallets(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/wallets` is only available in Callers HQ.")
        return

    wallets = await get_top_wallets(limit=10)
    if not wallets:
        await message.reply(
            "🔧 No wallets scored yet — Agent 2 is still analyzing winning tokens.\n"
            "_Check back after the next hourly run._",
            parse_mode="Markdown",
        )
        return

    lines = ["👛 *TOP WALLETS*\n"]
    for i, w in enumerate(wallets, 1):
        short = f"{w.address[:4]}...{w.address[-4:]}"
        lines.append(
            f"#{i} `{short}` | Score: {w.score:.0f} | "
            f"{w.wins}W {w.losses}L | Tier {w.tier} | Avg: {w.avg_multiple:.1f}x"
        )
    await message.reply("\n".join(lines), parse_mode="Markdown")


# ── /autotrade off|paper|live ─────────────────────────────────────────────────

@router.message(Command("autotrade"))
async def cmd_autotrade(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/autotrade` is only available in Callers HQ.")
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Only admins can toggle autotrade.")
        return

    parts = (message.text or "").split()
    valid_modes = ("off", "paper", "live", "on")
    if len(parts) < 2 or parts[1].lower() not in valid_modes:
        await message.reply(
            f"⚡ Trade mode: *{_mode_label()}*\n"
            f"Usage: `/autotrade off` | `/autotrade paper` | `/autotrade live`",
            parse_mode="Markdown",
        )
        return

    mode = parts[1].lower()
    if mode == "on":
        mode = "live"  # legacy compat

    state.trade_mode = mode
    state.autotrade_enabled = (mode == "live")

    mode_msgs = {
        "off":   "🔴 OFF — Scanner running. Monitoring only.",
        "paper": "📋 PAPER — Scanner running. Paper trades will execute.",
        "live":  "🟢 LIVE — Scanner running. Real buys via Jupiter.",
    }
    await message.reply(
        f"⚡ Trade mode: *{_mode_label()}*\n_{mode_msgs[mode]}_",
        parse_mode="Markdown",
    )
    logger.info("Trade mode set to %s by %s", mode, message.from_user.username or message.from_user.id)


# ── /papertrades ──────────────────────────────────────────────────────────────

@router.message(Command("papertrades"))
async def cmd_papertrades(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/papertrades` is only available in Callers HQ.")
        return

    ps = await get_paper_trade_stats()
    lines = [
        "📋 PAPER TRADING RESULTS",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Total: {ps['total']} | Win rate: {ps['win_rate']}%",
        f"Paper P&L: {ps['total_pnl']:+.4f} SOL",
        f"Open: {ps['open_count']} | Today: {ps['today_count']} ({ps['today_pnl']:+.4f} SOL)",
        "",
    ]
    if ps["recent"]:
        lines.append("Recent trades:")
        for pt in ps["recent"]:
            n = pt.token_name or "?"
            if pt.paper_pnl_sol and pt.paper_pnl_sol > 0:
                lines.append(f"  ✅ {n} — {pt.peak_multiple or 0:.1f}x +{pt.paper_pnl_sol:.4f} SOL")
            else:
                lines.append(f"  ❌ {n} — {pt.close_reason} {pt.paper_pnl_sol or 0:.4f} SOL")
    else:
        lines.append("No paper trades yet. Set mode to PAPER: /autotrade paper")

    await message.reply("\n".join(lines), parse_mode=None)


# ── /patterns ─────────────────────────────────────────────────────────────────

_PATTERN_LABELS = {
    "new_launch":     "NEW LAUNCH",
    "insider_wallet": "INSIDER BUY",
    "volume_spike":   "VOLUME SPIKE",
}


@router.message(Command("patterns"))
async def cmd_patterns(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/patterns` is only available in Callers HQ.")
        return

    params = await get_all_trade_params()
    if not params:
        await message.reply(
            "🎯 *AI TRADE PATTERNS*\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "_No patterns learned yet — need more closed trades._",
            parse_mode="Markdown",
        )
        return

    lines = ["🎯 *AI TRADE PATTERNS*", "━━━━━━━━━━━━━━━━━━━━", ""]

    for p in params:
        label = _PATTERN_LABELS.get(p.pattern_type, p.pattern_type.upper())
        using = "AI learned" if p.sample_size >= 10 else "Manual presets (need 10+ trades)"
        lines += [
            f"🎯 *{label}*",
            f"TP: `{p.optimal_tp_x:.1f}x` | SL: `{p.optimal_sl_pct:.0f}%` | Size: `{p.optimal_position_pct:.0f}%` wallet",
            f"Win rate: `{p.win_rate * 100:.0f}%` | Avg: `{p.avg_multiple:.1f}x` | Sample: `{p.sample_size}` trades",
            f"Using: _{using}_",
            "",
        ]

    # Chart pattern win rates
    chart_wr = await get_chart_pattern_win_rates()
    if chart_wr:
        lines += ["📊 *CHART PATTERN WIN RATES*", ""]
        for cw in chart_wr:
            name = cw["pattern"].replace("_", " ").title()
            lines.append(
                f"  `{name}` — {cw['win_rate']}% WR | {cw['trades']} trades"
            )
        lines.append("")

    lines.append(f"_Updated: {datetime.utcnow().strftime('%H:%M:%S')} UTC_")
    await message.reply("\n".join(lines), parse_mode="Markdown")


# ── Analyze: shared logic ─────────────────────────────────────────────────────

async def _run_analysis(address: str, status_msg: Message) -> None:
    """Runs full agent analysis on address, edits status_msg with report."""
    try:
        # ── Fetch token data — try both DexScreener endpoints ────────────
        logger.info("Analyze: fetching DexScreener for %s", address)
        pair = await fetch_token_data(address)

        if not pair:
            # Fallback: try v2 Solana-specific endpoint
            logger.info("Analyze: v1 returned no pairs, trying v2 endpoint")
            try:
                v2_url = f"https://api.dexscreener.com/tokens/v1/solana/{address}"
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as _session:
                    async with _session.get(v2_url) as resp:
                        logger.info("Analyze: v2 status=%d", resp.status)
                        if resp.status == 200:
                            v2_data = await resp.json(content_type=None)
                            # v2 returns a list of pairs directly
                            pairs_list = v2_data if isinstance(v2_data, list) else v2_data.get("pairs") or []
                            if pairs_list:
                                pairs_list.sort(
                                    key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                                    reverse=True,
                                )
                                pair = pairs_list[0]
                                logger.info("Analyze: v2 found %d pairs", len(pairs_list))
                            else:
                                logger.info("Analyze: v2 returned empty list")
            except Exception as exc:
                logger.warning("Analyze: v2 endpoint failed: %s", exc)

        if not pair:
            await status_msg.edit_text(
                f"⛔ Token not found on DexScreener.\nAddress: {address}",
                parse_mode=None,
            )
            return

        logger.info("Analyze: found pair for %s — %s/%s",
                     address[:12], pair.get("baseToken", {}).get("symbol", "?"),
                     pair.get("quoteToken", {}).get("symbol", "?"))

        metrics = parse_token_metrics(pair)
        name      = metrics.get("name", "Unknown")
        symbol    = metrics.get("symbol", "???")
        mcap      = metrics.get("market_cap", 0) or 0
        liquidity = metrics.get("liquidity_usd", 0) or 0
        price     = metrics.get("price_usd", 0) or 0

        # ── Agent 1: Harvester — in DB? ──────────────────────────────────
        in_db = await token_exists(address)
        if not in_db:
            await save_token(
                mint=address, name=name, symbol=symbol,
                price_usd=price, market_cap=mcap,
                liquidity_usd=liquidity,
                volume_24h=metrics.get("volume_24h"),
                source="manual_analyze",
            )
            harvester_line = "🌾 Harvester: New token — added to DB ⚠️"
        else:
            harvester_line = "🌾 Harvester: In DB ✅"

        # ── Agent 2: Wallet signal + early buyer detection ────────────────
        tier_wallets = await get_tier_wallets(max_tier=2)
        known_addresses = {w.address for w in tier_wallets}

        early_buyers = await _get_early_buyers(address, window_minutes=10)
        insider_count = 0
        new_wallets_added = 0
        known_insiders = []

        for buyer in early_buyers:
            if buyer in known_addresses:
                # Known tracked wallet bought early
                insider_count += 1
                # Find their tier
                for w in tier_wallets:
                    if w.address == buyer:
                        known_insiders.append(f"{buyer[:4]}..{buyer[-4:]} (T{w.tier})")
                        break
            else:
                # New wallet — add with initial score
                score, tier = _score_wallet(
                    wins=1, losses=0, total_trades=1,
                    avg_multiple=1.0, early_entry_rate=1.0,
                )
                if tier > 0:
                    await upsert_wallet(
                        address=buyer, score=score, tier=tier,
                        win_rate=1.0, avg_multiple=1.0,
                        wins=1, losses=0, total_trades=1,
                        avg_entry_mcap=mcap,
                    )
                    new_wallets_added += 1

        wallet_line = f"👛 Early Buyers: {len(early_buyers)} found"
        if new_wallets_added:
            wallet_line += f" | {new_wallets_added} new wallets added"
        if insider_count:
            insider_names = ", ".join(known_insiders[:3])
            wallet_line += f"\n🔥 Tier match: {insider_count} known insiders bought early"
            if insider_names:
                wallet_line += f"\n     {insider_names}"

        # ── Caller check ─────────────────────────────────────────────────
        caller_scanned = await has_caller_scanned(address)
        caller_line = (
            "📞 Caller: Previously scanned ✅"
            if caller_scanned
            else "📞 Caller: Not scanned by callers"
        )

        # ── Agent 3: Pattern match ───────────────────────────────────────
        pattern = await get_pattern_by_type("winner_2x")
        if pattern:
            from bot.agents.scanner_agent import _fingerprint_match
            match_score = _fingerprint_match(pattern, mcap, liquidity, 0)
            if match_score >= 50:
                pattern_line = f"🧩 Pattern: winner_2x ✅ (score: {match_score:.0f})"
            else:
                pattern_line = f"🧩 Pattern: Weak match (score: {match_score:.0f})"
        else:
            match_score = 50
            pattern_line = "🧩 Pattern: No patterns learned yet"

        # ── Agent 7: Chart patterns ──────────────────────────────────────
        chart_result = await analyze_chart({
            "mint": address, "name": name, "symbol": symbol,
            "mcap": mcap, "liquidity": liquidity, "insider_count": 0,
        })
        chart_score = chart_result.get("chart_score", 0)
        chart_pattern = chart_result.get("pattern_name", "none")
        chart_patterns = chart_result.get("patterns_detected", [])

        if chart_pattern != "none":
            chart_display = chart_pattern.replace("_", " ").title()
            chart_line = f"📈 Chart: {chart_display} detected | Score: {chart_score:.0f}"
        else:
            chart_line = f"📈 Chart: No pattern detected | Score: {chart_score:.0f}"
        if len(chart_patterns) > 1:
            extras = ", ".join(p.replace("_", " ").title() for p in chart_patterns[1:3])
            chart_line += f"\n     Also: {extras}"

        # ── Rugcheck ─────────────────────────────────────────────────────
        from bot.agents.scanner_agent import _fetch_rugcheck
        rc_data = await _fetch_rugcheck(address)
        rc_score = (rc_data or {}).get("score")
        rc_norm = (rc_data or {}).get("score_normalised")

        if rc_norm is not None:
            # Convert normalised (1-10 risk) to 1-100 safety
            safety_100 = max(1, min(100, round(100 - rc_norm * 10)))
        elif rc_score is not None:
            safety_100 = max(1, min(100, round(100 - min(rc_score, 1000) / 10)))
        else:
            safety_100 = None

        if safety_100 is not None:
            rc_icon = "✅" if safety_100 >= 70 else "⚠️" if safety_100 >= 40 else "🔴"
            rug_line = f"🛡️ Rug Safety: {safety_100}/100 {rc_icon}"
        else:
            rug_line = "🛡️ Rug Safety: No data"

        # ── Agent 5: Full confidence score ───────────────────────────────
        scan_data = await scan_token(address)
        ai_score = scan_data.get("total", 0) if scan_data else 0

        candidate = {
            "mint": address, "name": name, "symbol": symbol,
            "mcap": mcap, "liquidity": liquidity, "ai_score": ai_score,
            "match_score": match_score, "rugcheck": rc_score,
            "rugcheck_normalised": rc_norm,
            "source": "manual_analyze", "insider_count": insider_count,
        }
        scored = await score_candidate(candidate)
        confidence = scored.get("confidence_score", 0)
        decision = scored.get("decision", "discard")

        decision_map = {
            "execute_full": "🟢 AUTO-EXECUTE (full position)",
            "execute_half": "🟡 AUTO-EXECUTE (half position)",
            "monitor":      "🔵 MONITOR (high interest)",
            "discard":      "⚪ DISCARD (below threshold)",
        }
        decision_display = decision_map.get(decision, decision)

        if decision in ("execute_full", "execute_half"):
            action_line = "⚡ Would auto-buy if autotrade ON"
        elif confidence >= 70:
            action_line = f"⚡ Close — needs {80 - confidence:.0f} more points"
        else:
            action_line = "⚡ Would auto-buy at 80+ confidence"

        tp = scored.get("trade_tp_x", 3.0)
        sl = scored.get("trade_sl_pct", 30.0)
        ps = scored.get("params_source", "default")

        # ── Build report (plain text — no parse_mode to avoid escaping issues) ──
        mc_str = _format_usd(mcap) if mcap else "?"
        liq_str = _format_usd(liquidity) if liquidity else "?"

        fp_s  = scored.get("fingerprint_score", 0)
        ins_s = scored.get("insider_score", 0)
        rug_s = scored.get("rug_score", 0)
        cal_s = scored.get("caller_score", 0)
        mkt_s = scored.get("market_score", 0)

        report = "\n".join([
            "🔍 MANUAL AGENT ANALYSIS",
            "━━━━━━━━━━━━━━━━━━━━",
            f"🪙 {name} (${symbol})",
            f"📍 {address}",
            f"💰 MC: {mc_str} | Liq: {liq_str}",
            "",
            "📊 AGENT SCORES",
            harvester_line,
            wallet_line,
            caller_line,
            pattern_line,
            chart_line,
            rug_line,
            "",
            f"🎯 Confidence: {confidence:.0f}/100",
            f"   FP:{fp_s:.0f} Ins:{ins_s:.0f} Chart:{chart_score:.0f} "
            f"Rug:{rug_s:.0f} Call:{cal_s:.0f} Mkt:{mkt_s:.0f}",
            "",
            f"💡 {decision_display}",
            action_line,
            f"📐 TP: {tp:.1f}x | SL: {sl:.0f}% | Params: {ps}",
            "",
            f"Analyzed {datetime.utcnow().strftime('%H:%M:%S')} UTC",
        ])

        await status_msg.edit_text(report, parse_mode=None)

    except Exception as exc:
        logger.error("Analyze command failed for %s: %s", address, exc)
        await status_msg.edit_text(f"⛔ Analysis failed: {exc}", parse_mode=None)


# ── /analyze <address> command ─────────────────────────────────────────────────

@router.message(Command("analyze"))
async def cmd_analyze(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/analyze` is only available in Callers HQ.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        # No address provided — prompt for it (same as button flow)
        _analyze_waiting.add(message.chat.id)
        await message.reply("🔍 Send the contract address to analyze:", parse_mode=None)
        return

    raw_addr = parts[1].strip()
    address = re.sub(r'[^\w]', '', raw_addr) if not raw_addr.isalnum() else raw_addr
    logger.info("Analyze cmd: raw=%r cleaned=%r len=%d", raw_addr, address, len(address))

    if len(address) < 32 or len(address) > 44:
        await message.reply(f"⛔ Invalid address (len={len(address)}).", parse_mode=None)
        return

    status_msg = await message.reply("🔍 Running full agent analysis...", parse_mode=None)
    await _run_analysis(address, status_msg)


# ── Analyze: catch address after button/prompt ────────────────────────────────

@router.message(lambda m: m.chat.id in _analyze_waiting and m.text and not m.text.startswith("/"))
async def handle_analyze_address(message: Message):
    _analyze_waiting.discard(message.chat.id)

    raw_addr = (message.text or "").strip()
    address = re.sub(r'[^\w]', '', raw_addr) if not raw_addr.isalnum() else raw_addr

    if len(address) < 32 or len(address) > 44:
        await message.reply(f"⛔ Invalid address (len={len(address)}).", parse_mode=None)
        return

    logger.info("Analyze button: address=%r len=%d", address, len(address))
    status_msg = await message.reply("🔍 Running full agent analysis...", parse_mode=None)
    await _run_analysis(address, status_msg)


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply("Bot is alive")


# ── /scan <address> ───────────────────────────────────────────────────────────

@router.message(Command("scan"))
async def cmd_scan(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ `/scan` is only available in the designated callers group.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(
            "⚠️ Usage: `/scan <contract_address>`\n"
            "Example: `/scan So11111111111111111111111111111111111111112`",
            parse_mode="Markdown",
        )
        return

    address = parts[1].strip()
    await _do_scan(message, address)


# ── /addcaller ────────────────────────────────────────────────────────────────

@router.message(Command("addcaller"))
async def cmd_addcaller(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("⚠️ Usage: `/addcaller <telegram_id>`", parse_mode="Markdown")
        return

    telegram_id = int(parts[1])
    added = await add_caller(telegram_id)
    if added:
        await message.reply(f"✅ User `{telegram_id}` added to approved callers.", parse_mode="Markdown")
    else:
        await message.reply(f"ℹ️ User `{telegram_id}` is already an approved caller.", parse_mode="Markdown")


# ── PnL helpers ───────────────────────────────────────────────────────────────

_RUG_REASONS = {"rug_mc", "rug_liquidity"}

def _status_icon(status: str, close_reason: str | None = None) -> str:
    if close_reason in _RUG_REASONS:
        return "💀"
    return {"win": "🟢", "break_even": "🟡", "loss": "🔴"}.get(status, "🟡")

def _status_label(status: str, close_reason: str | None = None) -> str:
    if close_reason == "rug_mc":
        return "RUG 💀 (MC collapsed)"
    if close_reason == "rug_liquidity":
        return "RUG 💀 (liquidity drained)"
    if close_reason == "expired":
        return f"{status.upper()} (expired)"
    return status.upper()


async def _build_pnl_text(scan, current_mc: float) -> str:
    curr_mult = current_mc / scan.entry_price if scan.entry_price else 0
    peak_mult = scan.peak_multiplier or 1.0
    icon      = _status_icon(scan.status, scan.close_reason)
    label     = _status_label(scan.status, scan.close_reason)

    age = datetime.utcnow() - scan.scanned_at
    remaining = timedelta(days=7) - age
    is_closed = scan.status in ("win", "break_even", "loss")
    if is_closed:
        time_str = "✅ Closed"
    elif remaining.total_seconds() > 0:
        total_secs = int(remaining.total_seconds())
        d, rem     = divmod(total_secs, 86400)
        h, rem     = divmod(rem, 3600)
        m          = rem // 60
        time_str   = f"⏱ {d}d {h}h {m}m remaining"
    else:
        time_str = "⏱ Expiring soon"

    return (
        f"{icon} *{scan.token_name}*\n\n"
        f"Entry MC:   `{_format_usd(scan.entry_price)}`\n"
        f"Current MC: `{_format_usd(current_mc)}`\n"
        f"Peak MC:    `{_format_usd(scan.peak_market_cap or scan.entry_price)}`\n\n"
        f"Current X:  *{curr_mult:.2f}x*\n"
        f"Peak X:     *{peak_mult:.2f}x*\n"
        f"Points:     *{scan.points:.2f}*\n\n"
        f"Status:  `{label}`\n"
        f"{time_str}\n\n"
        f"_Called by @{scan.scanned_by}_"
    )


# ── /pnl <contract> ──────────────────────────────────────────────────────────

@router.message(Command("pnl"))
async def cmd_pnl(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Please provide a contract address: `/pnl <contract>`", parse_mode="Markdown")
        return

    address = parts[1].strip()
    scan = await get_scan_by_address(address)
    if scan is None:
        await message.reply("No scan found for this contract.")
        return
    if not scan.entry_price:
        await message.reply("⚠️ This scan has no entry market cap recorded.")
        return

    status_msg = await message.reply("📊 Fetching market cap…")
    current_mc = await fetch_current_market_cap(address)
    if current_mc is None:
        await status_msg.edit_text("❌ Could not fetch current market cap from DexScreener.")
        return

    if scan.status == "open":
        scan = await update_scan_pnl(scan.id, current_mc)

    text = await _build_pnl_text(scan, current_mc)
    await status_msg.edit_text(text, parse_mode="Markdown", reply_markup=pnl_keyboard(address))


# ── Callback: Refresh PnL ────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("pnl:"))
async def cb_pnl_refresh(callback: CallbackQuery):
    address = callback.data.split(":", 1)[1]
    await callback.answer("🔄 Refreshing…")

    scan = await get_scan_by_address(address)
    if scan is None or not scan.entry_price:
        await callback.answer("No data found.", show_alert=True)
        return

    current_mc = await fetch_current_market_cap(address)
    if current_mc is None:
        await callback.answer("❌ Could not fetch price.", show_alert=True)
        return

    if scan.status == "open":
        scan = await update_scan_pnl(scan.id, current_mc)

    text = await _build_pnl_text(scan, current_mc)
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=pnl_keyboard(address))


# ── /sl — Signal Leaders ──────────────────────────────────────────────────────

@router.message(Command("sl"))
async def cmd_sl(message: Message):
    rows = await get_signal_leaders()
    if not rows:
        await message.reply("📭 No scans recorded yet.")
        return

    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 *Signal Leaders*\n"]
    for i, row in enumerate(rows):
        prefix = medals[i] if i < 3 else f"{i + 1}."
        lines.append(
            f"{prefix} `@{row['username']}` | "
            f"{row['scans']} calls | "
            f"🟢{row['wins']}W 🔴{row['losses']}L | "
            f"{row['win_pct']}% | "
            f"{row['total_points']}pts"
        )
    await message.reply("\n".join(lines), parse_mode="Markdown")


# ── /lb & /leaderboard — Top Calls ───────────────────────────────────────────

_TC_SINCE = {
    "24H": lambda: datetime.utcnow() - timedelta(hours=24),
    "1W":  lambda: datetime.utcnow() - timedelta(weeks=1),
    "1M":  lambda: datetime.utcnow() - timedelta(days=30),
    "ALL": lambda: None,
}


async def _build_top_calls(timeframe: str = "ALL") -> tuple[str, object]:
    since = _TC_SINCE.get(timeframe, lambda: None)()
    rows  = await get_top_calls(limit=10, since=since)
    stats = await get_top_calls_stats(since=since)

    lines = [
        "📈 *Top Calls*",
        f"_{stats['total']} calls tracked | {stats['avg_x']}x avg_",
        "",
    ]

    if not rows:
        lines.append("_No calls recorded yet._")
    else:
        _RUG = {"rug_mc", "rug_liquidity"}
        for i, row in enumerate(rows, 1):
            peak_x   = row["peak_multiplier"] or 0
            is_rug   = row["close_reason"] in _RUG
            suffix   = " 💀" if is_rug else (" 🔥" if peak_x >= 5 else "")
            caller   = row["scanned_by"].replace("_", "\\_")
            name     = row["token_name"].replace("_", "\\_")
            lines.append(f"{i}. {name} » @{caller} {peak_x:.2f}x{suffix}")

    return "\n".join(lines), top_calls_keyboard(active=timeframe)


@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message):
    text, keyboard = await _build_top_calls("ALL")
    await message.reply(text, parse_mode="Markdown", reply_markup=keyboard)


@router.message(Command("lb"))
async def cmd_lb(message: Message):
    text, keyboard = await _build_top_calls("ALL")
    await message.reply(text, parse_mode="Markdown", reply_markup=keyboard)


# ── Callback: Top Calls timeframe ────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("tc:"))
async def cb_top_calls_timeframe(callback: CallbackQuery):
    timeframe = callback.data.split(":", 1)[1]
    if timeframe not in _TC_SINCE:
        await callback.answer()
        return
    await callback.answer()
    text, keyboard = await _build_top_calls(timeframe)
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)


# ── Callback: Share Signal ────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("share:"))
async def cb_share_signal(callback: CallbackQuery, bot: Bot):
    address = callback.data.split(":", 1)[1]

    if MAIN_GROUP_ID == 0:
        await callback.answer(
            "⚠️ MAIN_GROUP_ID is not configured yet. Set it in .env", show_alert=True
        )
        return

    await callback.answer("📢 Sharing signal to main group…")

    data = await scan_token(address)
    if data is None:
        await callback.answer("❌ Failed to re-fetch token data.", show_alert=True)
        return

    card_text = build_trade_card(data)
    keyboard = trade_card_keyboard(dex_url=data.get("dex_url", ""), contract_address=address)

    shared_by = callback.from_user.username or str(callback.from_user.id)
    await bot.send_message(
        chat_id=MAIN_GROUP_ID,
        text=f"📢 *Signal shared by @{shared_by}*\n\n{card_text}",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


# ── Callback: Flag as Risky ───────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("flag:"))
async def cb_flag_risky(callback: CallbackQuery):
    address = callback.data.split(":", 1)[1]
    flagger = callback.from_user.username or str(callback.from_user.id)

    logger.warning("Token flagged as risky: %s by %s", address, flagger)

    await callback.answer(
        f"🚩 Token {address[:8]}… flagged as risky. Thanks for the heads-up!",
        show_alert=True,
    )


# ── Callback: Refresh Trade Card ─────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("refresh:"))
async def cb_refresh_trade_card(callback: CallbackQuery):
    address = callback.data.split(":", 1)[1]
    await callback.answer("🔄 Refreshing…")

    card_text, data = await _build_card_text(address)
    if data is None:
        await callback.answer("❌ Could not fetch token data.", show_alert=True)
        return

    keyboard = trade_card_keyboard(
        dex_url=data.get("dex_url", ""),
        contract_address=address,
    )
    try:
        await callback.message.edit_text(card_text, parse_mode="Markdown", reply_markup=keyboard)
    except Exception:
        # Message content unchanged — silently ignore
        pass


# ── Auto-scan (bare address) — MUST be last so it doesn't shadow commands ─────

@router.message()
async def auto_scan(message: Message):
    if message.chat.id != -1003852140576:
        return
    if not message.text:
        return
    text = message.text.strip()
    if text.startswith("/"):
        return
    if len(text) < 32 or len(text) > 50 or ' ' in text:
        return
    await _do_scan(message, text)
