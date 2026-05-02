import asyncio
import html
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
    get_pumpfun_count_today,
    token_exists, save_token, get_token_by_mint,
    get_tier_wallets, get_pattern_by_type, has_caller_scanned,
    upsert_wallet, get_paper_trade_stats, get_open_paper_trades,
    get_all_params, get_recent_param_changes,
    upsert_pattern, set_param, compute_paper_balance,
    get_all_tokens, update_token_market_cap,
    get_tokens_batch, set_token_launch_mc, get_token_count,
    get_gmgn_stats,
)
from bot.agents.confidence_engine import score_candidate
from bot.agents.chart_detector import analyze_chart
from bot.agents.wallet_analyst import _get_early_buyers, _score_wallet, _get_signatures, _parse_transactions

logger = logging.getLogger(__name__)
router = Router()

CALLER_GROUP_ID = -1003852140576


# Users waiting to input an address for /analyze (chat_id -> True)
_analyze_waiting: set[int] = set()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _verdict_emoji(verdict: str) -> str:
    return {
        "STRONG BUY": "🟢",
        "GOOD ENTRY": "🟢",
        "PROMISING":  "🟡",
        "WATCH":      "🟡",
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


def _build_verdict_reasoning(data: dict) -> str:
    """Generate a full breakdown: quality analysis, timing analysis, and suggested action."""
    c = data["components"]
    mc = data.get("market_cap", 0)
    liq = data.get("liquidity_usd", 0)
    vol = data.get("volume_24h", 0)
    pct = data.get("price_change_24h", 0)
    holders = data.get("estimated_holders", 0)
    timing_details = data.get("timing_details", {})
    timing_score = data.get("timing_score", 50)
    quality = data.get("quality_score", data.get("total", 0))
    verdict = data.get("verdict", "")
    rr = data.get("risk_reward", "")

    lines = []

    # ── Token Quality Breakdown ──
    lines.append("*Token Quality*")

    liq_score = c["liquidity"]
    ratio_pct = (liq / mc * 100) if mc > 0 else 0
    if liq_score >= 16:
        lines.append(f"💧 Liquidity {ratio_pct:.1f}% of MC — deep pool, clean exits")
    elif liq_score >= 10:
        lines.append(f"💧 Liquidity {ratio_pct:.1f}% of MC — adequate")
    elif liq_score >= 5:
        lines.append(f"💧 Liquidity only {ratio_pct:.1f}% of MC — thin, slippage risk")
    else:
        lines.append(f"💧 Dangerously low liquidity — hard to exit")

    vol_score = c["volume"]
    turnover = (vol / liq) if liq > 0 else 0
    if vol_score >= 16:
        lines.append(f"📊 {turnover:.1f}x pool turnover — heavy buying interest")
    elif vol_score >= 10:
        lines.append(f"📊 {_format_usd(vol)} volume — moderate activity")
    else:
        lines.append(f"📊 Low volume — minimal attention")

    hold_score = c["holder_distribution"]
    if hold_score >= 12:
        lines.append(f"👥 {holders:,} active traders — organic market")
    elif hold_score >= 8:
        lines.append(f"👥 {holders} traders — decent distribution")
    else:
        lines.append(f"👥 {holders} traders — concentrated, early stage")

    safety_score = c["contract_safety"]
    if safety_score >= 13:
        lines.append(f"🛡️ Clean — no honeypot or wash trading signals")
    elif safety_score >= 8:
        lines.append(f"🛡️ Minor flags — watch liq/volume ratio")
    else:
        lines.append(f"⚠️ Safety concerns detected")

    # ── Entry Timing Breakdown ──
    lines.append("")
    lines.append("*Entry Timing*")
    for key in ("age", "run", "pattern", "flow", "mc_room"):
        if key in timing_details:
            icon = {"age": "⏰", "run": "📈", "pattern": "📐", "flow": "💰", "mc_room": "🎯"}.get(key, "•")
            lines.append(f"{icon} {timing_details[key]}")

    if rr:
        lines.append(f"⚖️ R/R: {rr}")

    # ── Suggested Action ──
    lines.append("")
    if verdict == "STRONG BUY":
        lines.append(
            "📝 Quality and timing both strong. Enter with a "
            "trailing stop at 20% below peak. Let it run."
        )
    elif verdict == "GOOD ENTRY":
        lines.append(
            "📝 Good token, decent timing. Consider a position "
            "but size conservatively. Trail stop to protect gains."
        )
    elif verdict == "WATCH":
        lines.append(
            f"📝 Strong token but timing is off. Already up "
            f"{pct:+.0f}%. Wait for a pullback to "
            f"{_format_usd(mc * 0.6)} MC before entering. "
            f"If already in, trail stop at 20% below peak."
        )
    elif verdict == "PROMISING":
        lines.append(
            "📝 Some positive signals but not a clear setup. "
            "Watch for volume surge or breakout confirmation "
            "before committing. Small position only."
        )
    elif verdict == "RISKY":
        lines.append(
            "📝 Multiple warning signs. Only with money you "
            "can afford to lose. Tight stops mandatory."
        )
    else:
        lines.append(
            "📝 Too many red flags. Stay away."
        )

    return "\n".join(lines)


def build_trade_card(data: dict) -> str:
    emoji = _verdict_emoji(data["verdict"])
    change_sign = "+" if data["price_change_24h"] >= 0 else ""

    filled = int(data["total"] / 10)
    score_bar = "█" * filled + "░" * (10 - filled)

    reasoning = _build_verdict_reasoning(data)

    quality = data.get("quality_score", data["total"])
    timing_score = data.get("timing_score", 50)
    timing_label = data.get("timing_label", "?")
    rr = data.get("risk_reward", "")

    q_filled = int(quality / 10)
    q_bar = "█" * q_filled + "░" * (10 - q_filled)
    t_filled = int(timing_score / 10)
    t_bar = "█" * t_filled + "░" * (10 - t_filled)

    lines = [
        f"{'─' * 34}",
        f"🤖 *AI TRADE SIGNAL*",
        f"{'─' * 34}",
        f"",
        f"🪙 *{data['name']}* (${data['symbol']})",
        f"🔗 `{data['address']}`",
        f"",
        f"💵 Price:       {_format_price(data['price_usd'])}",
        f"📊 Market Cap:  {_format_usd(data['market_cap'])}",
        f"💧 Liquidity:   {_format_usd(data['liquidity_usd'])}",
        f"📈 Volume 24h:  {_format_usd(data['volume_24h'])}",
        f"🕯 Change 24h:  {change_sign}{data['price_change_24h']:.1f}%",
        f"",
        f"{'─' * 34}",
        f"🧠 *Token Quality:* [{q_bar}] *{quality:.0f}/100*",
        f"",
        f"  • Liquidity Health:     {data['components']['liquidity']:.1f}/20",
        f"  • Volume Velocity:      {data['components']['volume']:.1f}/20",
        f"  • Momentum:             {data['components']['momentum']:.1f}/20",
        f"  • Holder Activity:      {data['components']['holder_distribution']:.1f}/15",
        f"  • Contract Safety:      {data['components']['contract_safety']:.1f}/15",
        f"  • Market Strength:      {data['components'].get('market_strength', data['components'].get('deployer_reputation', 0)):.1f}/10",
        f"",
        f"⏱ *Entry Timing:* [{t_bar}] *{timing_score}/100 — {timing_label}*",
        f"⚖️ *R/R:* {rr}" if rr else "",
        f"",
        f"{'─' * 34}",
        f"📋 Verdict: {emoji} *{data['verdict']}*",
        f"{'─' * 34}",
        f"",
        reasoning,
    ]

    # Intel section (similar tokens + insider activity)
    intel = data.get("intel_section")
    if intel:
        lines.append(f"")
        lines.append(f"{'─' * 34}")
        lines.append(f"🔍 *Intelligence*")
        lines.append(intel)

    lines.append(f"")
    lines.append(f"_Scanned at {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_")

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
        f"🪙 Hold: {balance:,.0f} tokens ({pct_supply:.3f}% supply)",
        f"💵 Value: {_format_usd(value_usd)} / {value_sol:.4f} SOL",
    ]

    if pos and pos.entry_mc and pos.entry_mc > 0 and current_mc > 0:
        pct_change = (current_mc - pos.entry_mc) / pos.entry_mc * 100
        sign       = "+" if pct_change >= 0 else ""
        lines.append(
            f"📈 Avg Entry: {_format_usd(pos.entry_mc)} MC"
            f" | Now: {_format_usd(current_mc)} MC"
            f" | {sign}{pct_change:.1f}%"
        )

    return "\n".join(lines)


async def _build_card_text(address: str) -> tuple[str | None, dict | None]:
    """
    Fetches live token data + wallet position concurrently.
    Returns (card_text, data) where card_text includes the position block if held,
    or (None, None) if the token could not be fetched.
    """
    data = await scan_token(address, allow_any_dex=True)
    if data is None:
        return None, None

    # Enrich with similar token outcomes + insider activity from DB
    try:
        from database.models import AsyncSessionLocal, select, PaperTrade, Wallet, WalletTokenTrade

        mc = data.get("market_cap", 0) or 0
        intel_lines = []

        # Similar token outcomes — find closed paper trades at similar MC
        if mc > 0:
            mc_low = mc * 0.5
            mc_high = mc * 2.0
            async with AsyncSessionLocal() as session:
                similar = (await session.execute(
                    select(PaperTrade)
                    .where(
                        PaperTrade.entry_mc.between(mc_low, mc_high),
                        PaperTrade.close_reason.isnot(None),
                        PaperTrade.peak_multiple.isnot(None),
                    )
                    .limit(50)
                )).scalars().all()
            if similar:
                peaks = [float(t.peak_multiple or 1.0) for t in similar]
                avg_peak = sum(peaks) / len(peaks)
                wins = sum(1 for p in peaks if p >= 1.5)
                intel_lines.append(
                    f"📊 Similar setups: avg peak {avg_peak:.1f}x from entry "
                    f"({wins}/{len(similar)} hit 1.5x) based on {len(similar)} matches"
                )

        # Insider activity — check if tracked wallets hold this token
        async with AsyncSessionLocal() as session:
            insider_trades = (await session.execute(
                select(WalletTokenTrade)
                .where(WalletTokenTrade.token_address == address)
                .limit(20)
            )).scalars().all()
        if insider_trades:
            still_holding = sum(1 for t in insider_trades if t.last_sell_at is None)
            exited = len(insider_trades) - still_holding
            if still_holding > 0:
                intel_lines.append(
                    f"👛 Insider status: {still_holding} tracked wallet(s) still holding"
                )
            elif exited > 0:
                intel_lines.append(
                    f"👛 Insider status: all {exited} tracked wallet(s) have exited ⚠️"
                )

        if intel_lines:
            data["intel_section"] = "\n".join(intel_lines)
    except Exception as exc:
        logger.debug("Scan card intel enrichment failed: %s", exc)

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

    # Only create a new scan if this token hasn't been scanned before.
    # Rescanning the same token should update the existing record, not
    # create a duplicate that resets the peak multiplier.
    from database.models import get_scan_by_address, update_scan_pnl
    existing = await get_scan_by_address(address)
    if existing:
        # Update existing scan with current MC (tracks peak via max())
        if entry_mc:
            await update_scan_pnl(existing.id, entry_mc)
        logger.info("SCAN: updated existing scan id=%d for %s", existing.id, data["name"])
    else:
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


async def _hub_keyboard(autotrade: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    # Row 1: Paper sim toggle
    if state.trade_mode == "paper":
        paper_label = "📋 Paper Sim: ✅ ON"
    else:
        paper_label = "📋 Paper Sim: ❌ OFF"
    builder.row(InlineKeyboardButton(text=paper_label, callback_data="hub:toggle_paper"))

    # Row 2: Live trading — locked
    builder.row(InlineKeyboardButton(text="🟢 Live Trading: LOCKED 🔒", callback_data="hub:live_locked"))

    # Row 2b: External CA broadcast toggle (Phanes group)
    try:
        from database.models import get_param as _get_param
        ext_v = await _get_param("external_ca_post_enabled")
        ext_on = ext_v is None or ext_v >= 0.5
    except Exception:
        ext_on = True
    ext_label = "📤 Share to Group: ✅ ON" if ext_on else "📤 Share to Group: ❌ OFF"
    builder.row(InlineKeyboardButton(text=ext_label, callback_data="hub:toggle_extpost"))

    # Individual close buttons for each open paper trade
    try:
        open_trades = await get_open_paper_trades()
        for idx, pt in enumerate(open_trades[:5], 1):
            name = (pt.token_name or "?")[:15]
            builder.row(InlineKeyboardButton(
                text=f"❌ Close #{idx} — {name}",
                callback_data=f"hub:close_trade:{pt.id}",
            ))
    except Exception:
        pass

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

    # Row 5: Manual close-all + reset balance
    builder.row(
        InlineKeyboardButton(text="🗑️ Close All",     callback_data="hub:close_all"),
        InlineKeyboardButton(text="💰 Reset Balance", callback_data="hub:reset_confirm"),
    )
    return builder.as_markup()


def _hub_reset_confirm_keyboard() -> InlineKeyboardMarkup:
    """Yes / No inline keyboard shown when the Reset Balance button is tapped."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Yes, reset", callback_data="hub:reset_yes"),
        InlineKeyboardButton(text="❌ No",          callback_data="hub:reset_no"),
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
    remaining = max(0, 3 - (state.learning_loop_total_closed - state.learning_loop_last_analyzed))
    regime = getattr(state, "market_regime", "NEUTRAL")
    regime_icon = {"GOOD": "🟢", "NEUTRAL": "🟡", "BAD": "🔴"}.get(regime, "⚪")
    sol_chg = getattr(state, "sol_24h_change", 0.0)

    if state.learning_loop_last_run is None:
        return f"✅ Learning Loop — {regime_icon} {regime} | SOL {sol_chg:+.1f}% | waiting..."

    elapsed_min = int((datetime.utcnow() - state.learning_loop_last_run).total_seconds() / 60)
    age = f"{elapsed_min}min ago" if elapsed_min < 60 else f"{elapsed_min // 60}h ago"

    # Show last 3 param changes
    try:
        recent = await get_recent_param_changes(3)
    except Exception:
        recent = []
    if recent:
        adj_lines = []
        for c in recent:
            short_name = c.param_name.replace("scanner_", "").replace("conf_", "").replace("_mc_", ".")
            adj_lines.append(f"{short_name}: {c.old_value:g}→{c.new_value:g}")
        adj_str = " | ".join(adj_lines)
    else:
        adj_str = "No adjustments yet"

    return (
        f"✅ Learning Loop — {regime_icon} {regime} | SOL {sol_chg:+.1f}% | last {age}\n"
        f"     🧠 {adj_str}"
    )


async def _manual_close_all_open(bot) -> int:
    """
    Close every open paper trade at its current market price, regardless
    of dead-detection rules. Used by /hub's "🗑️ Close All" button.
    Close reason is "manual_close". Broadcasts one summary line per kill.
    """
    from bot.scanner import fetch_live_data
    from database.models import (
        get_open_paper_trades, close_paper_trade, compute_paper_balance,
    )

    trades = await get_open_paper_trades()
    if not trades:
        return 0

    closed = 0
    for pt in trades:
        try:
            live = await fetch_live_data(pt.token_address)
            current_mc = (live or {}).get("market_cap") or 0

            entry_mc = pt.entry_mc or 1
            # If we can't get a live price, close at entry (0 PnL) rather
            # than skipping — user asked for all positions closed.
            if current_mc <= 0:
                current_mc = entry_mc
            current_mult = current_mc / entry_mc if entry_mc > 0 else 1.0
            peak_mc = max(pt.peak_mc or 0, current_mc)
            peak_mult = max(pt.peak_multiple or 1.0, current_mult)

            sol = pt.paper_sol_spent
            pnl = round(sol * (current_mult - 1), 4)
            await close_paper_trade(pt.id, "manual_close", pnl, peak_mc, peak_mult)
            closed += 1

            name = (pt.token_name or "Unknown").replace("_", " ")
            pct = (current_mult - 1) * 100
            icon = "✅" if pnl > 0 else ("🟡" if pnl == 0 else "❌")
            try:
                await bot.send_message(CALLER_GROUP_ID, "\n".join([
                    f"{icon} PAPER TRADE CLOSED (manual)",
                    f"🪙 ${name} | {current_mult:.2f}x | {pct:+.0f}%",
                    f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                    f"PnL: {pnl:+.4f} SOL",
                ]))
            except Exception:
                pass
        except Exception as exc:
            # Bumped from debug to info so a silent per-trade failure
            # doesn't make the user think close_all worked when it
            # didn't. If you ever see "closed 0 positions" but trades
            # are still open, check Railway logs for these lines.
            logger.info("manual close_all SKIPPED trade id=%s %s: %s",
                        getattr(pt, "id", "?"),
                        (pt.token_address or "?")[:12], exc)

    state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    logger.info("Manual close_all: closed %d of %d open positions",
                closed, len(trades))

    return closed


def _esc(value) -> str:
    """HTML-escape an arbitrary value for Telegram HTML parse mode."""
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


async def _build_hub_text(autotrade: bool) -> str:
    """
    Revolt Agent Hub — scannable dashboard rendered with parse_mode=HTML.
    HTML is used instead of Markdown so contract addresses can be wrapped
    in <code>...</code> for one-tap copy on mobile. Every user-provided
    string must be passed through _esc() before interpolation.
    """
    DIVIDER = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    stats = await get_hub_stats()
    today_pnl           = stats["today_pnl"]
    alltime_pnl         = stats["alltime_pnl"]
    win_rate            = stats["win_rate"]
    total_closed        = stats["total_closed"]
    token_count         = stats["token_count"]
    last_harvest        = stats["last_harvest"]
    wallet_total        = stats["wallet_total"]
    wallet_tier1        = stats["wallet_tier1"]
    wallet_tier2        = stats["wallet_tier2"]
    wallet_tier3        = stats.get("wallet_tier3", 0)
    last_analyst        = stats["last_analyst"]
    pattern_total       = stats["pattern_total"]
    last_pattern_engine = stats["last_pattern_engine"]

    try:
        ce_stats = await get_candidate_stats_today()
    except Exception:
        ce_stats = {"scored_today": 0, "high_conf": 0, "executed_today": 0}

    try:
        paper_stats = await get_paper_trade_stats()
    except Exception:
        paper_stats = {"total": 0, "closed": 0, "wins": 0, "win_rate": 0,
                       "total_pnl": 0.0, "today_count": 0, "today_pnl": 0.0,
                       "open_count": 0, "recent": []}

    # ── Header ───────────────────────────────────────────────────────
    mode_word  = {"live": "LIVE", "paper": "PAPER SIM"}.get(state.trade_mode, "OFF")

    real_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    state.paper_balance = real_balance
    starting = state.PAPER_STARTING_BALANCE or 1.0
    pnl_val = real_balance - starting
    pnl_pct = ((real_balance / starting) - 1) * 100

    # Decision mode badge (HQ admin's KeyBot decision_mode)
    try:
        from database.models import get_keybot_settings as _kb
        hq_owner = ADMIN_IDS[0] if ADMIN_IDS else None
        kb = await _kb(hq_owner) if hq_owner is not None else None
        dec_mode = (kb.decision_mode if kb and kb.decision_mode else "ai").lower()
        dec_badge = "🤖 AI" if dec_mode == "ai" else "✋ Manual"
    except Exception:
        dec_badge = "🤖 AI"

    lines = [
        DIVIDER,
        "🔑 REVOLT AGENT HUB",
        f"Trade Mode: {mode_word}  |  Decision: {dec_badge}",
        f"Balance: {real_balance:.2f} / {starting:.2f} SOL  |  P&amp;L: {pnl_val:+.2f} SOL ({pnl_pct:+.1f}%)",
        DIVIDER,
        "",
        "⚙️ AGENTS",
    ]

    # ── Agents block (column-aligned best-effort) ───────────────────
    def _fmt_age(dt_or_secs, already_seconds: bool = False) -> str:
        if dt_or_secs is None:
            return "—"
        if already_seconds:
            secs = int(dt_or_secs)
        else:
            secs = int((datetime.utcnow() - dt_or_secs).total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        mins = secs // 60
        if mins < 60:
            return f"{mins}min ago"
        hours = mins // 60
        if hours < 24:
            return f"{hours}h ago"
        return f"{hours // 24}d ago"

    def _fmt_agent(name: str, icon: str, detail: str, age: str) -> str:
        return f"{name:<11} {icon}  {detail:<22} {age}"

    scanner_age = _fmt_age(state.scanner_last_run)
    lines.append(_fmt_agent(
        "Scanner", "✅",
        f"{state.scanner_candidates_today} today",
        scanner_age,
    ))

    harvest_age = _fmt_age(last_harvest.run_at if last_harvest else None)
    lines.append(_fmt_agent(
        "Harvester", "✅",
        f"{token_count:,} tokens",
        harvest_age,
    ))

    analyst_age = _fmt_age(last_analyst.run_at if last_analyst else None)
    lines.append(_fmt_agent(
        "Wallets", "✅",
        f"{wallet_total} total ({wallet_tier1}T1/{wallet_tier2}T2/{wallet_tier3}T3)",
        analyst_age,
    ))

    try:
        gmgn = await get_gmgn_stats()
        gmgn_detail = (
            f"{gmgn['wallets']} wallets "
            f"({gmgn.get('tier1', 0)}T1/{gmgn.get('tier2', 0)}T2/{gmgn.get('tier3', 0)}T3) "
            f"{gmgn.get('trending', 0):,} trending"
        )
    except Exception:
        gmgn_detail = "starting..."
    lines.append(_fmt_agent("GMGN", "✅", gmgn_detail, "—"))

    pattern_age = _fmt_age(last_pattern_engine.run_at if last_pattern_engine else None)
    lines.append(_fmt_agent(
        "Patterns", "✅",
        f"{pattern_total} active",
        pattern_age,
    ))

    ce_icon = "✅" if autotrade else "⚙️"
    lines.append(_fmt_agent(
        "Confidence", ce_icon,
        f"{ce_stats.get('scored_today', 0)} scored",
        "—",
    ))

    regime = getattr(state, "market_regime", "NEUTRAL")
    ll_icon = "⚠️" if regime == "BAD" else "✅"
    ll_age = _fmt_age(state.learning_loop_last_run)
    lines.append(_fmt_agent(
        "Learning", ll_icon,
        f"{regime} regime",
        ll_age,
    ))

    try:
        cs = await get_chart_pattern_stats_today()
        chart_detail = f"{cs['detected']} detected"
    except Exception:
        chart_detail = "—"
    lines.append(_fmt_agent("Charts", "✅", chart_detail, "—"))

    # ── Performance (strategy-only, meta excluded) ──────────────────
    candidates_today = ce_stats.get("scored_today", 0) or state.scanner_candidates_today
    perf_today_pnl = paper_stats.get("today_strategy_pnl", 0.0)
    perf_alltime_pnl = paper_stats.get("strategy_pnl", 0.0)
    perf_wr = paper_stats.get("strategy_win_rate", 0)
    perf_closed = paper_stats.get("strategy_closed", 0)
    lines += [
        "",
        DIVIDER,
        "📊 PERFORMANCE",
        f"Today: {perf_today_pnl:+.2f} SOL  |  All Time: {perf_alltime_pnl:+.2f} SOL",
        f"Win Rate: {perf_wr}%  |  Closed: {perf_closed} bot  |  Candidates: {candidates_today}",
    ]

    # ── Paper trading breakdown (strategy-only, meta excluded) ──────
    p_open            = paper_stats.get("open_count", 0)
    p_today_n         = paper_stats.get("today_count", 0)
    p_today_strat_pnl = paper_stats.get("today_strategy_pnl", 0.0)
    p_today_strat_wr  = paper_stats.get("today_strategy_win_rate", 0)
    p_strat_wr        = paper_stats.get("strategy_win_rate", 0)
    p_strat_n         = paper_stats.get("strategy_closed", 0)
    p_strat_pnl       = paper_stats.get("strategy_pnl", 0.0)
    p_meta_pnl        = paper_stats.get("meta_pnl", 0.0)

    lines += [
        "",
        DIVIDER,
        f"📋 PAPER TRADING ({p_open} open)",
        f"Today: {p_today_n} trades  |  Win Rate: {p_today_strat_wr}%  |  {p_today_strat_pnl:+.2f} SOL",
        f"Bot-closed: {p_strat_n} trades  |  {p_strat_wr}% WR  |  {p_strat_pnl:+.2f} SOL",
        f"Manual: {p_meta_pnl:+.2f} SOL — not learned from",
        f"All Time: {p_strat_wr}% WR  |  {p_strat_pnl:+.2f} SOL",
    ]

    # ── Open trades ─────────────────────────────────────────────────
    open_trades = await get_open_paper_trades()
    if open_trades:
        live_mcs = await asyncio.gather(
            *[fetch_current_market_cap(pt.token_address) for pt in open_trades],
            return_exceptions=True,
        )

        # Total unrealized P&L across all open positions
        unrealized = 0.0
        for pt, live_mc in zip(open_trades, live_mcs):
            entry_mc = pt.entry_mc or 0
            current_mc = live_mc if isinstance(live_mc, (int, float)) and live_mc else 0
            if entry_mc > 0 and current_mc > 0:
                mult = current_mc / entry_mc
                unrealized += (pt.paper_sol_spent or 0) * (mult - 1)

        lines += [
            "",
            DIVIDER,
            f"📂 OPEN TRADES ({len(open_trades)})  |  Unrealized: {unrealized:+.2f} SOL",
        ]

        now = datetime.utcnow()
        for idx, (pt, live_mc) in enumerate(zip(open_trades, live_mcs), 1):
            raw_name = (pt.token_name or "?").replace("_", " ")
            entry_mc = pt.entry_mc or 0
            current_mc = live_mc if isinstance(live_mc, (int, float)) and live_mc else 0
            multiplier = (current_mc / entry_mc) if (entry_mc > 0 and current_mc > 0) else 0

            if multiplier >= 1.3:
                color = "🟢"
            elif multiplier >= 1.0:
                color = "🟡"
            else:
                color = "🔴"

            tp_mc = entry_mc * (pt.take_profit_x or 0) if entry_mc > 0 else 0
            sl_mc = entry_mc * (1 - (pt.stop_loss_pct or 0) / 100) if entry_mc > 0 else 0

            elapsed = now - pt.opened_at if pt.opened_at else None
            if elapsed is None:
                age = "—"
            else:
                mins = int(elapsed.total_seconds() // 60)
                if mins < 1:
                    age = "just now"
                elif mins < 60:
                    age = f"{mins}min ago"
                elif mins < 1440:
                    age = f"{mins // 60}h ago"
                else:
                    age = f"{mins // 1440}d ago"

            mult_str = f"{multiplier:.2f}x" if multiplier else "?"
            ca = pt.token_address or "?"

            lines.append("")
            lines.append(f"{idx}. ${_esc(raw_name)}  {mult_str} {color}")
            lines.append(f"   MC: {_format_usd(entry_mc)} → {_format_usd(current_mc)}")
            lines.append(
                f"   TP: {(pt.take_profit_x or 0):.1f}x ({_format_usd(tp_mc)})  "
                f"SL: {(pt.stop_loss_pct or 0):.0f}% ({_format_usd(sl_mc)})"
            )
            lines.append(f"   CA: <code>{_esc(ca)}</code>")
            lines.append(f"   Opened: {age}")

    # ── Top wallets ─────────────────────────────────────────────────
    lines += ["", DIVIDER, "🧠 TOP WALLETS"]
    top_wallets = await get_top_wallets(limit=5)
    if not top_wallets:
        lines.append("No wallets scored yet — Agent 2 is analyzing...")
    else:
        for i, w in enumerate(top_wallets, 1):
            short = f"{w.address[:4]}...{w.address[-4:]}"
            wr = int((w.win_rate or 0) * 100)
            wl_col = f"{w.wins}W-{w.losses}L"
            wtype = getattr(w, "wallet_type", None) or ""
            lines.append(
                f"#{i}  {_esc(short)}  Score:{w.score:.0f}  "
                f"{wl_col:<8} {wr}%  T{w.tier}  {_esc(wtype)}".rstrip()
            )

    # Summary footer — counts by wallet_type + clusters
    try:
        from database.models import (
            AsyncSessionLocal as _ASL, select as _select, func as _func,
            Wallet as _Wallet, get_all_wallet_clusters as _gacs,
        )
        async with _ASL() as _session:
            summary_total = (await _session.execute(
                _select(_func.count(_Wallet.address))
            )).scalar() or 0
            early_insider_count = (await _session.execute(
                _select(_func.count(_Wallet.address)).where(
                    _Wallet.wallet_type == "early_insider"
                )
            )).scalar() or 0
            coordinated_count = (await _session.execute(
                _select(_func.count(_Wallet.address)).where(
                    _Wallet.wallet_type == "coordinated_group"
                )
            )).scalar() or 0
        cluster_rows = await _gacs()
        cluster_total = len(cluster_rows)
    except Exception:
        summary_total = early_insider_count = coordinated_count = cluster_total = 0

    lines.append(
        f"👛 {summary_total} total  |  "
        f"{early_insider_count} early_insider  |  "
        f"{coordinated_count} coordinated  |  "
        f"{cluster_total} clusters"
    )

    # ── Recent trades ───────────────────────────────────────────────
    lines += ["", DIVIDER, "📋 RECENT TRADES"]
    reason_map = {
        "tp_hit":         "TP hit",
        "sl_hit":         "SL hit",
        "trail_hit":      "trail",
        "breakeven_stop": "BE stop",
        "profit_trail":   "prof trl",
        "stale":          "stale",
        "expired":        "expired",
        "manual_close":   "manual",
        "dead_token":     "dead",
    }
    recent = paper_stats.get("recent") or []
    if recent:
        for pt in recent[:5]:
            raw_name = (pt.token_name or "?").replace("_", " ")
            name_col = _esc(raw_name[:15])
            if pt.status == "open":
                lines.append(f"🟡 {name_col:<15}  open")
            elif pt.paper_pnl_sol and pt.paper_pnl_sol > 0:
                mult = f"{(pt.peak_multiple or 0):.1f}x"
                lines.append(f"✅ {name_col:<15}  {mult:<8} {pt.paper_pnl_sol:+.2f} SOL")
            else:
                reason = reason_map.get(pt.close_reason or "", pt.close_reason or "?")
                pnl = pt.paper_pnl_sol or 0
                lines.append(f"❌ {name_col:<15}  {reason:<8} {pnl:+.2f} SOL")
    else:
        lines.append("No recent trades yet")

    lines.append(DIVIDER)
    return "\n".join(lines)


# ── Subscriber-scoped /hub view ─────────────────────────────────────────────
# Renders only the subscriber's own ledger — their balance, their open trades,
# their recent closes, their win rate. Never shows HQ stats, agent status,
# scanner internals, or admin controls.

async def _build_subscriber_hub_text(sub) -> str:
    """Subscriber /hub — mirrors the HQ dashboard look but scoped to their
    own ledger only. HTML formatted with token names escaped so any
    pump.fun name with &, <, >, _, * renders cleanly."""
    import html as _html
    from database.models import (
        get_subscriber_paper_trade_stats, get_subscriber_paper_trades,
        get_keybot_settings as _kb,
    )
    from bot.scanner import fetch_current_market_cap

    DIVIDER = "━" * 30

    stats = await get_subscriber_paper_trade_stats(sub.telegram_id)
    trades = await get_subscriber_paper_trades(sub.telegram_id, limit=20)
    open_trades = [t for t in trades if t.status == "open"]
    closed_trades = [t for t in trades if t.status == "closed"][:5]

    balance = float(sub.paper_balance or 0.0)
    pnl_total = float(stats.get("total_pnl") or 0)
    starting = 20.0
    pnl_pct = ((balance - starting) / starting * 100) if starting > 0 else 0
    pnl_emoji = "🟢" if pnl_total >= 0 else "🔴"

    mode_word = "PAPER SIM" if (sub.trade_mode or "paper") == "paper" else "LIVE"

    sub_kb = await _kb(sub.telegram_id)
    dec = (sub_kb.decision_mode if sub_kb and sub_kb.decision_mode else "ai").lower()
    dec_badge = "🤖 AI" if dec == "ai" else "✋ Manual"

    lines = [
        DIVIDER,
        "🔑 <b>YOUR TRADING HUB</b>",
        f"Trade Mode: {mode_word}  |  Decision: {dec_badge}",
        f"Balance: <b>{balance:.2f}</b> / {starting:.0f} SOL  |  "
        f"P&amp;L: {pnl_emoji} {pnl_total:+.4f} SOL ({pnl_pct:+.1f}%)",
        DIVIDER,
        "",
        "📊 <b>PERFORMANCE</b>",
        f"Today / All Time: {pnl_total:+.4f} SOL",
        f"Win Rate: <b>{stats.get('win_rate', 0)}%</b>  |  "
        f"Wins: {stats.get('wins', 0)} / {stats.get('closed', 0)} closed",
        DIVIDER,
        "",
    ]

    if open_trades:
        unrealized = 0.0
        # Live MC fetch per trade for accurate unrealized PnL — same as HQ
        lines.append(f"📂 <b>OPEN TRADES ({len(open_trades)})</b>")
        lines.append("")
        for idx, pt in enumerate(open_trades, 1):
            entry_mc = pt.entry_mc or 0
            try:
                cur_mc = await fetch_current_market_cap(pt.token_address) or entry_mc
            except Exception:
                cur_mc = entry_mc
            mult = (cur_mc / entry_mc) if entry_mc > 0 else 1.0
            sol = pt.paper_sol_spent or 0
            unr = sol * (mult - 1)
            unrealized += unr
            emoji = "🟢" if mult >= 1.0 else ("🟡" if mult >= 0.95 else "🔴")
            name = _html.escape((pt.token_name or "?")[:24])
            entry_mc_str = (
                f"${entry_mc/1_000_000:.2f}M" if entry_mc >= 1_000_000
                else f"${entry_mc/1000:.1f}K"
            )
            cur_mc_str = (
                f"${cur_mc/1_000_000:.2f}M" if cur_mc >= 1_000_000
                else f"${cur_mc/1000:.1f}K"
            )
            tp_x = pt.take_profit_x or 0
            sl_pct = pt.stop_loss_pct or 0
            tp_mc = entry_mc * tp_x if entry_mc and tp_x else 0
            sl_mc = entry_mc * (1 - sl_pct / 100) if entry_mc else 0
            tp_mc_str = (
                f"${tp_mc/1_000_000:.2f}M" if tp_mc >= 1_000_000
                else f"${tp_mc/1000:.1f}K"
            )
            sl_mc_str = f"${sl_mc/1000:.1f}K"
            lines.append(f"{idx}. <b>${name}</b>  {mult:.2f}x {emoji}")
            lines.append(f"   MC: {entry_mc_str} → {cur_mc_str}")
            lines.append(f"   Size: {sol:.2f} SOL  |  Unr: {unr:+.4f} SOL")
            lines.append(f"   TP: {tp_x:.1f}x ({tp_mc_str})  SL: {sl_pct:.0f}% ({sl_mc_str})")
            lines.append(f"   <code>{pt.token_address or ''}</code>")
            lines.append("")
        lines.append(f"Unrealized total: <b>{unrealized:+.4f} SOL</b>")
        lines.append(DIVIDER)
        lines.append("")
    else:
        lines.append("📂 <b>OPEN TRADES</b>")
        lines.append("None — waiting for next signal")
        lines.append(DIVIDER)
        lines.append("")

    if closed_trades:
        lines.append("📋 <b>RECENT TRADES</b>")
        for pt in closed_trades:
            name = _html.escape((pt.token_name or "?")[:20])
            pnl = pt.paper_pnl_sol or 0
            mult = pt.peak_multiple or 0
            if pnl > 0:
                lines.append(f"✅ {name}  {mult:.1f}x peak  <b>{pnl:+.4f} SOL</b>")
            else:
                reason = (pt.close_reason or "?").replace("_", " ")
                lines.append(f"❌ {name}  {reason}  {pnl:+.4f} SOL")
        lines.append(DIVIDER)
        lines.append("")

    lines.append("👛 <b>YOUR WALLET</b>")
    lines.append(f"<code>{sub.wallet_address or '(none)'}</code>")
    lines.append(DIVIDER)

    return "\n".join(lines)


async def _subscriber_hub_keyboard(sub) -> InlineKeyboardMarkup:
    from database.models import get_subscriber_paper_trades
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="subhub:refresh"))

    open_trades = [
        t for t in await get_subscriber_paper_trades(sub.telegram_id, limit=20)
        if t.status == "open"
    ]
    for idx, pt in enumerate(open_trades[:5], 1):
        name = (pt.token_name or "?")[:15]
        builder.row(InlineKeyboardButton(
            text=f"❌ Close #{idx} — {name}",
            callback_data=f"subhub:close:{pt.id}",
        ))

    builder.row(
        InlineKeyboardButton(text="📊 My Stats", callback_data="subhub:stats"),
        InlineKeyboardButton(text="👛 My Wallet", callback_data="subhub:wallet"),
    )
    return builder.as_markup()


@router.callback_query(lambda c: c.data and c.data.startswith("subhub:"))
async def cb_subhub(callback: CallbackQuery):
    """Subscriber hub callbacks. Each action enforces ownership: a subscriber
    can only close their own trades, never anyone else's."""
    sub = await get_subscriber(callback.from_user.id)
    if sub is None or sub.status != "active":
        await callback.answer("⛔ Not an active subscriber.", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]

    if action == "refresh":
        await callback.answer("🔄 Refreshing…")
        try:
            text = await _build_subscriber_hub_text(sub)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _subscriber_hub_keyboard(sub),
            )
        except Exception:
            pass
        return

    if action.startswith("close:"):
        try:
            pt_id = int(action.split(":", 1)[1])
        except ValueError:
            await callback.answer("⚠️ Bad trade id")
            return
        from database.models import (
            AsyncSessionLocal, PaperTrade, select as _select, close_paper_trade,
            Subscriber as _Sub,
        )
        async with AsyncSessionLocal() as session:
            pt = (await session.execute(
                _select(PaperTrade).where(PaperTrade.id == pt_id)
            )).scalar_one_or_none()
        if pt is None or pt.subscriber_id != sub.telegram_id:
            await callback.answer("⛔ Not your trade.", show_alert=True)
            return
        if pt.status != "open":
            await callback.answer("Trade already closed.")
            return
        # Close at current MC
        from bot.scanner import fetch_current_market_cap
        try:
            cur_mc = await fetch_current_market_cap(pt.token_address) or (pt.entry_mc or 0)
        except Exception:
            cur_mc = pt.entry_mc or 0
        entry_mc = pt.entry_mc or 0
        mult = (cur_mc / entry_mc) if entry_mc > 0 else 1.0
        sol = pt.paper_sol_spent or 0
        pnl = round(sol * (mult - 1), 4)
        await close_paper_trade(pt.id, "manual_close", pnl, pt.peak_mc, pt.peak_multiple)
        # Refund subscriber balance
        async with AsyncSessionLocal() as session:
            s = (await session.execute(
                _select(_Sub).where(_Sub.telegram_id == sub.telegram_id)
            )).scalar_one_or_none()
            if s:
                s.paper_balance = round((s.paper_balance or 0) + sol + pnl, 4)
                s.paper_pnl = round((s.paper_pnl or 0) + pnl, 4)
                await session.commit()
                sub = s
        await callback.answer(f"Closed at {mult:.2f}x: {pnl:+.4f} SOL")
        try:
            text = await _build_subscriber_hub_text(sub)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _subscriber_hub_keyboard(sub),
            )
        except Exception:
            pass
        return

    if action == "stats":
        from database.models import get_subscriber_paper_trade_stats
        stats = await get_subscriber_paper_trade_stats(sub.telegram_id)
        await callback.answer(
            f"Closed: {stats['closed']}  |  Wins: {stats['wins']} ({stats['win_rate']}% WR)\n"
            f"PnL: {stats['total_pnl']:+.4f} SOL  |  Open: {stats['open_count']}",
            show_alert=True,
        )
        return

    if action == "wallet":
        await callback.answer(
            f"Wallet: {sub.wallet_address}\n"
            f"Balance: {sub.paper_balance:.2f} SOL ({sub.trade_mode})",
            show_alert=True,
        )
        return


@router.message(Command("hub"))
async def cmd_hub(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ /hub is only available in Callers HQ.")
        return

    # Subscriber DM: render their own scoped dashboard, never HQ data
    if message.chat.type == "private" and message.from_user.id not in ADMIN_IDS:
        from database.models import get_subscriber
        sub = await get_subscriber(message.from_user.id)
        if sub is None or sub.status != "active":
            await message.reply("⛔ Not an active subscriber. Send /start.")
            return
        try:
            text = await _build_subscriber_hub_text(sub)
            try:
                kb = await _subscriber_hub_keyboard(sub)
            except Exception as kb_exc:
                logger.error(
                    "Subscriber hub keyboard failed for %s: %s",
                    sub.telegram_id, kb_exc, exc_info=True,
                )
                kb = None
            await message.reply(text, parse_mode="HTML", reply_markup=kb)
        except Exception as exc:
            logger.error(
                "Subscriber hub render failed for %s: %s",
                sub.telegram_id, exc, exc_info=True,
            )
            try:
                import html as _h
                await message.reply(
                    f"Hub error: {_h.escape(str(exc))}", parse_mode="HTML",
                )
            except Exception:
                pass
        return

    try:
        text = await _build_hub_text(state.autotrade_enabled)
        await message.reply(
            text, parse_mode="HTML",
            reply_markup=await _hub_keyboard(state.autotrade_enabled),
        )
    except Exception as exc:
        logger.error("Hub render failed: %s", exc)
        await message.reply(f"⛔ Hub error: {exc}", parse_mode=None)


@router.callback_query(lambda c: c.data and c.data.startswith("hub:"))
async def cb_hub(callback: CallbackQuery):
    action = callback.data.split(":", 1)[1]

    if action == "refresh":
        await callback.answer("🔄 Refreshing…")
        text = await _build_hub_text(state.autotrade_enabled)
        try:
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass  # unchanged

    elif action == "toggle_paper":
        if state.trade_mode == "paper":
            state.trade_mode = "off"
            state.autotrade_enabled = False
            await set_param("trade_mode", 0, "Toggled off via hub")
            await callback.answer("📋 Paper trading OFF")
        else:
            state.trade_mode = "paper"
            state.autotrade_enabled = False
            await set_param("trade_mode", 1, "Toggled on via hub")
            await callback.answer("📋 Paper trading ON ✅")
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

    elif action == "live_locked":
        await callback.answer(
            "🔒 Live trading will be enabled after paper trading validation.\n"
            "Use /autotrade live when ready.",
            show_alert=True,
        )

    elif action == "toggle_extpost":
        from database.models import get_param as _get_param
        cur = await _get_param("external_ca_post_enabled")
        cur_on = cur is None or cur >= 0.5
        new_val = 0.0 if cur_on else 1.0
        await set_param(
            "external_ca_post_enabled", new_val,
            f"Toggled via /hub by admin {callback.from_user.id}",
        )
        await callback.answer(
            "📤 Share to external group: OFF" if cur_on else "📤 Share to external group: ON ✅"
        )
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

    elif action == "reset_confirm":
        await callback.answer()
        prompt = (
            f"💰 Reset paper balance to {state.PAPER_STARTING_BALANCE:.0f} SOL?\n\n"
            f"This will close every open position as 'reset' (PnL 0) and\n"
            f"nudge the offset so your effective balance is exactly\n"
            f"{state.PAPER_STARTING_BALANCE:.0f} SOL. Historical trades are preserved.\n\n"
            f"Yes / No?"
        )
        try:
            await callback.message.edit_text(
                prompt, parse_mode=None,
                reply_markup=_hub_reset_confirm_keyboard(),
            )
        except Exception:
            pass

    elif action == "reset_yes":
        closed, archived = await _do_reset_paper_balance("Manual reset via /hub button")
        await callback.answer(
            f"✅ Balance reset to {state.PAPER_STARTING_BALANCE:.0f} SOL",
            show_alert=False,
        )
        try:
            await callback.message.reply(
                f"✅ Paper balance reset to {state.PAPER_STARTING_BALANCE:.0f} SOL\n"
                f"Closed {closed} open position(s)\n"
                f"Archived {archived} historical trade(s) from strategy stats\n"
                f"All rows preserved — only stats reset.",
                parse_mode=None,
            )
        except Exception:
            pass
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

    elif action == "reset_no":
        await callback.answer("Reset cancelled")
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

    elif action == "close_all":
        await callback.answer("🗑️ Closing all open positions…")
        closed = await _manual_close_all_open(callback.bot)
        if closed == 0:
            await callback.message.reply("ℹ️ No open positions to close.")
        else:
            await callback.message.reply(f"🗑️ Closed {closed} open position(s).")
        # Refresh the hub so Open Paper Trades block reflects the closures
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

    elif action.startswith("close_trade:"):
        trade_id = int(action.split(":")[1])
        await callback.answer("Closing trade...")
        try:
            from database.models import close_paper_trade, AsyncSessionLocal, PaperTrade
            # Get trade for PnL calc
            async with AsyncSessionLocal() as session:
                pt = await session.get(PaperTrade, trade_id)
            if pt and pt.status == "open":
                entry_mc = pt.entry_mc or 0
                current_mc = 0
                try:
                    live_mc = await fetch_current_market_cap(pt.token_address)
                    if live_mc:
                        current_mc = live_mc
                except Exception:
                    pass
                mult = current_mc / entry_mc if entry_mc > 0 and current_mc > 0 else 1.0
                remaining = float(getattr(pt, "remaining_pct", 100) or 100)
                realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)
                remaining_sol = (pt.paper_sol_spent or 0) * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (mult - 1), 4)
                await close_paper_trade(pt.id, "manual_close", pnl, pt.peak_mc, pt.peak_multiple)
                emoji = "🟢" if pnl >= 0 else "🔴"
                name = (pt.token_name or "?")[:20]
                await callback.message.reply(f"{emoji} Closed {name} | {mult:.2f}x | {pnl:+.4f} SOL")
            else:
                await callback.message.reply("Trade already closed.")
        except Exception as exc:
            await callback.message.reply(f"Close failed: {exc}")
        # Refresh hub
        try:
            text = await _build_hub_text(state.autotrade_enabled)
            await callback.message.edit_text(
                text, parse_mode="HTML",
                reply_markup=await _hub_keyboard(state.autotrade_enabled),
            )
        except Exception:
            pass

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
                    f"#{i} {short} | Score: {w.score:.0f} | "
                    f"{w.wins}W {w.losses}L | {w.win_rate * 100:.0f}% | {w.avg_multiple:.1f}x | T{w.tier}"
                + (f" | {w.source}" if getattr(w, "source", None) else "")
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
            "📋 PAPER TRADING RESULTS",
            f"Total: {ps['total']} | Win rate: {ps['win_rate']}%",
            f"Paper P&L: {ps['total_pnl']:+.4f} SOL",
            f"Open: {ps['open_count']} | Today: {ps['today_count']}",
            "",
        ]
        if ps["recent"]:
            lines.append("Recent:")
            for pt in ps["recent"]:
                n = pt.token_name or "?"
                flag = ""
                if pt.sold_too_early:
                    flag = " SOLD TOO EARLY"
                elif pt.sold_too_late:
                    flag = " SOLD TOO LATE"
                if pt.paper_pnl_sol and pt.paper_pnl_sol > 0:
                    lines.append(f"  ✅ {n} — {pt.peak_multiple or 0:.1f}x +{pt.paper_pnl_sol:.4f} SOL{flag}")
                else:
                    lines.append(f"  ❌ {n} — {pt.close_reason} {pt.paper_pnl_sol or 0:.4f} SOL{flag}")
        else:
            lines.append("No paper trades yet")
        await callback.message.reply("\n".join(lines), parse_mode=None)

    else:
        await callback.answer()


# ── /wallets — Top Scored Wallets ─────────────────────────────────────────────

@router.message(Command("wallets"))
async def cmd_wallets(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ /wallets is only available in Callers HQ.")
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
        wtype = getattr(w, "wallet_type", None) or "unknown"
        cluster = getattr(w, "cluster_id", None)
        # Wrap wtype + cluster in backticks so their underscores don't
        # trigger Telegram Markdown italics and break the whole message
        cluster_suffix = f" [{cluster}]" if cluster else ""
        lines.append(
            f"#{i} {short} | Score: {w.score:.0f} | "
            f"{w.wins}W {w.losses}L | {w.win_rate * 100:.0f}% | Tier {w.tier} | "
            f"{wtype}{cluster_suffix}"
        )
    await message.reply("\n".join(lines), parse_mode="Markdown")


# ── /autotrade off|paper|live ─────────────────────────────────────────────────

@router.message(Command("autotrade"))
async def cmd_autotrade(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ /autotrade is only available in Callers HQ.")
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Only admins can toggle autotrade.")
        return

    parts = (message.text or "").split()
    valid_modes = ("off", "paper", "live", "on")
    if len(parts) < 2 or parts[1].lower() not in valid_modes:
        await message.reply(
            f"⚡ Trade mode: *{_mode_label()}*\n"
            f"Usage: /autotrade off | /autotrade paper | /autotrade live",
            parse_mode="Markdown",
        )
        return

    mode = parts[1].lower()
    if mode == "on":
        mode = "live"  # legacy compat

    state.trade_mode = mode
    state.autotrade_enabled = (mode == "live")
    mode_val = {"off": 0, "paper": 1, "live": 2}[mode]
    await set_param("trade_mode", mode_val, f"Set via /autotrade {mode}")

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

# ── /nukepaper — DESTRUCTIVE full paper trade wipe ─────────────────────────

@router.message(Command("nukepaper"))
async def cmd_nukepaper(message: Message):
    """
    DESTRUCTIVE: wipe all paper_trades rows, reset balance to 20 SOL,
    zero ai_trade_params sample counts. Use only when the historical
    data has been polluted (compounding-size bug, manual-close lookahead,
    etc.) and a clean slate is cheaper than filtering.

    Dry-run default — shows what would be deleted without touching the
    DB. Pass confirm as the argument to actually do it.

    Usage:
      /nukepaper            → dry-run preview
      /nukepaper confirm    → actually wipe
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    parts = (message.text or "").split()
    confirmed = len(parts) >= 2 and parts[1].lower() == "confirm"

    from database.models import (
        AsyncSessionLocal, select, func,
        PaperTrade, AITradeParams,
        nuke_paper_trades, reset_ai_trade_params_samples,
    )

    # Always show the preview counts first
    async with AsyncSessionLocal() as session:
        open_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
        )).scalar() or 0
        closed_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "closed")
        )).scalar() or 0
        atp_count = (await session.execute(
            select(func.count(AITradeParams.id))
        )).scalar() or 0

    total = open_count + closed_count

    if not confirmed:
        lines = [
            "🔥 NUKE PAPER (DRY RUN — nothing deleted)",
            "━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            f"Would delete {total} paper_trades rows:",
            f"  {open_count} open",
            f"  {closed_count} closed",
            "",
            f"Would zero sample_size / win_rate / avg_multiple / "
            f"confidence on {atp_count} ai_trade_params rows",
            "(preserves tp/sl/trail config — just tells Agent 6 'no data')",
            "",
            "Would reset:",
            "  paper_starting_balance → 20.0",
            "  paper_balance_offset   → 0.0",
            "  state.paper_balance    → 20.0",
            "  state.pending_candidates cleared",
            "",
            "⚠️ This is destructive and cannot be undone.",
            "Run /nukepaper confirm to actually execute.",
        ]
        await message.reply("\n".join(lines), parse_mode=None)
        return

    # Confirmed — execute
    try:
        nuke_result = await nuke_paper_trades()
        atp_reset = await reset_ai_trade_params_samples()
    except Exception as exc:
        logger.exception("nukepaper failed")
        await message.reply(f"❌ nukepaper error: {exc}", parse_mode=None)
        return

    # Clear in-memory state so the next scanner tick doesn't
    # dedupe against stale mints
    state.pending_candidates.clear()
    state.paper_balance = 20.0
    state.PAPER_STARTING_BALANCE = 20.0
    state.learning_loop_last_analyzed = 0
    state.paper_trades_today = 0

    # Safety refresh of the computed balance so /hub reads clean
    try:
        state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    except Exception:
        pass

    lines = [
        "🔥 NUKE PAPER — EXECUTED",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"Deleted {nuke_result['deleted_total']} paper_trades rows",
        f"  {nuke_result['deleted_open']} open",
        f"  {nuke_result['deleted_closed']} closed",
        "",
        f"Zeroed samples on {atp_reset} ai_trade_params rows",
        "",
        f"Balance reset: {state.paper_balance:.4f} SOL / "
        f"{state.PAPER_STARTING_BALANCE:.0f} SOL",
        f"pending_candidates: cleared",
        f"learning_loop_last_analyzed: 0",
        "",
        "Next scanner tick will treat every candidate as fresh.",
        "Agent 6 will start learning from a clean slate.",
    ]
    logger.warning(
        "NUKE PAPER executed by %s: deleted=%d, atp_reset=%d",
        message.from_user.id, nuke_result["deleted_total"], atp_reset,
    )
    await message.reply("\n".join(lines), parse_mode=None)


# ── /report — What Agent 6 has learned ───────────────────────────────────────

@router.message(Command("report"))
async def cmd_report(message: Message):
    """Full learning report: what the system has learned from trading."""
    from database.models import (
        AsyncSessionLocal, PaperTrade, AITradeParams,
        get_all_trade_params,
    )
    from sqlalchemy import select, func

    try:
        async with AsyncSessionLocal() as session:
            # Total stats
            total = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.status == "closed")
            )).scalar() or 0

            wins = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol > 0,
                )
            )).scalar() or 0

            total_pnl = (await session.execute(
                select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0)).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol.isnot(None),
                )
            )).scalar() or 0

            # By source
            source_rows = (await session.execute(
                select(
                    PaperTrade.pattern_type,
                    func.count(PaperTrade.id).label("cnt"),
                    func.sum(func.cast(PaperTrade.paper_pnl_sol > 0, Integer)).label("wins"),
                    func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0).label("pnl"),
                    func.coalesce(func.avg(PaperTrade.peak_multiple), 1).label("avg_peak"),
                ).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol.isnot(None),
                ).group_by(PaperTrade.pattern_type)
                .order_by(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0).desc())
                .limit(15)
            )).all()

            # Best trades
            best_trades = (await session.execute(
                select(PaperTrade.token_name, PaperTrade.peak_multiple, PaperTrade.paper_pnl_sol)
                .where(PaperTrade.status == "closed", PaperTrade.paper_pnl_sol > 0)
                .order_by(PaperTrade.paper_pnl_sol.desc())
                .limit(5)
            )).all()

            # Worst trades
            worst_trades = (await session.execute(
                select(PaperTrade.token_name, PaperTrade.peak_multiple, PaperTrade.paper_pnl_sol)
                .where(PaperTrade.status == "closed", PaperTrade.paper_pnl_sol < 0)
                .order_by(PaperTrade.paper_pnl_sol.asc())
                .limit(5)
            )).all()

            # Close reasons
            reason_rows = (await session.execute(
                select(
                    PaperTrade.close_reason,
                    func.count(PaperTrade.id).label("cnt"),
                    func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0).label("pnl"),
                ).where(
                    PaperTrade.status == "closed",
                    PaperTrade.close_reason.isnot(None),
                ).group_by(PaperTrade.close_reason)
                .order_by(func.count(PaperTrade.id).desc())
            )).all()

            # Avg hold time for wins vs losses
            avg_win_hold = (await session.execute(
                select(func.avg(
                    func.extract('epoch', PaperTrade.closed_at) - func.extract('epoch', PaperTrade.opened_at)
                )).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol > 0,
                    PaperTrade.closed_at.isnot(None),
                )
            )).scalar() or 0

            avg_loss_hold = (await session.execute(
                select(func.avg(
                    func.extract('epoch', PaperTrade.closed_at) - func.extract('epoch', PaperTrade.opened_at)
                )).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol <= 0,
                    PaperTrade.closed_at.isnot(None),
                )
            )).scalar() or 0

        # Get learned TP/SL params
        trade_params = await get_all_trade_params()

        wr = (wins / total * 100) if total > 0 else 0

        lines = [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "<b>🧠 AGENT 6 LEARNING REPORT</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            f"<b>OVERVIEW</b>",
            f"Trades: {total}  |  Wins: {wins}  |  WR: {wr:.0f}%",
            f"Total PnL: {float(total_pnl):+.2f} SOL",
            f"Avg win hold: {avg_win_hold / 60:.0f} min  |  Avg loss hold: {avg_loss_hold / 60:.0f} min",
        ]

        # Source performance
        if source_rows:
            lines += ["", "<b>BY SOURCE</b>"]
            for row in source_rows:
                tags = (row.pattern_type or "unknown")[:25]
                cnt = row.cnt
                w = row.wins or 0
                pnl = float(row.pnl)
                avg_pk = float(row.avg_peak)
                src_wr = (w / cnt * 100) if cnt > 0 else 0
                emoji = "🟢" if pnl > 0 else "🔴"
                lines.append(
                    f"{emoji} {tags}: {cnt} trades | {src_wr:.0f}% WR | {pnl:+.2f} SOL | avg peak {avg_pk:.1f}x"
                )

        # Close reasons
        if reason_rows:
            lines += ["", "<b>CLOSE REASONS</b>"]
            for row in reason_rows:
                reason = row.close_reason or "?"
                pnl = float(row.pnl)
                emoji = "🟢" if pnl > 0 else "🔴"
                lines.append(f"{emoji} {reason}: {row.cnt}x | {pnl:+.2f} SOL")

        # Best trades
        if best_trades:
            lines += ["", "<b>TOP 5 WINNERS</b>"]
            for t in best_trades:
                name = (t.token_name or "?")[:18]
                lines.append(f"🏆 {name} | {t.peak_multiple:.1f}x peak | +{t.paper_pnl_sol:.2f} SOL")

        # Worst trades
        if worst_trades:
            lines += ["", "<b>TOP 5 LOSSES</b>"]
            for t in worst_trades:
                name = (t.token_name or "?")[:18]
                lines.append(f"💀 {name} | {t.peak_multiple:.1f}x peak | {t.paper_pnl_sol:.2f} SOL")

        # Learned TP/SL per pattern
        if trade_params:
            trained = [p for p in trade_params if (p.sample_size or 0) >= 3]
            if trained:
                lines += ["", "<b>LEARNED PARAMS (3+ samples)</b>"]
                trained.sort(key=lambda p: p.sample_size or 0, reverse=True)
                for p in trained[:10]:
                    lines.append(
                        f"  {p.pattern_type}: TP={p.optimal_tp_x:.1f}x SL={p.optimal_sl_pct:.0f}% "
                        f"WR={p.win_rate * 100:.0f}% n={p.sample_size}"
                    )

        lines += ["", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]

        await message.reply("\n".join(lines), parse_mode="HTML")

    except Exception as exc:
        logger.error("Report command failed: %s", exc)
        await message.reply(f"Report failed: {exc}")


# ── /forcenuke — Immediate paper trade wipe, no confirmation ──────────────

# ── /close <number> — Close individual paper trade ───────────────────────────

@router.message(Command("close"))
async def cmd_close_paper_trade(message: Message):
    """Close a specific paper trade by its position number from /hub."""
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /close <number>\nGet the number from /hub open trades list.")
        return

    try:
        idx = int(args[1]) - 1  # hub shows 1-indexed
    except ValueError:
        await message.reply("Invalid number. Use /close 1, /close 2, etc.")
        return

    open_trades = await get_open_paper_trades()
    if not open_trades or idx < 0 or idx >= len(open_trades):
        await message.reply(f"No open trade at position {idx + 1}. You have {len(open_trades)} open trades.")
        return

    pt = open_trades[idx]
    entry_mc = pt.entry_mc or 0
    current_mc = 0

    # Get live MC for PnL calculation
    try:
        live_mc = await fetch_current_market_cap(pt.token_address)
        if live_mc:
            current_mc = live_mc
    except Exception:
        pass

    mult = current_mc / entry_mc if entry_mc > 0 and current_mc > 0 else 1.0
    remaining = float(getattr(pt, "remaining_pct", 100) or 100)
    realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)
    remaining_sol = (pt.paper_sol_spent or 0) * (remaining / 100.0)
    pnl = round(realized + remaining_sol * (mult - 1), 4)

    from database.models import close_paper_trade
    await close_paper_trade(pt.id, "manual_close", pnl, pt.peak_mc, pt.peak_multiple)

    emoji = "🟢" if pnl >= 0 else "🔴"
    name = (pt.token_name or "?")[:20]
    await message.reply(
        f"{emoji} Closed #{idx + 1} — {name}\n"
        f"PnL: {pnl:+.4f} SOL | {mult:.2f}x"
    )


@router.message(Command("forcenuke"))
async def cmd_forcenuke(message: Message):
    """
    DESTRUCTIVE + IMMEDIATE. Wipes all paper_trades, zeros ai_trade_params
    samples, resets balance to 20 SOL. No dry-run, no confirm gate, runs
    the instant you send the command. Use this when /nukepaper's dry-run
    flow is too slow or you just want it gone.

    Performs the SQL equivalent of:
      DELETE FROM paper_trades;
      UPDATE agent_params SET param_value=0  WHERE param_name='paper_balance_offset';
      UPDATE agent_params SET param_value=20 WHERE param_name='paper_starting_balance';
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        from database.models import (
            AsyncSessionLocal, select, func,
            PaperTrade, AITradeParams, AgentParam,
        )

        # Step 1: count + delete
        async with AsyncSessionLocal() as session:
            before = (await session.execute(
                select(func.count(PaperTrade.id))
            )).scalar() or 0
            await session.execute(PaperTrade.__table__.delete())
            await session.commit()

        # Step 2: zero ai_trade_params samples (preserve tp/sl/trail config)
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(select(AITradeParams))).scalars().all()
            atp_count = 0
            for r in rows:
                r.sample_size = 0
                r.win_rate = 0.0
                r.avg_multiple = 1.0
                r.confidence = 0.0
                r.updated_at = datetime.utcnow()
                atp_count += 1
            await session.commit()

        # Step 3: reset balance params
        async with AsyncSessionLocal() as session:
            for name, val in [
                ("paper_balance_offset", 0.0),
                ("paper_starting_balance", 20.0),
            ]:
                row = (await session.execute(
                    select(AgentParam).where(AgentParam.param_name == name)
                )).scalar_one_or_none()
                if row is None:
                    session.add(AgentParam(param_name=name, param_value=val))
                else:
                    row.param_value = val
                    row.updated_at = datetime.utcnow()
            await session.commit()

        # Step 4: clear in-memory state so next scanner/monitor tick
        # doesn't act on stale cached data
        state.pending_candidates.clear()
        state.paper_balance = 20.0
        state.PAPER_STARTING_BALANCE = 20.0
        state.paper_trades_today = 0
        state.learning_loop_last_analyzed = 0

        # Step 5: verify from DB
        async with AsyncSessionLocal() as session:
            after = (await session.execute(
                select(func.count(PaperTrade.id))
            )).scalar() or 0
            real_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)

        state.paper_balance = real_balance

    except Exception as exc:
        logger.exception("forcenuke failed")
        await message.reply(f"❌ forcenuke error: {exc}", parse_mode=None)
        return

    lines = [
        "💥 FORCE NUKE — EXECUTED",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"paper_trades before: {before}",
        f"paper_trades after : {after}  {'OK' if after == 0 else 'LEAKED ' + str(after)}",
        "",
        f"ai_trade_params samples zeroed: {atp_count}",
        "",
        "Balance:",
        f"  paper_starting_balance = 20.0",
        f"  paper_balance_offset   = 0.0",
        f"  computed balance       = {real_balance:.4f} SOL",
        "",
        "In-memory state:",
        f"  state.paper_balance   = {state.paper_balance:.4f}",
        f"  paper_trades_today    = 0",
        f"  pending_candidates    = cleared",
        "",
        "Next scanner tick will see a fully clean DB.",
    ]
    logger.warning(
        "FORCE NUKE executed by %s: deleted=%d → after=%d, atp=%d, bal=%.4f",
        message.from_user.id, before, after, atp_count, real_balance,
    )
    await message.reply("\n".join(lines), parse_mode=None)


# ── /forcecheck — Trigger paper_monitor._check_open_trades immediately ────

@router.message(Command("forcecheck"))
async def cmd_forcecheck(message: Message):
    """
    Kicks paper_monitor._check_open_trades() right now instead of waiting
    for the next POLL_INTERVAL tick. Fetches live MC for every open paper
    trade and evaluates TP/trail/SL/dead checks. Reports how many closed.
    Use when you suspect SL isn't firing between ticks.
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        from bot.agents.paper_monitor import _check_open_trades
        from database.models import AsyncSessionLocal, select, func, PaperTrade

        async with AsyncSessionLocal() as session:
            before = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
            )).scalar() or 0

        await _check_open_trades(message.bot)

        async with AsyncSessionLocal() as session:
            after = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
            )).scalar() or 0

    except Exception as exc:
        logger.exception("forcecheck failed")
        await message.reply(f"❌ forcecheck error: {exc}", parse_mode=None)
        return

    closed_count = before - after
    lines = [
        "🔄 PAPER MONITOR FORCE CHECK",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"Open trades before: {before}",
        f"Open trades after : {after}",
        f"Closed this run   : {closed_count}",
        "",
        "Check Railway logs for per-trade 'Paper check id=...' lines",
        "showing current_mult vs sl_threshold for every open trade.",
    ]
    await message.reply("\n".join(lines), parse_mode=None)


# ── /scannerwhy — Diagnose why scanner isn't opening paper trades ─────────

@router.message(Command("scannerwhy"))
async def cmd_scannerwhy(message: Message):
    """
    Shows the state of every gate that could block a paper trade open.
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        from database.models import (
            AsyncSessionLocal, select, func,
            AgentParam, Candidate, PaperTrade,
        )
        from datetime import timedelta

        now = datetime.utcnow()
        recent_cutoff = now - timedelta(hours=2)
        cooldown_cutoff = now - timedelta(hours=24)

        async with AsyncSessionLocal() as session:
            trade_mode_val = (await session.execute(
                select(AgentParam.param_value).where(AgentParam.param_name == "trade_mode")
            )).scalar_one_or_none()

            conf_thresh = (await session.execute(
                select(AgentParam.param_value).where(AgentParam.param_name == "conf_paper_threshold")
            )).scalar_one_or_none()

            cooldown_hours = (await session.execute(
                select(AgentParam.param_value).where(AgentParam.param_name == "manual_close_cooldown_hours")
            )).scalar_one_or_none()

            open_count = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
            )).scalar() or 0

            recent_manual_close = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.close_reason == "manual_close",
                    PaperTrade.closed_at >= cooldown_cutoff,
                )
            )).scalar() or 0

            recent_candidates = (await session.execute(
                select(Candidate)
                .where(Candidate.created_at >= recent_cutoff)
                .order_by(Candidate.id.desc())
                .limit(10)
            )).scalars().all()

    except Exception as exc:
        logger.exception("scannerwhy failed")
        await message.reply(f"❌ scannerwhy error: {exc}", parse_mode=None)
        return

    tm_int = int(trade_mode_val or 0)
    mode_name = {0: "off", 1: "paper", 2: "live"}.get(tm_int, "?")
    thresh_val = float(conf_thresh) if conf_thresh is not None else 20.0
    cooldown_val = float(cooldown_hours) if cooldown_hours is not None else 24.0

    lines = [
        "🔍 SCANNER DIAGNOSTIC",
        "━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"trade_mode (DB)           = {tm_int} ({mode_name})  "
        f"{'OK' if tm_int == 1 else 'WRONG - must be paper=1'}",
        f"state.trade_mode (memory) = {state.trade_mode}",
        f"conf_paper_threshold      = {thresh_val:.0f}",
        f"manual_close_cooldown_hr  = {cooldown_val:.1f}",
        "",
        f"Open paper trades: {open_count}",
        f"Manual closes in last 24h: {recent_manual_close}",
        "",
    ]

    if not recent_candidates:
        lines += [
            "No Candidate rows in the last 2h.",
            "Scanner may not be finding anything OR crashing before scoring.",
        ]
    else:
        lines.append("LAST 10 CANDIDATES SCORED (past 2h):")
        lines.append("")
        for c in recent_candidates:
            name = (c.token_name or "?")[:18]
            conf = c.confidence_score or 0
            pass_mark = "OK" if conf >= thresh_val else "--"
            icon = {
                "execute_full": "FULL",
                "execute_half": "HALF",
                "monitor":      "MON ",
                "discard":      "DISC",
            }.get(c.decision, "?   ")
            lines.append(
                f"{icon} {name:<18} conf={conf:>5.1f} {pass_mark} "
                f"src={(c.source or '?')[:10]}"
            )

    lines += [
        "",
        "If stuck, fixes:",
        "  trade_mode != 1 → /autotrade paper",
        "  all conf < thresh → /setparam conf_paper_threshold 15",
        "  high recent manual_close → wait or /setparam manual_close_cooldown_hours 0",
    ]

    await message.reply("\n".join(lines), parse_mode=None)


# ── /closedcheck — Raw state of the last 10 paper_trades rows ─────────────

@router.message(Command("closedcheck"))
async def cmd_closedcheck(message: Message):
    """
    Dumps the last 10 paper_trades rows regardless of status so you can
    verify whether manually-closed trades actually moved to status='closed'
    in the DB (vs being re-opened, duplicated, or stuck open).
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        from database.models import get_recent_paper_trades
        rows = await get_recent_paper_trades(limit=10)
    except Exception as exc:
        logger.exception("closedcheck failed")
        await message.reply(f"❌ closedcheck error: {exc}", parse_mode=None)
        return

    if not rows:
        await message.reply("No paper_trades rows yet.", parse_mode=None)
        return

    now = datetime.utcnow()
    lines = [
        "🔎 LAST 10 PAPER_TRADES ROWS",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for pt in rows:
        mint_short = (pt.token_address or "?")[:8]
        name = (pt.token_name or "?")[:18]
        status = pt.status or "?"
        reason = pt.close_reason or "-"
        pnl = pt.paper_pnl_sol
        pnl_str = f"{pnl:+7.3f}" if pnl is not None else "  none "
        sol = pt.paper_sol_spent or 0
        opened = pt.opened_at.strftime("%H:%M:%S") if pt.opened_at else "?"
        closed = pt.closed_at.strftime("%H:%M:%S") if pt.closed_at else "-"

        icon = {"open": "🟡", "closed": "✅" if (pnl or 0) > 0 else "❌"}.get(status, "?")
        lines.append(
            f"{icon} id={pt.id:<4} {mint_short}..  {name:<18} "
            f"{status:<6} {reason:<13} sol={sol:>5.2f} "
            f"pnl={pnl_str}  o={opened} c={closed}"
        )

    lines.append("")
    lines.append("icon: 🟡=open  ✅=closed win  ❌=closed loss/flat")
    await message.reply("\n".join(lines), parse_mode=None)


# ── /balancecheck — Audit paper balance math + show today's closes ─────────

@router.message(Command("balancecheck"))
async def cmd_balancecheck(message: Message):
    """
    Full audit of the paper balance calculation. Shows the exact math:
      starting − sum(open.paper_sol_spent) + sum(closed.paper_pnl_sol) + offset

    Then dumps today's closed trades (top 15 winners + bottom 5 losers)
    so the +X SOL today figure can be verified against the underlying rows.
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        from database.models import (
            AsyncSessionLocal, select, func, PaperTrade, AgentParam,
        )

        now = datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        async with AsyncSessionLocal() as session:
            starting_row = (await session.execute(
                select(AgentParam.param_value).where(
                    AgentParam.param_name == "paper_starting_balance"
                )
            )).scalar_one_or_none()
            starting = float(starting_row) if starting_row is not None else state.PAPER_STARTING_BALANCE

            offset_row = (await session.execute(
                select(AgentParam.param_value).where(
                    AgentParam.param_name == "paper_balance_offset"
                )
            )).scalar_one_or_none()
            offset = float(offset_row) if offset_row is not None else 0.0

            open_locked = (await session.execute(
                select(func.coalesce(func.sum(PaperTrade.paper_sol_spent), 0.0))
                .where(PaperTrade.status == "open")
            )).scalar() or 0.0
            open_count = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
            )).scalar() or 0

            closed_pnl_total = (await session.execute(
                select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0))
                .where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
            )).scalar() or 0.0
            closed_count = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
            )).scalar() or 0

            today_pnl = (await session.execute(
                select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0))
                .where(
                    PaperTrade.closed_at >= today_start,
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
            )).scalar() or 0.0
            today_count = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.closed_at >= today_start,
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
            )).scalar() or 0

            # Top 15 winners + bottom 5 losers (descending, then tail)
            today_winners = (await session.execute(
                select(PaperTrade)
                .where(
                    PaperTrade.closed_at >= today_start,
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
                .order_by(PaperTrade.paper_pnl_sol.desc())
                .limit(15)
            )).scalars().all()

            today_losers = (await session.execute(
                select(PaperTrade)
                .where(
                    PaperTrade.closed_at >= today_start,
                    PaperTrade.paper_pnl_sol.is_not(None),
                )
                .order_by(PaperTrade.paper_pnl_sol.asc())
                .limit(5)
            )).scalars().all()

        computed_balance = round(
            float(starting) - float(open_locked) + float(closed_pnl_total) + float(offset),
            4,
        )

        lines = [
            "💰 PAPER BALANCE AUDIT",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            "Formula: starting − open_locked + closed_pnl + offset",
            "",
            f"  starting    = {float(starting):>+11.4f} SOL",
            f"  open_locked = {float(open_locked):>+11.4f} SOL ({open_count} trades)",
            f"  closed_pnl  = {float(closed_pnl_total):>+11.4f} SOL ({closed_count} trades)",
            f"  offset      = {float(offset):>+11.4f} SOL",
            "  ─────────────────────────",
            f"  computed    = {computed_balance:>+11.4f} SOL",
            "",
            f"state.paper_balance = {state.paper_balance:.4f} SOL",
            "",
            f"Today: {today_count} closed trades, total = {float(today_pnl):+.4f} SOL",
        ]

        if computed_balance != round(state.paper_balance, 4):
            lines.append(f"⚠️ In-memory cache diverges from DB by "
                         f"{state.paper_balance - computed_balance:+.4f}")

        def _render(pt):
            pnl = pt.paper_pnl_sol or 0
            sol = pt.paper_sol_spent or 0
            entry_mc = pt.entry_mc or 0
            peak_mc = pt.peak_mc or 0
            peak_mult = pt.peak_multiple or 0
            implied = (peak_mc / entry_mc) if (entry_mc and peak_mc) else 0
            name = (pt.token_name or "?")[:18]
            icon = "✅" if pnl > 0 else "❌"
            return (
                f"{icon} {name:<18} sol={sol:>5.2f} peak={peak_mult:>4.1f}x "
                f"impl={implied:>4.1f}x {(pt.close_reason or '?'):<11} "
                f"pnl={pnl:>+7.3f}"
            )

        if today_winners:
            lines += ["", f"TOP WINNERS (up to 15 of {today_count}):"]
            lines += [_render(pt) for pt in today_winners]

        if today_losers and today_count > 15:
            # Only show separate losers block if there are enough trades
            # that we didn't already show all of them in winners
            lines += ["", f"WORST LOSERS (up to 5 of {today_count}):"]
            lines += [_render(pt) for pt in today_losers]

        # Running sum sanity check over what we fetched
        fetched = list(today_winners) + [
            pt for pt in today_losers if pt.id not in {w.id for w in today_winners}
        ]
        fetched_sum = sum((pt.paper_pnl_sol or 0) for pt in fetched)
        lines += ["", f"Fetched-rows sum: {fetched_sum:+.4f} SOL "
                      f"({len(fetched)} of {today_count} rows)"]

        text = "\n".join(lines)
        # Telegram hard limit is 4096 chars — chunk if needed
        if len(text) <= 4000:
            await message.reply(text, parse_mode=None)
        else:
            # Split on blank lines to keep sections together
            chunks = []
            buf = []
            for line in lines:
                buf.append(line)
                if sum(len(x) + 1 for x in buf) > 3800:
                    chunks.append("\n".join(buf))
                    buf = []
            if buf:
                chunks.append("\n".join(buf))
            for chunk in chunks:
                await message.reply(chunk, parse_mode=None)

    except Exception as exc:
        logger.exception("balancecheck failed")
        try:
            await message.reply(f"❌ balancecheck error: {exc}", parse_mode=None)
        except Exception:
            pass


# ── /tradeparams — Dump ai_trade_params table ───────────────────────────────

@router.message(Command("tradeparams"))
async def cmd_tradeparams(message: Message):
    """
    Dumps the current contents of ai_trade_params so you can verify
    whether Agent 6 is actually mutating TP/SL per pattern_type. Shows
    sample_size, tp_x, sl_pct, win_rate, avg_multiple, updated_at for
    every row sorted by most-recently-updated.
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from database.models import AsyncSessionLocal, select, AITradeParams

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(AITradeParams).order_by(AITradeParams.updated_at.desc())
        )).scalars().all()

    if not rows:
        await message.reply("No ai_trade_params rows yet.", parse_mode=None)
        return

    now = datetime.utcnow()
    lines = [
        "🎯 AI TRADE PARAMS (per pattern_type)",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    for r in rows:
        age_min = 0
        if r.updated_at:
            age_min = int((now - r.updated_at).total_seconds() / 60)
        if age_min < 1:
            age = "just now"
        elif age_min < 60:
            age = f"{age_min}m"
        elif age_min < 1440:
            age = f"{age_min // 60}h{age_min % 60:02d}m"
        else:
            age = f"{age_min // 1440}d"

        trail_str = ""
        if int(getattr(r, "trail_sl_enabled", 0) or 0) == 1:
            trail_str = f" trail@{(r.trail_sl_trigger_pct or 0.5) * 100:.0f}%"

        lines.append(
            f"{r.pattern_type:<20} tp={r.optimal_tp_x:.2f}x sl={r.optimal_sl_pct:.0f}% "
            f"pos={r.optimal_position_pct:.1f}% "
            f"n={r.sample_size} wr={r.win_rate * 100:.0f}% "
            f"avg={r.avg_multiple:.2f}x{trail_str} ({age})"
        )

    await message.reply("\n".join(lines), parse_mode=None)


# ── /whyloss — Diagnostic: why did the last N closed paper trades lose? ────

@router.message(Command("whyloss"))
async def cmd_whyloss(message: Message):
    """
    Dumps the last 10 (or N) closed paper trades with every signal that
    fired at entry time plus what actually happened. Used to figure out
    why the confidence engine is scoring losing tokens highly — strategy
    diagnosis, not code debugging.
    """
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    parts = (message.text or "").split()
    n = 10
    if len(parts) >= 2:
        try:
            n = max(1, min(30, int(parts[1])))
        except ValueError:
            pass

    from database.models import (
        AsyncSessionLocal, select, PaperTrade, Candidate,
    )

    async with AsyncSessionLocal() as session:
        closed_trades = (await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
            .order_by(PaperTrade.closed_at.desc())
            .limit(n)
        )).scalars().all()

    if not closed_trades:
        await message.reply("No closed paper trades yet.", parse_mode=None)
        return

    lines = [
        f"🔬 LAST {len(closed_trades)} CLOSED PAPER TRADES",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    wins = 0
    losses = 0
    for pt in closed_trades:
        # Look up the most-recent Candidate row for this token (the one
        # that triggered the trade, or close to it)
        async with AsyncSessionLocal() as session:
            cand = (await session.execute(
                select(Candidate)
                .where(Candidate.token_address == pt.token_address)
                .order_by(Candidate.id.desc())
                .limit(1)
            )).scalar_one_or_none()

        name = (pt.token_name or "?")[:24]
        conf = pt.confidence_score or 0
        pnl = pt.paper_pnl_sol or 0
        peak = pt.peak_multiple or 1.0
        reason = pt.close_reason or "?"
        tags = pt.pattern_type or "-"
        # Truncate tags to one line
        if len(tags) > 70:
            tags = tags[:67] + "..."

        icon = "✅" if pnl > 0 else "❌"
        if pnl > 0:
            wins += 1
        else:
            losses += 1

        # Component scores (may be None if Candidate row is stale or missing)
        if cand is not None:
            chart = cand.chart_score or 0
            insider = cand.insider_score or 0
            rug = cand.rug_score or 0
            caller = cand.caller_score or 0
            fp = cand.fingerprint_score or 0
            chart_pat = cand.chart_pattern or "-"
            scores_line = (
                f"   chart={chart:.0f} insider={insider:.0f} rug={rug:.0f} "
                f"caller={caller:.0f} fp={fp:.0f}"
            )
            pattern_line = f"   chart_pattern: {chart_pat}"
        else:
            scores_line = "   (no Candidate row found — scores unavailable)"
            pattern_line = None

        header = f"{icon} {name}  conf={conf:.0f}  peak={peak:.2f}x  {reason}  pnl={pnl:+.4f}"
        lines.append(header)
        lines.append(scores_line)
        if pattern_line:
            lines.append(pattern_line)
        lines.append(f"   tags: {tags}")
        lines.append("")

    # Summary
    total = wins + losses
    wr = (wins / total * 100) if total else 0
    lines.append(f"Summary: {wins}W {losses}L | {wr:.0f}% win rate")

    # Diagnostic hints
    lines.append("")
    lines.append("Look for:")
    lines.append("  • chart>75 + insider<30 losing: chart pattern overweight")
    lines.append("  • rug<50 losing: rugcheck gate too loose")
    lines.append("  • conf<40 triggered anyway: paper_threshold too low")
    lines.append("  • same tags on losers: that combo is losing")

    # Plain text — no markdown — so we don't have to escape token names
    await message.reply("\n".join(lines), parse_mode=None)


@router.message(Command("papertrades"))
async def cmd_papertrades(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ /papertrades is only available in Callers HQ.")
        return

    ps = await get_paper_trade_stats()
    lines = [
        "📋 PAPER TRADING RESULTS",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Total: {ps['total']} | Combined WR: {ps['win_rate']}% | "
        f"Combined PnL: {ps['total_pnl']:+.2f} SOL",
        "",
        "STRATEGY (what Agent 6 learns from):",
        f"  Closed: {ps.get('strategy_closed', 0)} | "
        f"WR: {ps.get('strategy_win_rate', 0)}% | "
        f"PnL: {ps.get('strategy_pnl', 0.0):+.2f} SOL",
        "",
        "META (manual_close + reset, excluded from learning):",
        f"  PnL: {ps.get('meta_pnl', 0.0):+.2f} SOL "
        f"(today: {ps.get('today_meta_pnl', 0.0):+.2f})",
        "",
        f"Open: {ps['open_count']} | Today: {ps['today_count']} "
        f"strat {ps.get('today_strategy_pnl', 0.0):+.2f} / "
        f"meta {ps.get('today_meta_pnl', 0.0):+.2f} SOL",
        "",
    ]
    if ps["recent"]:
        lines.append("Recent trades:")
        for pt in ps["recent"]:
            n = pt.token_name or "?"
            flag = ""
            if pt.sold_too_early:
                peak_x = (pt.peak_after_close or 0) / (pt.entry_mc or 1) if pt.entry_mc else 0
                flag = f" | peaked {peak_x:.1f}x after SOLD TOO EARLY"
            elif pt.sold_too_late:
                flag = " | SOLD TOO LATE (recovered)"

            if pt.paper_pnl_sol and pt.paper_pnl_sol > 0:
                lines.append(f"  ✅ {n} — {pt.peak_multiple or 0:.1f}x +{pt.paper_pnl_sol:.4f} SOL{flag}")
            else:
                lines.append(f"  ❌ {n} — {pt.close_reason} {pt.paper_pnl_sol or 0:.4f} SOL{flag}")
    else:
        lines.append("No paper trades yet. Set mode to PAPER: /autotrade paper")

    # Post-close summary
    from database.models import get_post_close_stats
    pc = await get_post_close_stats()
    if pc["total_tracked"] > 0:
        lines += [
            "",
            "Post-close analysis:",
            f"  Tracked: {pc['total_tracked']} | Sold early: {pc['early_pct']}% | Sold late: {pc['late_pct']}%",
        ]

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
        await message.reply("⛔ /patterns is only available in Callers HQ.")
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
            f"TP: {p.optimal_tp_x:.1f}x | SL: {p.optimal_sl_pct:.0f}% | Size: {p.optimal_position_pct:.0f}% wallet",
            f"Win rate: {p.win_rate * 100:.0f}% | Avg: {p.avg_multiple:.1f}x | Sample: {p.sample_size} trades",
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
                f"  {name} — {cw['win_rate']}% WR | {cw['trades']} trades"
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

        # Get token creation time from DexScreener pair data
        created_at_ms = pair.get("pairCreatedAt")
        early_buyers = await _get_early_buyers(
            address, window_minutes=10, created_at_ms=created_at_ms,
        )
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
            f"⚖️ Weight Set: {scored.get('weight_set', 'default')}",
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
        await message.reply("⛔ /analyze is only available in Callers HQ.")
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


# ── /params — Show all agent parameters ──────────────────────────────────────

@router.message(Command("params"))
async def cmd_params(message: Message):
    params = await get_all_params()
    changes = await get_recent_param_changes(10)

    # Group params by category
    groups = {
        "Scanner": [k for k in sorted(params) if k.startswith("scanner_")],
        "Confidence": [k for k in sorted(params) if k.startswith("conf_")],
        "Weights (Low MC)": [k for k in sorted(params) if k.startswith("low_mc_")],
        "Weights (Mid MC)": [k for k in sorted(params) if k.startswith("mid_mc_")],
        "Weights (High MC)": [k for k in sorted(params) if k.startswith("high_mc_")],
        "Position Sizing": [k for k in sorted(params) if k.startswith("size_")],
        "Wallet Tiers": [k for k in sorted(params) if k.startswith("tier")],
        "Learning": [k for k in sorted(params) if k.startswith("learning_")],
        "Other": [k for k in sorted(params) if not any(
            k.startswith(p) for p in ("scanner_", "conf_", "low_mc_", "mid_mc_", "high_mc_",
                                       "size_", "tier", "learning_"))],
    }

    lines = ["🧠 AGENT PARAMETERS", "━━━━━━━━━━━━━━━━━━━━", ""]
    for group_name, keys in groups.items():
        if not keys:
            continue
        lines.append(f"[{group_name}]")
        for k in keys:
            v = params[k]
            # Format nicely
            if "weight" in k or "mc_" in k or "winrate" in k:
                lines.append(f"  {k}: {v:.4f}")
            elif "threshold" in k or "score" in k or "batch" in k:
                lines.append(f"  {k}: {v:.0f}")
            else:
                lines.append(f"  {k}: {v:g}")
        lines.append("")

    if changes:
        lines.append("RECENT CHANGES (last 10)")
        for c in changes:
            age = ""
            if c.changed_at:
                mins = int((datetime.utcnow() - c.changed_at).total_seconds() / 60)
                age = f"{mins}m ago" if mins < 60 else f"{mins // 60}h ago"
            reason = (c.reason or "")[:40]
            lines.append(f"  {c.param_name}: {c.old_value:g}->{c.new_value:g} | {reason} | {age}")

    await message.reply("\n".join(lines), parse_mode=None)


# ── /deepanalyze <address> — Full launch fingerprint + wallet extraction ──────

@router.message(Command("deepanalyze"))
async def cmd_deepanalyze(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("Only available in Callers HQ.")
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Usage: /deepanalyze <contract_address>", parse_mode=None)
        return

    address = parts[1].strip()
    if len(address) < 32:
        await message.reply("Invalid address.", parse_mode=None)
        return

    status = await message.reply("🔬 Running deep analysis... (this takes ~30 seconds)", parse_mode=None)

    try:
        # ── 1. Fetch DexScreener data ────────────────────────────────────
        pair = await fetch_token_data(address)
        if not pair:
            v2_url = f"https://api.dexscreener.com/tokens/v1/solana/{address}"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                async with s.get(v2_url) as resp:
                    if resp.status == 200:
                        v2 = await resp.json(content_type=None)
                        pl = v2 if isinstance(v2, list) else v2.get("pairs") or []
                        if pl:
                            pl.sort(key=lambda p: (p.get("liquidity") or {}).get("usd", 0), reverse=True)
                            pair = pl[0]
        if not pair:
            await status.edit_text("Token not found on DexScreener.", parse_mode=None)
            return

        metrics = parse_token_metrics(pair)
        name = metrics.get("name", "Unknown")
        symbol = metrics.get("symbol", "???")
        mcap = metrics.get("market_cap", 0) or 0
        liquidity = metrics.get("liquidity_usd", 0) or 0
        created_at_ms = pair.get("pairCreatedAt")

        # ── 2. Find early buyers (retry + pair fallback + DexScreener) ────
        early_wallet_list = await _get_early_buyers(
            address, window_minutes=10, created_at_ms=created_at_ms,
        )
        early_wallets: set[str] = set(early_wallet_list)

        # Also get sig count + volume via direct Helius call for stats
        from bot.helius import get_signatures_for_address as _get_sigs_helius
        all_sigs = await _get_sigs_helius(address, limit=200, label="deep_analyze")
        valid_sigs = [s for s in all_sigs if s.get("blockTime") and not s.get("err")]

        launch_ts = (created_at_ms // 1000) if created_at_ms else (
            min(s["blockTime"] for s in valid_sigs) if valid_sigs else 0
        )

        # Estimate early volume from parsed transactions
        early_sol_volume = 0.0
        if early_wallet_list:
            cutoff_10m = launch_ts + 600
            early_sig_ids = [s["signature"] for s in valid_sigs if s["blockTime"] <= cutoff_10m][:50]
            if early_sig_ids:
                early_parsed = await _parse_transactions(early_sig_ids)
                for tx in early_parsed:
                    for nt in (tx.get("nativeTransfers") or []):
                        early_sol_volume += abs(nt.get("amount", 0)) / 1e9

        # ── 3. Check tracked wallets ─────────────────────────────────────
        tier_wallets = await get_tier_wallets(max_tier=3)
        known_set = {w.address for w in tier_wallets}
        insider_matches = [w for w in early_wallets if w in known_set]

        # ── 5. Score and save all early buyer wallets ────────────────────
        new_wallets = 0
        for wallet in early_wallets:
            if wallet in known_set:
                continue
            score, tier = _score_wallet(wins=1, losses=0, total_trades=1,
                                        avg_multiple=5.0, early_entry_rate=1.0)
            if tier > 0:
                await upsert_wallet(
                    address=wallet, score=score, tier=tier,
                    win_rate=1.0, avg_multiple=5.0,
                    wins=1, losses=0, total_trades=1,
                    avg_entry_mcap=mcap * 0.01 if mcap else None,  # estimate launch MC
                )
                new_wallets += 1

        # ── 6. Time to 2x ────────────────────────────────────────────────
        # Find when MC first hit 2x of launch estimate
        launch_mc_est = mcap * 0.01 if mcap else 0  # rough: current MC / 100 = launch MC
        # We can't get historical MC from sigs alone, so estimate from pair age

        # ── 7. Save mega_runner pattern ──────────────────────────────────
        launch_mc = launch_mc_est or 50000  # fallback estimate
        await upsert_pattern(
            pattern_type="mega_runner",
            outcome_threshold="50x",
            sample_count=1,
            avg_entry_mcap=launch_mc,
            mcap_range_low=launch_mc * 0.5,
            mcap_range_high=launch_mc * 2.0,
            avg_liquidity=liquidity * 0.1 if liquidity else 5000,
            min_liquidity=3000,
            avg_ai_score=75,
            avg_rugcheck_score=5,
            best_hours=None,
            best_days=None,
            confidence_score=90.0,
        )

        # ── 8. Rugcheck ──────────────────────────────────────────────────
        from bot.agents.scanner_agent import _fetch_rugcheck
        rc_data = await _fetch_rugcheck(address)
        rc_score = (rc_data or {}).get("score", "?")

        # ── 9. Social links ──────────────────────────────────────────────
        token_row = await get_token_by_mint(address)
        socials = "None"
        if token_row and token_row.social_links:
            import json as _json
            try:
                sl = _json.loads(token_row.social_links)
                parts_s = []
                if sl.get("twitter"): parts_s.append("Twitter")
                if sl.get("telegram"): parts_s.append("Telegram")
                if sl.get("website"): parts_s.append("Website")
                socials = ", ".join(parts_s) if parts_s else "None"
            except Exception:
                pass

        # ── Build report ─────────────────────────────────────────────────
        from datetime import timezone
        launch_dt = datetime.fromtimestamp(launch_ts, tz=timezone.utc) if launch_ts else None
        launch_str = launch_dt.strftime("%Y-%m-%d %H:%M UTC") if launch_dt else "Unknown"

        mc_str = _format_usd(mcap) if mcap else "?"
        liq_str = _format_usd(liquidity) if liquidity else "?"
        launch_mc_str = _format_usd(launch_mc) if launch_mc else "?"

        report = "\n".join([
            "🔬 DEEP ANALYSIS",
            "━━━━━━━━━━━━━━━━━━━━",
            f"🪙 {name} (${symbol})",
            f"📍 {address}",
            "",
            "📅 LAUNCH DATA",
            f"  Created: {launch_str}",
            f"  Total transactions: {len(valid_sigs)}",
            f"  Est. launch MC: {launch_mc_str}",
            f"  Current MC: {mc_str} | Liq: {liq_str}",
            f"  Rugcheck risk: {rc_score}",
            f"  Socials: {socials}",
            "",
            "👛 FIRST 10 MIN BUYERS",
            f"  Unique wallets: {len(early_wallets)}",
            f"  Early buyers: {len(early_wallets)}",
            f"  Est. early volume: {early_sol_volume:.1f} SOL",
            f"  Known insiders: {len(insider_matches)}",
            *([f"    {a[:6]}..{a[-4:]}" for a in insider_matches[:5]] if insider_matches else []),
            f"  New wallets scored: {new_wallets}",
            "",
            "🧩 PATTERN SAVED",
            f"  Type: mega_runner (50x+ outcome)",
            f"  Launch MC range: {_format_usd(launch_mc * 0.5)}–{_format_usd(launch_mc * 2.0)}",
            f"  Confidence: 90/100",
            "",
            f"Analysis complete — {len(early_wallets)} wallets + pattern saved",
        ])

        # Save token to DB if not there
        if not await token_exists(address):
            await save_token(
                mint=address, name=name, symbol=symbol,
                price_usd=metrics.get("price_usd"), market_cap=mcap,
                liquidity_usd=liquidity, volume_24h=metrics.get("volume_24h"),
                source="deep_analyze",
            )

        await status.edit_text(report, parse_mode=None)

    except Exception as exc:
        logger.error("Deep analyze failed for %s: %s", address, exc)
        await status.edit_text(f"Deep analysis failed: {exc}", parse_mode=None)


# ── /dbcheck — Inspect token database ─────────────────────────────────────────

@router.message(Command("dbcheck"))
async def cmd_dbcheck(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from database.models import get_all_tokens, AsyncSessionLocal, select, func, Token

    async with AsyncSessionLocal() as session:
        total = (await session.execute(select(func.count(Token.mint)))).scalar() or 0
        has_mc = (await session.execute(
            select(func.count(Token.mint)).where(Token.market_cap > 0)
        )).scalar() or 0
        has_launch = (await session.execute(
            select(func.count(Token.mint)).where(Token.launch_mc > 0)
        )).scalar() or 0
        no_mc = total - has_mc

        # Sample 5 tokens with MC
        samples_mc = (await session.execute(
            select(Token).where(Token.market_cap > 0).order_by(Token.first_seen_at.desc()).limit(5)
        )).scalars().all()

        # Sample 5 tokens without MC
        samples_nomc = (await session.execute(
            select(Token).where(Token.market_cap.is_(None) | (Token.market_cap == 0))
            .order_by(Token.first_seen_at.desc()).limit(5)
        )).scalars().all()

    lines = [
        "🔍 DATABASE CHECK",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Total tokens: {total}",
        f"With market_cap > 0: {has_mc}",
        f"With launch_mc > 0: {has_launch}",
        f"No market_cap: {no_mc}",
        "",
        "Tokens WITH MC (newest 5):",
    ]
    for t in samples_mc:
        lmc = getattr(t, "launch_mc", None)
        lines.append(f"  {(t.name or t.symbol or '?')[:15]} | MC: ${t.market_cap or 0:,.0f} | Launch: ${lmc or 0:,.0f} | {t.source or '?'}")

    lines.append("")
    lines.append("Tokens WITHOUT MC (newest 5):")
    for t in samples_nomc:
        lines.append(f"  {(t.name or t.symbol or '?')[:15]} | {t.source or '?'} | {t.mint[:12]}...")

    # Fetch live MC for the first 3 tokens with MC
    lines.append("")
    lines.append("Live DexScreener check (3 tokens):")
    for t in samples_mc[:3]:
        pair = await fetch_token_data(t.mint)
        if pair:
            m = parse_token_metrics(pair)
            live_mc = m.get("market_cap", 0) or 0
            stored = t.market_cap or 0
            lmc = getattr(t, "launch_mc", None) or 0
            mult = live_mc / lmc if lmc > 0 else 0
            lines.append(f"  {(t.name or '?')[:12]} | Stored: ${stored:,.0f} | Launch: ${lmc:,.0f} | Live: ${live_mc:,.0f} | {mult:.1f}x")
        else:
            lines.append(f"  {(t.name or '?')[:12]} | Not found on DexScreener")

    await message.reply("\n".join(lines), parse_mode=None)


# ── /resetbalance — Reset paper trading virtual balance ───────────────────────

async def _do_reset_paper_balance(reason: str) -> tuple[int, int]:
    """
    Shared reset logic used by both /resetbalance and the /hub button.

    Three steps:
      1. Close every open paper trade as "reset" (PnL=0).
      2. Reclassify every historical strategy-close (tp_hit, sl_hit,
         trail_hit, dead_token, breakeven_stop, profit_trail) to "reset"
         so strategy_pnl / strategy_win_rate aggregates fall back to
         zero. Rows are preserved for audit, just dropped from the
         learning corpus.
      3. Nudge paper_balance_offset so compute_paper_balance() returns
         exactly PAPER_STARTING_BALANCE.

    Returns (closed_open_count, archived_history_count).
    """
    from database.models import (
        get_open_paper_trades, close_paper_trade, get_param,
        reclassify_strategy_history_as_reset,
    )

    open_trades = await get_open_paper_trades()
    for pt in open_trades:
        await close_paper_trade(pt.id, "reset", 0.0, pt.peak_mc, pt.peak_multiple)

    archived = await reclassify_strategy_history_as_reset()

    current = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    current_offset = await get_param("paper_balance_offset")
    new_offset = round(current_offset + (state.PAPER_STARTING_BALANCE - current), 4)
    await set_param("paper_balance_offset", new_offset, reason)

    state.paper_balance = state.PAPER_STARTING_BALANCE
    state.paper_resets += 1
    state.paper_trades_today = 0

    return len(open_trades), archived


@router.message(Command("resetbalance"))
async def cmd_resetbalance(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    closed, archived = await _do_reset_paper_balance("Manual reset via /resetbalance")
    await message.reply(
        f"✅ Balance reset to {state.PAPER_STARTING_BALANCE:.0f} SOL\n"
        f"Closed {closed} open position(s)\n"
        f"Archived {archived} historical trade(s) from strategy stats\n"
        f"All rows preserved — only stats reset",
        parse_mode=None,
    )


# ── /mccheck — Debug MC repair ────────────────────────────────────────────────

@router.message(Command("mccheck"))
async def cmd_mccheck(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    try:
        await _do_mccheck(message)
    except Exception as exc:
        await message.reply(f"mccheck error: {exc}", parse_mode=None)


async def _do_mccheck(message: Message):
    from database.models import count_tokens_no_mc, AsyncSessionLocal, select, func, Token

    # Quick stats first
    no_mc = await count_tokens_no_mc()
    async with AsyncSessionLocal() as session:
        total = (await session.execute(select(func.count(Token.mint)))).scalar() or 0
        dead = (await session.execute(
            select(func.count(Token.mint)).where(Token.source == "dead")
        )).scalar() or 0

        # Get 3 tokens with no MC
        tokens = (await session.execute(
            select(Token).where(
                (Token.market_cap.is_(None)) | (Token.market_cap == 0),
                Token.source != "dead",
            ).order_by(Token.first_seen_at.desc()).limit(3)
        )).scalars().all()

    lines = [
        f"Total: {total} | No MC: {no_mc} | Dead: {dead}",
        "",
    ]

    for tok in tokens:
        name = (tok.name or "?")[:12]
        age = (datetime.utcnow() - tok.first_seen_at).days if tok.first_seen_at else "?"
        lines.append(f"{name} | {age}d | {tok.source or '?'}")
        lines.append(f"{tok.mint}")

        # DexScreener only (fast)
        try:
            pair = await fetch_token_data(tok.mint)
            if pair:
                m = parse_token_metrics(pair)
                lines.append(f"  Dex: MC=${m.get('market_cap',0) or 0:,.0f}")
            else:
                lines.append(f"  Dex: no pairs")
        except Exception as e:
            lines.append(f"  Dex: {e}")

        lines.append("")

    await message.reply("\n".join(lines), parse_mode=None)


# ── /backfill — Full token backfill (all tokens, batched) ─────────────────────

@router.message(Command("backfill"))
async def cmd_backfill(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return
    if state.backfill_running:
        await message.reply(f"Backfill already running: {state.backfill_progress}", parse_mode=None)
        return

    total = await get_token_count()
    status = await message.reply(f"🔄 Starting backfill of {total} tokens...", parse_mode=None)
    asyncio.create_task(_run_backfill(message.bot, status))


@router.message(Command("backfill_status"))
async def cmd_backfill_status(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return
    running = "🟢 Running" if state.backfill_running else "⚪ Not running"
    await message.reply(f"{running}\n{state.backfill_progress}", parse_mode=None)


async def _run_backfill(bot, status_msg) -> None:
    """
    Background task: process ALL tokens in batches of 200.

    For each token:
    - If launch_mc missing: use stored market_cap as launch_mc
    - Fetch current MC from DexScreener
    - If DexScreener returns nothing: mark as dead
    - If current_mc / launch_mc >= 2: winner → extract wallets + save pattern
    """
    state.backfill_running = True
    state.backfill_progress = "Starting..."

    try:
        total = await get_token_count()
        offset = 0
        batch_size = 200
        stats = {"processed": 0, "dex_found": 0, "dead": 0, "winners": 0,
                 "wallets": 0, "repaired_mc": 0, "errors": 0, "skipped_no_mc": 0}

        while offset < total:
            batch = await get_tokens_batch(offset, batch_size)
            if not batch:
                break

            for tok in batch:
                stats["processed"] += 1

                try:
                    # Step 1: Determine launch_mc
                    launch_mc = getattr(tok, "launch_mc", None)
                    if not launch_mc or launch_mc <= 0:
                        # Use stored market_cap as launch proxy
                        launch_mc = tok.market_cap
                        if launch_mc and launch_mc > 0:
                            await set_token_launch_mc(tok.mint, launch_mc)
                            stats["repaired_mc"] += 1

                    if not launch_mc or launch_mc <= 0:
                        stats["skipped_no_mc"] += 1
                        continue

                    # Step 2: Fetch current MC from DexScreener
                    pair = await fetch_token_data(tok.mint)

                    if not pair:
                        stats["dead"] += 1
                        continue

                    metrics = parse_token_metrics(pair)
                    current_mc = metrics.get("market_cap", 0) or 0

                    if current_mc <= 0:
                        stats["dead"] += 1
                        continue

                    stats["dex_found"] += 1

                    # Update current MC in DB
                    await update_token_market_cap(tok.mint, current_mc)

                    # Step 3: Calculate multiple
                    multiple = current_mc / launch_mc

                    # Step 4: Winner detection
                    if multiple >= 2.0:
                        stats["winners"] += 1
                        name = tok.name or tok.symbol or tok.mint[:12]
                        created_at_ms = pair.get("pairCreatedAt")

                        # Extract early buyers
                        early_buyers = await _get_early_buyers(
                            tok.mint, window_minutes=10, created_at_ms=created_at_ms,
                        )

                        # Score and save new wallets
                        tier_wallets = await get_tier_wallets(max_tier=3)
                        known = {w.address for w in tier_wallets}
                        batch_wallets = 0

                        for wallet in early_buyers:
                            if wallet in known:
                                continue
                            score, tier = _score_wallet(
                                wins=1, losses=0, total_trades=1,
                                avg_multiple=multiple, early_entry_rate=1.0,
                            )
                            if tier > 0:
                                await upsert_wallet(
                                    address=wallet, score=score, tier=tier,
                                    win_rate=1.0, avg_multiple=round(multiple, 2),
                                    wins=1, losses=0, total_trades=1,
                                    avg_entry_mcap=launch_mc,
                                )
                                batch_wallets += 1
                                stats["wallets"] += 1

                        # Save winner pattern
                        liq = metrics.get("liquidity_usd", 0) or 0
                        threshold = "10x" if multiple >= 10 else "5x" if multiple >= 5 else "2x"
                        await upsert_pattern(
                            pattern_type=f"winner_{threshold}",
                            outcome_threshold=threshold,
                            sample_count=stats["winners"],
                            avg_entry_mcap=launch_mc,
                            mcap_range_low=launch_mc * 0.5,
                            mcap_range_high=launch_mc * 2.0,
                            avg_liquidity=liq * 0.5 if liq else 5000,
                            min_liquidity=3000,
                            avg_ai_score=70,
                            avg_rugcheck_score=5,
                            best_hours=None, best_days=None,
                            confidence_score=min(95, 50 + multiple * 5),
                        )

                        logger.info(
                            "Backfill WINNER: %s %.1fx launch=$%.0f now=$%.0f buyers=%d wallets=%d",
                            name, multiple, launch_mc, current_mc, len(early_buyers), batch_wallets,
                        )

                    # Rate limit
                    await asyncio.sleep(0.3)

                except Exception as exc:
                    stats["errors"] += 1
                    logger.debug("Backfill error %s: %s", tok.mint[:12], exc)

            offset += batch_size

            # Progress update
            state.backfill_progress = (
                f"{stats['processed']}/{total} | "
                f"DexOK:{stats['dex_found']} Dead:{stats['dead']} NoMC:{stats['skipped_no_mc']} | "
                f"Winners:{stats['winners']} Wallets:{stats['wallets']}"
            )
            try:
                await status_msg.edit_text(f"🔄 {state.backfill_progress}", parse_mode=None)
            except Exception:
                pass

        # Final report
        report = "\n".join([
            "✅ BACKFILL COMPLETE",
            "━━━━━━━━━━━━━━━━━━━━",
            f"Tokens processed: {stats['processed']}/{total}",
            f"DexScreener found: {stats['dex_found']}",
            f"Dead/no data: {stats['dead']}",
            f"No launch MC (skipped): {stats['skipped_no_mc']}",
            f"Launch MC repaired: {stats['repaired_mc']}",
            f"Winners found (2x+): {stats['winners']}",
            f"New wallets added: {stats['wallets']}",
            f"Errors: {stats['errors']}",
        ])

        state.backfill_progress = report
        await status_msg.edit_text(report, parse_mode=None)

        try:
            await bot.send_message(CALLER_GROUP_ID, report)
        except Exception:
            pass

        logger.info("Backfill complete: %s", stats)

    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        state.backfill_progress = f"Failed: {exc}"
        try:
            await status_msg.edit_text(f"Backfill failed: {exc}", parse_mode=None)
        except Exception:
            pass
    finally:
        state.backfill_running = False


# ── /testgmgn — Test GMGN API connection ─────────────────────────────────────

@router.message(Command("testgmgn"))
async def cmd_testgmgn(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from bot.agents.gmgn_agent import (
        gmgn_trending, gmgn_smart_money_trades, gmgn_token_info,
        _run_cli, _poll_gmgn_tokens,
    )
    from bot.config import GMGN_API_KEY

    status = await message.reply("Testing GMGN API...", parse_mode=None)
    lines = ["🔬 GMGN API TEST", "━━━━━━━━━━━━━━━━━━━━"]

    key_preview = GMGN_API_KEY[:15] + "..." if GMGN_API_KEY else "NOT SET"
    lines.append(f"Key: {key_preview}")

    # Test 1: gmgn-cli available?
    try:
        proc = await asyncio.create_subprocess_exec(
            "npx", "gmgn-cli", "--version",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        ver = stdout.decode().strip()
        lines.append(f"gmgn-cli: ✅ v{ver}" if ver else "gmgn-cli: ✅ installed")
    except Exception as exc:
        lines.append(f"gmgn-cli: ❌ {exc}")

    # Test 2: Trending via CLI
    try:
        tokens = await gmgn_trending(interval="1h", limit=3)
        lines.append(f"Trending: {'✅' if tokens else '❌'} {len(tokens)} tokens")
        if tokens:
            t = tokens[0]
            lines.append(f"  #1: {t.get('symbol')} MC=${t.get('market_cap',0):,.0f}")
    except Exception as exc:
        lines.append(f"Trending: ❌ {exc}")

    # Test 3: Smart money trades
    try:
        trades = await gmgn_smart_money_trades(limit=3)
        lines.append(f"Smart trades: {'✅' if trades else '❌'} {len(trades)}")
        if trades:
            t = trades[0]
            sym = (t.get("base_token") or {}).get("symbol") or "?"
            lines.append(f"  Last: {t.get('side','?').upper()} ${sym}")
    except Exception as exc:
        lines.append(f"Smart trades: ❌ {exc}")

    # Test 4: Token info
    try:
        info = await gmgn_token_info("EgiJdQ8dbQHWu1uKS6cQBPaF2sK3a7WFpkjHEgdDpump")
        if info:
            lines.append(f"Token info: ✅ {info.get('symbol','?')} ${info.get('price',0)}")
        else:
            lines.append("Token info: ❌ no data")
    except Exception as exc:
        lines.append(f"Token info: ❌ {exc}")

    # Test 5: Poll and save
    try:
        saved = await _poll_gmgn_tokens()
        lines.append(f"Token poll: {saved} new saved")
    except Exception as exc:
        lines.append(f"Token poll: FAILED — {exc}")

    lines.append(f"\nGMGN today: {state.harvester_gmgn_today}")

    await status.edit_text("\n".join(lines), parse_mode=None)


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(Command("test"))
async def cmd_test(message: Message):
    await message.reply(f"Bot works. Your ID: {message.from_user.id}")


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start in DMs — show subscriber info or welcome message."""
    uid = message.from_user.id

    from database.models import get_subscriber

    sub = await get_subscriber(uid)
    if sub:
        if sub.status == "active":
            await message.reply(
                f"Welcome back!\n\n"
                f"Wallet: {sub.wallet_address}\n"
                f"Mode: {sub.trade_mode}\n"
                f"Balance: {sub.paper_balance:.2f} SOL\n\n"
                f"Commands:\n"
                f"/hub — your dashboard (open trades, PnL, win rate)\n"
                f"/mywallet — wallet info\n"
                f"/myperformance — your stats\n"
                f"/keybot — trading settings (AI/Manual mode)"
            )
        else:
            await message.reply("Your account is suspended. Contact admin.")
    else:
        # Check if admin pre-approved this user
        if uid in ADMIN_IDS:
            await message.reply(
                f"Welcome admin!\n"
                f"Your ID: {uid}\n\n"
                f"Use /hub for dashboard\n"
                f"Use /adduser <id> to add subscribers"
            )
        else:
            await message.reply(
                "Welcome to Revolt AI Trading.\n\n"
                "This bot requires a subscription.\n"
                "Contact the admin to get access.\n\n"
                f"Your ID: {uid}\n"
                "Share this with the admin."
            )


@router.message(Command("mywallet"))
async def cmd_mywallet(message: Message):
    """Show subscriber's wallet info."""
    from database.models import get_subscriber
    sub = await get_subscriber(message.from_user.id)
    if not sub:
        await message.reply("Not registered. Send /start first.")
        return

    balance_str = f"{sub.paper_balance:.2f} SOL (paper)"
    if sub.trade_mode == "live" and sub.wallet_address:
        try:
            from bot.wallet import get_sol_balance
            real_bal = await get_sol_balance(sub.wallet_address)
            if real_bal is not None:
                balance_str = f"{real_bal:.4f} SOL (live)"
        except Exception:
            pass

    await message.reply(
        f"Wallet: {sub.wallet_address}\n"
        f"Balance: {balance_str}\n"
        f"Mode: {sub.trade_mode}\n"
        f"Tier: {sub.tier}\n"
        f"Status: {sub.status}"
    )


@router.message(Command("myperformance"))
async def cmd_myperformance(message: Message):
    """Show subscriber's trading performance — pulls per-subscriber numbers
    from paper_trades WHERE subscriber_id = telegram_id, separate from HQ."""
    from database.models import get_subscriber, get_subscriber_paper_trade_stats
    sub = await get_subscriber(message.from_user.id)
    if not sub:
        await message.reply("Not registered. Send /start first.")
        return

    stats = await get_subscriber_paper_trade_stats(message.from_user.id)
    balance = sub.paper_balance or 20.0
    starting = 20.0
    pnl_total = float(stats["total_pnl"])
    pnl_pct = ((balance - starting) / starting * 100) if starting > 0 else 0
    emoji = "🟢" if pnl_total >= 0 else "🔴"

    await message.reply(
        f"Your Performance\n\n"
        f"Mode: {sub.trade_mode}\n"
        f"Balance: {balance:.2f} SOL\n"
        f"{emoji} PnL: {pnl_total:+.4f} SOL ({pnl_pct:+.1f}%)\n"
        f"Starting: {starting:.2f} SOL\n\n"
        f"Trades: {stats['closed']} closed, {stats['open_count']} open\n"
        f"Wins: {stats['wins']} ({stats['win_rate']}% WR)"
    )


@router.message(Command("adduser"))
async def cmd_adduser_direct(message: Message):
    """Add a subscriber directly."""
    if message.from_user.id not in ADMIN_IDS:
        await message.reply(f"Not admin. Your ID: {message.from_user.id}\nAdmin IDs: {ADMIN_IDS}")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /adduser <telegram_id>")
        return

    try:
        tid = int(args[1])
    except ValueError:
        await message.reply("Invalid ID.")
        return

    try:
        from database.models import get_subscriber, set_subscriber_status, create_subscriber
        sub = await get_subscriber(tid)
        if sub:
            await set_subscriber_status(tid, "active")
            await message.reply(f"Reactivated subscriber {tid}.")
        else:
            from bot.subscriber import _generate_wallet
            pub, priv = _generate_wallet()
            await create_subscriber(
                telegram_id=tid, username=None,
                wallet_address=pub, wallet_key_hash=priv,
            )
            await message.reply(f"Subscriber {tid} added.\nWallet: {pub}\nMode: paper (20 SOL)")
    except Exception as exc:
        await message.reply(f"Error: {exc}")


@router.message(Command("removeuser"))
async def cmd_removeuser_direct(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /removeuser <telegram_id>")
        return
    try:
        tid = int(args[1])
        from database.models import set_subscriber_status
        ok = await set_subscriber_status(tid, "suspended")
        await message.reply(f"Subscriber {tid} {'suspended' if ok else 'not found'}.")
    except Exception as exc:
        await message.reply(f"Error: {exc}")


@router.message(Command("subscribers"))
async def cmd_subscribers_direct(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        from database.models import get_all_active_subscribers
        subs = await get_all_active_subscribers()
        if not subs:
            await message.reply("No active subscribers.")
            return
        lines = [f"Active Subscribers ({len(subs)})", ""]
        for s in subs:
            name = s.username or str(s.telegram_id)
            lines.append(f"  {name} | {s.tier} | {s.trade_mode}")
        await message.reply("\n".join(lines))
    except Exception as exc:
        await message.reply(f"Error: {exc}")


# ── /scan <address> ───────────────────────────────────────────────────────────

@router.message(Command("scan"))
async def cmd_scan(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        await message.reply("⛔ /scan is only available in the designated callers group.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(
            "⚠️ Usage: /scan <contract_address>\n"
            "Example: /scan So11111111111111111111111111111111111111112",
            parse_mode="Markdown",
        )
        return

    address = parts[1].strip()
    await _do_scan(message, address)


# ── /settradeparam — edit a row in ai_trade_params ───────────────────────────

_SETTRADEPARAM_FIELDS = {
    "optimal_tp_x":         float,
    "optimal_sl_pct":       float,
    "trail_sl_trigger_pct": float,
    "trail_sl_enabled":     int,   # 0 or 1
}


@router.message(Command("settradeparam"))
async def cmd_settradeparam(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return

    parts = (message.text or "").split()
    if len(parts) != 4:
        await message.reply(
            "⚠️ Usage: /settradeparam <pattern_type> <field> <value>\n\n"
            f"Fields: {', '.join(_SETTRADEPARAM_FIELDS.keys())}\n"
            "Pattern types: new_launch, insider_wallet, volume_spike, "
            "low_mc, mid_mc, high_mc, high_chart, high_caller\n\n"
            "Example: /settradeparam high_chart trail_sl_enabled 1",
            parse_mode="Markdown",
        )
        return

    _, ptype, field, raw_value = parts

    if field not in _SETTRADEPARAM_FIELDS:
        await message.reply(
            f"⚠️ Unknown field {field}. Allowed: {', '.join(_SETTRADEPARAM_FIELDS.keys())}",
            parse_mode="Markdown",
        )
        return

    try:
        value = _SETTRADEPARAM_FIELDS[field](raw_value)
    except ValueError:
        await message.reply(f"⚠️ Could not parse {raw_value} as {_SETTRADEPARAM_FIELDS[field].__name__}",
                            parse_mode="Markdown")
        return

    if field == "trail_sl_enabled" and value not in (0, 1):
        await message.reply("⚠️ trail_sl_enabled must be 0 or 1.", parse_mode="Markdown")
        return

    from database.models import AsyncSessionLocal, AITradeParams, select as _select
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            _select(AITradeParams).where(AITradeParams.pattern_type == ptype)
        )).scalar_one_or_none()
        if row is None:
            await message.reply(
                f"⚠️ No ai_trade_params row for {ptype}. Known rows are seeded on boot.",
                parse_mode="Markdown",
            )
            return

        old_value = getattr(row, field)
        setattr(row, field, value)
        row.updated_at = datetime.utcnow()
        await session.commit()

    await message.reply(
        f"✅ {ptype}.{field}: {old_value} → {value}",
        parse_mode="Markdown",
    )
    logger.info("settradeparam: %s.%s %s -> %s by admin %d",
                ptype, field, old_value, value, message.from_user.id)


# ── /whoami — reply with caller's telegram user id ─────────────────────────

@router.message(Command("whoami"))
async def cmd_whoami(message: Message):
    uid = message.from_user.id if message.from_user else None
    uname = message.from_user.username if message.from_user else None
    await message.reply(
        f"Your Telegram user id: {uid}\n"
        f"Username: @{uname if uname else '(none)'}\n\n"
        f"Add this id to the ADMIN_IDS env var in Railway to get admin access.",
    )


# ── /setparam — edit a row in agent_params (global tunables) ────────────────

@router.message(Command("setparam"))
async def cmd_setparam(message: Message):
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) != 3:
        await message.reply(
            "⚠️ Usage: /setparam <name> <value>\n\n"
            "Example: /setparam conf_paper_threshold 45\n"
            "Use /params to see current values.",
        )
        return

    _, name, raw_value = parts
    try:
        value = float(raw_value)
    except ValueError:
        await message.reply(f"⚠️ Could not parse {raw_value} as a number.")
        return

    from database.models import AsyncSessionLocal, AgentParam, select as _select
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            _select(AgentParam).where(AgentParam.param_name == name)
        )).scalar_one_or_none()
        old_value = row.param_value if row else None

    await set_param(name, value, f"Manual override via /setparam by admin {message.from_user.id}")

    old_str = f"{old_value}" if old_value is not None else "(new)"
    await message.reply(f"✅ {name}: {old_str} → {value}")
    logger.info("setparam: %s %s -> %s by admin %d",
                name, old_value, value, message.from_user.id)


# ── /sharetoggle — flip external CA broadcast on/off ────────────────────────

@router.message(Command("aimode"))
async def cmd_aimode(message: Message):
    """Flip the caller's KeyBot decision_mode to 'ai' — confidence engine
    + Agent 6 control TP/SL/size per trade. Default mode."""
    from database.models import upsert_keybot_settings
    await upsert_keybot_settings(message.from_user.id, decision_mode="ai")
    await message.reply(
        "🤖 <b>AI Mode</b>\nAgents control TP, SL, and trade size based on "
        "learned patterns. Open trades keep their original rules.",
        parse_mode="HTML",
    )


@router.message(Command("manualmode"))
async def cmd_manualmode(message: Message):
    """Flip the caller's KeyBot decision_mode to 'manual' — KeyBot static
    values override AI for new trades."""
    from database.models import upsert_keybot_settings, get_keybot_settings
    s = await get_keybot_settings(message.from_user.id)
    if s is None:
        s = await upsert_keybot_settings(message.from_user.id, decision_mode="manual")
    else:
        await upsert_keybot_settings(message.from_user.id, decision_mode="manual")
        s = await get_keybot_settings(message.from_user.id)
    await message.reply(
        f"✋ <b>Manual Mode</b>\n"
        f"New trades will use YOUR KeyBot values:\n"
        f"  Buy: {s.buy_amount_sol:.2f} SOL\n"
        f"  TP: {s.take_profit_x:.1f}x\n"
        f"  SL: {s.stop_loss_pct:.0f}%\n"
        f"Trail / breakeven still AI-managed mid-flight.\n"
        f"Open trades keep their original rules.",
        parse_mode="HTML",
    )


@router.message(Command("sharetoggle"))
async def cmd_sharetoggle(message: Message):
    """Toggle whether each new trade's CA is auto-posted to the external
    Phanes group via Telethon. Admin-only."""
    if message.from_user.id not in ADMIN_IDS:
        return
    from database.models import get_param as _get_param
    cur = await _get_param("external_ca_post_enabled")
    cur_on = cur is None or cur >= 0.5
    new_val = 0.0 if cur_on else 1.0
    await set_param(
        "external_ca_post_enabled", new_val,
        f"Toggled via /sharetoggle by admin {message.from_user.id}",
    )
    state_str = "ON ✅" if not cur_on else "OFF"
    await message.reply(
        f"📤 External CA broadcast: {state_str}\n"
        f"({'New trades will post to external group' if not cur_on else 'CAs stay private to HQ + subscribers'})"
    )


# ── /agent6force ──────────────────────────────────────────────────────────────

@router.message(Command("agent6force"))
async def cmd_agent6force(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from bot.agents.learning_loop import run_once as agent6_run_once
    from database.models import get_total_closed_count, get_current_weights

    total_before = await get_total_closed_count()
    row_before = await get_current_weights()
    analyzed_before = row_before.trades_analyzed if row_before else 0

    await message.reply(
        f"🧠 Forcing Agent 6 learning cycle…\n"
        f"Closed trades: {total_before} | Already analyzed: {analyzed_before} | "
        f"Pending: {total_before - analyzed_before}",
        parse_mode="Markdown",
    )

    try:
        ran = await agent6_run_once(message.bot, force=True)
    except Exception as exc:
        logger.exception("agent6force failed")
        await message.reply(f"❌ Agent 6 run raised: {exc}", parse_mode="Markdown")
        return

    row_after = await get_current_weights()
    analyzed_after = row_after.trades_analyzed if row_after else 0
    weights_txt = "—"
    if row_after:
        weights_txt = (
            f"fp={row_after.fingerprint_weight:.2%}  ins={row_after.insider_weight:.2%}  "
            f"chart={row_after.chart_weight:.2%}  rug={row_after.rug_weight:.2%}  "
            f"caller={row_after.caller_weight:.2%}  market={row_after.market_weight:.2%}"
        )

    last_change = getattr(state, "learning_loop_last_change", "—")
    regime = getattr(state, "market_regime", "—")
    thresholds = getattr(state, "confidence_thresholds", {})

    lines = [
        f"{'✅' if ran else 'ℹ️'} Agent 6 cycle {'completed' if ran else 'did not run (nothing to analyze)'}",
        f"Analyzed: {analyzed_before} → {analyzed_after}",
        f"Regime: {regime}",
        f"Weights: {weights_txt}",
        f"Thresholds: {thresholds}",
        f"Changes: _{last_change}_",
    ]
    await message.reply("\n".join(lines), parse_mode="Markdown")


# ── /healthcheck — Quick system summary ──────────────────────────────────────

@router.message(Command("healthcheck"))
async def cmd_healthcheck(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from database.models import (
        AsyncSessionLocal,
        get_total_closed_count,
        get_open_paper_trades,
        get_token_count,
    )
    from sqlalchemy import text as _text

    now = datetime.utcnow()
    db_ok = False
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(_text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        logger.error("healthcheck DB probe failed: %s", exc)

    open_paper = await get_open_paper_trades()
    closed_count = await get_total_closed_count()
    token_count = await get_token_count()
    balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)

    def _ago(ts):
        if ts is None:
            return "never"
        mins = int((now - ts).total_seconds() / 60)
        if mins < 1:
            return "just now"
        if mins < 60:
            return f"{mins}m ago"
        return f"{mins // 60}h{mins % 60:02d}m ago"

    scanner_age = _ago(state.scanner_last_run)
    learning_age = _ago(state.learning_loop_last_run)

    lines = [
        "🩺 *HEALTHCHECK*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"DB: {'✅' if db_ok else '❌ UNREACHABLE'}",
        f"Mode: {state.trade_mode} | Autotrade: {state.autotrade_enabled}",
        f"Paper balance: {balance:.4f} SOL / {state.PAPER_STARTING_BALANCE:.0f} SOL",
        f"Open paper: {len(open_paper)} | Closed: {closed_count}",
        f"Tokens tracked: {token_count}",
        "",
        "*Agent liveness*",
        f"Scanner last run: {scanner_age} ({state.scanner_status})",
        f"Learning loop last run: {learning_age}",
        f"Market regime: {state.market_regime} | SOL 24h: {state.sol_24h_change:+.1f}%",
        "",
        f"_Checked {now.strftime('%H:%M:%S')} UTC_",
    ]
    try:
        await message.reply("\n".join(lines), parse_mode="Markdown")
    except Exception:
        await message.reply("\n".join(lines).replace("*", "").replace("", ""), parse_mode=None)


# ── /agent_status — Per-agent last run table ─────────────────────────────────

@router.message(Command("agent_status"))
async def cmd_agent_status(message: Message):
    if message.chat.id != CALLER_GROUP_ID and message.chat.type != "private":
        return

    from database.models import AsyncSessionLocal, AgentLog
    from sqlalchemy import select as _select, func as _func

    # Pull latest row per agent_name (max run_at)
    async with AsyncSessionLocal() as session:
        subq = (
            _select(AgentLog.agent_name, _func.max(AgentLog.run_at).label("last"))
            .group_by(AgentLog.agent_name)
            .subquery()
        )
        rows = (await session.execute(
            _select(AgentLog)
            .join(subq, (AgentLog.agent_name == subq.c.agent_name) & (AgentLog.run_at == subq.c.last))
            .order_by(AgentLog.run_at.desc())
        )).scalars().all()

    now = datetime.utcnow()

    def _ago(ts):
        if ts is None:
            return "never"
        secs = int((now - ts).total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        mins = secs // 60
        if mins < 60:
            return f"{mins}m ago"
        return f"{mins // 60}h{mins % 60:02d}m ago"

    lines = [
        "🤖 *AGENT STATUS*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if not rows:
        lines.append("_No agent runs logged yet._")
    else:
        for r in rows:
            notes = (r.notes or "")[:40]
            lines.append(
                f"{r.agent_name:<16} {_ago(r.run_at)} | "
                f"found={r.tokens_found} saved={r.tokens_saved}"
                + (f" | {notes}" if notes else "")
            )

    # Also show the live in-memory stats that aren't always logged to agent_logs
    lines += [
        "",
        "*Live state*",
        f"Scanner: {state.scanner_status} | last tick {_ago(state.scanner_last_run)}",
        f"Agent 6: analyzed={state.learning_loop_last_analyzed}"
        f" closed={state.learning_loop_total_closed}"
        f" | last run {_ago(state.learning_loop_last_run)}",
        f"Regime: {state.market_regime} | SOL 24h: {state.sol_24h_change:+.1f}%",
        "",
        f"_Checked {now.strftime('%H:%M:%S')} UTC_",
    ]

    try:
        await message.reply("\n".join(lines), parse_mode="Markdown")
    except Exception:
        await message.reply("\n".join(lines).replace("*", "").replace("", ""), parse_mode=None)


# ── /addcaller ────────────────────────────────────────────────────────────────

@router.message(Command("addcaller"))
async def cmd_addcaller(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("⚠️ Usage: /addcaller <telegram_id>", parse_mode="Markdown")
        return

    telegram_id = int(parts[1])
    added = await add_caller(telegram_id)
    if added:
        await message.reply(f"✅ User {telegram_id} added to approved callers.", parse_mode="Markdown")
    else:
        await message.reply(f"ℹ️ User {telegram_id} is already an approved caller.", parse_mode="Markdown")


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
        f"Entry MC:   {_format_usd(scan.entry_price)}\n"
        f"Current MC: {_format_usd(current_mc)}\n"
        f"Peak MC:    {_format_usd(scan.peak_market_cap or scan.entry_price)}\n\n"
        f"Current X:  *{curr_mult:.2f}x*\n"
        f"Peak X:     *{peak_mult:.2f}x*\n"
        f"Points:     *{scan.points:.2f}*\n\n"
        f"Status:  {label}\n"
        f"{time_str}\n\n"
        f"_Called by @{scan.scanned_by}_"
    )


# ── /pnl <contract> ──────────────────────────────────────────────────────────

@router.message(Command("pnl"))
async def cmd_pnl(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Please provide a contract address: /pnl <contract>", parse_mode="Markdown")
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
            f"{prefix} @{row['username']} | "
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


def _rtn_arrow(x: float) -> str:
    if x >= 3.0:
        return "^^"
    if x >= 2.0:
        return "^"
    if x >= 1.5:
        return ">"
    return "v"


async def _build_top_calls(timeframe: str = "ALL") -> tuple[str, object]:
    since = _TC_SINCE.get(timeframe, lambda: None)()
    rows  = await get_top_calls(limit=10, since=since)
    stats = await get_top_calls_stats(since=since)

    lines = [
        "<pre>",
        "REVOLT CAPITAL",
        "─────────────────────────────────────────────",
        "SIGNAL PERFORMANCE REPORT",
        f"{stats['total']} Signals Tracked  |  {stats['avg_x']}x Avg Return",
        "─────────────────────────────────────────────",
        "",
        "RK  SIGNAL              OPERATOR              RTN",
        "─────────────────────────────────────────────────",
    ]

    if not rows:
        lines.append("No signals recorded yet.")
    else:
        for i, row in enumerate(rows, 1):
            peak_x = row["peak_multiplier"] or 0
            name   = (row["token_name"] or "?")[:18]
            caller = ("@" + row["scanned_by"])[:20]
            arrow  = _rtn_arrow(peak_x)
            lines.append(f"{i:02d}  {name:<18s}  {caller:<20s}  {peak_x:.2f}x {arrow}")

    lines.append("─────────────────────────────────────────────────")

    leaders = await get_signal_leaders(limit=10)
    if leaders:
        lines.append("")
        lines.append("OPERATOR SUMMARY")
        lines.append("─────────────────────────────────────────────")
        for op in leaders:
            username = ("@" + op["username"])[:20]
            calls = op["scans"]
            wins = op["wins"]
            losses = op["losses"]
            total_pts = op["total_points"]
            win_pct = op["win_pct"]
            avg_x = round(total_pts / calls, 2) if calls > 0 else 0
            lines.append(f"{username:<20s} {calls:>3d} signals  {wins}W-{losses}L  {avg_x:.2f}x avg  {win_pct}%WR")
        lines.append("─────────────────────────────────────────────")

    lines.append("")
    lines.append(f"Revolt Capital  |  {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    lines.append("</pre>")

    return "\n".join(lines), top_calls_keyboard(active=timeframe)


@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message):
    text, keyboard = await _build_top_calls("ALL")
    await message.reply(text, parse_mode="HTML", reply_markup=keyboard)


@router.message(Command("lb"))
async def cmd_lb(message: Message):
    text, keyboard = await _build_top_calls("ALL")
    await message.reply(text, parse_mode="HTML", reply_markup=keyboard)


# ── Callback: Top Calls timeframe ────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("tc:"))
async def cb_top_calls_timeframe(callback: CallbackQuery):
    timeframe = callback.data.split(":", 1)[1]
    if timeframe not in _TC_SINCE:
        await callback.answer()
        return
    await callback.answer()
    text, keyboard = await _build_top_calls(timeframe)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)


# ── Callback: Share Signal ────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("share:"))
async def cb_share_signal(callback: CallbackQuery, bot: Bot):
    address = callback.data.split(":", 1)[1]
    logger.info("Share signal clicked for %s by user %s", address[:12], callback.from_user.id)

    if MAIN_GROUP_ID == 0:
        logger.warning("Share: MAIN_GROUP_ID is 0 — not configured")
        await callback.answer(
            "MAIN_GROUP_ID is not configured. Set it in Railway env vars.", show_alert=True
        )
        return

    logger.info("Share: targeting group %s", MAIN_GROUP_ID)

    from bot.scanner import fetch_token_data, parse_token_metrics, calculate_ai_score
    pair = await fetch_token_data(address, allow_any_dex=True)
    if pair is None:
        logger.warning("Share: fetch_token_data returned None for %s", address[:12])
        await callback.answer("Failed to re-fetch token data.", show_alert=True)
        return

    metrics = parse_token_metrics(pair)
    score_data = calculate_ai_score(metrics)
    data = {**metrics, **score_data}

    card_text = build_trade_card(data)
    keyboard = trade_card_keyboard(dex_url=data.get("dex_url", ""), contract_address=address)

    shared_by = callback.from_user.username or str(callback.from_user.id)
    msg_text = f"📢 *Signal shared by @{shared_by}*\n\n{card_text}"

    try:
        await bot.send_message(
            chat_id=MAIN_GROUP_ID,
            text=msg_text,
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        logger.info("Share: posted to group %s successfully", MAIN_GROUP_ID)
        await callback.answer("Signal shared to Revolt!")
    except Exception as exc:
        logger.error("Share signal failed (Markdown): %s", exc)
        # Retry without parse_mode in case Markdown chars in token name break it
        try:
            await bot.send_message(
                chat_id=MAIN_GROUP_ID,
                text=msg_text.replace("*", "").replace("_", ""),
                reply_markup=keyboard,
            )
            logger.info("Share: posted to group %s (plain text fallback)", MAIN_GROUP_ID)
            await callback.answer("Signal shared to Revolt!")
        except Exception as exc2:
            logger.error("Share signal failed (plain text): %s", exc2)
            await callback.answer(f"Failed: {exc2}", show_alert=True)


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
    await callback.answer("Refreshing...")

    # Clear cache so refresh gets live data
    from bot.scanner import _token_cache
    _token_cache.pop(address, None)

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
