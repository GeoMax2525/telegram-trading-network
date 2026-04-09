import asyncio
import logging
import re
from datetime import datetime, timezone, timedelta

from aiogram import Router, Bot, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.config import MAIN_GROUP_ID, ADMIN_IDS
from bot.scanner import scan_token, fetch_current_market_cap, fetch_live_data, fetch_sol_price_usd
from bot.keyboards import trade_card_keyboard, pnl_keyboard, top_calls_keyboard
from bot.wallet import get_wallet_address, get_token_holding
from database.models import (
    log_scan, get_leaderboard, get_break_evens_count, add_caller,
    get_open_scans, update_scan_pnl, get_scan_by_address, close_old_scans,
    get_signal_leaders, get_top_calls, get_top_calls_stats,
    get_any_open_position_by_token, get_hub_stats,
)

logger = logging.getLogger(__name__)
router = Router()

CALLER_GROUP_ID = -1003852140576


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

_autotrade_enabled: bool = False


def _hub_keyboard(autotrade: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Refresh",       callback_data="hub:refresh"),
        InlineKeyboardButton(text="🤖 Agent Details", callback_data="hub:agents"),
        InlineKeyboardButton(text="👛 Top Wallets",   callback_data="hub:wallets"),
    )
    at_label = "⚡ Autotrade ON" if autotrade else "⚡ Autotrade OFF"
    builder.row(
        InlineKeyboardButton(text="📈 Trade History", callback_data="hub:history"),
        InlineKeyboardButton(text=at_label,           callback_data="hub:autotrade"),
        InlineKeyboardButton(text="⚙️ Settings",      callback_data="hub:settings"),
    )
    return builder.as_markup()


async def _build_hub_text(autotrade: bool) -> str:
    stats = await get_hub_stats()

    scans_today  = stats["scans_today"]
    trades_today = stats["trades_today"]
    today_pnl    = stats["today_pnl"]
    alltime_pnl  = stats["alltime_pnl"]
    win_rate     = stats["win_rate"]
    total_closed = stats["total_closed"]
    recent       = stats["recent_trades"]

    at_status = "ON 🟢" if autotrade else "OFF 🔴"

    ce_icon = "✅" if autotrade else "🔧"
    ce_line = f"{ce_icon} Confidence Engine — {trades_today} auto-trades today"

    lines = [
        "🔑 *LOWKEY ALPHA HUB*",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        "🤖 *AGENTS*",
        f"✅ Scanner — {scans_today} candidates today",
        "🔧 Harvester — _Building..._",
        "🔧 Wallet Analyst — _Building..._",
        "🔧 Pattern Engine — _Building..._",
        ce_line,
        "🔧 Learning Loop — _Building..._",
        "🔧 Chart Detector — _Building..._",
        f"⚡ Autotrade: *{at_status}*",
        "",
        "📊 *PERFORMANCE*",
        f"Today: `{today_pnl:+.4f} SOL` | All Time: `{alltime_pnl:+.4f} SOL`",
        f"Win Rate: `{win_rate}%` | Closed Trades: `{total_closed}`",
        "",
        "🔥 *TOP WALLETS*",
        "_🔧 Wallet tracking not built yet_",
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
    text = await _build_hub_text(_autotrade_enabled)
    await message.reply(text, parse_mode="Markdown", reply_markup=_hub_keyboard(_autotrade_enabled))


@router.callback_query(lambda c: c.data and c.data.startswith("hub:"))
async def cb_hub(callback: CallbackQuery):
    global _autotrade_enabled
    action = callback.data.split(":", 1)[1]

    if action == "refresh":
        await callback.answer("🔄 Refreshing…")
        text = await _build_hub_text(_autotrade_enabled)
        try:
            await callback.message.edit_text(
                text, parse_mode="Markdown",
                reply_markup=_hub_keyboard(_autotrade_enabled),
            )
        except Exception:
            pass  # unchanged

    elif action == "autotrade":
        _autotrade_enabled = not _autotrade_enabled
        status = "ON 🟢" if _autotrade_enabled else "OFF 🔴"
        await callback.answer(f"⚡ Autotrade {status}")
        text = await _build_hub_text(_autotrade_enabled)
        await callback.message.edit_text(
            text, parse_mode="Markdown",
            reply_markup=_hub_keyboard(_autotrade_enabled),
        )

    elif action == "agents":
        await callback.answer(
            "🔧 Agent Details: most agents are still being built.", show_alert=True
        )

    elif action == "wallets":
        await callback.answer(
            "🔧 Top Wallets: wallet tracking not built yet.", show_alert=True
        )

    elif action == "history":
        await callback.answer(
            "🔧 Trade History: full history view coming soon.", show_alert=True
        )

    elif action == "settings":
        await callback.answer(
            "⚙️ Use /keybot to manage your trading settings.", show_alert=True
        )

    else:
        await callback.answer()


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
