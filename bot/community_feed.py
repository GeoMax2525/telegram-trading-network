"""
community_feed.py — mirrors selected events to a public Telegram channel.

The community channel is broadcast-only; the bot must be admin there with
"Post Messages" permission. Silent no-op if COMMUNITY_CHANNEL_ID is 0.

What gets posted:
  - 🚀  Trade opens (entry MC, position size, source)
  - ✅/❌  Trade closes (PnL, peak, close reason)
  - 🧠  Claude postmortems (Phase 5 commentary)

What does NOT get posted:
  - Admin command output (/setparam, /resetpaper, etc.)
  - Subscriber DMs
  - Operator-only debug messages
  - Anything with sensitive op detail

Failures are swallowed and logged — never blocks the primary HQ send.
"""

import logging
from datetime import datetime

from bot.config import COMMUNITY_CHANNEL_ID

logger = logging.getLogger(__name__)


def community_enabled() -> bool:
    return COMMUNITY_CHANNEL_ID != 0


async def post_to_community(bot, text: str, *, parse_mode: str | None = None) -> None:
    """Best-effort post to the community channel. Never raises."""
    if not community_enabled() or bot is None:
        return
    try:
        await bot.send_message(
            COMMUNITY_CHANNEL_ID, text, parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.warning(
            "community_feed: post failed (channel=%d): %s",
            COMMUNITY_CHANNEL_ID, exc,
        )


def _short_mint(mint: str) -> str:
    if not mint or len(mint) < 8:
        return mint or "?"
    return f"{mint[:4]}…{mint[-4:]}"


async def post_trade_open(
    bot,
    *,
    token_name: str,
    token_address: str,
    entry_mc: float,
    paper_sol: float,
    pattern_type: str,
    tp_x: float,
    sl_pct: float,
) -> None:
    """Post a clean 'opened' card to the community channel."""
    if not community_enabled():
        return
    source = "⚡ 4AM Signal" if "tg_signal" in (pattern_type or "") else "🔍 Scanner"
    safe_name = (token_name or "?").replace("<", "").replace(">", "")[:40]
    text = (
        f"🚀 OPENED — {safe_name}\n"
        f"\n"
        f"Source: {source}\n"
        f"Entry MC: ${entry_mc:,.0f}\n"
        f"Size: {paper_sol:.3f} SOL (paper)\n"
        f"Target: {tp_x:.1f}x  ·  Stop: -{sl_pct:.0f}%\n"
        f"CA: {_short_mint(token_address)}\n"
        f"\n"
        f"Watching it for you. Postmortem at close."
    )
    await post_to_community(bot, text)


async def post_trade_close(
    bot,
    *,
    token_name: str,
    pnl_sol: float,
    peak_mult: float,
    close_reason: str,
    paper_sol_spent: float,
    pattern_type: str,
    age_min: float,
) -> None:
    """Post a clean 'closed' card to the community channel."""
    if not community_enabled():
        return
    icon = "✅" if (pnl_sol or 0) > 0 else ("➖" if (pnl_sol or 0) == 0 else "❌")
    source = "⚡ 4AM" if "tg_signal" in (pattern_type or "") else "🔍 Scanner"
    safe_name = (token_name or "?").replace("<", "").replace(">", "")[:40]

    if age_min < 60:
        age_str = f"{age_min:.0f}m"
    elif age_min < 1440:
        age_str = f"{age_min/60:.1f}h"
    else:
        age_str = f"{age_min/1440:.1f}d"

    reason_pretty = {
        "tp_hit": "🎯 TP hit",
        "sl_hit": "🛑 SL hit",
        "trail_hit": "📉 Trail stop",
        "profit_trail": "📉 Profit trail",
        "breakeven_stop": "↔ Breakeven stop",
        "time_stop": "⏱ Time stop",
        "hard_timeout": "⏱ Hard timeout",
        "manual_close": "✋ Manual close",
        "rug_mc": "🚨 Rug (MC)",
        "rug_liquidity": "🚨 Rug (LP)",
        "dead_api": "❓ Data gone",
        "dead_token": "❓ Token gone",
    }.get(close_reason, close_reason or "?")

    text = (
        f"{icon} CLOSED — {safe_name}\n"
        f"\n"
        f"Source: {source}  ·  Held: {age_str}\n"
        f"Peak: {peak_mult:.2f}x\n"
        f"PnL: {pnl_sol:+.4f} SOL on {paper_sol_spent:.3f} SOL\n"
        f"Reason: {reason_pretty}"
    )
    await post_to_community(bot, text)


async def post_commentary(bot, commentary_text: str) -> None:
    """Mirror Claude's postmortem to the community channel."""
    if not community_enabled() or not commentary_text:
        return
    await post_to_community(bot, f"🧠 {commentary_text}")


async def post_scale_in(
    bot,
    *,
    token_name: str,
    pattern_type: str,
    current_mult: float,
    add_sol: float,
    total_sol: float,
) -> None:
    """Position got added to at 1.5x confirmation. Show the community
    we doubled down on the runner."""
    if not community_enabled():
        return
    source = "⚡ 4AM" if "tg_signal" in (pattern_type or "") else "🔍 Scanner"
    safe_name = (token_name or "?").replace("<", "").replace(">", "")[:40]
    text = (
        f"💪 SCALED IN — {safe_name}\n"
        f"\n"
        f"Source: {source}\n"
        f"Confirmed at: {current_mult:.1f}x\n"
        f"Added: +{add_sol:.2f} SOL\n"
        f"Total position: {total_sol:.2f} SOL\n"
        f"\n"
        f"Bot doubled down — sees more upside."
    )
    await post_to_community(bot, text)


async def post_scale_out(
    bot,
    *,
    token_name: str,
    pattern_type: str,
    current_mult: float,
    sell_pct: float,
    realized_sol: float,
    remaining_pct: float,
) -> None:
    """Partial exit at a ladder milestone. Tell the community what got
    locked and how much is still running."""
    if not community_enabled():
        return
    source = "⚡ 4AM" if "tg_signal" in (pattern_type or "") else "🔍 Scanner"
    safe_name = (token_name or "?").replace("<", "").replace(">", "")[:40]
    text = (
        f"📈 TOOK PROFIT — {safe_name}\n"
        f"\n"
        f"Source: {source}\n"
        f"Sold: {sell_pct:.0f}% at {current_mult:.1f}x\n"
        f"Locked: +{realized_sol:.4f} SOL\n"
        f"Still running: {remaining_pct:.0f}% of position\n"
        f"\n"
        f"Bot locks profit on the way up. Trail covers the rest."
    )
    await post_to_community(bot, text)
