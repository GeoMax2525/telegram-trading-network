"""
signal_relay.py — Copies trades to subscribers + posts CA to external group.

When the main bot opens a trade, this module:
1. Posts the CA to the external group via Telethon (as your account)
2. Waits 30 seconds (your trade fills first)
3. Opens the same trade for each active subscriber
4. Notifies each subscriber via DM
"""

import asyncio
import logging

from aiogram import Bot

from database.models import (
    get_all_active_subscribers,
    AsyncSessionLocal, Subscriber, PaperTrade,
)

logger = logging.getLogger(__name__)

# Global bot reference — set from main.py
_bot: Bot | None = None
RELAY_DELAY = 30  # seconds after admin trade before subscriber trades

# External group for CA posting via Telethon (your account)
EXTERNAL_GROUP_ID = -1002170009255
EXTERNAL_THREAD_ID = 496012


def set_relay_bot(bot: Bot):
    global _bot
    _bot = bot


async def post_ca_to_group(token_address: str, token_name: str):
    """Post the CA to external group using Telethon (your user account).
    Posts as YOU, not as a bot. Phanes picks up the CA automatically."""
    try:
        from bot.agents.tg_scraper import TG_API_ID, TG_API_HASH, TG_SESSION_STRING
        if not TG_SESSION_STRING:
            return

        from telethon import TelegramClient
        from telethon.sessions import StringSession

        client = TelegramClient(
            StringSession(TG_SESSION_STRING),
            int(TG_API_ID),
            TG_API_HASH,
        )
        await client.start()

        # Post just the CA as a standalone message — no reply, no thread
        await client.send_message(
            EXTERNAL_GROUP_ID,
            token_address,
        )
        logger.info("Posted CA to external group: %s (%s)", token_name[:20], token_address[:12])

        await client.disconnect()
    except Exception as exc:
        logger.warning("CA post to external group failed: %s", exc)


async def relay_trade_to_subscribers(
    token_address: str,
    token_name: str,
    entry_mc: float,
    tp_x: float,
    sl_pct: float,
    pattern_type: str | None,
    trade_reasoning: str | None,
    confidence: float,
):
    """
    Called after the admin's trade opens. Waits 30s then opens
    the same trade for each active subscriber.
    """
    # Post CA to external group immediately (no delay)
    asyncio.create_task(post_ca_to_group(token_address, token_name))

    # Relay to subscribers with delay (runs in background)
    asyncio.create_task(_relay_delayed(
        token_address, token_name, entry_mc,
        tp_x, sl_pct, pattern_type, trade_reasoning, confidence,
    ))


async def _relay_delayed(
    token_address: str,
    token_name: str,
    entry_mc: float,
    tp_x: float,
    sl_pct: float,
    pattern_type: str | None,
    trade_reasoning: str | None,
    confidence: float,
):
    """Wait 30 seconds then copy trade to all subscribers."""
    await asyncio.sleep(RELAY_DELAY)

    subs = await get_all_active_subscribers()
    if not subs:
        return

    # Re-fetch MC (price may have moved in 30 seconds)
    from bot.scanner import fetch_current_market_cap
    fresh_mc = await fetch_current_market_cap(token_address)
    if not fresh_mc or fresh_mc <= 0:
        fresh_mc = entry_mc  # fallback to admin entry

    opened = 0
    for sub in subs:
        try:
            # Check subscriber balance
            if sub.paper_balance < 0.15:
                continue

            # Check if subscriber already has this token open
            async with AsyncSessionLocal() as session:
                from sqlalchemy import select
                existing = (await session.execute(
                    select(PaperTrade).where(
                        PaperTrade.token_address == token_address,
                        PaperTrade.status == "open",
                    )
                )).scalars().all()
                # Check if any belong to this subscriber
                sub_has_open = any(
                    getattr(t, "subscriber_id", None) == sub.telegram_id
                    for t in existing
                )
                if sub_has_open:
                    continue

            # Open paper trade for subscriber
            async with AsyncSessionLocal() as session:
                pt = PaperTrade(
                    token_address=token_address,
                    token_name=token_name,
                    entry_mc=fresh_mc,
                    entry_price=fresh_mc,
                    paper_sol_spent=0.1,  # probe size
                    confidence_score=confidence,
                    pattern_type=pattern_type,
                    take_profit_x=tp_x,
                    stop_loss_pct=sl_pct,
                    status="open",
                    peak_mc=fresh_mc,
                    peak_multiple=1.0,
                    trade_reasoning=f"[RELAY] {trade_reasoning or ''}",
                )
                session.add(pt)
                await session.commit()

            # Update subscriber balance
            async with AsyncSessionLocal() as session:
                s = (await session.execute(
                    select(Subscriber).where(Subscriber.telegram_id == sub.telegram_id)
                )).scalar_one_or_none()
                if s:
                    s.paper_balance -= 0.1
                    await session.commit()

            # Notify subscriber via DM
            if _bot:
                try:
                    await _bot.send_message(
                        sub.telegram_id,
                        f"⚡ <b>NEW TRADE OPENED</b>\n\n"
                        f"🪙 {token_name}\n"
                        f"MC: ${fresh_mc / 1000:.1f}K\n"
                        f"Size: 0.1 SOL\n"
                        f"TP: {tp_x:.1f}x | SL: {sl_pct:.0f}%\n\n"
                        f"<i>{trade_reasoning or 'AI signal'}</i>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass  # user may have blocked the bot

            opened += 1

        except Exception as exc:
            logger.warning("Relay failed for subscriber %s: %s", sub.telegram_id, exc)

    if opened:
        logger.info("Signal relay: opened trade on %s for %d subscribers (30s delay)",
                     token_name[:20], opened)


async def notify_subscribers_close(
    token_name: str,
    close_reason: str,
    pnl: float,
    multiplier: float,
):
    """Notify all subscribers when a trade closes."""
    if not _bot:
        return

    subs = await get_all_active_subscribers()
    emoji = "✅" if pnl >= 0 else "❌"

    for sub in subs:
        try:
            await _bot.send_message(
                sub.telegram_id,
                f"{emoji} <b>TRADE CLOSED</b>\n\n"
                f"🪙 {token_name}\n"
                f"Reason: {close_reason}\n"
                f"Result: {multiplier:.1f}x | {pnl:+.4f} SOL",
                parse_mode="HTML",
            )
        except Exception:
            pass
