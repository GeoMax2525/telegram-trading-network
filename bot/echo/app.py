"""
app.py — the Echo Telegram bot: a separate Bot/Dispatcher (ECHO_BOT_TOKEN) that
silently records every CA sighting across all groups and fires consensus alerts.
Zero trading code, and NO admin command surface — all controls + intelligence
live on the HQ bot (bot/echo/hq.py), so nothing is exposed inside the external
groups Echo sits in.
"""

import asyncio
import logging
from collections import deque
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties

from bot.echo import core
from bot.echo.signals import maybe_fire_signal, echo_tracker_loop

logger = logging.getLogger(__name__)
router = Router()

# ── Anti-abuse: per-chat rate limit (messages with CAs processed per minute) ──
_chat_hits: dict[int, deque] = {}
_MAX_PER_MIN = 30


def _rate_ok(chat_id: int) -> bool:
    now = datetime.utcnow().timestamp()
    dq = _chat_hits.setdefault(chat_id, deque())
    while dq and now - dq[0] > 60:
        dq.popleft()
    if len(dq) >= _MAX_PER_MIN:
        return False
    dq.append(now)
    return True


# ── Ingest: the Trojan Horse data collection layer ──────────────────────────
@router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message) -> None:
    text = message.text or message.caption or ""
    cas = core.extract_cas(text)
    if not cas:
        return

    chat = message.chat
    user = message.from_user
    if await core.is_blacklisted(chat.id, user.id if user else None):
        return
    if not _rate_ok(chat.id):
        return

    link = core.message_link(chat.id, chat.username, message.message_id)
    for ca in cas:
        try:
            is_new_group = await core.record_sighting(
                ca=ca, chat_id=chat.id, chat_title=chat.title,
                user_id=user.id if user else None,
                username=(user.username or user.full_name) if user else None,
                message_id=message.message_id, msg_link=link,
            )
            if is_new_group:
                # A new distinct group called this CA — re-check consensus.
                await maybe_fire_signal(message.bot, ca)
        except Exception as exc:
            logger.debug("echo: ingest error %s: %s", ca[:8], exc)


# ── Startup ─────────────────────────────────────────────────────────────────
async def _set_commands(echo_bot) -> None:
    """Register Echo's themed command menu (the BotFather command list)."""
    from aiogram.types import (
        BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats,
    )
    try:
        # Groups see ONLY /pod (the one public command). The full operator menu
        # is scoped to private chats.
        await echo_bot.set_my_commands(
            [BotCommand(command="pod", description="See the Pod rankings")],
            scope=BotCommandScopeAllGroupChats(),
        )
        await echo_bot.set_my_commands([
            BotCommand(command="dive", description="Dive in — main menu"),
            BotCommand(command="pod", description="See how the pod is performing"),
            BotCommand(command="echoers", description="View top echoers and their points"),
            BotCommand(command="sonar", description="Run sonar — check current signals"),
            BotCommand(command="waves", description="See all commands and how to use them"),
        ], scope=BotCommandScopeAllPrivateChats())
    except Exception as exc:
        logger.debug("echo: set_my_commands failed: %s", exc)


async def start_echo() -> None:
    """Boot the Echo bot (own token, own polling) + its tracker loop. No-op if
    ECHO_BOT_TOKEN is unset, so the trading bot is unaffected."""
    if not core.echo_enabled():
        logger.info("Echo: ECHO_BOT_TOKEN not set — Echo dormant")
        return
    echo_bot = Bot(core.ECHO_BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
    dp = Dispatcher()
    # Themed commands FIRST so /dive etc. match before the ingest catch-all.
    from bot.echo.themed import router as themed_router
    dp.include_router(themed_router)
    dp.include_router(router)
    asyncio.create_task(echo_tracker_loop(echo_bot))
    logger.info("Echo: starting (admins=%s)", core.ECHO_ADMIN_IDS)
    try:
        await echo_bot.delete_webhook(drop_pending_updates=True)
        await _set_commands(echo_bot)
        await dp.start_polling(echo_bot)
    finally:
        await echo_bot.session.close()
