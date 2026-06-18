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
from aiogram.types import Message, ChatMemberUpdated
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
def _message_cas(message: Message) -> list[str]:
    """All CAs in a message — visible text/caption AND hidden link URLs (a
    'Chart' hyperlink pointing at dexscreener.com/solana/<CA> etc.)."""
    parts = [message.text or "", message.caption or ""]
    for ent in (message.entities or []) + (message.caption_entities or []):
        if getattr(ent, "url", None):   # text_link entities carry the hidden URL
            parts.append(ent.url)
    return core.extract_cas(" ".join(parts))


async def _ingest(message: Message) -> None:
    cas = _message_cas(message)
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


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_group_message(message: Message) -> None:
    await _ingest(message)


@router.edited_message(F.chat.type.in_({"group", "supergroup"}))
async def on_edited_group_message(message: Message) -> None:
    await _ingest(message)


# ── Referral attribution: who added ECCO to each group ──────────────────────
@router.my_chat_member()
async def on_my_member(update: ChatMemberUpdated) -> None:
    chat = update.chat
    if chat.type not in ("group", "supergroup"):
        return
    status = update.new_chat_member.status
    is_admin = status == "administrator"
    active = status in ("member", "administrator", "creator", "restricted")
    actor = update.from_user
    actor_id = actor.id if actor else None
    actor_name = (actor.username or actor.full_name) if actor else None

    # Credit the SHARER who referred the adder, else the adder themselves.
    credit_id, credit_name = actor_id, actor_name
    if actor_id:
        ref = await core.get_referrer(actor_id)
        if ref:
            credit_id, credit_name = ref, None  # name resolved from EchoUser later

    member_count = None
    try:
        member_count = await update.bot.get_chat_member_count(chat.id)
    except Exception:
        pass

    try:
        await core.record_bot_membership(
            chat.id, credit_id, credit_name, chat.title,
            is_admin=is_admin, active=active, member_count=member_count,
        )
        logger.info("ecco: membership %s in %s by %s -> credit %s (admin=%s, members=%s)",
                    status, chat.id, actor_id, credit_id, is_admin, member_count)
    except Exception as exc:
        logger.debug("ecco: membership record failed: %s", exc)


# ── Startup ─────────────────────────────────────────────────────────────────
async def _set_commands(echo_bot) -> None:
    """Register Echo's themed command menu (the BotFather command list)."""
    from aiogram.types import (
        BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats,
        BotCommandScopeDefault, BotCommandScopeChat,
    )
    try:
        # Groups: only /pod (the one command that works in a group).
        await echo_bot.set_my_commands(
            [BotCommand(command="pod", description="See the Pod rankings")],
            scope=BotCommandScopeAllGroupChats(),
        )
        # Everyone else (non-allowed users): only the public commands that
        # actually work for them — so they never see a command that does nothing.
        public = [
            BotCommand(command="pod", description="Pod rankings"),
            BotCommand(command="referral", description="Referral leaderboard + your link"),
            BotCommand(command="shill", description="Get a promo to recruit a group"),
        ]
        await echo_bot.set_my_commands(public, scope=BotCommandScopeAllPrivateChats())
        await echo_bot.set_my_commands(public, scope=BotCommandScopeDefault())
        # Allowed users (operator + co-admins): the FULL menu, scoped to their
        # own DM with the bot.
        full = [
            BotCommand(command="dive", description="Intelligence dashboard"),
            BotCommand(command="pod", description="Pod rankings"),
            BotCommand(command="rank", description="Your echoer + referral rank"),
            BotCommand(command="referral", description="Referral leaderboard + your link"),
            BotCommand(command="shill", description="Recruit-a-group promo"),
            BotCommand(command="echoers", description="Top echoers"),
            BotCommand(command="sonar", description="Current signals"),
            BotCommand(command="waves", description="Help"),
        ]
        for uid in core.ECHO_ADMIN_IDS:
            try:
                await echo_bot.set_my_commands(full, scope=BotCommandScopeChat(chat_id=uid))
            except Exception:
                pass
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
