"""
app.py — the Echo Telegram bot: a separate Bot/Dispatcher (ECHO_BOT_TOKEN),
the ingest handler that silently records every CA sighting across all groups,
admin controls, and startup wiring. Zero trading code.
"""

import asyncio
import logging
from collections import deque
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
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


# ── Admin controls ──────────────────────────────────────────────────────────
def _is_admin(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id in core.ECHO_ADMIN_IDS)


@router.message(Command("echo_help"))
async def cmd_help(message: Message) -> None:
    if not _is_admin(message):
        return
    await message.reply(
        "🛰️ *Echo admin*\n"
        "/echo_stats — top groups + callers\n"
        "/echo_threshold <n> — consensus threshold\n"
        "/echo_groups — groups Echo is in\n"
        "/echo_blacklist_group <chat_id> | /echo_unblacklist_group <chat_id>\n"
        "/echo_blacklist_user <user_id> | /echo_unblacklist_user <user_id>\n"
        "/echo_points <user_id> <delta> — manual point adjust",
        parse_mode="Markdown",
    )


@router.message(Command("echo_stats"))
async def cmd_stats(message: Message) -> None:
    if not _is_admin(message):
        return
    from database.models import AsyncSessionLocal, select, EchoGroup, EchoUser
    async with AsyncSessionLocal() as s:
        groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(10)
        )).scalars().all())
        users = list((await s.execute(
            select(EchoUser).order_by(EchoUser.points.desc()).limit(10)
        )).scalars().all())
    lines = ["🏆 *Top Groups*"]
    for g in groups:
        lines.append(f"  {(g.chat_title or g.chat_id)} — {g.points:.0f} pts ({g.wins}W/{g.losses}L)")
    lines.append("\n🎯 *Top Callers*")
    for u in users:
        lines.append(f"  @{u.username or u.user_id} — {u.points:.0f} pts ({u.wins}W/{u.losses}L)")
    await message.reply("\n".join(lines) or "No data yet.", parse_mode="Markdown")


@router.message(Command("echo_threshold"))
async def cmd_threshold(message: Message) -> None:
    if not _is_admin(message):
        return
    from database.models import set_param
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply("Usage: /echo_threshold <n>")
        return
    await set_param("echo_consensus_threshold", float(parts[1]), "Echo admin")
    await message.reply(f"✅ Consensus threshold set to {parts[1]} groups.")


@router.message(Command("echo_groups"))
async def cmd_groups(message: Message) -> None:
    if not _is_admin(message):
        return
    from database.models import AsyncSessionLocal, select, EchoGroup
    async with AsyncSessionLocal() as s:
        groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.last_active_at.desc()).limit(30)
        )).scalars().all())
    lines = [f"📡 *Groups ({len(groups)})*"]
    for g in groups:
        bl = " 🚫" if g.blacklisted else ""
        lines.append(f"  {(g.chat_title or g.chat_id)} — {g.calls} calls{bl}")
    await message.reply("\n".join(lines) or "No groups yet.", parse_mode="Markdown")


async def _set_blacklist(message: Message, *, is_group: bool, value: bool) -> None:
    if not _is_admin(message):
        return
    from database.models import AsyncSessionLocal, EchoGroup, EchoUser
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: <command> <id>")
        return
    target_id = int(parts[1])
    async with AsyncSessionLocal() as s:
        if is_group:
            row = await s.get(EchoGroup, target_id) or EchoGroup(chat_id=target_id)
        else:
            row = await s.get(EchoUser, target_id) or EchoUser(user_id=target_id)
        row.blacklisted = value
        s.add(row)
        await s.commit()
    await message.reply(f"{'🚫 Blacklisted' if value else '✅ Un-blacklisted'} {target_id}.")


@router.message(Command("echo_blacklist_group"))
async def cmd_bl_group(message: Message) -> None:
    await _set_blacklist(message, is_group=True, value=True)


@router.message(Command("echo_unblacklist_group"))
async def cmd_unbl_group(message: Message) -> None:
    await _set_blacklist(message, is_group=True, value=False)


@router.message(Command("echo_blacklist_user"))
async def cmd_bl_user(message: Message) -> None:
    await _set_blacklist(message, is_group=False, value=True)


@router.message(Command("echo_unblacklist_user"))
async def cmd_unbl_user(message: Message) -> None:
    await _set_blacklist(message, is_group=False, value=False)


@router.message(Command("echo_points"))
async def cmd_points(message: Message) -> None:
    if not _is_admin(message):
        return
    from database.models import AsyncSessionLocal, EchoUser
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /echo_points <user_id> <delta>")
        return
    try:
        uid, delta = int(parts[1]), float(parts[2])
    except ValueError:
        await message.reply("Usage: /echo_points <user_id> <delta>")
        return
    async with AsyncSessionLocal() as s:
        u = await s.get(EchoUser, uid) or EchoUser(user_id=uid)
        u.points = (u.points or 0) + delta
        s.add(u)
        await s.commit()
        new_pts = u.points
    await message.reply(f"✅ {uid} adjusted by {delta:+.0f} → {new_pts:.0f} pts.")


# ── Ingest: the Trojan Horse data collection layer ──────────────────────────
# Registered LAST so the /echo_* command handlers above match first; this
# catch-all then handles every other group message.
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
async def start_echo() -> None:
    """Boot the Echo bot (own token, own polling) + its tracker loop. No-op if
    ECHO_BOT_TOKEN is unset, so the trading bot is unaffected."""
    if not core.echo_enabled():
        logger.info("Echo: ECHO_BOT_TOKEN not set — Echo dormant")
        return
    echo_bot = Bot(core.ECHO_BOT_TOKEN, default=DefaultBotProperties(parse_mode="Markdown"))
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(echo_tracker_loop(echo_bot))
    logger.info("Echo: starting (admins=%s)", core.ECHO_ADMIN_IDS)
    try:
        await echo_bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(echo_bot)
    finally:
        await echo_bot.session.close()
