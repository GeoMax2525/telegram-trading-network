"""
themed.py — ECCO's Ecco-the-Dolphin-styled command interface.

The game-screen menus: /dive /pod /echoers /sonar /waves + inline-button nav.
GATED TO THE OPERATOR (ECCO_ADMIN_IDS): external groups never get any command
access — they only ever see the bot-posted SONAR REPORT alerts. All ranking
data stays with HQ. Registered BEFORE the ingest catch-all so commands match
first (and then get dropped for non-admins).
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from bot.echo import core, style

logger = logging.getLogger(__name__)
router = Router()


def _op(user) -> bool:
    """Only the operator (ECCO admin) may use any command/button."""
    return bool(user and user.id in core.ECHO_ADMIN_IDS)


async def _dive_text() -> str:
    """The /dive screen IS the full hub — totals, top pods, echoers, sonar."""
    return style.hub_dashboard(await core.hub_stats())


@router.message(Command("start"))
@router.message(Command("dive"))
async def cmd_dive(message: Message) -> None:
    if not _op(message.from_user):
        return
    await message.answer(await _dive_text(), parse_mode="Markdown", reply_markup=style.kb_menu())


@router.message(Command("pod"))
async def cmd_pod(message: Message) -> None:
    if not _op(message.from_user):
        return
    n, avg = await core.pod_overview()
    await message.answer(style.pod_status(n, avg), parse_mode="Markdown", reply_markup=style.kb_pod_links())


@router.message(Command("echoers"))
async def cmd_echoers(message: Message) -> None:
    if not _op(message.from_user):
        return
    users = await core.top_users(10)
    await message.answer(style.echoers(users), parse_mode="Markdown")


@router.message(Command("sonar"))
async def cmd_sonar(message: Message) -> None:
    if not _op(message.from_user):
        return
    active = await core.active_signals(12)
    await message.answer(style.sonar_sweep(active), parse_mode="Markdown")


@router.message(Command("waves"))
async def cmd_waves(message: Message) -> None:
    if not _op(message.from_user):
        return
    await message.answer(style.waves_help(), parse_mode="Markdown")


# ── Inline-button navigation (operator only) ────────────────────────────────
@router.callback_query(F.data.startswith("echo:"))
async def on_nav(cb: CallbackQuery) -> None:
    if not _op(cb.from_user):
        try:
            await cb.answer()
        except Exception:
            pass
        return
    action = (cb.data or "").split(":", 1)[-1]
    try:
        if action == "pod":
            text, kb = style.pod_rankings(await core.top_groups(10)), None
        elif action == "echoers":
            text, kb = style.echoers(await core.top_users(10)), None
        elif action == "sonar":
            text, kb = style.sonar_sweep(await core.active_signals(12)), None
        elif action == "waves":
            text, kb = style.waves_help(), None
        else:  # dive / default
            text, kb = await _dive_text(), style.kb_menu()
        await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)
    except Exception as exc:
        logger.debug("ecco nav error: %s", exc)
    try:
        await cb.answer()
    except Exception:
        pass
