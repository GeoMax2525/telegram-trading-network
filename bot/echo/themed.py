"""
themed.py — Echo's public, Ecco-the-Dolphin-styled command interface.

The game-screen menus shown to groups/users: /dive /pod /echoers /sonar /waves
plus the inline-button navigation. Read-only — all controls live on the HQ bot.
Registered BEFORE the ingest catch-all so commands match first.
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery

from bot.echo import core, style

logger = logging.getLogger(__name__)
router = Router()


async def _dive_text() -> str:
    n, _ = await core.pod_overview()
    return style.dive_menu(n)


@router.message(Command("start"))
@router.message(Command("dive"))
async def cmd_dive(message: Message) -> None:
    await message.answer(await _dive_text(), parse_mode="Markdown", reply_markup=style.kb_menu())


@router.message(Command("pod"))
async def cmd_pod(message: Message) -> None:
    n, avg = await core.pod_overview()
    await message.answer(style.pod_status(n, avg), parse_mode="Markdown", reply_markup=style.kb_pod_links())


@router.message(Command("echoers"))
async def cmd_echoers(message: Message) -> None:
    users = await core.top_users(10)
    await message.answer(style.echoers(users), parse_mode="Markdown")


@router.message(Command("sonar"))
async def cmd_sonar(message: Message) -> None:
    active = await core.active_signals(12)
    await message.answer(style.sonar_sweep(active), parse_mode="Markdown")


@router.message(Command("waves"))
async def cmd_waves(message: Message) -> None:
    await message.answer(style.waves_help(), parse_mode="Markdown")


# ── Inline-button navigation ────────────────────────────────────────────────
@router.callback_query(F.data.startswith("echo:"))
async def on_nav(cb: CallbackQuery) -> None:
    action = (cb.data or "").split(":", 1)[-1]
    try:
        if action == "pod":
            groups = await core.top_groups(10)
            text, kb = style.pod_rankings(groups), None
        elif action == "echoers":
            users = await core.top_users(10)
            text, kb = style.echoers(users), None
        elif action == "sonar":
            active = await core.active_signals(12)
            text, kb = style.sonar_sweep(active), None
        elif action == "waves":
            text, kb = style.waves_help(), None
        else:  # dive / default
            text, kb = await _dive_text(), style.kb_menu()
        await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)
    except Exception as exc:
        logger.debug("echo nav error: %s", exc)
    try:
        await cb.answer()
    except Exception:
        pass
