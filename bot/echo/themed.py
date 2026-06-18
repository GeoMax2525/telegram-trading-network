"""
themed.py — ECCO's command interface, with a strict access model.

ACCESS MODEL (the only data a chat may see):
  • /pod  — PUBLIC. Pod rankings (group leaderboard) + that group's own stats.
            The single command that works inside groups.
  • /dive /echoers /sonar /waves — OPERATOR + PRIVATE-CHAT ONLY. These reveal
            cross-group intelligence (full hub, caller rankings, signal sweeps),
            so they're DM-only — their output can never land in a group, even
            if the operator runs them there.
Registered before the ingest catch-all so commands match first.
"""

import logging

from aiogram import Router, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import Message, CallbackQuery

from bot.echo import core, style

logger = logging.getLogger(__name__)
router = Router()


def _op(user) -> bool:
    return bool(user and user.id in core.ECHO_ADMIN_IDS)


def _op_dm(message: Message) -> bool:
    """Operator AND in a private chat — gate for the sensitive commands so their
    output can never be posted into a group."""
    return _op(message.from_user) and message.chat.type == "private"


# ── PUBLIC: /pod — the only command that works in groups ────────────────────
@router.message(Command("pod"))
async def cmd_pod(message: Message) -> None:
    own = None
    if message.chat.type in ("group", "supergroup"):
        own = await core.group_stats(message.chat.id)
    total = await core.network_group_count()
    await message.answer(
        style.pod_screen(await core.top_groups(10), own, total),
        parse_mode="Markdown",
    )


# ── PUBLIC, DM-only: /rank — your own standing ──────────────────────────────
@router.message(Command("rank"))
async def cmd_rank(message: Message) -> None:
    if message.chat.type != "private":
        return
    uid = message.from_user.id if message.from_user else 0
    echoer = await core.user_echoer_stats(uid)
    referral = await core.user_referral_stats(uid)
    await message.answer(style.rank_screen(echoer, referral), parse_mode="Markdown")


# ── PUBLIC, DM-only: /start (captures referrals) + /referral ────────────────
@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject) -> None:
    if message.chat.type != "private":
        return
    u = message.from_user
    if u:
        await core.upsert_referrer_username(u.id, u.username or u.full_name)
    payload = (command.args or "").strip()
    if payload.isdigit() and u:
        await core.set_referred_by(u.id, int(payload))  # credited to the sharer
    me = await message.bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={u.id if u else ''}"
    add_link = f"https://t.me/{me.username}?startgroup=true"
    await message.answer(
        style.welcome(core.ECCO_CONTACT) + f"\n\n➕ Add ECCO to a group: {add_link}\n🔗 Your referral link: {ref_link}",
        parse_mode="Markdown",
    )


@router.message(Command("shill"))
async def cmd_shill(message: Message) -> None:
    """Hands the user a forwardable recruit-a-group promo with their referral
    link baked in (so they get the credit)."""
    if message.chat.type != "private":
        return
    u = message.from_user
    if u:
        await core.upsert_referrer_username(u.id, u.username or u.full_name)
    me = await message.bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={u.id if u else ''}"
    await message.answer("📡 Forward the message below to any group to recruit it — you get the credit:")
    await message.answer(style.shill(ref_link, core.ECCO_CONTACT), parse_mode="Markdown")


@router.message(Command("referral"))
async def cmd_referral(message: Message) -> None:
    if message.chat.type != "private":
        return  # keep groups clean — DM only (but open to anyone)
    u = message.from_user
    if u:
        await core.upsert_referrer_username(u.id, u.username or u.full_name)
    me = await message.bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={u.id if u else ''}"
    add_link = f"https://t.me/{me.username}?startgroup=true"
    stats = await core.user_referral_stats(u.id if u else 0)
    board = await core.referral_leaderboard(5)
    await message.answer(
        style.referral_screen(stats, board)
        + f"\n\n🔗 Your referral link: {ref_link}\n➕ Add to a group: {add_link}",
        parse_mode="Markdown",
    )


# ── OPERATOR + DM ONLY ──────────────────────────────────────────────────────
async def _dive_text() -> str:
    return style.hub_dashboard(
        await core.hub_stats(),
        footer="More:  /pod   /echoers   /sonar   /waves",
    )


@router.message(Command("dive"))
async def cmd_dive(message: Message) -> None:
    if not _op_dm(message):
        return
    await message.answer(await _dive_text(), parse_mode="Markdown", reply_markup=style.kb_menu())


@router.message(Command("echoers"))
async def cmd_echoers(message: Message) -> None:
    if not _op_dm(message):
        return
    await message.answer(style.echoers(await core.top_users(10)), parse_mode="Markdown")


@router.message(Command("sonar"))
async def cmd_sonar(message: Message) -> None:
    if not _op_dm(message):
        return
    await message.answer(style.sonar_sweep(await core.active_signals(12)), parse_mode="Markdown")


@router.message(Command("waves"))
async def cmd_waves(message: Message) -> None:
    if not _op_dm(message):
        return
    await message.answer(style.waves_help(), parse_mode="Markdown")


# ── Inline-button navigation (operator DM only) ─────────────────────────────
@router.callback_query(F.data.startswith("echo:"))
async def on_nav(cb: CallbackQuery) -> None:
    if not _op(cb.from_user) or (cb.message and cb.message.chat.type != "private"):
        try:
            await cb.answer()
        except Exception:
            pass
        return
    action = (cb.data or "").split(":", 1)[-1]
    try:
        if action == "pod":
            text, kb = style.pod_screen(await core.top_groups(10), total=await core.network_group_count()), None
        elif action == "echoers":
            text, kb = style.echoers(await core.top_users(10)), None
        elif action == "sonar":
            text, kb = style.sonar_sweep(await core.active_signals(12)), None
        elif action == "waves":
            text, kb = style.waves_help(), None
        else:
            text, kb = await _dive_text(), style.kb_menu()
        await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)
    except Exception as exc:
        logger.debug("ecco nav error: %s", exc)
    try:
        await cb.answer()
    except Exception:
        pass
