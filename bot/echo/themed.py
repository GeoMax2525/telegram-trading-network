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
def _ref_link(bot_username: str, user_id) -> str:
    """Referral link that opens Telegram's group picker. When an admin selects
    a group, ECCO is added and the sharer automatically gets referral credit."""
    return f"https://t.me/{bot_username}?startgroup=ref_{user_id}"


@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject) -> None:
    """DM /start — show welcome. Group /start ref_XXX — record referral credit."""
    u = message.from_user
    payload = (command.args or "").strip()

    # Group flow: someone added ECCO via a referral link (?startgroup=ref_XXX).
    # Telegram sends /start ref_XXX into the group. Record the credit immediately
    # — this overrides any auto-detected adder credit since it's explicit intent.
    if message.chat.type in ("group", "supergroup"):
        if payload.startswith("ref_") and payload[4:].isdigit():
            referrer_id = int(payload[4:])
            try:
                member_count = await message.bot.get_chat_member_count(message.chat.id)
            except Exception:
                member_count = None
            await core.record_bot_membership(
                message.chat.id, referrer_id, None, message.chat.title,
                is_admin=True, active=True, member_count=member_count,
            )
            logger.info("ecco: referral credit %s -> group %s via startgroup link",
                        referrer_id, message.chat.id)
        return

    # DM flow — standard welcome screen.
    if u:
        await core.upsert_referrer_username(u.id, u.username or u.full_name)
    me = await message.bot.get_me()
    ref_link = _ref_link(me.username, u.id if u else "")
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 My Referral Stats", callback_data="echo:referral")],
        [InlineKeyboardButton(text="➕ Add ECCO to a group", url=ref_link)],
    ])
    await message.answer(
        style.welcome(core.ECCO_CONTACT) + f"\n\n🔗 Your referral link:\n{ref_link}",
        parse_mode="Markdown",
        reply_markup=kb,
    )


@router.message(Command("shill"))
async def cmd_shill(message: Message) -> None:
    if message.chat.type != "private":
        return
    u = message.from_user
    if u:
        await core.upsert_referrer_username(u.id, u.username or u.full_name)
    me = await message.bot.get_me()
    ref_link = _ref_link(me.username, u.id if u else "")
    await message.answer("📡 Forward the message below to recruit a group — you get the credit:")
    await message.answer(style.shill(ref_link, core.ECCO_CONTACT), parse_mode="Markdown")


@router.message(Command("referral"))
async def cmd_referral(message: Message) -> None:
    if message.chat.type != "private":
        return
    u = message.from_user
    try:
        if u:
            await core.upsert_referrer_username(u.id, u.username or u.full_name)
        me = await message.bot.get_me()
        ref_link = _ref_link(me.username, u.id if u else "")
        stats = await core.user_referral_stats(u.id if u else 0)
        board = await core.referral_leaderboard(5)
        await message.answer(
            style.referral_screen(stats, board)
            + f"\n\n🔗 Share this link — when someone adds ECCO via it, you get credit:\n{ref_link}",
            parse_mode="Markdown",
        )
    except Exception as exc:
        logger.warning("echo: /referral error: %s", exc)
        await message.answer(f"⚠️ Referral screen error: {exc}", parse_mode="")


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
    # HQ/operator only (global top callers = cross-group data, never public).
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
    action = (cb.data or "").split(":", 1)[-1]
    # Referral stats are public (anyone can check their own standing).
    # All other echo: callbacks are operator-only and DM-only.
    if action != "referral" and (not _op(cb.from_user) or not cb.message or cb.message.chat.type != "private"):
        try:
            await cb.answer()
        except Exception:
            pass
        return
    try:
        if action == "pod":
            text, kb = style.pod_screen(await core.top_groups(10), total=await core.network_group_count()), None
        elif action == "echoers":
            text, kb = style.echoers(await core.top_users(10)), None
        elif action == "sonar":
            text, kb = style.sonar_sweep(await core.active_signals(12)), None
        elif action == "waves":
            text, kb = style.waves_help(), None
        elif action == "referral":
            u = cb.from_user
            me = await cb.bot.get_me()
            ref_link = _ref_link(me.username, u.id if u else "")
            stats = await core.user_referral_stats(u.id if u else 0)
            board = await core.referral_leaderboard(5)
            text = (style.referral_screen(stats, board)
                    + f"\n\n🔗 Share this link — when someone adds ECCO via it, you get credit:\n{ref_link}")
            kb = None
        else:
            text, kb = await _dive_text(), style.kb_menu()
        # Refresh (dive) edits in place; other views post a new message below.
        if action == "dive":
            try:
                await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
            except Exception:
                await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)
        else:
            await cb.message.answer(text, parse_mode="Markdown", reply_markup=kb)
    except Exception as exc:
        logger.debug("ecco nav error: %s", exc)
    try:
        await cb.answer()
    except Exception:
        pass
