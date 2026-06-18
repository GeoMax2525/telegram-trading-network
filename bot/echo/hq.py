"""
hq.py — Echo intelligence + controls, exposed on the MAIN (HQ) bot only.

The Echo bot itself just ingests + alerts; all private rankings and admin
controls live here, on the trading bot, restricted to HQ admins. This reads
the shared Data Hub directly (same PostgreSQL), so no admin surface is ever
exposed inside the external groups Echo sits in.
"""

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.config import ADMIN_IDS

logger = logging.getLogger(__name__)
router = Router()


def _ok(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id in ADMIN_IDS)


@router.message(Command("ecco"))
@router.message(Command("echo"))
async def cmd_echo_dashboard(message: Message) -> None:
    """ECCO intelligence dashboard — the private cross-group picture."""
    if not _ok(message):
        return
    from bot.echo import core, style
    text = style.hub_dashboard(
        await core.hub_stats(),
        footer="More:  /echo_stats   /echo_groups   /echo_signals   /echo_help",
    )
    await message.reply(text, parse_mode="Markdown")


@router.message(Command("echo_help"))
async def cmd_help(message: Message) -> None:
    if not _ok(message):
        return
    await message.reply(
        "🛰️ ECCO — Edge Consensus Crypto Oracle (HQ-only)\n"
        "/ecco — intelligence dashboard\n"
        "/echo_stats — top groups + callers\n"
        "/echo_signals — recent signals + outcomes\n"
        "/echo_groups — groups Echo is in\n"
        "/echo_threshold <n> — consensus threshold\n"
        "/echo_blacklist_group <chat_id> | /echo_unblacklist_group <chat_id>\n"
        "/echo_blacklist_user <user_id> | /echo_unblacklist_user <user_id>\n"
        "/echo_points <user_id> <delta> — manual point adjust",
        parse_mode="",
    )


@router.message(Command("echo_stats"))
async def cmd_stats(message: Message) -> None:
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, select, EchoGroup, EchoUser
    async with AsyncSessionLocal() as s:
        groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(10)
        )).scalars().all())
        users = list((await s.execute(
            select(EchoUser).order_by(EchoUser.points.desc()).limit(10)
        )).scalars().all())
    lines = ["🏆 TOP GROUPS"]
    for g in groups:
        lines.append(f"  {(g.chat_title or g.chat_id)} — {g.points:.0f} pts ({g.wins}W/{g.losses}L)")
    lines.append("\n🎯 TOP CALLERS")
    for u in users:
        lines.append(f"  @{u.username or u.user_id} — {u.points:.0f} pts ({u.wins}W/{u.losses}L)")
    await message.reply("\n".join(lines) if (groups or users) else "No Echo data yet.", parse_mode="")


@router.message(Command("echo_signals"))
async def cmd_signals(message: Message) -> None:
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, select, EchoSignal, EchoToken
    async with AsyncSessionLocal() as s:
        sigs = list((await s.execute(
            select(EchoSignal).order_by(EchoSignal.id.desc()).limit(15)
        )).scalars().all())
        lines = ["📡 RECENT ECHO SIGNALS"]
        for sig in sigs:
            tok = await s.get(EchoToken, sig.ca)
            outcome = "tracking"
            if tok:
                outcome = f"{tok.status} ({tok.ath_mult:.1f}x)" if tok.resolved else f"{tok.ath_mult:.1f}x peak"
            lines.append(f"  {(tok.token_name if tok else None) or sig.ca[:8]} — {sig.num_groups} grp · {sig.quality or '?'} · {outcome}")
    await message.reply("\n".join(lines) if sigs else "No signals fired yet.", parse_mode="")


@router.message(Command("echo_reset_scores"))
async def cmd_reset_scores(message: Message) -> None:
    """Wipe Echo win/loss/points (keeps groups, users, sightings, referrals).
    Use after a scoring-logic fix so the leaderboard starts clean."""
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, EchoGroup, EchoUser, EchoToken, select
    async with AsyncSessionLocal() as s:
        for g in (await s.execute(select(EchoGroup))).scalars().all():
            g.points = 0.0; g.wins = 0; g.losses = 0; g.calls = 0
        for u in (await s.execute(select(EchoUser))).scalars().all():
            u.points = 0.0; u.wins = 0; u.losses = 0; u.calls = 0
        # Re-track all tokens from scratch under the corrected logic.
        for t in (await s.execute(select(EchoToken))).scalars().all():
            t.status = "tracking"; t.resolved = False
            t.ath_mult = 1.0; t.first_mc = None; t.ath_mc = None
        await s.commit()
    await message.reply("✅ Echo scores wiped. Tokens re-tracking from scratch under the new logic.", parse_mode="")


@router.message(Command("echo_referrals"))
async def cmd_referrals(message: Message) -> None:
    """Referral leaderboard — who put ECCO in the most groups as admin (rewards)."""
    if not _ok(message):
        return
    from bot.echo import core
    board = await core.referral_leaderboard(20)
    lines = ["🤝 REFERRAL LEADERBOARD", "(groups where they added ECCO as admin)", ""]
    for i, e in enumerate(board, 1):
        who = f"@{e['username']}" if e["username"] and not str(e["username"]).isdigit() else str(e["user_id"])
        lines.append(f"{i}. {who[:22]} — {e['groups']} groups  (id {e['user_id']})")
    await message.reply("\n".join(lines) if board else "No referrals yet.", parse_mode="")


@router.message(Command("echo_groups"))
async def cmd_groups(message: Message) -> None:
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, select, EchoGroup
    async with AsyncSessionLocal() as s:
        groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.last_active_at.desc()).limit(30)
        )).scalars().all())
    lines = [f"📡 GROUPS ({len(groups)})"]
    for g in groups:
        bl = " 🚫" if g.blacklisted else ""
        lines.append(f"  {(g.chat_title or g.chat_id)} — {g.calls} calls · {g.points:.0f} pts{bl}")
    await message.reply("\n".join(lines) if groups else "No groups yet.", parse_mode="")


@router.message(Command("echo_threshold"))
async def cmd_threshold(message: Message) -> None:
    if not _ok(message):
        return
    from database.models import set_param
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply("Usage: /echo_threshold <n>", parse_mode="")
        return
    await set_param("echo_consensus_threshold", float(parts[1]), "Echo HQ admin")
    await message.reply(f"✅ Echo consensus threshold → {parts[1]} groups.", parse_mode="")


async def _set_blacklist(message: Message, *, is_group: bool, value: bool) -> None:
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, EchoGroup, EchoUser
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply("Usage: <command> <id>", parse_mode="")
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
    await message.reply(f"{'🚫 Blacklisted' if value else '✅ Un-blacklisted'} {target_id}.", parse_mode="")


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
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, EchoUser
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /echo_points <user_id> <delta>", parse_mode="")
        return
    try:
        uid, delta = int(parts[1]), float(parts[2])
    except ValueError:
        await message.reply("Usage: /echo_points <user_id> <delta>", parse_mode="")
        return
    async with AsyncSessionLocal() as s:
        u = await s.get(EchoUser, uid) or EchoUser(user_id=uid)
        u.points = (u.points or 0) + delta
        s.add(u)
        await s.commit()
        new_pts = u.points
    await message.reply(f"✅ {uid} adjusted {delta:+.0f} → {new_pts:.0f} pts.", parse_mode="")
