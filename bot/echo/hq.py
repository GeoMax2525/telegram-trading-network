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


@router.message(Command("echo_check"))
async def cmd_echo_check(message: Message) -> None:
    """Did ECCO see a given CA? Shows sightings (which chats, when) + its score.
    Usage: /echo_check <CA>"""
    if not _ok(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply("Usage: /echo_check <CA>", parse_mode="")
        return
    ca = parts[1].strip()
    from database.models import AsyncSessionLocal, select, EchoSighting, EchoToken
    async with AsyncSessionLocal() as s:
        rows = (await s.execute(
            select(EchoSighting.chat_title, EchoSighting.seen_at)
            .where(EchoSighting.ca == ca).order_by(EchoSighting.seen_at.asc())
        )).all()
        tok = await s.get(EchoToken, ca)
    if not rows:
        await message.reply(
            f"❌ NOT SEEN — {ca[:14]}…\n"
            "ECCO never received a message with this CA.\n"
            "Most likely it was posted by a BOT (ECCO can't see other bots), "
            "or ECCO isn't in/admin of that chat.",
            parse_mode="")
        return
    groups: dict = {}
    for title, seen in rows:
        groups.setdefault(title or "?", seen)
    lines = [f"✅ SEEN — {len(rows)} sightings · {len(groups)} group(s)", ""]
    for g, seen in list(groups.items())[:10]:
        lines.append(f"• {g[:24]} — {seen:%m-%d %H:%M}")
    if tok:
        state = ("resolved" if tok.resolved else "tracking")
        lines += ["", f"Score: {tok.status} · {tok.ath_mult:.1f}x peak · {state}"]
    await message.reply("\n".join(lines), parse_mode="")


@router.message(Command("echo_set_referrer"))
async def cmd_set_referrer(message: Message) -> None:
    """Manually credit a group's referral to a user (for groups added before
    attribution worked). Usage: /echo_set_referrer <group_chat_id> <@user|id>"""
    if not _ok(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Usage: /echo_set_referrer <group_chat_id> <@username|user_id>\n"
                            "(find chat ids with /echo_groups)", parse_mode="")
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        await message.reply("chat_id must be a number — see /echo_groups.", parse_mode="")
        return
    target = parts[2].lstrip("@")
    from database.models import AsyncSessionLocal, select, EchoUser, EchoReferralGroup, EchoGroup
    async with AsyncSessionLocal() as s:
        if target.isdigit():
            uid = int(target)
            u = await s.get(EchoUser, uid)
            uname = u.username if u else target
        else:
            u = (await s.execute(select(EchoUser).where(EchoUser.username == target))).scalars().first()
            if u is None:
                await message.reply(
                    f"@{target} hasn't interacted with ECCO yet, so I don't have their "
                    f"user ID. Get it (forward one of their msgs to @userinfobot) and run:\n"
                    f"/echo_set_referrer {chat_id} <their_user_id>", parse_mode="")
                return
            uid, uname = u.user_id, u.username
        title = None
        eg = await s.get(EchoGroup, chat_id)
        if eg:
            title = eg.chat_title
        g = await s.get(EchoReferralGroup, chat_id)
        if g is None:
            g = EchoReferralGroup(chat_id=chat_id, chat_title=title, is_admin=True, active=True)
            s.add(g)
        g.referrer_id = uid
        g.referrer_username = uname
        await s.commit()
    # Also put it on the pod board (it lives in a separate table).
    from bot.echo import core
    await core.ensure_group(chat_id, title)
    await message.reply(f"✅ Group {chat_id} credited to @{uname} (id {uid}) + added to the pod board.", parse_mode="")


@router.message(Command("echo_rescore"))
async def cmd_rescore(message: Message) -> None:
    """Re-score all resolved tokens with the current formula from scratch.
    Resets group/user points+calls+wins+losses to 0, clears the score ledger,
    then re-applies every resolved token so the final numbers are clean."""
    if not _ok(message):
        return
    from database.models import (
        AsyncSessionLocal, select, EchoToken, EchoScore, EchoGroup, EchoUser,
    )
    from bot.echo.core import apply_token_score, _credit_status_only
    await message.reply("⏳ Re-scoring resolved tokens…", parse_mode="")
    async with AsyncSessionLocal() as s:
        tokens = list((await s.execute(
            select(EchoToken).where(
                EchoToken.resolved.is_(True),
                EchoToken.status != "void",
            )
        )).scalars().all())
        # Zero counters and clear ledger only AFTER we have the token list.
        for g in (await s.execute(select(EchoGroup))).scalars().all():
            g.points = 0.0; g.wins = 0; g.losses = 0; g.calls = 0
        for u in (await s.execute(select(EchoUser))).scalars().all():
            u.points = 0.0; u.wins = 0; u.losses = 0; u.calls = 0
        for sc in (await s.execute(select(EchoScore))).scalars().all():
            await s.delete(sc)
        for t in tokens:
            t.awarded_points = 0.0
        await s.commit()

    n, skipped = 0, 0
    for tok in tokens:
        try:
            peak = tok.ath_mc
            if not peak and tok.ath_mult and tok.first_mc:
                peak = float(tok.ath_mult) * float(tok.first_mc)
            if not peak:
                # Token has no price data at all — credit it as a win/loss based
                # on status so it doesn't disappear from the record.
                if tok.status == "win":
                    await _credit_status_only(tok.ca, "win")
                elif tok.status in ("loss", "rug"):
                    await _credit_status_only(tok.ca, tok.status)
                skipped += 1
                continue
            await apply_token_score(tok.ca, peak)
            n += 1
        except Exception:
            skipped += 1
    await message.reply(
        f"✅ Re-scored {n} tokens. {skipped} used status fallback (no price data).",
        parse_mode=""
    )


@router.message(Command("echo_reset_scores"))
async def cmd_reset_scores(message: Message) -> None:
    """Wipe Echo win/loss/points (keeps groups, users, sightings, referrals).
    Use after a scoring-logic fix so the leaderboard starts clean."""
    if not _ok(message):
        return
    from database.models import AsyncSessionLocal, EchoGroup, EchoUser, EchoToken, select, set_param
    async with AsyncSessionLocal() as s:
        for g in (await s.execute(select(EchoGroup))).scalars().all():
            g.points = 0.0; g.wins = 0; g.losses = 0; g.calls = 0
        for u in (await s.execute(select(EchoUser))).scalars().all():
            u.points = 0.0; u.wins = 0; u.losses = 0; u.calls = 0
        for t in (await s.execute(select(EchoToken))).scalars().all():
            await s.delete(t)
        await s.commit()
    await set_param("echo_rug_liq_usd", 200.0, "Echo reset: lower rug floor")
    await message.reply("✅ Echo scores wiped + tokens cleared. New calls track fresh from the call.", parse_mode="")


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
        lines.append(f"{i}. {who[:22]} — {e['groups']} grp ({e.get('qualified', 0)} qual)  (id {e['user_id']})")
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
        lines.append(f"  {(g.chat_title or '?')[:20]} ({g.chat_id}) — {g.calls} calls · {g.points:.0f} pts{bl}")
    await message.reply("\n".join(lines) if groups else "No groups yet.", parse_mode="")


@router.message(Command("echo_health"))
async def cmd_health(message: Message) -> None:
    """Per-group health: 👑=admin, when ECCO last RECEIVED a message there, and
    CA sightings in 24h. ⚠️ flags a group gone quiet (>6h) — likely removed,
    demoted, or privacy not applied. The surest 'is it working everywhere' view."""
    if not _ok(message):
        return
    from datetime import datetime, timedelta
    from database.models import (
        AsyncSessionLocal, select, func, EchoGroup, EchoReferralGroup, EchoSighting,
    )
    now = datetime.utcnow()
    day_ago = now - timedelta(hours=24)
    async with AsyncSessionLocal() as s:
        groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.last_active_at.desc()).limit(40)
        )).scalars().all())
        refs = {r.chat_id: r for r in
                (await s.execute(select(EchoReferralGroup))).scalars().all()}
        lines = [f"🩺 GROUP HEALTH ({len(groups)})",
                 "👑=admin · last msg seen · CAs/24h", ""]
        live = 0
        for g in groups:
            last = g.last_active_at
            if last is None:
                ago, quiet = "never", True
            else:
                secs = (now - last).total_seconds()
                quiet = secs > 6 * 3600
                m = secs / 60.0
                ago = (f"{int(m)}m" if m < 60 else
                       f"{int(m/60)}h" if m < 1440 else f"{int(m/1440)}d")
            r = refs.get(g.chat_id)
            adm = "👑" if (r and r.is_admin) else "  "
            cnt = (await s.execute(
                select(func.count(EchoSighting.id)).where(
                    EchoSighting.chat_id == g.chat_id, EchoSighting.seen_at >= day_ago)
            )).scalar() or 0
            if not quiet:
                live += 1
            flag = " ⚠️QUIET" if quiet else ""
            lines.append(f"{adm} {(g.chat_title or str(g.chat_id))[:20]} — {ago} · {cnt}/24h{flag}")
        lines += ["", f"✅ {live}/{len(groups)} active in last 6h"]
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
