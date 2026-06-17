"""
core.py — Echo config, CA detection, Data Hub DB helpers, and the points engine.
"""

import os
import re
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
# ECCO = Edge Consensus Crypto Oracle. Accept the new ECCO_BOT_TOKEN, falling
# back to the legacy ECHO_BOT_TOKEN so either Railway var works.
ECHO_BOT_TOKEN = (os.getenv("ECCO_BOT_TOKEN") or os.getenv("ECHO_BOT_TOKEN") or "").strip()


def echo_enabled() -> bool:
    return bool(ECHO_BOT_TOKEN)


# ECCO co-admins granted access in code (numeric Telegram IDs). Merged in
# regardless of env, so granting one person ECCO access needs no Railway change.
# This is ECCO-only access — NOT trading-bot/wallet access (that's ADMIN_IDS).
ECCO_CO_ADMIN_IDS: set[int] = {435533326}


def _admin_ids() -> set[int]:
    raw = (os.getenv("ECCO_ADMIN_IDS") or os.getenv("ECHO_ADMIN_IDS") or "").strip()
    ids: set[int] = set()
    if raw:
        for part in raw.replace(";", ",").split(","):
            part = part.strip()
            if part.lstrip("-").isdigit():
                ids.add(int(part))
    if not ids:
        try:
            from bot.config import ADMIN_IDS
            ids = set(ADMIN_IDS)
        except Exception:
            pass
    ids |= ECCO_CO_ADMIN_IDS
    return ids


ECHO_ADMIN_IDS = _admin_ids()

# ── CA detection ────────────────────────────────────────────────────────────
# Solana mint: base58, 32-44 chars. Same pattern the trading scraper uses.
_CA_PATTERN = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
# Obvious non-CA base58 noise to drop (system/native program ids etc.)
_CA_IGNORE = {
    "So11111111111111111111111111111111111111112",   # wrapped SOL
    "11111111111111111111111111111111",               # system program
}


def extract_cas(text: str) -> list[str]:
    """Return de-duplicated candidate Solana CAs found in a message."""
    if not text:
        return []
    out: list[str] = []
    seen = set()
    for m in _CA_PATTERN.findall(text):
        if m in _CA_IGNORE or m in seen:
            continue
        seen.add(m)
        out.append(m)
    return out


def message_link(chat_id: int, chat_username: str | None, message_id: int | None) -> str | None:
    if not message_id:
        return None
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    # private supergroup deep link: -100<internal> -> t.me/c/<internal>/<msg>
    s = str(chat_id)
    if s.startswith("-100"):
        return f"https://t.me/c/{s[4:]}/{message_id}"
    return None


# ── Data Hub helpers ────────────────────────────────────────────────────────
async def get_echo_param(name: str, default: float) -> float:
    from database.models import get_param
    try:
        v = await get_param(name)
        return float(v) if v is not None else default
    except Exception:
        return default


async def is_blacklisted(chat_id: int | None, user_id: int | None) -> bool:
    from database.models import AsyncSessionLocal, EchoGroup, EchoUser
    async with AsyncSessionLocal() as s:
        if chat_id is not None:
            g = await s.get(EchoGroup, chat_id)
            if g is not None and g.blacklisted:
                return True
        if user_id is not None:
            u = await s.get(EchoUser, user_id)
            if u is not None and u.blacklisted:
                return True
    return False


async def record_sighting(*, ca, chat_id, chat_title, user_id, username,
                          message_id, msg_link, token_name=None, token_symbol=None,
                          entry_mc=None) -> bool:
    """Record one CA sighting + ensure group/user/token rows. Returns True if
    this is a NEW (ca, chat_id) pair (i.e. counts toward consensus)."""
    from database.models import (
        AsyncSessionLocal, select, func,
        EchoSighting, EchoGroup, EchoUser, EchoToken,
    )
    now = datetime.utcnow()
    async with AsyncSessionLocal() as s:
        # New distinct group for this CA?
        existing = (await s.execute(
            select(func.count(EchoSighting.id)).where(
                EchoSighting.ca == ca, EchoSighting.chat_id == chat_id,
            )
        )).scalar() or 0
        is_new_group = existing == 0

        s.add(EchoSighting(
            ca=ca, chat_id=chat_id, chat_title=chat_title, user_id=user_id,
            username=username, message_id=message_id, message_link=msg_link,
            token_name=token_name, token_symbol=token_symbol, entry_mc=entry_mc,
            seen_at=now,
        ))

        grp = await s.get(EchoGroup, chat_id)
        if grp is None:
            s.add(EchoGroup(chat_id=chat_id, chat_title=chat_title, last_active_at=now))
        else:
            grp.last_active_at = now
            if chat_title:
                grp.chat_title = chat_title

        if user_id is not None:
            usr = await s.get(EchoUser, user_id)
            if usr is None:
                s.add(EchoUser(user_id=user_id, username=username))
            elif username:
                usr.username = username

        tok = await s.get(EchoToken, ca)
        if tok is None:
            s.add(EchoToken(
                ca=ca, token_name=token_name, symbol=token_symbol,
                first_seen_at=now, first_mc=entry_mc, ath_mc=entry_mc,
                ath_mult=1.0, last_checked_at=now,
            ))
        await s.commit()
    return is_new_group


async def consensus_state(ca: str, window_min: float) -> tuple[int, int]:
    """(distinct groups calling this CA in the window, total active groups)."""
    from database.models import AsyncSessionLocal, select, func, EchoSighting, EchoGroup
    cutoff = datetime.utcnow() - timedelta(minutes=window_min)
    async with AsyncSessionLocal() as s:
        callers = (await s.execute(
            select(func.count(func.distinct(EchoSighting.chat_id))).where(
                EchoSighting.ca == ca, EchoSighting.seen_at >= cutoff,
            )
        )).scalar() or 0
        total = (await s.execute(
            select(func.count(EchoGroup.chat_id)).where(
                EchoGroup.last_active_at >= cutoff,
                EchoGroup.blacklisted.is_(False),
            )
        )).scalar() or 0
    return int(callers), int(total)


async def calling_group_ids(ca: str, window_min: float) -> list[int]:
    from database.models import AsyncSessionLocal, select, func, EchoSighting
    cutoff = datetime.utcnow() - timedelta(minutes=window_min)
    async with AsyncSessionLocal() as s:
        rows = (await s.execute(
            select(func.distinct(EchoSighting.chat_id)).where(
                EchoSighting.ca == ca, EchoSighting.seen_at >= cutoff,
            )
        )).scalars().all()
    return [int(r) for r in rows]


# ── Read helpers for the themed menus ───────────────────────────────────────
async def pod_overview() -> tuple[int, float]:
    """(active signal count, avg performance x of winning signals)."""
    from database.models import AsyncSessionLocal, select, func, EchoToken
    async with AsyncSessionLocal() as s:
        active = (await s.execute(
            select(func.count(EchoToken.ca)).where(
                EchoToken.signaled.is_(True), EchoToken.resolved.is_(False),
            )
        )).scalar() or 0
        avg = (await s.execute(
            select(func.avg(EchoToken.ath_mult)).where(
                EchoToken.signaled.is_(True), EchoToken.ath_mult >= 2.0,
            )
        )).scalar()
    return int(active), float(avg or 0.0)


async def active_signals(limit: int = 12) -> list[tuple]:
    """[(label, ath_mult, quality)] for signaled tokens, best first."""
    from database.models import AsyncSessionLocal, select, EchoToken, EchoSignal
    out = []
    async with AsyncSessionLocal() as s:
        toks = list((await s.execute(
            select(EchoToken).where(EchoToken.signaled.is_(True))
            .order_by(EchoToken.ath_mult.desc()).limit(limit)
        )).scalars().all())
        for t in toks:
            sig = (await s.execute(
                select(EchoSignal).where(EchoSignal.ca == t.ca)
                .order_by(EchoSignal.id.desc()).limit(1)
            )).scalar_one_or_none()
            out.append((t.token_name or t.ca[:6], t.ath_mult or 1.0,
                        sig.quality if sig else "Medium Quality Signal"))
    return out


async def top_groups(n: int = 10) -> list:
    from database.models import AsyncSessionLocal, select, EchoGroup
    async with AsyncSessionLocal() as s:
        return list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(n)
        )).scalars().all())


async def top_users(n: int = 10) -> list:
    from database.models import AsyncSessionLocal, select, EchoUser
    async with AsyncSessionLocal() as s:
        return list((await s.execute(
            select(EchoUser).order_by(EchoUser.points.desc()).limit(n)
        )).scalars().all())


async def hub_stats() -> dict:
    """Everything for the full hub dashboard: totals + top groups/callers/signals."""
    from database.models import (
        AsyncSessionLocal, select, func,
        EchoGroup, EchoUser, EchoSighting, EchoSignal, EchoToken,
    )
    async with AsyncSessionLocal() as s:
        n_groups = (await s.execute(select(func.count(EchoGroup.chat_id)))).scalar() or 0
        n_users = (await s.execute(select(func.count(EchoUser.user_id)))).scalar() or 0
        n_sightings = (await s.execute(select(func.count(EchoSighting.id)))).scalar() or 0
        n_signals = (await s.execute(select(func.count(EchoSignal.id)))).scalar() or 0
        n_wins = (await s.execute(
            select(func.count(EchoToken.ca)).where(EchoToken.status == "win"))).scalar() or 0
        n_losses = (await s.execute(
            select(func.count(EchoToken.ca)).where(EchoToken.status == "loss"))).scalar() or 0
        top_groups = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(5))).scalars().all())
        top_users_ = list((await s.execute(
            select(EchoUser).order_by(EchoUser.points.desc()).limit(5))).scalars().all())
        recent = list((await s.execute(
            select(EchoSignal).order_by(EchoSignal.id.desc()).limit(5))).scalars().all())
    return {
        "n_groups": n_groups, "n_users": n_users, "n_sightings": n_sightings,
        "n_signals": n_signals, "n_wins": n_wins, "n_losses": n_losses,
        "top_groups": top_groups, "top_users": top_users_, "recent": recent,
    }


# ── Points engine ───────────────────────────────────────────────────────────
RUG_PENALTY = -200.0
WIN_BASE = 100.0
EARLY_BONUS = 50.0


def win_points(ath_mult: float, is_first_caller: bool) -> float:
    """Positive points for a winning call, scaled by multiplier + early bonus."""
    pts = WIN_BASE + min(float(ath_mult), 100.0) * 10.0
    if is_first_caller:
        pts += EARLY_BONUS
    return round(pts, 1)


async def quality_grade(ca: str, window_min: float) -> str:
    """Public grade from the PRIVATE points of the groups currently calling it.
    Reflects caller reputation, computed at signal time (before resolution)."""
    from database.models import AsyncSessionLocal, select, EchoGroup, EchoSighting, func
    cutoff = datetime.utcnow() - timedelta(minutes=window_min)
    async with AsyncSessionLocal() as s:
        chat_ids = (await s.execute(
            select(func.distinct(EchoSighting.chat_id)).where(
                EchoSighting.ca == ca, EchoSighting.seen_at >= cutoff,
            )
        )).scalars().all()
        if not chat_ids:
            return "Medium Quality Signal"
        avg = (await s.execute(
            select(func.avg(EchoGroup.points)).where(EchoGroup.chat_id.in_(list(chat_ids)))
        )).scalar()
    avg = float(avg or 0.0)
    if avg >= 300:
        return "High Quality Signal"
    if avg >= 0:
        return "Medium Quality Signal"
    return "Low Quality Signal"


async def award_resolution(ca: str, ath_mult: float, win: bool) -> None:
    """On win/loss, credit/debit every group + user that called this CA.
    First caller (earliest sighting) gets the early bonus on a win."""
    from database.models import (
        AsyncSessionLocal, select, EchoSighting, EchoGroup, EchoUser,
    )
    async with AsyncSessionLocal() as s:
        sightings = (await s.execute(
            select(EchoSighting).where(EchoSighting.ca == ca).order_by(EchoSighting.seen_at.asc())
        )).scalars().all()
        if not sightings:
            return
        first_chat = sightings[0].chat_id
        first_user = sightings[0].user_id
        seen_groups: set[int] = set()
        seen_users: set[int] = set()
        for sg in sightings:
            if sg.chat_id is not None and sg.chat_id not in seen_groups:
                seen_groups.add(sg.chat_id)
                grp = await s.get(EchoGroup, sg.chat_id)
                if grp and not grp.blacklisted:
                    grp.calls = (grp.calls or 0) + 1
                    if win:
                        grp.wins = (grp.wins or 0) + 1
                        grp.points = (grp.points or 0) + win_points(ath_mult, sg.chat_id == first_chat)
                    else:
                        grp.losses = (grp.losses or 0) + 1
                        grp.points = (grp.points or 0) + RUG_PENALTY
            if sg.user_id is not None and sg.user_id not in seen_users:
                seen_users.add(sg.user_id)
                usr = await s.get(EchoUser, sg.user_id)
                if usr and not usr.blacklisted:
                    usr.calls = (usr.calls or 0) + 1
                    if win:
                        usr.wins = (usr.wins or 0) + 1
                        usr.points = (usr.points or 0) + win_points(ath_mult, sg.user_id == first_user)
                    else:
                        usr.losses = (usr.losses or 0) + 1
                        usr.points = (usr.points or 0) + RUG_PENALTY
        await s.commit()
