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


# Operator contact shown in the welcome + shill (questions). Override on
# Railway with ECCO_CONTACT if the handle ever changes.
ECCO_CONTACT = os.getenv("ECCO_CONTACT", "@miracleXwhip").strip()


async def historical_ath_mult(ca: str, since_dt) -> float | None:
    """TRUE peak multiple since the call, from GeckoTerminal OHLCV — so a brief
    spike between spot checks is never missed. Returns peak_high / call_open,
    or None if unavailable. Used at resolution to confirm a win before a loss."""
    import aiohttp
    base = "https://api.geckoterminal.com/api/v2/networks/solana"
    headers = {"accept": "application/json"}
    try:
        since_ts = int(since_dt.timestamp())
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{base}/tokens/{ca}/pools?page=1", headers=headers,
                                timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    return None
                pools = (await r.json()).get("data") or []
            if not pools:
                return None
            pool = (pools[0].get("attributes") or {}).get("address")
            if not pool:
                return None
            async with sess.get(f"{base}/pools/{pool}/ohlcv/hour?aggregate=1&limit=500",
                                headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status != 200:
                    return None
                ohlcv = ((await r.json()).get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
        # ohlcv rows: [timestamp, open, high, low, close, volume]
        rel = sorted([c for c in ohlcv if c and c[0] >= since_ts], key=lambda c: c[0])
        if not rel:
            return None
        call_open = float(rel[0][1] or 0)
        peak = max(float(c[2] or 0) for c in rel)
        if call_open <= 0 or peak <= 0:
            return None
        return peak / call_open
    except Exception as exc:
        logger.debug("echo: historical_ath fetch failed %s: %s", ca[:8], exc)
        return None


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


async def recent_unsignaled_cas(window_min: float, limit: int = 300) -> list[str]:
    """Distinct CAs sighted within the window that haven't fired a signal yet —
    so the bot lane can fire consensus alerts for CAs recorded by EITHER lane
    (bot or the Telethon user-account listener)."""
    from database.models import AsyncSessionLocal, select, func, EchoSighting, EchoToken
    cutoff = datetime.utcnow() - timedelta(minutes=window_min)
    async with AsyncSessionLocal() as s:
        cas = (await s.execute(
            select(func.distinct(EchoSighting.ca)).where(EchoSighting.seen_at >= cutoff)
        )).scalars().all()
        if not cas:
            return []
        # One batch query for the already-signaled set instead of N s.get() calls.
        signaled = set((await s.execute(
            select(EchoToken.ca).where(EchoToken.ca.in_(cas), EchoToken.signaled.is_(True))
        )).scalars().all())
    return [c for c in cas if c not in signaled][:limit]


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


# ── Referral / rewards attribution ──────────────────────────────────────────
async def set_referred_by(user_id, referrer_id) -> None:
    """Record who referred a user (first referrer wins). Called when someone
    opens ECCO via a t.me/<bot>?start=<referrerId> link."""
    from database.models import AsyncSessionLocal, EchoReferredUser
    if not user_id or not referrer_id or int(user_id) == int(referrer_id):
        return
    async with AsyncSessionLocal() as s:
        if await s.get(EchoReferredUser, user_id) is None:
            s.add(EchoReferredUser(user_id=user_id, referrer_id=int(referrer_id)))
            await s.commit()


async def get_referrer(user_id) -> int | None:
    from database.models import AsyncSessionLocal, EchoReferredUser
    async with AsyncSessionLocal() as s:
        row = await s.get(EchoReferredUser, user_id)
        return row.referrer_id if row else None


async def upsert_referrer_username(user_id, username) -> None:
    """Keep a username on file (in EchoUser) so the leaderboard can show @handles
    even for referrers who never posted a CA themselves."""
    from database.models import AsyncSessionLocal, EchoUser
    if not user_id:
        return
    async with AsyncSessionLocal() as s:
        u = await s.get(EchoUser, user_id)
        if u is None:
            s.add(EchoUser(user_id=user_id, username=username))
        elif username:
            u.username = username
        await s.commit()


async def record_bot_membership(chat_id, credit_id, credit_username, title,
                                *, is_admin: bool, active: bool, member_count=None) -> None:
    """Record/refresh a group's referral row. credit_id = who gets the credit
    (the sharer who referred the adder, else the adder)."""
    from database.models import AsyncSessionLocal, EchoReferralGroup
    async with AsyncSessionLocal() as s:
        row = await s.get(EchoReferralGroup, chat_id)
        if row is None:
            s.add(EchoReferralGroup(
                chat_id=chat_id, referrer_id=credit_id, referrer_username=credit_username,
                chat_title=title, is_admin=is_admin, active=active,
                member_count=int(member_count or 0),
            ))
        else:
            row.is_admin = is_admin
            row.active = active
            if title:
                row.chat_title = title
            if member_count is not None:
                row.member_count = int(member_count)
            if not row.referrer_id and credit_id:  # backfill if first add was missed
                row.referrer_id, row.referrer_username = credit_id, credit_username
        await s.commit()


async def _qualified_referral_groups() -> list:
    """Referral rows that COUNT toward rewards: active admin groups with enough
    members AND enough distinct CA posters — the anti-fake-group gate."""
    from database.models import AsyncSessionLocal, select, func, EchoReferralGroup, EchoSighting
    min_members = await get_echo_param("echo_ref_min_members", 20.0)
    min_posters = await get_echo_param("echo_ref_min_posters", 3.0)
    out = []
    async with AsyncSessionLocal() as s:
        groups = (await s.execute(
            select(EchoReferralGroup).where(
                EchoReferralGroup.active.is_(True),
                EchoReferralGroup.is_admin.is_(True),
                EchoReferralGroup.referrer_id.isnot(None),
                EchoReferralGroup.member_count >= min_members)
        )).scalars().all()
        for g in groups:
            posters = (await s.execute(
                select(func.count(func.distinct(EchoSighting.user_id))).where(
                    EchoSighting.chat_id == g.chat_id, EchoSighting.user_id.isnot(None))
            )).scalar() or 0
            if posters >= min_posters:
                out.append(g)
    return out


async def referral_leaderboard(n: int = 20) -> list[dict]:
    """[{user_id, username, groups, qualified}] — EVERY referrer who brought an
    active admin group shows up (so new referrers appear immediately). Ranked by
    qualified then total. 'qualified' = groups meeting the reward bar (real
    members + activity)."""
    from database.models import AsyncSessionLocal, select, EchoReferralGroup, EchoUser
    qualified_ids = {g.chat_id for g in await _qualified_referral_groups()}
    async with AsyncSessionLocal() as s:
        groups = (await s.execute(
            select(EchoReferralGroup).where(
                EchoReferralGroup.active.is_(True),
                EchoReferralGroup.is_admin.is_(True),
                EchoReferralGroup.referrer_id.isnot(None))
        )).scalars().all()
    tally: dict = {}
    for g in groups:
        e = tally.setdefault(g.referrer_id, [0, 0])  # [total, qualified]
        e[0] += 1
        if g.chat_id in qualified_ids:
            e[1] += 1
    board = []
    async with AsyncSessionLocal() as s:
        for uid, (total, qual) in tally.items():
            u = await s.get(EchoUser, uid)
            board.append({"user_id": uid, "username": (u.username if u else None),
                          "groups": total, "qualified": qual})
    board.sort(key=lambda e: (e["qualified"], e["groups"]), reverse=True)
    return board[:n]


async def user_referral_stats(user_id: int) -> dict:
    board = await referral_leaderboard(10000)
    entry = next((e for e in board if e["user_id"] == user_id), None)
    rank = next((i for i, e in enumerate(board, 1) if e["user_id"] == user_id), None)
    return {
        "qualified_groups": (entry["qualified"] if entry else 0),
        "total_groups": (entry["groups"] if entry else 0),
        "rank": rank, "total_referrers": len(board),
    }


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


async def group_rank(chat_id: int) -> tuple[int, int]:
    """(rank, total) of this group on the pod leaderboard by points. 1 = best."""
    from database.models import AsyncSessionLocal, select, func, EchoGroup
    async with AsyncSessionLocal() as s:
        total = (await s.execute(select(func.count(EchoGroup.chat_id)))).scalar() or 0
        g = await s.get(EchoGroup, chat_id)
        if g is None:
            return (total + 1, total + 1)
        higher = (await s.execute(
            select(func.count(EchoGroup.chat_id)).where(EchoGroup.points > (g.points or 0))
        )).scalar() or 0
    return (higher + 1, total)


async def ensure_group(chat_id: int, title) -> None:
    """Create/refresh an EchoGroup row so a group shows on the board the moment
    ECCO is added — before any CA is posted there."""
    from database.models import AsyncSessionLocal, EchoGroup
    async with AsyncSessionLocal() as s:
        g = await s.get(EchoGroup, chat_id)
        if g is None:
            s.add(EchoGroup(chat_id=chat_id, chat_title=title, last_active_at=datetime.utcnow()))
        else:
            g.last_active_at = datetime.utcnow()
            if title:
                g.chat_title = title
        await s.commit()


async def backfill_groups_from_referrals() -> None:
    """Ensure every group ECCO was added to (recorded as a referral group) has an
    EchoGroup row, so it shows on the pod board even if it's been quiet."""
    from database.models import AsyncSessionLocal, select, EchoReferralGroup, EchoGroup
    async with AsyncSessionLocal() as s:
        refs = (await s.execute(
            select(EchoReferralGroup).where(EchoReferralGroup.active.is_(True))
        )).scalars().all()
        for r in refs:
            if await s.get(EchoGroup, r.chat_id) is None:
                s.add(EchoGroup(chat_id=r.chat_id, chat_title=r.chat_title,
                                last_active_at=datetime.utcnow()))
        await s.commit()


async def network_group_count() -> int:
    from database.models import AsyncSessionLocal, select, func, EchoGroup
    async with AsyncSessionLocal() as s:
        return (await s.execute(select(func.count(EchoGroup.chat_id)))).scalar() or 0


async def user_echoer_stats(user_id: int) -> dict:
    """A user's own caller (echoer) standing: points, W/L, rank among echoers."""
    from database.models import AsyncSessionLocal, select, func, EchoUser
    async with AsyncSessionLocal() as s:
        u = await s.get(EchoUser, user_id)
        total = (await s.execute(select(func.count(EchoUser.user_id)))).scalar() or 0
        if u is None:
            return {"points": 0, "wins": 0, "losses": 0, "rank": None, "total": total}
        higher = (await s.execute(
            select(func.count(EchoUser.user_id)).where(EchoUser.points > (u.points or 0))
        )).scalar() or 0
        return {"points": u.points or 0, "wins": u.wins or 0, "losses": u.losses or 0,
                "rank": higher + 1, "total": total}


async def group_avg_x(chat_id: int) -> float:
    """Average peak multiple across the resolved tokens this group called."""
    from database.models import AsyncSessionLocal, select, func, EchoSighting, EchoToken
    async with AsyncSessionLocal() as s:
        cas = select(EchoSighting.ca).where(EchoSighting.chat_id == chat_id)
        avg = (await s.execute(
            select(func.avg(EchoToken.ath_mult)).where(
                EchoToken.ca.in_(cas), EchoToken.resolved.is_(True))
        )).scalar()
    return float(avg or 0.0)


async def user_avg_x(user_id: int) -> float:
    """Average peak multiple across the resolved tokens this caller called."""
    from database.models import AsyncSessionLocal, select, func, EchoSighting, EchoToken
    async with AsyncSessionLocal() as s:
        cas = select(EchoSighting.ca).where(EchoSighting.user_id == user_id)
        avg = (await s.execute(
            select(func.avg(EchoToken.ath_mult)).where(
                EchoToken.ca.in_(cas), EchoToken.resolved.is_(True))
        )).scalar()
    return float(avg or 0.0)


async def group_stats(chat_id: int) -> dict | None:
    """A single group's own stats block (for the /pod 'YOUR POD' section)."""
    from database.models import AsyncSessionLocal, EchoGroup
    async with AsyncSessionLocal() as s:
        g = await s.get(EchoGroup, chat_id)
        if g is None:
            return None
        te = await _top_echoer_for_group(s, chat_id)
        title = g.chat_title or str(chat_id)
        wins, losses, points = g.wins or 0, g.losses or 0, g.points or 0
    rank, total = await group_rank(chat_id)
    return {"title": title, "wins": wins, "losses": losses, "points": points,
            "rank": rank, "total": total, "top_echoer": te}


async def top_groups(n: int = 10) -> list:
    from database.models import AsyncSessionLocal, select, EchoGroup
    async with AsyncSessionLocal() as s:
        return list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(n)
        )).scalars().all())


async def top_users(n: int = 10) -> list:
    # Phanes rule: you need at least 1 point to appear on the top callers list.
    from database.models import AsyncSessionLocal, select, EchoUser
    async with AsyncSessionLocal() as s:
        return list((await s.execute(
            select(EchoUser).where(EchoUser.points >= 1)
            .order_by(EchoUser.points.desc()).limit(n)
        )).scalars().all())


async def _top_echoer_for_group(s, chat_id) -> tuple | None:
    """(username, wins-in-this-group) of the BEST-performing echoer in a group —
    the caller with the most winning (>=2x) calls here, NOT the loudest poster.
    Ties broken by total calls. Returns None until someone actually has a win."""
    from database.models import select, func, EchoSighting, EchoToken, EchoUser
    row = (await s.execute(
        select(
            EchoSighting.user_id,
            func.count(func.distinct(EchoToken.ca)).label("wins"),
        )
        .join(EchoToken, EchoToken.ca == EchoSighting.ca)
        .where(
            EchoSighting.chat_id == chat_id,
            EchoSighting.user_id.isnot(None),
            EchoToken.status == "win",
        )
        .group_by(EchoSighting.user_id)
        .order_by(func.count(func.distinct(EchoToken.ca)).desc())
        .limit(1)
    )).first()
    if not row or row[0] is None:
        return None
    u = await s.get(EchoUser, row[0])
    if u is None:
        return None
    return (u.username or str(u.user_id), int(row[1] or 0))


async def hub_stats() -> dict:
    """Enriched data for the intelligence dashboard: totals + win rates + top
    groups (with per-group top echoer) + top echoers + recent signals."""
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
            select(func.count(EchoToken.ca)).where(EchoToken.status.in_(("loss", "rug"))))).scalar() or 0

        groups_raw = list((await s.execute(
            select(EchoGroup).order_by(EchoGroup.points.desc()).limit(10))).scalars().all())
        top_groups = []
        for g in groups_raw:
            top_groups.append({
                "title": g.chat_title or str(g.chat_id),
                "wins": g.wins or 0, "losses": g.losses or 0, "points": g.points or 0,
                "avg_x": await group_avg_x(g.chat_id),
                "top_echoer": await _top_echoer_for_group(s, g.chat_id),
            })

        users_raw = list((await s.execute(
            select(EchoUser).order_by(EchoUser.points.desc()).limit(10))).scalars().all())
        top_users = []
        for u in users_raw:
            top_users.append({
                "name": (u.username or str(u.user_id)),
                "wins": u.wins or 0, "losses": u.losses or 0, "points": u.points or 0,
                "avg_x": await user_avg_x(u.user_id),
            })

        sigs = list((await s.execute(
            select(EchoSignal).order_by(EchoSignal.id.desc()).limit(5))).scalars().all())
        recent = []
        for sig in sigs:
            tok = await s.get(EchoToken, sig.ca)
            recent.append({
                "name": (tok.token_name if tok and tok.token_name else sig.ca[:8]),
                "quality": (sig.quality or "Medium Quality Signal").replace(" Signal", ""),
                "mult": (tok.ath_mult if tok else 1.0) or 1.0,
            })
    return {
        "n_groups": n_groups, "n_users": n_users, "n_sightings": n_sightings,
        "n_signals": n_signals, "n_wins": n_wins, "n_losses": n_losses,
        "top_groups": top_groups, "top_users": top_users, "recent": recent,
    }


# ── Points engine (all tunable via /setparam) ───────────────────────────────
# win  = ath_mult * echo_win_pts_per_x   (2x -> +20, 5x -> +50, 10x -> +100)
# loss = echo_loss_pts                    (faded, never hit 2x)        default -20
# rug  = echo_rug_pts                     (collapsed < rug threshold)  default -40


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


# ── Phanes-style points: bracket on the post-call peak, scaled by MC tier ────
def _points_for_return(mult) -> float:
    """Points purely from the post-call return multiple (peak MC / call MC)."""
    m = float(mult or 0.0)
    if m < 1.0:   return -2.0
    if m < 1.3:   return -1.0
    if m < 1.8:   return 0.0
    if m < 5.0:   return 1.0
    if m < 10.0:  return 2.0
    if m < 20.0:  return 3.0
    if m < 50.0:  return 4.0
    if m < 100.0: return 7.0
    if m < 200.0: return 10.0
    return 15.0


def _mc_tier_mult(mc) -> float:
    """Positive points scale by how hard the call was — a pump from $20k is easy,
    a pump from $1M is not. Unknown MC = full credit (no penalty). Losses are
    never scaled (a bad call is a bad call regardless of size)."""
    if not mc or mc <= 0:
        return 1.0
    if mc < 25_000:    return 0.5
    if mc < 50_000:    return 0.65
    if mc < 100_000:   return 0.8
    if mc < 250_000:   return 0.9
    return 1.0


def phanes_points(peak, first_mc) -> float:
    pts = _points_for_return(peak)
    if pts > 0:
        pts = round(pts * _mc_tier_mult(first_mc), 2)
    return pts


async def apply_token_score(ca: str, peak: float, first_mc, win_mult: float = 2.0) -> None:
    """Score a resolved token to every group + caller that called it — idempotent
    and incremental. Points = Phanes bracket of the post-call peak (positive
    scaled by call-time MC tier). Re-running applies only the DELTA, so a runner
    climbing into a higher bracket — or a loss that later tops 2x — updates
    cleanly without double-counting. Win/loss counters track hit-rate (peak>=2x)."""
    from database.models import (
        AsyncSessionLocal, select, EchoToken, EchoSighting, EchoGroup, EchoUser,
    )
    target = phanes_points(peak, first_mc)
    is_win = float(peak or 0.0) >= float(win_mult)
    async with AsyncSessionLocal() as s:
        tok = await s.get(EchoToken, ca)
        if tok is None:
            return
        prev_pts = float(tok.awarded_points or 0.0)
        was_scored = bool(tok.scored)
        was_win = bool(tok.score_win)
        d_pts = round(target - prev_pts, 2)
        if was_scored and abs(d_pts) < 0.01 and was_win == is_win:
            return  # nothing changed since last score — skip the writes
        sightings = (await s.execute(
            select(EchoSighting).where(EchoSighting.ca == ca)
        )).scalars().all()

        def _apply(e):
            e.points = round((e.points or 0) + d_pts, 2)
            if not was_scored:
                e.calls = (e.calls or 0) + 1
                if is_win:
                    e.wins = (e.wins or 0) + 1
                else:
                    e.losses = (e.losses or 0) + 1
            elif was_win != is_win:
                if is_win:
                    e.wins = (e.wins or 0) + 1
                    e.losses = max(0, (e.losses or 0) - 1)
                else:
                    e.losses = (e.losses or 0) + 1
                    e.wins = max(0, (e.wins or 0) - 1)

        seen_g: set[int] = set()
        seen_u: set[int] = set()
        for sg in sightings:
            if sg.chat_id is not None and sg.chat_id not in seen_g:
                seen_g.add(sg.chat_id)
                g = await s.get(EchoGroup, sg.chat_id)
                if g and not g.blacklisted:
                    _apply(g)
            if sg.user_id is not None and sg.user_id not in seen_u:
                seen_u.add(sg.user_id)
                u = await s.get(EchoUser, sg.user_id)
                if u and not u.blacklisted:
                    _apply(u)
        tok.awarded_points = target
        tok.scored = True
        tok.score_win = is_win
        await s.commit()
