"""
signals.py — Echo consensus alerts, rug filter, milestone follow-ups, and the
performance tracker that resolves wins/losses and drives the points engine.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from bot.echo import core, style

logger = logging.getLogger(__name__)

MILESTONES = [5, 10, 25, 50, 100]


async def _price_mc(ca: str):
    """(market_cap, liquidity_usd, pair) via DexScreener. (None, None, None) if
    no pair (not tradeable / delisted)."""
    try:
        from bot.scanner import fetch_token_data, parse_token_metrics
        pair = await fetch_token_data(ca, allow_any_dex=True)
        if pair is None:
            return None, None, None
        m = parse_token_metrics(pair)
        mc = float(m.get("market_cap") or 0) or None
        liq = float(m.get("liquidity_usd") or 0)
        return mc, liq, pair
    except Exception as exc:
        logger.debug("echo: price fetch failed %s: %s", ca[:8], exc)
        return None, None, None


async def _rug_ok(ca: str, pair) -> tuple[bool, str]:
    if await core.get_echo_param("echo_rug_filter_enabled", 1.0) < 0.5:
        return True, "rug_filter_off"
    try:
        from bot.agents.entry_filter import check_entry_filters
        ok, reason = await check_entry_filters(ca, pair)
        return bool(ok), str(reason)
    except Exception as exc:
        logger.warning("echo: rug filter error %s: %s — failing closed", ca[:8], exc)
        return False, f"rug_check_error:{exc}"


async def maybe_fire_signal(echo_bot, ca: str) -> None:
    """If a CA has crossed the cross-group consensus threshold, rug-check it and
    post the clean Entry alert into every group that called it. Once per CA."""
    from database.models import AsyncSessionLocal, EchoToken, EchoSignal

    threshold = await core.get_echo_param("echo_consensus_threshold", 4.0)
    window = await core.get_echo_param("echo_active_window_min", 60.0)

    callers, total = await core.consensus_state(ca, window)
    if callers < threshold:
        return

    async with AsyncSessionLocal() as s:
        tok = await s.get(EchoToken, ca)
        if tok is None or tok.signaled:
            return  # already alerted (or unknown)

    mc, _liq, pair = await _price_mc(ca)
    if not mc or pair is None:
        return  # not tradeable / indexable yet — wait for the next sighting

    rug_ok, rug_reason = await _rug_ok(ca, pair)
    if not rug_ok:
        logger.info("echo: signal blocked by rug filter %s — %s", ca[:8], rug_reason)
        return

    quality = await core.quality_grade(ca, window)
    pct = round(100.0 * callers / max(total, callers), 0)

    name = sym = None
    try:
        from bot.scanner import parse_token_metrics
        m = parse_token_metrics(pair)
        name, sym = m.get("name"), m.get("symbol")
    except Exception:
        pass
    label = name or sym or (ca[:6] + "…")

    # Mark signaled + persist + set baseline MC, all before posting (idempotent).
    async with AsyncSessionLocal() as s:
        tok = await s.get(EchoToken, ca)
        if tok is None or tok.signaled:
            return
        tok.signaled = True
        if tok.first_mc is None:
            tok.first_mc = mc
        if name:
            tok.token_name = name
        if sym:
            tok.symbol = sym
        s.add(EchoSignal(ca=ca, num_groups=callers, pct_chats=pct, quality=quality, entry_mc=mc))
        await s.commit()

    # Personalized broadcast — each pod sees its OWN leaderboard rank (no
    # cross-group leak); pod_strength = how many pods detected it.
    kb = style.kb_copy(ca)
    for chat_id in await core.calling_group_ids(ca, window):
        try:
            rank = await core.group_rank(chat_id)
            text = style.sonar_report(label, mc, pct, quality, callers, rank=rank)
            await echo_bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=kb)
        except Exception as exc:
            logger.debug("echo: alert to %s failed: %s", chat_id, exc)
    logger.info("echo: SIGNAL %s (%s) — %d/%d groups, %s", label, ca[:8], callers, total, quality)


async def _broadcast(echo_bot, ca: str, window: float, text: str, reply_markup=None) -> None:
    """Post text to every group currently calling this CA."""
    group_ids = await core.calling_group_ids(ca, window)
    for chat_id in group_ids:
        try:
            await echo_bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=reply_markup)
        except Exception as exc:
            logger.debug("echo: broadcast to %s failed: %s", chat_id, exc)


# ── Performance tracker ─────────────────────────────────────────────────────
async def echo_tracker_loop(echo_bot) -> None:
    """Track every Echo token's price/ATH, resolve win (>=2x) / loss (<2x after
    the resolution window) to drive the points engine, and post milestone
    follow-ups (5x/10x/…) into the groups that received the signal."""
    await asyncio.sleep(90)
    while True:
        try:
            await _tracker_tick(echo_bot)
        except Exception as exc:
            logger.debug("echo tracker tick error: %s", exc)
        await asyncio.sleep(60)


async def _tracker_tick(echo_bot) -> None:
    from database.models import AsyncSessionLocal, select, EchoToken

    win_mult = await core.get_echo_param("echo_win_mult", 2.0)
    resolution_h = await core.get_echo_param("echo_resolution_hours", 24.0)
    window = await core.get_echo_param("echo_active_window_min", 60.0)
    rug_liq = await core.get_echo_param("echo_rug_liq_usd", 1000.0)
    rug_min_age = await core.get_echo_param("echo_rug_min_age_min", 15.0) / 60.0  # hours
    now = datetime.utcnow()

    async with AsyncSessionLocal() as s:
        tokens = list((await s.execute(
            select(EchoToken).where(EchoToken.resolved.is_(False)).limit(40)
        )).scalars().all())

    for tok in tokens:
        mc, liq, pair = await _price_mc(tok.ca)
        async with AsyncSessionLocal() as s:
            t = await s.get(EchoToken, tok.ca)
            if t is None or t.resolved:
                continue
            if t.first_mc is None and mc:
                t.first_mc = mc
            base = t.first_mc or mc
            cur_mult = 1.0
            if mc and base:
                cur_mult = mc / base
                if cur_mult > (t.ath_mult or 1.0):
                    t.ath_mult = cur_mult
                    t.ath_mc = mc
            t.last_checked_at = now
            ath_mult = t.ath_mult or 1.0
            signaled = t.signaled
            posted = set(x for x in (t.milestones or "").split(",") if x)
            age_h = (now - (t.first_seen_at or now)).total_seconds() / 3600.0

            # Win the moment it crosses the win multiple. A RUG is detected by
            # LIQUIDITY, not price — LP pulled / drained below the rug floor
            # means you can't sell, and we flag it the moment it happens (any
            # tick). A token that just FADED (still has liquidity, never hit 2x)
            # is a plain loss, resolved at the timeout. A token gone from
            # DexScreener (no pair) at the timeout is treated as delisted = rug.
            resolved_now = False
            kind = None
            if ath_mult >= win_mult:
                # Hit 2x from the call at ANY point = WIN, forever (ATH-based).
                t.status, t.resolved, resolved_now, kind = "win", True, True, "win"
            elif liq is not None and liq < rug_liq and age_h >= rug_min_age:
                # Real LP pull: low liquidity AFTER it had time to establish —
                # not a fresh/thin token still building or one about to run.
                t.status, t.resolved, resolved_now, kind = "rug", True, True, "rug"
            elif age_h >= resolution_h:
                if t.first_mc is None:
                    # Never got a real price (junk base58 / non-token) — VOID,
                    # no points, so noise can't pollute the record.
                    t.status, t.resolved, resolved_now, kind = "void", True, True, "void"
                else:
                    t.status, t.resolved, resolved_now, kind = "loss", True, True, "loss"

            # New milestones to announce (signaled tokens only)
            new_ms = []
            if signaled:
                for ms in MILESTONES:
                    if ath_mult >= ms and str(ms) not in posted:
                        posted.add(str(ms))
                        new_ms.append(ms)
                if new_ms:
                    t.milestones = ",".join(sorted(posted, key=lambda x: int(x)))
            await s.commit()

        if resolved_now and kind in ("win", "loss", "rug"):
            await core.award_resolution(tok.ca, ath_mult, kind)
            logger.info("echo: RESOLVED %s — %s at %.1fx", tok.ca[:8], kind.upper(), ath_mult)

        for ms in new_ms:
            await _broadcast(
                echo_bot, tok.ca, window,
                style.sonar_pulse(tok.token_name or tok.ca[:6], ms),
            )
