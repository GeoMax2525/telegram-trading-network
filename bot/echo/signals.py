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
    """Current market cap for a CA via DexScreener, or None if not tradeable yet."""
    try:
        from bot.scanner import fetch_token_data, parse_token_metrics
        pair = await fetch_token_data(ca, allow_any_dex=True)
        if pair is None:
            return None, None
        m = parse_token_metrics(pair)
        return float(m.get("market_cap") or 0) or None, pair
    except Exception as exc:
        logger.debug("echo: price fetch failed %s: %s", ca[:8], exc)
        return None, None


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

    mc, pair = await _price_mc(ca)
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

    text = style.sonar_report(label, mc, pct, quality)
    await _broadcast(echo_bot, ca, window, text, reply_markup=style.kb_copy(ca))
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
    now = datetime.utcnow()

    async with AsyncSessionLocal() as s:
        tokens = list((await s.execute(
            select(EchoToken).where(EchoToken.resolved.is_(False)).limit(40)
        )).scalars().all())

    for tok in tokens:
        mc, pair = await _price_mc(tok.ca)
        async with AsyncSessionLocal() as s:
            t = await s.get(EchoToken, tok.ca)
            if t is None or t.resolved:
                continue
            if t.first_mc is None and mc:
                t.first_mc = mc
            base = t.first_mc or mc
            if mc and base:
                mult = mc / base
                if mult > (t.ath_mult or 1.0):
                    t.ath_mult = mult
                    t.ath_mc = mc
            t.last_checked_at = now
            ath_mult = t.ath_mult or 1.0
            signaled = t.signaled
            posted = set(x for x in (t.milestones or "").split(",") if x)
            age_h = (now - (t.first_seen_at or now)).total_seconds() / 3600.0

            # Resolve win as soon as it crosses the win multiple; resolve loss
            # only after the resolution window has elapsed with no 2x.
            resolved_now = win = False
            if ath_mult >= win_mult:
                t.status, t.resolved, resolved_now, win = "win", True, True, True
            elif age_h >= resolution_h:
                t.status, t.resolved, resolved_now, win = "loss", True, True, False

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

        if resolved_now:
            await core.award_resolution(tok.ca, ath_mult, win)
            logger.info("echo: RESOLVED %s — %s at %.1fx", tok.ca[:8], "WIN" if win else "LOSS", ath_mult)

        for ms in new_ms:
            await _broadcast(
                echo_bot, tok.ca, window,
                style.sonar_pulse(tok.token_name or tok.ca[:6], ms),
            )
