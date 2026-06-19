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


async def _pumpfun_mc(ca: str) -> float | None:
    """Bonding-curve market cap (USD) from the pump.fun API — for pre-graduation
    tokens DexScreener hasn't indexed yet. pump-suffix mints only."""
    if not ca.endswith("pump"):
        return None
    import aiohttp
    url = f"https://frontend-api-v3.pump.fun/coins/{ca}"
    headers = {"accept": "application/json", "user-agent": "Mozilla/5.0"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
        mc = float(d.get("usd_market_cap") or 0)
        return mc or None
    except Exception as exc:
        logger.debug("echo: pumpfun fetch failed %s: %s", ca[:8], exc)
        return None


async def _price_mc(ca: str):
    """(market_cap, liquidity_usd, pair). Falls back to the pump.fun API for
    pre-graduation tokens DexScreener hasn't indexed (mc set, liq/pair None)."""
    try:
        from bot.scanner import fetch_token_data, parse_token_metrics
        pair = await fetch_token_data(ca, allow_any_dex=True)
        if pair is not None:
            m = parse_token_metrics(pair)
            mc = float(m.get("market_cap") or 0) or None
            liq = float(m.get("liquidity_usd") or 0)
            return mc, liq, pair
    except Exception as exc:
        logger.debug("echo: dexscreener fetch failed %s: %s", ca[:8], exc)
    # Fallback: pump.fun bonding-curve MC (no DexScreener pair / liquidity data).
    return await _pumpfun_mc(ca), None, None


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


async def _consensus_sweep(echo_bot) -> None:
    """Fire consensus alerts for CAs that crossed the threshold — works for
    sightings recorded by EITHER lane (bot handler or the Telethon listener)."""
    window = await core.get_echo_param("echo_active_window_min", 60.0)
    for ca in await core.recent_unsignaled_cas(window):
        try:
            await maybe_fire_signal(echo_bot, ca)
        except Exception as exc:
            logger.debug("echo: consensus sweep %s: %s", ca[:8], exc)


# ── Performance tracker ─────────────────────────────────────────────────────
async def echo_tracker_loop(echo_bot) -> None:
    """Track every Echo token's price/ATH, resolve win (>=2x) / loss (<2x after
    the resolution window) to drive the points engine, and post milestone
    follow-ups (5x/10x/…) into the groups that received the signal."""
    await asyncio.sleep(90)
    while True:
        # Each step isolated — a failure in backfill or the consensus sweep must
        # NEVER block resolution (that's what stalled W/L scoring).
        try:
            await core.backfill_groups_from_referrals()
        except Exception as exc:
            logger.debug("echo backfill error: %s", exc)
        try:
            await _consensus_sweep(echo_bot)
        except Exception as exc:
            logger.debug("echo consensus sweep error: %s", exc)
        try:
            await _tracker_tick(echo_bot)
        except Exception as exc:
            logger.warning("echo tracker tick error: %s", exc)
        await asyncio.sleep(60)


async def _tracker_tick(echo_bot) -> None:
    from database.models import AsyncSessionLocal, select, EchoToken

    win_mult = await core.get_echo_param("echo_win_mult", 2.0)
    resolution_h = await core.get_echo_param("echo_resolution_hours", 24.0)
    window = await core.get_echo_param("echo_active_window_min", 60.0)
    rug_liq = await core.get_echo_param("echo_rug_liq_usd", 200.0)
    death_mult = await core.get_echo_param("echo_death_mult", 0.40)
    death_min_age = await core.get_echo_param("echo_death_min_age_min", 20.0) / 60.0  # hours
    max_upg_days = await core.get_echo_param("echo_max_upgrade_days", 14.0)
    now = datetime.utcnow()

    async with AsyncSessionLocal() as s:
        pending = list((await s.execute(
            select(EchoToken).where(EchoToken.resolved.is_(False)).limit(40)
        )).scalars().all())
        # Resolved LOSSES stay watched for a while — a loss can still become a
        # WIN if the token tops 2x days later ("based off the top of the call").
        upg_cutoff = now - timedelta(days=max_upg_days)
        upgradable = list((await s.execute(
            select(EchoToken).where(
                EchoToken.status == "loss",
                EchoToken.first_seen_at >= upg_cutoff,
            ).limit(40)
        )).scalars().all())
    tokens = pending + upgradable

    for tok in tokens:
        # Isolate every token — one bad price/DB/broadcast must NOT stop the rest
        # from resolving (that's what silently stalled W/L scoring).
        try:
            await _resolve_one_token(
                echo_bot, tok, now, win_mult, resolution_h,
                window, rug_liq, death_mult, death_min_age,
            )
        except Exception as exc:
            logger.warning("echo: token %s resolve failed: %s", tok.ca[:8], exc)


async def _resolve_one_token(echo_bot, tok, now, win_mult, resolution_h,
                             window, rug_liq, death_mult, death_min_age) -> None:
    from database.models import AsyncSessionLocal, select, EchoToken, EchoSighting

    mc, liq, pair = await _price_mc(tok.ca)

    # When a token is aging out (or a resolved loss) without a spot-confirmed
    # 2x, pull its TRUE peak from price history so a spike missed between
    # 60s spot checks still counts as a win. One history call per token, only
    # when it matters — not every tick.
    hist_ath = None
    try:
        tok_age_h = (now - (tok.first_seen_at or now)).total_seconds() / 3600.0
    except Exception:
        tok_age_h = 0.0
    needs_hist = (
        tok.first_mc is not None and (tok.ath_mult or 1.0) < win_mult
        and tok.first_seen_at is not None
        and ((not tok.resolved and tok_age_h >= resolution_h) or tok.status == "loss")
    )
    if needs_hist:
        hist_ath = await core.historical_ath_mult(tok.ca, tok.first_seen_at)

    upgraded = False
    resolved_now = False
    kind = None
    new_ms = []
    ath_mult = 1.0
    do_score = False
    peak_mc_value = None
    async with AsyncSessionLocal() as s:
        t = await s.get(EchoToken, tok.ca)
        if t is None:
            return
        if t.first_mc is None and mc:
            t.first_mc = mc
        if t.ath_mc is None and t.first_mc:
            t.ath_mc = t.first_mc  # peak MC is at least the call MC
        base = t.first_mc or mc
        cur_mult = 1.0
        if mc and base:
            cur_mult = mc / base
            if cur_mult > (t.ath_mult or 1.0):
                t.ath_mult = cur_mult
                t.ath_mc = mc
        if mc:
            # Capture each caller's OWN entry MC at ~their call time — the tracker
            # checks every token each tick, so a NULL snapshot is always recent
            # (filled within one tick of the sighting). Later callers get their
            # own, higher MC, so their points reflect their real entry.
            for sg in (await s.execute(
                select(EchoSighting).where(
                    EchoSighting.ca == tok.ca, EchoSighting.entry_mc.is_(None))
            )).scalars().all():
                sg.entry_mc = mc
        if hist_ath and hist_ath > (t.ath_mult or 1.0):
            t.ath_mult = hist_ath  # true historical peak — never miss a top
            t.ath_mc = max(t.ath_mc or 0.0, hist_ath * (t.first_mc or 0.0))
        t.last_checked_at = now
        ath_mult = t.ath_mult or 1.0
        signaled = t.signaled
        posted = set(x for x in (t.milestones or "").split(",") if x)
        age_h = (now - (t.first_seen_at or now)).total_seconds() / 3600.0

        if t.resolved:
            # Resolved non-void tokens are revisited within the upgrade window so
            # points track the peak: a loss that finally tops 2x flips to a win,
            # and a runner climbing into a higher bracket gets the extra points.
            if t.status == "loss" and ath_mult >= win_mult:
                t.status, upgraded = "win", True
        else:
            # WIN the moment it tops 2x (forever, ATH-based). Otherwise wait
            # the full window, THEN decide: never-priced junk = VOID (no
            # points); delisted or liquidity drained to ~zero = RUG; a token
            # that still trades but never hit 2x = LOSS. No eager rug — a
            # thin-but-alive memecoin is a loss, not a rug.
            if ath_mult >= win_mult:
                t.status, t.resolved, resolved_now, kind = "win", True, True, "win"
            elif (t.first_mc is not None and cur_mult <= death_mult
                  and age_h >= death_min_age):
                # Crashed — dumped to <= death_mult of the call = the call is
                # dead, resolve now (don't wait the full window). LP drained =
                # rug, otherwise a deep-dump loss. Still upgradeable to a win
                # if it somehow recovers past 2x within the upgrade window.
                k = "rug" if (liq is not None and liq < rug_liq) else "loss"
                t.status, t.resolved, resolved_now, kind = k, True, True, k
            elif age_h >= resolution_h:
                if t.first_mc is None:
                    t.status, t.resolved, resolved_now, kind = "void", True, True, "void"
                elif liq is not None and liq < rug_liq:
                    t.status, t.resolved, resolved_now, kind = "rug", True, True, "rug"
                else:
                    t.status, t.resolved, resolved_now, kind = "loss", True, True, "loss"

        # Milestone pings (signaled tokens only)
        if signaled:
            for ms in MILESTONES:
                if ath_mult >= ms and str(ms) not in posted:
                    posted.add(str(ms))
                    new_ms.append(ms)
            if new_ms:
                t.milestones = ",".join(sorted(posted, key=lambda x: int(x)))
        # Score when newly resolved (non-void), or re-check a resolved token in
        # the upgrade window (apply_token_score no-ops if the peak hasn't grown,
        # so runners climb brackets and losses can flip to wins).
        peak_mc_value = t.ath_mc
        do_score = (t.status != "void") and (resolved_now or t.resolved)
        await s.commit()

    if do_score:
        await core.apply_token_score(tok.ca, peak_mc_value, win_mult)
        if resolved_now:
            logger.info("echo: RESOLVED %s — %s peak %.2fx", tok.ca[:8], (kind or "?").upper(), ath_mult)
        elif upgraded:
            logger.info("echo: UPGRADED %s loss->win at %.2fx", tok.ca[:8], ath_mult)
    for ms in new_ms:
        await _broadcast(
            echo_bot, tok.ca, window,
            style.sonar_pulse(tok.token_name or tok.ca[:6], ms),
        )
