"""
paper_monitor.py — Paper Trade Monitor

Two jobs:
  1. Every 1 minute: check open paper trades for TP/SL hits
  2. Every 10 minutes: track post-close performance for 24 hours
     Records mc at 1h, 4h, 6h, 24h after close.

Dead-position auto-close has been REMOVED per user request. Trades
close only via TP hit, trailing stop, or fixed SL (or manually via
/hub Close All / /forcenuke).
"""

import asyncio
import logging

# Phase 5: trade close commentary — fire-and-forget Claude calls
_close_commentary_tasks: set = set()


# ── Shadow mode: run the pure exit engine alongside the live monitor ────────
# Every tick we compute the engine's verdict from the same inputs and stash it;
# on a real close we compare to what the monitor actually did. Divergences are
# logged + counted (see /shadowstats) so we can prove the engine matches the
# battle-tested monitor BEFORE cutting over to it. Fully isolated in try/except
# — shadow code can never affect a real trade.
_shadow: dict = {}
_shadow_stats: dict = {"closes": 0, "match": 0, "diverge": 0}


def shadow_stats() -> dict:
    return dict(_shadow_stats)


def _shadow_eval(pt, cfg, current_mult, peak_mult, age_hours, remaining,
                 realized, is_tg_signal, let_run, resolved) -> None:
    try:
        from bot.exit_engine import PositionState, decide_exit
        params = dict(cfg)
        params["_tp_x"] = pt.take_profit_x or 0
        params["_sl_pct"] = pt.stop_loss_pct or 30.0
        params["_trail_trigger_raw"] = (resolved or {}).get("trail_trigger", 0.3) if resolved else 0.3
        pos = PositionState(
            entry_mc=pt.entry_mc or 0, current_mc=(pt.entry_mc or 0) * current_mult,
            current_mult=current_mult, peak_mult=peak_mult, liq_usd=None,
            age_hours=age_hours, remaining_pct=remaining, realized_pnl=realized,
            size_sol=pt.paper_sol_spent or 0, is_tg_signal=is_tg_signal,
            is_bundle="bundle" in (pt.pattern_type or ""), let_run=let_run,
            subscriber=pt.subscriber_id is not None,
            ladder_disabled=bool(getattr(pt, "claude_ladder_disabled", False)),
            trail_override=getattr(pt, "claude_trail_override_pct", None),
        )
        _shadow[pt.id] = decide_exit(pos, params)
    except Exception as exc:
        logger.debug("shadow eval error id=%s: %s", getattr(pt, "id", "?"), exc)


def _shadow_compare(trade_id, actual_reason) -> None:
    try:
        sd = _shadow.pop(trade_id, None)
        if sd is None:
            return
        engine_reason = sd.reason if sd.action == "close" else (
            "scale_out" if sd.action == "scale_out" else "hold")
        _shadow_stats["closes"] += 1
        if engine_reason == actual_reason:
            _shadow_stats["match"] += 1
        else:
            _shadow_stats["diverge"] += 1
            logger.warning("SHADOW DIVERGENCE id=%s: monitor=%s engine=%s (action=%s)",
                           trade_id, actual_reason, engine_reason, sd.action)
    except Exception:
        pass


def _source_tag(pattern_type: str | None) -> str:
    p = (pattern_type or "")
    if "migration_dip" in p:
        return "🎓 Migration"
    if "tg_signal" in p:
        return "⚡ 4AM"
    return "🔍 Scanner"


def _close_card(emoji: str, title: str, name: str, mult: float, pnl: float,
                entry_mc: float, current_mc: float, note: str | None = None) -> list:
    """Build a clean, consistent HTML close card. Pass parse_mode='HTML' to
    _finalize_paper_close when using this. {bal:.2f} is filled there."""
    from html import escape as _esc
    icon = "🟢" if (pnl or 0) > 0 else "🔴"
    e_mc = f"${entry_mc/1000:.0f}K" if entry_mc else "?"
    c_mc = f"${current_mc/1000:.0f}K" if current_mc else "?"
    lines = [
        f"{emoji} <b>{title}</b>",
        "",
        f"🪙 {_esc(str(name)[:24])}",
        f"<b>Result:</b> {mult:.2f}x → <b>{pnl:+.4f} SOL</b> {icon}",
        f"MC: {e_mc} → {c_mc}",
    ]
    if note:
        lines.append(note)
    lines.append("💰 <b>Balance:</b> {bal:.2f} SOL")
    return lines


def _spawn_close_commentary(bot, pt, pnl: float, peak_mult: float, close_reason: str) -> None:
    """Spawn an async Claude commentary task that doesn't block the close
    broadcast. Posts a follow-up message with Claude's read of why this
    trade ended the way it did. Silent no-op if no API key."""
    try:
        from bot.agents.claude_reasoning import claude_available
        if not claude_available():
            return
        # Skip the essay on trivial churn — fast ejects and tiny PnL don't need a
        # paragraph. Only comment on meaningful closes (real wins/losses, rugs,
        # trails). Keeps the feed clean instead of an essay per micro-trade.
        if close_reason in ("no_momentum", "time_stop", "bundle_time_exit"):
            return
        if abs(float(pnl or 0)) < 0.03:
            return
        task = asyncio.create_task(
            _send_close_commentary(bot, pt, pnl, peak_mult, close_reason)
        )
        _close_commentary_tasks.add(task)
        task.add_done_callback(_close_commentary_tasks.discard)
    except Exception:
        pass  # never block close on commentary


async def _send_close_commentary(bot, pt, pnl: float, peak_mult: float, close_reason: str) -> None:
    """Generate and send a Claude-written 'why this happened' paragraph
    as a follow-up to the close card."""
    try:
        from bot.agents.claude_reasoning import (
            HAIKU_MODEL, call_claude, claude_available,
        )
        from bot.config import CALLER_GROUP_ID, SCAN_TOPIC_ID
        if not claude_available():
            return

        is_tg = "tg_signal" in (pt.pattern_type or "")
        source = "4am tg_signal" if is_tg else "scanner"
        name = pt.token_name or "?"

        from datetime import datetime as _dt
        age_min = 0.0
        if pt.opened_at:
            age_min = (_dt.utcnow() - pt.opened_at).total_seconds() / 60.0

        system = """You are a senior memecoin trader writing a one-paragraph postmortem on a closed paper trade for "Revolt Agent Hub". Write 2-3 sentences max. Be specific and honest — say what happened (chart distribution, momentum fade, rug pattern, hit TP) and what the bot did right or wrong. No fluff. No hedging. No markdown."""

        user = (
            f"Trade closed: {name}\n"
            f"Source: {source}\n"
            f"Close reason: {close_reason}\n"
            f"Entry MC: ${pt.entry_mc:,.0f}\n"
            f"Peak: {peak_mult:.2f}x\n"
            f"PnL: {pnl:+.4f} SOL on {pt.paper_sol_spent:.3f} SOL position\n"
            f"Held: {age_min:.1f} min\n"
            f"\n"
            f"Write the 2-3 sentence postmortem."
        )

        text = await call_claude(
            system=system, user=user, model=HAIKU_MODEL, max_tokens=200,
        )
        if not text:
            return
        text = text.strip()

        # Persist postmortem so /claude_report can analyze it later
        try:
            from sqlalchemy import update as _update
            from database.models import AsyncSessionLocal, PaperTrade
            async with AsyncSessionLocal() as s:
                await s.execute(
                    _update(PaperTrade)
                    .where(PaperTrade.id == pt.id)
                    .values(close_commentary=text[:1000])
                )
                await s.commit()
        except Exception as exc:
            logger.debug("close commentary persist failed: %s", exc)

        if not bot:
            return
        await bot.send_message(
            CALLER_GROUP_ID,
            f"🧠 {text}",
            message_thread_id=SCAN_TOPIC_ID,
        )

        # Mirror commentary to community channel (silent no-op if not configured)
        try:
            from bot.community_feed import post_commentary
            await post_commentary(bot, text)
        except Exception as exc:
            logger.debug("community_feed commentary mirror failed: %s", exc)
    except Exception as exc:
        # Logged but never raised — close commentary is best-effort
        logger.debug("close commentary failed for %s: %s",
                     (pt.token_name or '?')[:20], exc)
from datetime import datetime, timedelta

from bot import state
from bot.config import CALLER_GROUP_ID, SCAN_TOPIC_ID
from bot.scanner import fetch_live_data
from database.models import (
    get_open_paper_trades,
    close_paper_trade,
    update_paper_trade_peak,
    get_recently_closed_paper_trades,
    update_paper_post_close,
    compute_paper_balance,
    get_params,
    AsyncSessionLocal, PaperTrade,
)
from bot.scanner import mint_suffix_ok
from bot.agents.trade_profiles import resolve_trade_params, parse_pattern_tags

logger = logging.getLogger(__name__)

POLL_INTERVAL      = 15    # 15 seconds — Profit Protection v2 (was 30s, was
                           #   60s). pump.fun tokens round-trip 1.0x→2.4x→0.9x
                           #   inside a 30-60s window, so the peak (max of poll
                           #   snapshots) never recorded the spike and the trail
                           #   never armed. 15s + cache-bypass on the live fetch
                           #   below halves the blind window. Real fix (ws/Helius
                           #   high-water feed) is a later project.
POST_CLOSE_INTERVAL = 600  # 10 minutes — post-close tracking
STARTUP_DELAY      = 60
MAX_FETCH_FAILS    = 5     # consecutive failures before auto-close

# Per-trade consecutive fetch failure counter (in-memory, resets on bot restart)
_fetch_fail_counts: dict[int, int] = {}


async def _close_and_track(trade_id, reason, pnl, peak_mc, peak_mult, is_admin: bool = True) -> bool:
    """Close a paper trade AND update session context in one call.
    Returns False if the trade was already closed (lost the race to a
    concurrent closer) — callers must skip refunds/cards/streaks then.
    Session context only tracks admin (HQ) outcomes — subscriber relay
    closes don't influence HQ session streaks."""
    closed = await close_paper_trade(trade_id, reason, pnl, peak_mc, peak_mult)
    if not closed:
        logger.info("paper close skipped — trade %s already closed (race)", trade_id)
        return False
    _shadow_compare(trade_id, reason)  # validate engine vs monitor on every close
    # Bundle wallet scoring — if this was a bundle trade, attribute its final
    # peak multiple to every wallet we recorded for it, so the /bundlers board
    # ranks bundlers by the avg peak their tokens reach. Best-effort, off-path.
    try:
        from database.models import AsyncSessionLocal as _ASL, PaperTrade as _PT, score_bundle_sightings
        async with _ASL() as _s:
            _t = await _s.get(_PT, trade_id)
        if _t and "bundle" in (_t.pattern_type or "").lower() and _t.token_address:
            await score_bundle_sightings(_t.token_address, float(peak_mult or 1.0))
    except Exception as _bse:
        logger.debug("bundle sighting score failed for %s: %s", trade_id, _bse)
    if not is_admin:
        return True
    if reason in ("tp_hit", "trail_hit", "profit_trail"):
        _update_session_context("win")
    elif reason in ("breakeven_stop",):
        _update_session_context("be")
    elif reason == "sl_hit":
        _update_session_context("loss")
    # dead_api / dead_token are backup-SL meta closes — not counted as
    # strategy losses for streak/regime detection (they bypassed the
    # primary SL window via grace period or fetch failure).
    return True


async def _refund_subscriber_balance(sub_id: int, locked_sol: float, pnl: float) -> float:
    """Return locked SOL + realized PnL to subscriber's paper_balance and
    update their paper_pnl. Returns the new paper_balance."""
    from sqlalchemy import select as _select
    from database.models import Subscriber as _Sub
    async with AsyncSessionLocal() as session:
        sub = (await session.execute(
            _select(_Sub).where(_Sub.telegram_id == sub_id)
        )).scalar_one_or_none()
        if sub is None:
            return 0.0
        sub.paper_balance = round((sub.paper_balance or 0.0) + (locked_sol or 0.0) + (pnl or 0.0), 4)
        sub.paper_pnl = round((sub.paper_pnl or 0.0) + (pnl or 0.0), 4)
        await session.commit()
        return float(sub.paper_balance)


async def _finalize_paper_close(
    bot, pt, reason, pnl, peak_mc, peak_mult, lines, only_admin_session: bool = True,
    parse_mode: str | None = None,
):
    """
    Close trade, refresh balance for the right party, route notification.

    `lines` is a list of strings; any "{bal:.2f}" placeholder gets formatted
    with the owner's balance (admin state.paper_balance for HQ trades, or
    the subscriber's refreshed paper_balance for relay rows).

    HQ trade (subscriber_id IS NULL): posts to CALLER_GROUP_ID scan topic
    and updates HQ session streak.
    Relay trade: refunds subscriber balance and DMs only that subscriber;
    HQ feed is NOT touched and HQ session streak is NOT influenced.
    """
    is_admin = pt.subscriber_id is None
    closed = await _close_and_track(pt.id, reason, pnl, peak_mc, peak_mult, is_admin=is_admin)
    if not closed:
        # Lost the race to a concurrent closer — no card, no refund,
        # no streak. The winning close already handled all of it.
        return state.paper_balance

    # Mirror the exit on-chain if this trade was bought live (no-op unless
    # live_trading_armed AND a live buy exists for it). Single chokepoint for
    # every close reason, so a live position can never miss its sell.
    try:
        from bot.live_mirror import mirror_close
        _pnl_frac = float(pnl or 0) / float(pt.paper_sol_spent or 1)
        await mirror_close(pt.id, pt.token_address, pnl_frac=_pnl_frac)
    except Exception as exc:
        logger.error("live_mirror close hook failed: %s", exc)

    # Source tag — append to every close card so user can see at-a-glance
    # which source each trade came from. Also include trade age so user
    # can tell pre-toggle from post-toggle: a 🔍 Scanner trade opened
    # 12h ago is a pre-toggle leftover, while one opened 30s ago is a
    # real bypass bug.
    source_tag = _source_tag(pt.pattern_type)
    age_str = ""
    if pt.opened_at:
        age_secs = (datetime.utcnow() - pt.opened_at).total_seconds()
        if age_secs < 60:
            age_str = f"{int(age_secs)}s"
        elif age_secs < 3600:
            age_str = f"{int(age_secs / 60)}m"
        elif age_secs < 86400:
            age_str = f"{age_secs / 3600:.1f}h"
        else:
            age_str = f"{int(age_secs / 86400)}d"
    age_part = f" | age {age_str}" if age_str else ""
    lines_with_source = lines + [f"Source: {source_tag}{age_part}"]

    if is_admin:
        bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
        state.paper_balance = bal
        text = "\n".join(line.format(bal=bal) for line in lines_with_source)
        try:
            await bot.send_message(
                CALLER_GROUP_ID, text,
                message_thread_id=SCAN_TOPIC_ID,
                parse_mode=parse_mode,
            )
        except Exception:
            pass

        # Mirror the EXACT HQ close text to the community channel.
        # Silent no-op if COMMUNITY_CHANNEL_ID not configured.
        try:
            from bot.community_feed import post_to_community
            await post_to_community(bot, text, parse_mode=parse_mode)
        except Exception as exc:
            logger.debug("community_feed close mirror failed: %s", exc)

        # Phase 5: fire-and-forget Claude postmortem (HQ + community).
        # Silent no-op if ANTHROPIC_API_KEY isn't set.
        _spawn_close_commentary(bot, pt, pnl, peak_mult or 1.0, reason)
        return bal
    else:
        bal = await _refund_subscriber_balance(
            pt.subscriber_id, pt.paper_sol_spent or 0.0, pnl,
        )
        try:
            text = "\n".join(line.format(bal=bal) for line in lines_with_source)
            await bot.send_message(pt.subscriber_id, text)
        except Exception:
            pass
        return bal


async def _finalize_silent_close(pt, reason, pnl, peak_mc, peak_mult):
    """Close trade without posting any notification — used for time-based
    cleanups (stale/expired) and dead_api closes that intentionally stay
    quiet. Still refunds balance for the correct owner."""
    closed = await _close_and_track(pt.id, reason, pnl, peak_mc, peak_mult)
    if not closed:
        return state.paper_balance
    if pt.subscriber_id is None:
        if reason == "sl_hit":
            _update_session_context("loss")
        # dead_api / dead_token excluded from session streak — meta closes
        bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
        state.paper_balance = bal
        return bal
    else:
        return await _refund_subscriber_balance(
            pt.subscriber_id, pt.paper_sol_spent or 0.0, pnl,
        )


def _update_session_context(outcome: str) -> None:
    """Update session awareness state after every trade close.
    outcome: 'win', 'loss', or 'be' (breakeven)."""
    from datetime import datetime, timedelta

    # Track last 10 results
    state.session_recent_results.append(outcome)
    if len(state.session_recent_results) > 10:
        state.session_recent_results.pop(0)

    # Consecutive tracking
    if outcome == "win":
        state.session_consecutive_wins += 1
        state.session_consecutive_losses = 0
        state.session_today_wins += 1
    elif outcome == "loss":
        state.session_consecutive_losses += 1
        state.session_consecutive_wins = 0
        state.session_today_losses += 1
    else:
        state.session_consecutive_wins = 0
        state.session_consecutive_losses = 0

    # Streak detection
    state.session_hot_streak = state.session_consecutive_wins >= 3
    state.session_cold_streak = state.session_consecutive_losses >= 3

    # Auto-cooldown: pause trading after 5 consecutive losses
    if state.session_consecutive_losses >= 5:
        state.session_cooldown_until = datetime.utcnow() + timedelta(minutes=30)
        logger.warning(
            "SESSION: 5 consecutive losses — cooling down for 30 min until %s",
            state.session_cooldown_until.strftime("%H:%M UTC"),
        )

    state.session_last_close_reason = outcome
    logger.info(
        "SESSION: %s | streak W:%d L:%d | today W:%d L:%d | hot=%s cold=%s",
        outcome, state.session_consecutive_wins, state.session_consecutive_losses,
        state.session_today_wins, state.session_today_losses,
        state.session_hot_streak, state.session_cold_streak,
    )


# ── Open trade monitoring ────────────────────────────────────────────────────

_parse_pattern_tags = parse_pattern_tags


async def _check_open_trades(bot) -> None:
    # include_subscribers=True so the monitor manages TP/SL on subscriber
    # relay rows too (they're real paper trades that need exits).
    trades = await get_open_paper_trades(include_subscribers=True)
    # Always refresh the in-memory balance snapshot, even if there are
    # no open trades to iterate. Keeps state.paper_balance close to DB
    # reality between scanner ticks and /hub renders.
    try:
        state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    except Exception as exc:
        logger.debug("Paper monitor balance refresh failed: %s", exc)
    if not trades:
        return

    # ── Hard ecosystem eviction ─────────────────────────────────────
    # If the mint suffix allowlist rejects any open trade, close it as
    # "reset" (meta) on this tick. Fires once per stale position.
    evicted_trades = [pt for pt in trades if not mint_suffix_ok(pt.token_address)]
    if evicted_trades:
        for pt in evicted_trades:
            try:
                await _finalize_silent_close(
                    pt, "reset", 0.0, pt.peak_mc, pt.peak_multiple,
                )
                logger.info(
                    "Paper: EVICTED %s (%s) — mint suffix filter failed",
                    (pt.token_name or "?")[:18], (pt.token_address or "")[:12],
                )
            except Exception as exc:
                logger.debug("Paper evict close failed for %s: %s",
                             (pt.token_address or "")[:12], exc)
        trades = [pt for pt in trades if mint_suffix_ok(pt.token_address)]
        try:
            state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
        except Exception:
            pass
        if not trades:
            return

    # Time-based + profit-protection params (DB-driven so Agent 6 can learn).
    cfg = await get_params(
        "stale_exit_hours", "stale_exit_threshold",
        "expired_exit_hours", "expired_exit_threshold",
        "breakeven_trigger", "profit_trail_trigger", "profit_trail_pct",
        "tg_signal_trail_pct", "dead_token_threshold_usd",
        "paper_stop_slippage_pct",
        "time_stop_minutes", "entry_eject_after_sec", "entry_eject_peak_mult",
        "bundle_time_exit_min", "bundle_time_exit_mult",
        "tg_let_runners_run", "tg_moonbag_pct", "tg_moonbag_trail_pct",
    )

    for pt in trades:
        try:
            # bypass_cache: the 60s scanner cache made each poll read price up
            # to 60s stale, compounding the polling blind window. Open positions
            # need the freshest possible MC to catch fast reversals.
            live = await fetch_live_data(pt.token_address, bypass_cache=True)
            if not live or (live.get("market_cap") or 0) <= 0:
                _fetch_fail_counts[pt.id] = _fetch_fail_counts.get(pt.id, 0) + 1
                fails = _fetch_fail_counts[pt.id]
                logger.info(
                    "Paper check id=%s %s: no live data — fail %d/%d",
                    pt.id, (pt.token_name or "?")[:18], fails, MAX_FETCH_FAILS,
                )
                if fails >= MAX_FETCH_FAILS:
                    pnl = round(-pt.paper_sol_spent * 0.5, 4)
                    bal = await _finalize_silent_close(
                        pt, "dead_api", pnl, pt.peak_mc, pt.peak_multiple,
                    )
                    logger.warning(
                        "Paper: DEAD API %s — %d consecutive fetch failures, closing. pnl=%+.4f bal=%.4f",
                        (pt.token_name or "?")[:18], fails, pnl, bal,
                    )
                    _fetch_fail_counts.pop(pt.id, None)
                continue

            # Reset failure counter on successful fetch
            _fetch_fail_counts.pop(pt.id, None)

            current_mc = live.get("market_cap") or 0

            # Dead trade cleanup: if MC collapsed below $5K, the token is
            # dead (rug pulled, liquidity gone). Close immediately to free
            # the slot for real trades. Don't let dead tokens block entries.
            dead_threshold = float(cfg.get("dead_token_threshold_usd", 10000) or 10000)
            if current_mc < dead_threshold and current_mc > 0:
                sol = pt.paper_sol_spent or 0
                remaining = float(getattr(pt, "remaining_pct", 100) or 100)
                realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)
                remaining_sol = sol * (remaining / 100.0)
                mult = current_mc / (pt.entry_mc or 1)
                pnl = round(realized + remaining_sol * (mult - 1), 4)
                name = (pt.token_name or "?")[:18]
                source_tag = _source_tag(pt.pattern_type)
                logger.info("Paper: DEAD TOKEN %s — MC=$%.0f, source=%s, closing to free slot | pnl=%+.4f",
                            name, current_mc, source_tag, pnl)
                await _finalize_paper_close(
                    bot, pt, "dead_token", pnl, pt.peak_mc, pt.peak_multiple, [
                        f"💀 PAPER TRADE — DEAD TOKEN ({source_tag})",
                        f"🪙 {name} | MC collapsed to ${current_mc:.0f}",
                        f"PnL: {pnl:+.4f} SOL",
                        "Balance: {bal:.2f} SOL",
                    ],
                )
                continue

            entry_mc = pt.entry_mc or 0
            if entry_mc <= 0:
                logger.warning(
                    "Paper id=%s %s: entry_mc=0/None — closing as reset (BUG #1 row)",
                    pt.id, (pt.token_name or "?")[:18],
                )
                await _finalize_silent_close(
                    pt, "reset", 0.0, pt.peak_mc, pt.peak_multiple,
                )
                continue
            current_mult = current_mc / entry_mc

            # ── PHASE 2: Dynamic trail width ────────────────────────────
            # Trail tightens as gain grows. Wide early lets the move
            # breathe (memecoins are noisy); tight late locks the meat.
            # Research consensus (ATR-style trailing, multiple sources).
            #
            # Phase 5.5 Stage 2.5: Claude active can override the trail
            # width per-position via claude_trail_override_pct. When set
            # (0.0-1.0), it replaces the dynamic ladder for THIS trade
            # only — letting Claude loosen the trail on a runner he
            # believes in, or tighten it on a fading position.
            def _dynamic_trail_pct(peak_m: float) -> float:
                override = getattr(pt, "claude_trail_override_pct", None)
                if override is not None and 0.05 <= override <= 0.80:
                    return float(override)
                # 4am "let runners run": the report proved the P&L lives in the
                # fat tail (avg true peak 710x) and our tight trail capped us at
                # 0.87x. For 4am, trail MUCH wider so a 100x isn't shaken out at
                # 6x by a normal pullback. Median winners give back more — that's
                # the correct trade when the tail is this fat.
                if is_tg_signal and float(cfg.get("tg_let_runners_run", 1.0) or 0) >= 0.5:
                    wide = float(cfg.get("tg_moonbag_trail_pct", 0.60) or 0.60)
                    if peak_m < 2.0:   return 0.35
                    if peak_m < 5.0:   return 0.45
                    return wide                    # 5x+: let the monster run
                # Scanner: tighter ladder (no fat tail to protect — it scalps).
                if peak_m < 3.0:    return 0.30
                if peak_m < 6.0:    return 0.28
                return 0.20

            # ── SANITY CAP on corrupt MC readings ──────────────────────
            # DexScreener occasionally returns absurd MC values for new
            # pump.fun tokens (off by 1000x due to indexing race / supply
            # confusion). Without a cap, a single bad reading triggers
            # the trail-profit logic and produces phantom +500 SOL wins.
            # Real memecoins rarely hit 100x in our trade window; 500x
            # in a single tick is essentially always bad data.
            SUSPICIOUS_MULT_CAP = 100.0
            prev_peak = pt.peak_multiple or 1.0
            if (current_mult > SUSPICIOUS_MULT_CAP
                    and current_mult > prev_peak * 3.0):
                logger.warning(
                    "Paper monitor SUSPICIOUS MC: id=%s %s current_mc=%.0f "
                    "entry_mc=%.0f mult=%.1fx (prev_peak=%.1fx). Skipping "
                    "tick — likely DexScreener data corruption.",
                    pt.id, (pt.token_name or "?")[:24],
                    current_mc, entry_mc, current_mult, prev_peak,
                )
                continue  # skip this tick, retry next poll with fresh data

            is_tg_signal = "tg_signal" in (pt.pattern_type or "")
            # 4am "let runners run" — computed early so it guards EVERY 4am
            # exit (timeouts, no_momentum, time_stop, SL moonbag). The report
            # proved runners peak hours later, so 4am must survive the timeouts.
            _let_run = (is_tg_signal
                        and float(cfg.get("tg_let_runners_run", 1.0) or 0) >= 0.5)
            _skip_fast = _let_run
            peak_mc = max(pt.peak_mc or 0, current_mc)
            peak_mult = max(pt.peak_multiple or 1.0, current_mult)

            await update_paper_trade_peak(pt.id, current_mc, peak_mc, peak_mult)

            name = (pt.token_name or "Unknown").replace("_", " ")
            sol = pt.paper_sol_spent

            # Hoisted from later: time-exit / breakeven / profit-trail /
            # scale-in checks all reference these. Without the hoist,
            # `remaining` is undefined for any tick that doesn't hit the
            # dead-token branch first → "cannot access local variable" crash.
            remaining = float(getattr(pt, "remaining_pct", 100) or 100)
            realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)

            # Full per-tick diagnostic so "SL not hitting" can be
            # verified at a glance. Shows the exact comparison
            # values for every trade on every tick.
            tp_threshold = pt.take_profit_x or 3.0
            sl_threshold = 1.0 - ((pt.stop_loss_pct or 30.0) / 100.0)
            logger.info(
                "Paper check id=%s %s: mc=%.0f→%.0f mult=%.3fx "
                "tp=%.2fx sl@%.3fx (stop_loss_pct=%.1f) peak=%.2fx",
                pt.id, name[:18], entry_mc, current_mc, current_mult,
                tp_threshold, sl_threshold, pt.stop_loss_pct or 0, peak_mult,
            )

            # Resolve trailing-stop config at tick time so Agent 6 adjustments
            # take effect on open trades mid-flight. Cheap: one DB round-trip
            # per open trade per tick.
            tags = _parse_pattern_tags(pt.pattern_type)
            resolved = await resolve_trade_params(tags) if tags else None

            # Dead-position auto-close removed. Trades now close only via
            # TP, trailing stop, fixed SL, time-based cleanup, or profit
            # protection (break-even / profit-trail).

            # ── Time-based exits ─────────────────────────────────────────
            # Meta close reasons — NOT counted in Agent 6 win/loss math.
            age_hours = 0.0
            if pt.opened_at:
                age_hours = (datetime.utcnow() - pt.opened_at).total_seconds() / 3600.0

            # SHADOW: compute the pure engine's verdict from the same inputs.
            # Observational only — compared on close in _close_and_track.
            _shadow_eval(pt, cfg, current_mult, peak_mult, age_hours, remaining,
                         realized, is_tg_signal, _let_run, resolved)

            expired_h    = float(cfg.get("expired_exit_hours",     4.0) or 4.0)
            expired_thr  = float(cfg.get("expired_exit_threshold", 1.20) or 1.20)
            stale_h      = float(cfg.get("stale_exit_hours",       2.0) or 2.0)
            stale_thr    = float(cfg.get("stale_exit_threshold",   1.05) or 1.05)

            time_exit_reason = None
            if age_hours >= expired_h and current_mult < expired_thr:
                time_exit_reason = "expired"
            elif age_hours >= stale_h and current_mult < stale_thr:
                time_exit_reason = "stale"

            # 4am runners survive the time-based cleanups — the report proved
            # they moon hours later (peaked 2504x while "stale"). Only the
            # dead_token / dead_api floor (true collapse) closes a 4am moonbag.
            if time_exit_reason and _let_run:
                time_exit_reason = None

            if time_exit_reason:
                remaining_sol = sol * (remaining / 100.0)
                realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                bal = await _finalize_silent_close(
                    pt, time_exit_reason, pnl, peak_mc, peak_mult,
                )
                logger.info(
                    "Paper: %s %s — age=%.1fh mult=%.2fx pnl=%+.4f bal=%.4f tags=%s",
                    time_exit_reason.upper(), name, age_hours, current_mult, pnl, bal,
                    ",".join(tags) or "-",
                )
                # Intentionally no Telegram broadcast — these are cleanup
                # closes, not wins or losses. Keeping the group quiet.
                continue

            # ── PHASE 2: HARD TIMEOUT (4h max, exit regardless of mult) ─
            # Memecoins don't pump after 4h post-entry except in rare
            # cases. The existing "expired" check above only fires if
            # mult < 1.20 — this catches the 4h timeout regardless of
            # current price so capital doesn't get stuck riding a
            # consolidating token forever.
            hard_timeout_h = float(cfg.get("hard_timeout_hours", 4.0) or 4.0)
            if age_hours >= hard_timeout_h and not _let_run:
                remaining_sol = sol * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                logger.info(
                    "Paper: HARD-TIMEOUT %s — age=%.1fh mult=%.2fx pnl=%+.4f tags=%s",
                    name, age_hours, current_mult, pnl, ",".join(tags) or "-",
                )
                msg_icon = "✅" if pnl > 0 else "❌"
                await _finalize_paper_close(
                    bot, pt, "hard_timeout", pnl, peak_mc, peak_mult, [
                        f"{msg_icon} PAPER TRADE — HARD TIMEOUT (4h)",
                        f"🪙 {name} | {current_mult:.1f}x | {pnl:+.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        "Balance: {bal:.2f} SOL",
                    ],
                )
                continue

            # ── Post-entry momentum eject (report rec: kill DOA fast) ──
            # A dead-on-arrival entry that never showed life (peak < eject_peak)
            # and is below entry after the eject window (~90s) gets cut now
            # instead of riding the full time stop. Directly targets the
            # sl_hit/dead_token DOA leak (-5.7 SOL in the report).
            # (4am "let runners run" guard _skip_fast was computed at loop top.)
            eject_after_h = float(cfg.get("entry_eject_after_sec", 90.0) or 90.0) / 3600.0
            eject_peak = float(cfg.get("entry_eject_peak_mult", 1.10) or 1.10)
            _ts_min = float(cfg.get("time_stop_minutes", 5.0) or 5.0)
            if (not _skip_fast
                    and eject_after_h <= age_hours < (_ts_min / 60.0)
                    and peak_mult < eject_peak
                    and current_mult < 1.0):
                remaining_sol = sol * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                logger.info(
                    "Paper: NO-MOMENTUM EJECT %s — age=%.1fm peak=%.2fx now=%.2fx pnl=%+.4f",
                    name, age_hours * 60, peak_mult, current_mult, pnl,
                )
                await _finalize_paper_close(
                    bot, pt, "no_momentum", pnl, peak_mc, peak_mult,
                    _close_card("⏬", "NO MOMENTUM — ejected", name, current_mult,
                                pnl, entry_mc, current_mc,
                                note="Momentum died early — no follow-through."),
                    parse_mode="HTML",
                )
                continue

            # ── BUNDLE TIME-EXIT ─────────────────────────────────────────
            # A bundled launch pumps AND dumps fast (research: 73% collapse
            # below 40% within 20 min). If a bundle hasn't popped past the
            # exit mult within the dump window, cut it BEFORE the coordinated
            # dump rather than riding the normal (slower) time stop. Tagged at
            # entry via pattern_type containing "bundle".
            # Cut a FLAT/RED bundle before the dump, but let a GREEN one ride
            # (a bundle up 13% and climbing is working — don't cut it as 'flat').
            # Bundles use ONLY this time-cut (excluded from the generic time_stop).
            is_bundle_trade = "bundle" in (pt.pattern_type or "").lower()
            if is_bundle_trade:
                b_exit_min = float(cfg.get("bundle_time_exit_min", 15.0) or 15.0)
                b_flat = float(cfg.get("bundle_flat_mult", 1.05) or 1.05)
                if age_hours >= (b_exit_min / 60.0) and current_mult < b_flat:
                    remaining_sol = sol * (remaining / 100.0)
                    pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                    logger.info(
                        "Paper: BUNDLE TIME-EXIT %s — age=%.1fm peak=%.2fx now=%.2fx pnl=%+.4f",
                        name, age_hours * 60, peak_mult, current_mult, pnl,
                    )
                    await _finalize_paper_close(
                        bot, pt, "bundle_time_exit", pnl, peak_mc, peak_mult,
                        _close_card("📦", "BUNDLE EXIT — flat/red, cut early", name,
                                    current_mult, pnl, entry_mc, current_mc,
                                    note=f"Flat/red after {age_hours*60:.0f}m — cut before the dump."),
                        parse_mode="HTML",
                    )
                    continue

            # ── PHASE 2: TIME STOP (no movement after the time-stop window) ──
            # If trade hasn't moved meaningfully in time, exit. Memecoins die
            # fast; stale capital is dead capital. Cut from 8→5 min per report.
            time_stop_min = float(cfg.get("time_stop_minutes", 5.0) or 5.0)
            time_stop_thr = float(cfg.get("time_stop_threshold", 1.50) or 1.50)
            if (not _skip_fast and not is_bundle_trade
                    and age_hours >= (time_stop_min / 60.0)
                    and peak_mult < time_stop_thr
                    and current_mult < time_stop_thr):
                remaining_sol = sol * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                logger.info(
                    "Paper: TIME-STOP %s — age=%.1fm peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                    name, age_hours * 60, peak_mult, current_mult, pnl,
                    ",".join(tags) or "-",
                )
                # Silent — scanner time-stops fire often and would spam the feed.
                await _finalize_silent_close(
                    pt, "time_stop", pnl, peak_mc, peak_mult,
                )
                continue

            # ── Profit protection ────────────────────────────────────────
            # Break-even SL: once peak crossed the breakeven trigger,
            # never let the trade close below 1.0x from here on.
            be_trigger = float(cfg.get("breakeven_trigger", 2.0) or 2.0)
            if peak_mult >= be_trigger and current_mult <= 1.0:
                remaining_sol = sol * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                logger.info(
                    "Paper: BREAKEVEN %s — peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                    name, peak_mult, current_mult, pnl, ",".join(tags) or "-",
                )
                await _finalize_paper_close(
                    bot, pt, "breakeven_stop", pnl, peak_mc, peak_mult,
                    _close_card("🛡️", "BREAK-EVEN STOP", name, current_mult, pnl,
                                entry_mc, current_mc,
                                note=f"Peak {peak_mult:.1f}x — protected the entry."),
                    parse_mode="HTML",
                )
                continue

            # Profit trailing stop: once peak crosses the profit trail
            # trigger, follow the peak down by trail width.
            # PHASE 2: ALL trades (4am AND scanner) use DYNAMIC trail
            # width that tightens as peak grows. Wide early to breathe,
            # tight late to lock. Same logic regardless of source.
            if is_tg_signal:
                pt_trigger = float(cfg.get("tg_signal_trail_trigger", 1.5) or 1.5)
            else:
                pt_trigger = float(cfg.get("profit_trail_trigger", 2.0) or 2.0)
            pt_pct = _dynamic_trail_pct(peak_mult)
            if peak_mult >= pt_trigger:
                trail_stop_mult = peak_mult * (1.0 - pt_pct)
                if current_mult <= trail_stop_mult:
                    pnl = round(sol * (current_mult - 1), 4)
                    logger.info(
                        "Paper: PROFIT-TRAIL %s — peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                        name, peak_mult, current_mult, pnl, ",".join(tags) or "-",
                    )
                    await _finalize_paper_close(
                        bot, pt, "profit_trail", pnl, peak_mc, peak_mult,
                        _close_card("💰", "PROFIT TRAIL", name, current_mult, pnl,
                                    entry_mc, current_mc,
                                    note=f"Peak {peak_mult:.1f}x — trailed the runner."),
                        parse_mode="HTML",
                    )
                    continue

            # Scale-in removed (June 10 2026 audit): adding SOL mid-trade
            # without adjusting entry_mc booked phantom profit (~2x inflated
            # wins), and adds at 1.5-1.95x bought into the launch-chop bleed
            # zone right before the 2x ladder sold. May return later via the
            # Fable escalation lane with weighted-average entry math.

            # ── Scale OUT: sell partial at milestones ────────────────────
            # 30% at 2x, 25% at 5x, trail the remaining 45%

            # Scale-out partial sells are HQ-only — subscriber relay rows
            # ride the full position to TP/SL/trail. Realized PnL on relay
            # rows would also bypass _refund_subscriber_balance.
            # PHASE 2: 4-tranche ladder exits.
            # 40% at 2x (recovers initial cost)
            # 25% at 5x (lock big chunk)
            # 15% at 10x (lock most of meat)
            # 20% rides on dynamic trail (the moonbag)
            scale_out_done = False
            # Phase 5.5 Stage 2.5: Claude active can disable the auto
            # 2x/5x/10x ladder per-position. When ladder is disabled,
            # the position keeps 100% in flight and rides the (Claude-
            # widened) trail + TP. Use this when Claude has high
            # conviction the move has way more upside than the ladder
            # tranches would let through.
            ladder_disabled = bool(getattr(pt, "claude_ladder_disabled", False))
            if pt.subscriber_id is not None:
                pass  # skip scale-out for subscribers
            elif ladder_disabled:
                pass  # Claude let it ride — no auto-tranche exits
            elif remaining > 80 and current_mult >= 2.0:
                # First scale: 40% at 2x (Phase 2: was 30%)
                # Recovers initial cost + 40% profit on the slice sold
                sell_pct = 40.0
                sell_sol = sol * (sell_pct / 100.0) * (current_mult - 1)
                new_remaining = remaining - sell_pct
                new_realized = realized + sell_sol
                try:
                    async with AsyncSessionLocal() as sess:
                        trade = await sess.get(PaperTrade, pt.id)
                        if trade:
                            trade.remaining_pct = new_remaining
                            trade.realized_pnl_sol = new_realized
                            await sess.commit()
                except Exception:
                    pass
                logger.info(
                    "Paper SCALE OUT %s: sold 40%% at %.1fx | +%.4f SOL | remaining %.0f%%",
                    name, current_mult, sell_sol, new_remaining,
                )
                src_tag = "⚡ 4AM" if is_tg_signal else "🔍 Scanner"
                _scale_out_text = "\n".join([
                    f"📈 PAPER TRADE — SCALE OUT 40% ({src_tag})",
                    f"🪙 {name} | {current_mult:.1f}x | +{sell_sol:.4f} SOL",
                    f"Remaining: {new_remaining:.0f}% still running",
                ])
                try:
                    await bot.send_message(
                        CALLER_GROUP_ID, _scale_out_text,
                        message_thread_id=SCAN_TOPIC_ID,
                    )
                except Exception:
                    pass
                try:
                    from bot.community_feed import post_to_community
                    await post_to_community(bot, _scale_out_text)
                except Exception as exc:
                    logger.debug("community_feed scale-out 40 mirror failed: %s", exc)
                # Live mirror: sell the same tranche on-chain (sell_pct of the
                # pre-sale remaining = fraction of current live balance).
                try:
                    from bot.live_mirror import mirror_partial
                    await mirror_partial(pt.id, pt.token_address, sell_pct / max(remaining, 1.0))
                except Exception as exc:
                    logger.error("live_mirror partial(40) hook failed: %s", exc)
                scale_out_done = True

            elif pt.subscriber_id is None and remaining > 50 and remaining <= 80 and current_mult >= 5.0:
                # Second scale: sell 25% at 5x — let runners actually run
                sell_pct = 25.0
                sell_sol = sol * (sell_pct / 100.0) * (current_mult - 1)
                new_remaining = remaining - sell_pct
                new_realized = realized + sell_sol
                try:
                    async with AsyncSessionLocal() as sess:
                        trade = await sess.get(PaperTrade, pt.id)
                        if trade:
                            trade.remaining_pct = new_remaining
                            trade.realized_pnl_sol = new_realized
                            await sess.commit()
                except Exception:
                    pass
                logger.info(
                    "Paper SCALE OUT %s: sold 30%% at %.1fx | +%.4f SOL | remaining %.0f%%",
                    name, current_mult, sell_sol, new_remaining,
                )
                src_tag = "⚡ 4AM" if is_tg_signal else "🔍 Scanner"
                _scale_out_text = "\n".join([
                    f"📈 PAPER TRADE — SCALE OUT 25% ({src_tag})",
                    f"🪙 {name} | {current_mult:.1f}x | +{sell_sol:.4f} SOL",
                    f"Remaining: {new_remaining:.0f}% — trailing stop active",
                ])
                try:
                    await bot.send_message(
                        CALLER_GROUP_ID, _scale_out_text,
                        message_thread_id=SCAN_TOPIC_ID,
                    )
                except Exception:
                    pass
                try:
                    from bot.community_feed import post_to_community
                    await post_to_community(bot, _scale_out_text)
                except Exception as exc:
                    logger.debug("community_feed scale-out 25 mirror failed: %s", exc)
                # Live mirror: sell the same tranche on-chain.
                try:
                    from bot.live_mirror import mirror_partial
                    await mirror_partial(pt.id, pt.token_address, sell_pct / max(remaining, 1.0))
                except Exception as exc:
                    logger.error("live_mirror partial(25) hook failed: %s", exc)
                scale_out_done = True

            # Check TP — only on remaining position
            if current_mult >= pt.take_profit_x:
                # Close remaining position at TP
                remaining_sol = sol * (remaining / 100.0)
                pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                logger.info(
                    "Paper: TP hit %s — %.1fx +%.4f SOL tags=%s",
                    name, current_mult, pnl, ",".join(tags) or "-",
                )
                await _finalize_paper_close(
                    bot, pt, "tp_hit", pnl, peak_mc, peak_mult,
                    _close_card("✅", "WIN — take-profit hit", name, current_mult,
                                pnl, entry_mc, current_mc, note="🎯 Target reached."),
                    parse_mode="HTML",
                )
                continue

            # Trailing stop — PHASE 2: ALL trades use dynamic trail width
            if resolved and resolved["trail_enabled"]:
                if is_tg_signal:
                    trigger_mult = float(cfg.get("tg_signal_trail_trigger", 1.5) or 1.5)
                else:
                    trigger_mult = 1.0 + float(resolved["trail_trigger"])
                # Dynamic trail width applies universally
                trail_pct = _dynamic_trail_pct(peak_mult)

                if peak_mult >= trigger_mult:
                    trail_stop_mult = peak_mult * (1.0 - trail_pct)
                    if current_mult <= trail_stop_mult:
                        remaining_sol = sol * (remaining / 100.0)
                        pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                        logger.info(
                            "Paper: TRAIL hit %s — peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                            name, peak_mult, current_mult, pnl, ",".join(tags) or "-",
                        )
                        await _finalize_paper_close(
                            bot, pt, "trail_hit", pnl, peak_mc, peak_mult,
                            _close_card("📉", "TRAIL STOP", name, current_mult, pnl,
                                        entry_mc, current_mc,
                                        note=f"Peak {peak_mult:.1f}x — locked the trail."),
                            parse_mode="HTML",
                        )
                        continue

            # Phase-based SL — tight SL with brief grace period.
            # Research shows: 15-20% SL with fast entry is optimal.
            # Grace period shortened to 5 min (was 15) — just enough
            # to survive the first candle wick, not long enough to
            # hold a rug for 15 minutes.
            sl_threshold = 1.0 - (pt.stop_loss_pct / 100.0)
            catastrophic_threshold = 0.50  # 50% drop = rug, always exit

            # 4am signals get 10 min grace, regular get 5 min
            grace_min = 10.0 if is_tg_signal else 5.0

            if age_hours < (grace_min / 60.0):
                # Phase 1: grace period — only exit on catastrophic rug
                should_sl = current_mult <= catastrophic_threshold
            else:
                # Phase 2: after grace — full tight SL
                should_sl = current_mult <= sl_threshold

            if should_sl:
                slip = float(cfg.get("paper_stop_slippage_pct", 15.0) or 15.0) / 100.0
                fill_mult = current_mult * (1.0 - slip)

                # 4am MOONBAG: never fully stop out a 4am call. The report proved
                # 4am tokens dump-then-moon (closed 0x → peaked 2504x). So on SL we
                # sell down to the moonbag and let that slice ride with NO further
                # stop — it either dies (tiny capped loss) or catches the runner.
                moonbag_pct = (float(cfg.get("tg_moonbag_pct", 25.0) or 0)
                               if (is_tg_signal and _let_run) else 0.0)
                if moonbag_pct > 0 and remaining > moonbag_pct + 0.5:
                    sell_pct = remaining - moonbag_pct
                    sold_sol = sol * (sell_pct / 100.0)
                    realized_delta = round(sold_sol * (fill_mult - 1.0), 4)  # negative
                    new_realized = round(realized + realized_delta, 4)
                    try:
                        async with AsyncSessionLocal() as sess:
                            tr = await sess.get(PaperTrade, pt.id)
                            if tr:
                                tr.remaining_pct = moonbag_pct
                                tr.realized_pnl_sol = new_realized
                                await sess.commit()
                    except Exception:
                        pass
                    try:
                        from bot.live_mirror import mirror_partial
                        await mirror_partial(pt.id, pt.token_address, sell_pct / max(remaining, 1.0))
                    except Exception as exc:
                        logger.error("live_mirror moonbag partial failed: %s", exc)
                    logger.info(
                        "Paper: 4AM MOONBAG %s — SL'd %.0f%% at %.2fx, %.0f%% rides | realized %+.4f",
                        name, sell_pct, current_mult, moonbag_pct, new_realized,
                    )
                    from html import escape as _html_escape
                    _txt = (f"🌙 <b>4AM MOONBAG</b> — {_html_escape(str(name)[:22])}\n\n"
                            f"Cut {sell_pct:.0f}% at {current_mult:.2f}x — "
                            f"<b>{moonbag_pct:.0f}% rides for the moon</b> 🚀\n"
                            f"No stop on the moonbag. It moons or it doesn't.")
                    try:
                        await bot.send_message(CALLER_GROUP_ID, _txt,
                                               message_thread_id=SCAN_TOPIC_ID, parse_mode="HTML")
                    except Exception:
                        pass
                    try:
                        from bot.community_feed import post_to_community
                        await post_to_community(bot, _txt, parse_mode="HTML")
                    except Exception:
                        pass
                    continue
                if moonbag_pct > 0 and remaining <= moonbag_pct + 0.5:
                    # Already riding the moonbag — never SL it. It exits only via
                    # profit-trail (if it moons) or the dead_token/dead_api floor.
                    continue

                # Normal full SL close (scanner, or 4am with moonbag disabled).
                remaining_sol = sol * (remaining / 100.0)
                loss_on_remaining = round(-remaining_sol * (1.0 - fill_mult), 4)
                pnl = round(realized + loss_on_remaining, 4)
                logger.info(
                    "Paper: SL hit %s — %.2fx %.4f SOL tags=%s",
                    name, current_mult, pnl, ",".join(tags) or "-",
                )
                await _finalize_paper_close(
                    bot, pt, "sl_hit", pnl, peak_mc, peak_mult,
                    _close_card("❌", "LOSS — stop-loss hit", name, current_mult,
                                pnl, entry_mc, current_mc, note="🛑 SL triggered."),
                    parse_mode="HTML",
                )

        except Exception as exc:
            logger.error("Paper monitor error for %s: %s", pt.token_address[:12], exc)


# ── Post-close tracking ─────────────────────────────────────────────────────

async def _track_post_close() -> None:
    """
    For recently closed trades (last 25h), fetch current MC and record
    performance at 1h, 6h, 24h intervals after close.
    """
    trades = await get_recently_closed_paper_trades(hours=25)
    if not trades:
        return

    now = datetime.utcnow()

    for pt in trades:
        try:
            if not pt.closed_at:
                continue

            hours_since_close = (now - pt.closed_at).total_seconds() / 3600
            entry_mc = pt.entry_mc or 1
            close_mc = pt.peak_mc or entry_mc  # approximate close MC

            live = await fetch_live_data(pt.token_address)
            if not live:
                continue

            current_mc = live.get("market_cap") or 0
            if current_mc <= 0:
                continue

            # Update peak after close
            peak_after = max(pt.peak_after_close or 0, current_mc)

            # Fill in time-based snapshots
            mc_1h = pt.mc_1h_after
            mc_4h = getattr(pt, "mc_4h_after", None)
            mc_6h = pt.mc_6h_after
            mc_24h = pt.mc_24h_after

            if mc_1h is None and hours_since_close >= 1:
                mc_1h = current_mc
            if mc_4h is None and hours_since_close >= 4:
                mc_4h = current_mc
            if mc_6h is None and hours_since_close >= 6:
                mc_6h = current_mc
            if mc_24h is None and hours_since_close >= 24:
                mc_24h = current_mc

            # Determine sold_too_early: price went 50%+ higher after close
            close_mult = close_mc / entry_mc if entry_mc > 0 else 1.0
            peak_after_mult = peak_after / entry_mc if entry_mc > 0 else 1.0
            sold_too_early = peak_after_mult > close_mult * 1.5 if close_mult > 0 else False

            # Determine sold_too_late: SL hit but recovered within 1h
            sold_too_late = False
            if pt.close_reason == "sl_hit" and mc_1h is not None:
                if mc_1h > entry_mc:
                    sold_too_late = True

            await update_paper_post_close(
                trade_id=pt.id, mc_1h=mc_1h, mc_4h=mc_4h, mc_6h=mc_6h, mc_24h=mc_24h,
                peak_after=peak_after,
                sold_too_early=sold_too_early, sold_too_late=sold_too_late,
            )

            # Structured per-close log line for Agent 6 to grep and for humans
            # to spot-check. Fires once per closed trade per tick that adds
            # new data (mc_1h or mc_4h newly filled).
            name = (pt.token_name or "?")[:15]
            logger.info(
                "Agent6-signal close=%s tags=%s peak=%.2fx mc1h=%s mc4h=%s early=%s late=%s",
                pt.close_reason or "?",
                pt.pattern_type or "-",
                pt.peak_multiple or 1.0,
                f"{mc_1h/entry_mc:.2f}x" if mc_1h and entry_mc else "-",
                f"{mc_4h/entry_mc:.2f}x" if mc_4h and entry_mc else "-",
                sold_too_early, sold_too_late,
            )

        except Exception as exc:
            logger.debug("Post-close tracking error for %s: %s", pt.token_address[:12], exc)


# ── Background loops ─────────────────────────────────────────────────────────

async def paper_monitor_loop(bot) -> None:
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Paper monitor started — open trades every %ds, post-close every %ds",
                POLL_INTERVAL, POST_CLOSE_INTERVAL)

    last_post_close = datetime.utcnow()
    from bot.health import beat, register
    register("paper_monitor", stale_after_s=max(POLL_INTERVAL * 5, 300))

    while True:
        try:
            await _check_open_trades(bot)
            beat("paper_monitor")  # exit checks ran this cycle — the critical one

            # Post-close tracking every 10 minutes
            if (datetime.utcnow() - last_post_close).total_seconds() >= POST_CLOSE_INTERVAL:
                await _track_post_close()
                last_post_close = datetime.utcnow()

        except Exception as exc:
            logger.error("Paper monitor loop error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
