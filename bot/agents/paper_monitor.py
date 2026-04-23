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
)
from bot.scanner import mint_suffix_ok
from bot.agents.trade_profiles import resolve_trade_params, parse_pattern_tags

logger = logging.getLogger(__name__)

POLL_INTERVAL      = 30    # 30 seconds — faster SL/TP checks (was 60s,
                           #   too long for pump.fun volatility; trades
                           #   could drop below SL and recover between
                           #   ticks so SL never fired at check time)
POST_CLOSE_INTERVAL = 600  # 10 minutes — post-close tracking
STARTUP_DELAY      = 60
MAX_FETCH_FAILS    = 5     # consecutive failures before auto-close

# Per-trade consecutive fetch failure counter (in-memory, resets on bot restart)
_fetch_fail_counts: dict[int, int] = {}


async def _close_and_track(trade_id, reason, pnl, peak_mc, peak_mult):
    """Close a paper trade AND update session context in one call."""
    await close_paper_trade(trade_id, reason, pnl, peak_mc, peak_mult)
    # Determine outcome from reason
    if reason in ("tp_hit", "trail_hit", "profit_trail"):
        _update_session_context("win")
    elif reason in ("breakeven_stop",):
        _update_session_context("be")
    elif reason in ("sl_hit", "dead_api", "dead_token"):
        _update_session_context("loss")
    # stale, expired, reset = meta, don't track


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
    trades = await get_open_paper_trades()
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
                await close_paper_trade(
                    pt.id, "reset", 0.0, pt.peak_mc, pt.peak_multiple,
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
    )

    for pt in trades:
        try:
            live = await fetch_live_data(pt.token_address)
            if not live or (live.get("market_cap") or 0) <= 0:
                _fetch_fail_counts[pt.id] = _fetch_fail_counts.get(pt.id, 0) + 1
                fails = _fetch_fail_counts[pt.id]
                logger.info(
                    "Paper check id=%s %s: no live data — fail %d/%d",
                    pt.id, (pt.token_name or "?")[:18], fails, MAX_FETCH_FAILS,
                )
                if fails >= MAX_FETCH_FAILS:
                    pnl = round(-pt.paper_sol_spent * 0.5, 4)
                    await _close_and_track(pt.id, "dead_api", pnl, pt.peak_mc, pt.peak_multiple)
                    bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                    state.paper_balance = bal
                    logger.warning(
                        "Paper: DEAD API %s — %d consecutive fetch failures, closing. pnl=%+.4f bal=%.4f",
                        (pt.token_name or "?")[:18], fails, pnl, bal,
                    )
                    _fetch_fail_counts.pop(pt.id, None)
                continue

            # Reset failure counter on successful fetch
            _fetch_fail_counts.pop(pt.id, None)

            current_mc = live.get("market_cap") or 0

            entry_mc = pt.entry_mc or 0
            if entry_mc <= 0:
                logger.warning(
                    "Paper id=%s %s: entry_mc=0/None — closing as reset (BUG #1 row)",
                    pt.id, (pt.token_name or "?")[:18],
                )
                await close_paper_trade(pt.id, "reset", 0.0, pt.peak_mc, pt.peak_multiple)
                continue
            current_mult = current_mc / entry_mc
            peak_mc = max(pt.peak_mc or 0, current_mc)
            peak_mult = max(pt.peak_multiple or 1.0, current_mult)

            await update_paper_trade_peak(pt.id, current_mc, peak_mc, peak_mult)

            name = (pt.token_name or "Unknown").replace("_", " ")
            sol = pt.paper_sol_spent

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

            expired_h    = float(cfg.get("expired_exit_hours",     4.0) or 4.0)
            expired_thr  = float(cfg.get("expired_exit_threshold", 1.20) or 1.20)
            stale_h      = float(cfg.get("stale_exit_hours",       2.0) or 2.0)
            stale_thr    = float(cfg.get("stale_exit_threshold",   1.05) or 1.05)

            time_exit_reason = None
            if age_hours >= expired_h and current_mult < expired_thr:
                time_exit_reason = "expired"
            elif age_hours >= stale_h and current_mult < stale_thr:
                time_exit_reason = "stale"

            if time_exit_reason:
                pnl = round(sol * (current_mult - 1), 4)
                await close_paper_trade(pt.id, time_exit_reason, pnl, peak_mc, peak_mult)
                bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_balance = bal
                logger.info(
                    "Paper: %s %s — age=%.1fh mult=%.2fx pnl=%+.4f bal=%.4f tags=%s",
                    time_exit_reason.upper(), name, age_hours, current_mult, pnl, bal,
                    ",".join(tags) or "-",
                )
                # Intentionally no Telegram broadcast — these are cleanup
                # closes, not wins or losses. Keeping the group quiet.
                continue

            # ── Profit protection ────────────────────────────────────────
            # Break-even SL: once peak crossed the breakeven trigger,
            # never let the trade close below 1.0x from here on.
            be_trigger = float(cfg.get("breakeven_trigger", 1.5) or 1.5)
            if peak_mult >= be_trigger and current_mult <= 1.0:
                pnl = round(sol * (current_mult - 1), 4)
                await _close_and_track(pt.id, "breakeven_stop", pnl, peak_mc, peak_mult)
                bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_balance = bal
                logger.info(
                    "Paper: BREAKEVEN %s — peak=%.2fx now=%.2fx pnl=%+.4f bal=%.4f tags=%s",
                    name, peak_mult, current_mult, pnl, bal, ",".join(tags) or "-",
                )
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"🛡️ PAPER TRADE — BREAK EVEN STOP",
                        f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {bal:.2f} SOL",
                    ]), message_thread_id=SCAN_TOPIC_ID)
                except Exception:
                    pass
                continue

            # Profit trailing stop: once peak crosses the profit trail
            # trigger (e.g. 2.0x), follow the peak down by profit_trail_pct.
            pt_trigger = float(cfg.get("profit_trail_trigger", 2.0) or 2.0)
            pt_pct     = float(cfg.get("profit_trail_pct",     0.15) or 0.15)
            if peak_mult >= pt_trigger:
                trail_stop_mult = peak_mult * (1.0 - pt_pct)
                if current_mult <= trail_stop_mult:
                    pnl = round(sol * (current_mult - 1), 4)
                    await _close_and_track(pt.id, "profit_trail", pnl, peak_mc, peak_mult)
                    bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                    state.paper_balance = bal
                    logger.info(
                        "Paper: PROFIT-TRAIL %s — peak=%.2fx now=%.2fx pnl=%+.4f bal=%.4f tags=%s",
                        name, peak_mult, current_mult, pnl, bal, ",".join(tags) or "-",
                    )
                    try:
                        await bot.send_message(CALLER_GROUP_ID, "\n".join([
                            f"💰 PAPER TRADE — PROFIT TRAIL",
                            f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                            f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                            f"Balance: {bal:.2f} SOL",
                        ]), message_thread_id=SCAN_TOPIC_ID)
                    except Exception:
                        pass
                    continue

            # Check TP
            if current_mult >= pt.take_profit_x:
                pnl = round(sol * (current_mult - 1), 4)
                await _close_and_track(pt.id, "tp_hit", pnl, peak_mc, peak_mult)
                bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_balance = bal
                logger.info(
                    "Paper: TP hit %s — %.1fx +%.4f SOL bal=%.4f tags=%s",
                    name, current_mult, pnl, bal, ",".join(tags) or "-",
                )
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"✅ PAPER TRADE WIN",
                        f"🪙 {name} | {current_mult:.1f}x | +{pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {bal:.2f} SOL",
                    ]), message_thread_id=SCAN_TOPIC_ID)
                except Exception:
                    pass
                continue

            # Trailing stop: once peak_mult crosses (1 + trail_trigger),
            # abandon the fixed SL and follow the peak down by trail_pct.
            if resolved and resolved["trail_enabled"]:
                trigger_mult = 1.0 + float(resolved["trail_trigger"])
                if peak_mult >= trigger_mult:
                    trail_stop_mult = peak_mult * (1.0 - float(resolved["trail_pct"]))
                    if current_mult <= trail_stop_mult:
                        pnl = round(sol * (current_mult - 1), 4)
                        await _close_and_track(pt.id, "trail_hit", pnl, peak_mc, peak_mult)
                        bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                        state.paper_balance = bal
                        logger.info(
                            "Paper: TRAIL hit %s — peak=%.2fx now=%.2fx pnl=%+.4f bal=%.4f tags=%s",
                            name, peak_mult, current_mult, pnl, bal, ",".join(tags) or "-",
                        )
                        try:
                            msg_icon = "✅" if pnl > 0 else "❌"
                            await bot.send_message(CALLER_GROUP_ID, "\n".join([
                                f"{msg_icon} PAPER TRADE — TRAIL STOP",
                                f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                                f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                                f"Balance: {bal:.2f} SOL",
                            ]), message_thread_id=SCAN_TOPIC_ID)
                        except Exception:
                            pass
                        continue

            # Check fixed SL — with grace period.
            # Memecoins wick 30-50% on entry and recover. Don't sell the dip.
            # Grace period scales with MC: smaller = more volatile = longer grace.
            if entry_mc < 100_000:
                sl_grace_min = 10   # micro caps need 10 min to settle
            elif entry_mc < 500_000:
                sl_grace_min = 7
            else:
                sl_grace_min = 5
            sl_threshold = 1.0 - (pt.stop_loss_pct / 100.0)
            if current_mult <= sl_threshold and age_hours >= (sl_grace_min / 60.0):
                pnl = round(-sol * (1.0 - current_mult), 4)
                await _close_and_track(pt.id, "sl_hit", pnl, peak_mc, peak_mult)
                bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_balance = bal
                logger.info(
                    "Paper: SL hit %s — %.2fx %.4f SOL bal=%.4f tags=%s",
                    name, current_mult, pnl, bal, ",".join(tags) or "-",
                )
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"❌ PAPER TRADE LOSS",
                        f"🪙 {name} | SL hit | {pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"Balance: {bal:.2f} SOL",
                    ]), message_thread_id=SCAN_TOPIC_ID)
                except Exception:
                    pass

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

    while True:
        try:
            await _check_open_trades(bot)

            # Post-close tracking every 10 minutes
            if (datetime.utcnow() - last_post_close).total_seconds() >= POST_CLOSE_INTERVAL:
                await _track_post_close()
                last_post_close = datetime.utcnow()

        except Exception as exc:
            logger.error("Paper monitor loop error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
