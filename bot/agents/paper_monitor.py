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
    AsyncSessionLocal, PaperTrade,
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


async def _close_and_track(trade_id, reason, pnl, peak_mc, peak_mult, is_admin: bool = True):
    """Close a paper trade AND update session context in one call.
    Session context only tracks admin (HQ) outcomes — subscriber relay
    closes don't influence HQ session streaks."""
    await close_paper_trade(trade_id, reason, pnl, peak_mc, peak_mult)
    if not is_admin:
        return
    if reason in ("tp_hit", "trail_hit", "profit_trail"):
        _update_session_context("win")
    elif reason in ("breakeven_stop",):
        _update_session_context("be")
    elif reason == "sl_hit":
        _update_session_context("loss")
    # dead_api / dead_token are backup-SL meta closes — not counted as
    # strategy losses for streak/regime detection (they bypassed the
    # primary SL window via grace period or fetch failure).


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
    await _close_and_track(pt.id, reason, pnl, peak_mc, peak_mult, is_admin=is_admin)

    if is_admin:
        bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
        state.paper_balance = bal
        try:
            text = "\n".join(line.format(bal=bal) for line in lines)
            await bot.send_message(
                CALLER_GROUP_ID, text,
                message_thread_id=SCAN_TOPIC_ID,
            )
        except Exception:
            pass
        return bal
    else:
        bal = await _refund_subscriber_balance(
            pt.subscriber_id, pt.paper_sol_spent or 0.0, pnl,
        )
        try:
            text = "\n".join(line.format(bal=bal) for line in lines)
            await bot.send_message(pt.subscriber_id, text)
        except Exception:
            pass
        return bal


async def _finalize_silent_close(pt, reason, pnl, peak_mc, peak_mult):
    """Close trade without posting any notification — used for time-based
    cleanups (stale/expired) and dead_api closes that intentionally stay
    quiet. Still refunds balance for the correct owner."""
    await _close_and_track(pt.id, reason, pnl, peak_mc, peak_mult)
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
        "tg_signal_trail_pct",
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
            if current_mc < 5000 and current_mc > 0:
                sol = pt.paper_sol_spent or 0
                remaining = float(getattr(pt, "remaining_pct", 100) or 100)
                realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)
                remaining_sol = sol * (remaining / 100.0)
                mult = current_mc / (pt.entry_mc or 1)
                pnl = round(realized + remaining_sol * (mult - 1), 4)
                name = (pt.token_name or "?")[:18]
                logger.info("Paper: DEAD TOKEN %s — MC=$%.0f, closing to free slot | pnl=%+.4f",
                            name, current_mc, pnl)
                await _finalize_paper_close(
                    bot, pt, "dead_token", pnl, pt.peak_mc, pt.peak_multiple, [
                        f"💀 PAPER TRADE — DEAD TOKEN",
                        f"🪙 {name} | MC collapsed to ${current_mc:.0f}",
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
            is_tg_signal = "tg_signal" in (pt.pattern_type or "")
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
                    bot, pt, "breakeven_stop", pnl, peak_mc, peak_mult, [
                        f"🛡️ PAPER TRADE — BREAK EVEN STOP",
                        f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        "Balance: {bal:.2f} SOL",
                    ],
                )
                continue

            # Profit trailing stop: once peak crosses the profit trail
            # trigger, follow the peak down by profit_trail_pct.
            # 4am trades get wider settings to let runners run; trail %
            # is /setparam-tunable via tg_signal_trail_pct.
            if is_tg_signal:
                pt_trigger = 3.0   # 4am: don't trail until 3x
                pt_pct = float(cfg.get("tg_signal_trail_pct", 0.35) or 0.35)
            else:
                pt_trigger = float(cfg.get("profit_trail_trigger", 2.0) or 2.0)
                pt_pct     = float(cfg.get("profit_trail_pct",     0.15) or 0.15)
            if peak_mult >= pt_trigger:
                trail_stop_mult = peak_mult * (1.0 - pt_pct)
                if current_mult <= trail_stop_mult:
                    pnl = round(sol * (current_mult - 1), 4)
                    logger.info(
                        "Paper: PROFIT-TRAIL %s — peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                        name, peak_mult, current_mult, pnl, ",".join(tags) or "-",
                    )
                    await _finalize_paper_close(
                        bot, pt, "profit_trail", pnl, peak_mc, peak_mult, [
                            f"💰 PAPER TRADE — PROFIT TRAIL",
                            f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                            f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                            "Balance: {bal:.2f} SOL",
                        ],
                    )
                    continue

            # ── Scale IN: add size when token proves itself ────────────────
            # Probe with 0.1 SOL, then scale up if the token pumps.
            # This is how real traders work — don't risk big until it proves.
            # HQ-only: subscriber relay rows have a fixed 0.1 SOL probe and
            # don't scale; scaling them would also drain admin paper_balance.
            #
            # Upper cap at 1.95x: above 2.0x the scale-out branch fires in
            # the same tick, which used to mean we'd add 0.2 SOL THEN sell
            # 30% of it at the same price — wasteful round-trip. Confirmation
            # zone is 1.5x-1.95x (token is moving but not yet at first profit
            # target); scale-out owns 2.0x+.
            age_min = age_hours * 60
            if (pt.subscriber_id is None
                    and 1.5 <= current_mult < 1.95 and age_min >= 2 and age_min <= 10
                    and sol < 0.25 and remaining >= 100):
                # Token pumped 50%+ in first 2-10 min — add more size
                add_sol = 0.2
                if state.paper_balance >= add_sol + 0.1:
                    try:
                        async with AsyncSessionLocal() as sess:
                            trade = await sess.get(PaperTrade, pt.id)
                            if trade:
                                trade.paper_sol_spent = round(trade.paper_sol_spent + add_sol, 4)
                                await sess.commit()
                                sol = trade.paper_sol_spent  # update local var
                    except Exception:
                        pass
                    state.paper_balance -= add_sol
                    logger.info(
                        "Paper SCALE IN %s: added %.2f SOL at %.1fx | total position: %.2f SOL",
                        name, add_sol, current_mult, sol,
                    )
                    try:
                        await bot.send_message(CALLER_GROUP_ID, "\n".join([
                            f"💪 PAPER TRADE — SCALE IN",
                            f"🪙 {name} | {current_mult:.1f}x confirmed | +{add_sol} SOL added",
                            f"Total position: {sol:.2f} SOL",
                        ]), message_thread_id=SCAN_TOPIC_ID)
                    except Exception:
                        pass

            # ── Scale OUT: sell partial at milestones ────────────────────
            # 30% at 2x, 25% at 5x, trail the remaining 45%
            # remaining + realized hoisted earlier — re-read in case scale-in
            # mutated paper_sol_spent (it doesn't touch remaining_pct, but
            # safer to refresh against the row state).
            remaining = float(getattr(pt, "remaining_pct", 100) or 100)
            realized = float(getattr(pt, "realized_pnl_sol", 0) or 0)

            # Scale-out partial sells are HQ-only — subscriber relay rows
            # ride the full position to TP/SL/trail. Realized PnL on relay
            # rows would also bypass _refund_subscriber_balance.
            scale_out_done = False
            if pt.subscriber_id is not None:
                pass  # skip scale-out for subscribers
            elif remaining > 70 and current_mult >= 2.0:
                # First scale: sell 30% at 2x — recover initial + profit
                sell_pct = 30.0
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
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"📈 PAPER TRADE — SCALE OUT (30%)",
                        f"🪙 {name} | {current_mult:.1f}x | +{sell_sol:.4f} SOL",
                        f"Remaining: {new_remaining:.0f}% still running",
                    ]), message_thread_id=SCAN_TOPIC_ID)
                except Exception:
                    pass
                scale_out_done = True

            elif pt.subscriber_id is None and remaining > 40 and remaining <= 70 and current_mult >= 5.0:
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
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"📈 PAPER TRADE — SCALE OUT (30%)",
                        f"🪙 {name} | {current_mult:.1f}x | +{sell_sol:.4f} SOL",
                        f"Remaining: {new_remaining:.0f}% — trailing stop active",
                    ]), message_thread_id=SCAN_TOPIC_ID)
                except Exception:
                    pass
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
                    bot, pt, "tp_hit", pnl, peak_mc, peak_mult, [
                        f"✅ PAPER TRADE WIN",
                        f"🪙 {name} | {current_mult:.1f}x | +{pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        "Balance: {bal:.2f} SOL",
                    ],
                )
                continue

            # Trailing stop — 4am signals get wider trail + later activation
            if resolved and resolved["trail_enabled"]:
                if is_tg_signal:
                    # 4am trades: activate at 3x peak, trail width from param
                    trigger_mult = 3.0   # activate at 3x peak
                    trail_pct = float(cfg.get("tg_signal_trail_pct", 0.35) or 0.35)
                else:
                    trigger_mult = 1.0 + float(resolved["trail_trigger"])
                    trail_pct = float(resolved["trail_pct"])

                if peak_mult >= trigger_mult:
                    trail_stop_mult = peak_mult * (1.0 - trail_pct)
                    if current_mult <= trail_stop_mult:
                        remaining_sol = sol * (remaining / 100.0)
                        pnl = round(realized + remaining_sol * (current_mult - 1), 4)
                        logger.info(
                            "Paper: TRAIL hit %s — peak=%.2fx now=%.2fx pnl=%+.4f tags=%s",
                            name, peak_mult, current_mult, pnl, ",".join(tags) or "-",
                        )
                        msg_icon = "✅" if pnl > 0 else "❌"
                        await _finalize_paper_close(
                            bot, pt, "trail_hit", pnl, peak_mc, peak_mult, [
                                f"{msg_icon} PAPER TRADE — TRAIL STOP",
                                f"🪙 {name} | peak {peak_mult:.1f}x → now {current_mult:.1f}x | {pnl:+.4f} SOL",
                                f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                                "Balance: {bal:.2f} SOL",
                            ],
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
                # PnL accounts for remaining position + already realized profit
                remaining_sol = sol * (remaining / 100.0)
                loss_on_remaining = round(-remaining_sol * (1.0 - current_mult), 4)
                pnl = round(realized + loss_on_remaining, 4)
                logger.info(
                    "Paper: SL hit %s — %.2fx %.4f SOL tags=%s",
                    name, current_mult, pnl, ",".join(tags) or "-",
                )
                await _finalize_paper_close(
                    bot, pt, "sl_hit", pnl, peak_mc, peak_mult, [
                        f"❌ PAPER TRADE LOSS",
                        f"🪙 {name} | SL hit | {pnl:.4f} SOL",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        "Balance: {bal:.2f} SOL",
                    ],
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
