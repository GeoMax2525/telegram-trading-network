"""
paper_monitor.py — Paper Trade Monitor

Two jobs:
  1. Every 5 minutes: check open paper trades for TP/SL hits
  2. Every 10 minutes: track post-close performance for 24 hours
     Records mc at 1h, 6h, 24h after close.
     Flags sold_too_early (price went 50%+ higher) and
     sold_too_late (SL hit but recovered within 1h).
"""

import asyncio
import logging
from datetime import datetime, timedelta

from bot import state
from bot.config import CALLER_GROUP_ID
from bot.scanner import fetch_live_data
from database.models import (
    get_open_paper_trades,
    close_paper_trade,
    update_paper_trade_peak,
    update_paper_dead_tracking,
    get_recently_closed_paper_trades,
    update_paper_post_close,
    compute_paper_balance,
)
from bot.agents.trade_profiles import resolve_trade_params

# ── Dead-position detection thresholds ──────────────────────────────────────
ZERO_VOLUME_HOURS      = 2       # 0/null volume for 2+ hours → dead
PRICE_FLAT_HOURS       = 4       # < 2% move in 4+ hours → dead
PRICE_MOVE_THRESHOLD   = 0.02    # 2%
DEAD_LIQUIDITY_FLOOR   = 1000    # liquidity < $1K → rugged
STALE_AGE_HOURS        = 24      # age > 24h AND multi < 0.5 → dead
STALE_MULT_THRESHOLD   = 0.5

logger = logging.getLogger(__name__)

POLL_INTERVAL      = 300   # 5 minutes — open trade checks
POST_CLOSE_INTERVAL = 600  # 10 minutes — post-close tracking
STARTUP_DELAY      = 60


# ── Open trade monitoring ────────────────────────────────────────────────────

def _parse_pattern_tags(pattern_type: str | None) -> list[str]:
    """pattern_type stores a comma-separated list at open time."""
    if not pattern_type:
        return []
    return [t.strip() for t in pattern_type.split(",") if t.strip()]


async def _update_dead_tracking(pt, live: dict, current_mc: float, now: datetime) -> tuple:
    """
    Advance the dead-tracking state for this paper trade based on the
    current tick. Returns the (possibly-updated) (zero_volume_since,
    last_move_at, last_move_mc) so the caller can hand them to
    _is_dead_position without a second DB read.

    State transitions:
      - volume h1 == 0 and zero_volume_since is None  →  set it to now
      - volume h1 > 0  and zero_volume_since is set   →  clear it
      - |current_mc − last_move_mc| / last_move_mc ≥ 2%  →  bump last_move_*
      - last_move_at is None (fresh trade)             →  initialize to opened_at / entry_mc
    """
    volume_h1 = (live.get("volume") or {}).get("h1")
    zero_vol_since = pt.zero_volume_since
    last_move_at = pt.last_move_at
    last_move_mc = pt.last_move_mc

    # Initialize last_move_* for trades opened before this feature shipped
    if last_move_at is None or last_move_mc is None:
        last_move_at = pt.opened_at or now
        last_move_mc = pt.entry_mc or current_mc

    updates = {}

    # Volume tracking
    if volume_h1 is None or volume_h1 <= 0:
        if zero_vol_since is None:
            zero_vol_since = now
            updates["zero_volume_since"] = now
    else:
        if zero_vol_since is not None:
            zero_vol_since = None
            updates["zero_volume_since"] = None

    # Price movement tracking
    if last_move_mc and last_move_mc > 0:
        drift = abs(current_mc - last_move_mc) / last_move_mc
        if drift >= PRICE_MOVE_THRESHOLD:
            last_move_at = now
            last_move_mc = current_mc
            updates["last_move_at"] = now
            updates["last_move_mc"] = current_mc

    # Persist anything that actually changed; also persist the initialization
    # if last_move_at came in None from the DB
    if pt.last_move_at is None:
        updates.setdefault("last_move_at", last_move_at)
        updates.setdefault("last_move_mc", last_move_mc)

    if updates:
        await update_paper_dead_tracking(pt.id, **updates)

    return zero_vol_since, last_move_at, last_move_mc


def _check_dead(
    pt,
    live: dict,
    current_mc: float,
    zero_vol_since: datetime | None,
    last_move_at: datetime | None,
    now: datetime,
) -> tuple[bool, str]:
    """
    Returns (is_dead, reason_label). Uses pre-computed tracking state so
    we don't re-read from the DB. Rules match the user spec exactly.
    """
    # Rule 3: liquidity rug (cheapest check first)
    liquidity = live.get("liquidity_usd") or 0
    if liquidity < DEAD_LIQUIDITY_FLOOR:
        return True, f"liquidity ${liquidity:.0f}"

    # Rule 4: stale age with sub-0.5x multiplier
    opened = pt.opened_at
    entry_mc = pt.entry_mc or 1
    if opened:
        age_hours = (now - opened).total_seconds() / 3600
        mult = current_mc / entry_mc if entry_mc > 0 else 0
        if age_hours >= STALE_AGE_HOURS and mult < STALE_MULT_THRESHOLD:
            return True, f"stale {mult:.2f}x @ {age_hours:.0f}h"

    # Rule 1: no volume for 2+ hours
    if zero_vol_since is not None:
        hours_zero = (now - zero_vol_since).total_seconds() / 3600
        if hours_zero >= ZERO_VOLUME_HOURS:
            return True, "no volume 2h+"

    # Rule 2: price flat (< 2% move) for 4+ hours
    if last_move_at is not None:
        hours_flat = (now - last_move_at).total_seconds() / 3600
        if hours_flat >= PRICE_FLAT_HOURS:
            return True, f"flat {hours_flat:.0f}h"

    return False, ""


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

    for pt in trades:
        try:
            live = await fetch_live_data(pt.token_address)
            if not live:
                continue

            current_mc = live.get("market_cap") or 0
            if current_mc <= 0:
                continue

            entry_mc = pt.entry_mc or 1
            current_mult = current_mc / entry_mc if entry_mc > 0 else 1.0
            peak_mc = max(pt.peak_mc or 0, current_mc)
            peak_mult = max(pt.peak_multiple or 1.0, current_mult)

            await update_paper_trade_peak(pt.id, current_mc, peak_mc, peak_mult)

            name = (pt.token_name or "Unknown").replace("_", " ")
            sol = pt.paper_sol_spent

            # Resolve trailing-stop config at tick time so Agent 6 adjustments
            # take effect on open trades mid-flight. Cheap: one DB round-trip
            # per open trade per 5 min.
            tags = _parse_pattern_tags(pt.pattern_type)
            resolved = await resolve_trade_params(tags) if tags else None

            # Advance dead-tracking state and check before TP/SL so a dead
            # token doesn't accidentally trigger an SL at the drift floor.
            now = datetime.utcnow()
            zero_vol_since, last_move_at, _last_mc = await _update_dead_tracking(
                pt, live, current_mc, now,
            )
            is_dead, dead_reason = _check_dead(
                pt, live, current_mc, zero_vol_since, last_move_at, now,
            )
            if is_dead:
                pnl = round(sol * (current_mult - 1), 4)
                await close_paper_trade(pt.id, "dead_token", pnl, peak_mc, peak_mult)
                bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_balance = bal
                loss_pct = (current_mult - 1) * 100
                logger.info(
                    "Paper: DEAD %s — %s %.2fx %+.4f SOL bal=%.4f tags=%s",
                    name, dead_reason, current_mult, pnl, bal, ",".join(tags) or "-",
                )
                try:
                    await bot.send_message(CALLER_GROUP_ID, "\n".join([
                        f"💀 DEAD POSITION CLOSED",
                        f"🪙 ${name} | {dead_reason} | {loss_pct:+.0f}%",
                        f"MC: ${entry_mc/1000:.0f}K → ${current_mc/1000:.0f}K",
                        f"PnL: {pnl:+.4f} SOL | Balance: {bal:.2f} SOL",
                    ]))
                except Exception:
                    pass
                continue

            # Check TP
            if current_mult >= pt.take_profit_x:
                pnl = round(sol * (current_mult - 1), 4)
                await close_paper_trade(pt.id, "tp_hit", pnl, peak_mc, peak_mult)
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
                    ]))
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
                        await close_paper_trade(pt.id, "trail_hit", pnl, peak_mc, peak_mult)
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
                            ]))
                        except Exception:
                            pass
                        continue

            # Check fixed SL
            sl_threshold = 1.0 - (pt.stop_loss_pct / 100.0)
            if current_mult <= sl_threshold:
                pnl = round(-sol * (1.0 - current_mult), 4)
                await close_paper_trade(pt.id, "sl_hit", pnl, peak_mc, peak_mult)
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
                    ]))
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
