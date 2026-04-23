"""
learning_loop.py — Agent 6: The Learning Loop (Autonomous)

Fully autonomous self-adjusting system:
  - Every 10 closed trades: micro-adjust confidence weights
  - Every 50 closed trades: full parameter review + TP/SL optimization
  - Every 100 trades: major strategy recalibration

Self-adjusting parameters:
  - Confidence thresholds (execute_full, execute_half, monitor)
  - Position sizing per pattern type
  - TP/SL per pattern type
  - Market regime detection (GOOD / NEUTRAL / BAD)

Safety:
  - 10 consecutive SL hits → pause autotrade, alert admin
  - SOL down 8%+ → halve position sizes
  - Win rate < 40% for 20 trades → alert admin and pause

Posts to Callers HQ when parameters change significantly.
Weekly performance report every Monday 9am UTC.
"""

import asyncio
import logging
import random
import re
from datetime import datetime, timedelta

import aiohttp

from bot import state
from bot.config import CALLER_GROUP_ID
from bot.agents.trade_profiles import ALL_PATTERN_TYPES as _ALL_PATTERN_TYPES
from database.models import (
    log_agent_run,
    get_current_weights,
    save_weights,
    get_closed_positions_since,
    get_total_closed_count,
    get_candidate_by_token,
    update_wallet_tier,
    get_tier_wallets,
    get_weekly_performance,
    get_closed_positions_by_source,
    upsert_trade_params,
    get_trade_params,
    get_all_trade_params,
    Wallet,
    AsyncSessionLocal,
    select,
    Position,
    func,
    get_param,
    set_param,
    get_all_params,
    get_recent_param_changes,
    get_post_close_stats,
    MC_WEIGHT_DEFAULTS,
    MC_WEIGHT_DRIFT_HIGH,
    MC_WEIGHT_DRIFT_LOW,
    get_paper_trade_stats,
    get_token_count,
    get_top_wallets,
    PaperTrade,
    Candidate,
    AgentLog,
    Wallet as WalletModel,
    ParamChange,
)

logger = logging.getLogger(__name__)

# Intervals
MICRO_BATCH      = 10    # micro-adjust every 10 trades (stabilized — was 3)
FULL_BATCH       = 25    # full review every 25 trades
MAJOR_BATCH      = 50    # major recalibration every 50 trades
POLL_INTERVAL    = 60    # check every 1 minute (chaos: was 2 min)
STARTUP_DELAY    = 15    # lowered so heartbeat shows up fast in Railway logs
MAX_WEIGHT_SHIFT = 0.15  # raised from 0.10 (batch sizes increased)
MIN_WEIGHT       = 0.02
WEEKLY_HOUR      = 9

# ── Dynamic learning mode ───────────────────────────────────────────────────
# When effective WR tanks, Agent 6 switches into "aggressive" mode: smaller
# micro batches (tune faster) and a larger weight-shift cap (tune harder).
# Hysteresis prevents flapping between modes on every cycle.
AGGRESSIVE_WR_ENTER      = 0.40   # effective_wr below this → go aggressive
AGGRESSIVE_WR_EXIT       = 0.50   # effective_wr at/above this → back to normal
AGGRESSIVE_MICRO_BATCH   = 2      # vs normal MICRO_BATCH=3
AGGRESSIVE_MAX_SHIFT     = 0.30   # vs normal MAX_WEIGHT_SHIFT=0.15
NORMAL_MICRO_SHIFT_RATIO = 0.20   # micro-level shift = this * max_shift

# Default weights
DEFAULT_WEIGHTS = {
    "fingerprint": 0.25,
    "insider":     0.25,
    "chart":       0.20,
    "rug":         0.15,
    "caller":      0.10,
    "market":      0.05,
}

# Default thresholds (stored in state, adjusted dynamically)
DEFAULT_THRESHOLDS = {
    "execute_full": 80,
    "execute_half": 70,
    "monitor":      60,
}

COMPONENT_KEYS = ["fingerprint", "insider", "chart", "rug", "caller", "market"]
SCORE_FIELDS = {
    "fingerprint": "fingerprint_score",
    "insider":     "insider_score",
    "chart":       "chart_score",
    "rug":         "rug_score",
    "caller":      "caller_score",
    "market":      "market_score",
}

# Full set of pattern_types Agent 6 learns per. Re-exported from
# trade_profiles so there's exactly one place to edit when adding more.
PATTERN_TYPES = _ALL_PATTERN_TYPES
MIN_SAMPLE_FOR_LEARNING = 3   # lowered from 5 — start learning faster
DEFAULT_TP_X = 5.0
DEFAULT_SL_PCT = 25.0
DEFAULT_POSITION_PCT = 10.0

# Hard bounds for Agent 6 learning adjustments. SL_FLOOR was 10.0
# which was too tight — pump.fun tokens routinely wiggle 15-30% on
# normal price action, so 10% SL guaranteed insta-stopout on every
# trade. Raised to 20.0 so trades get meaningful room to breathe
# while still capping downside at a sane loss.
SL_FLOOR = 15.0
SL_CEILING = 50.0
TP_FLOOR = 2.0
TP_CEILING = 20.0

SOL_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd&include_24hr_change=true"


# ── Market regime detection ──────────────────────────────────────────────────

async def _detect_market_regime() -> str:
    """
    Returns market regime: GOOD / NEUTRAL / BAD.
    Based on SOL 24h change and recent trade outcomes.
    """
    # Check SOL price change
    sol_change = 0.0
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async with session.get(SOL_PRICE_URL) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    sol_change = data.get("solana", {}).get("usd_24h_change", 0) or 0
    except Exception as exc:
        logger.warning("SOL price fetch failed: %s", exc)

    # Check recent trade win rate (last 20)
    recent_wr = await _recent_win_rate(20)

    # Determine regime
    if sol_change <= -8 or recent_wr < 0.30:
        regime = "BAD"
    elif sol_change <= -4 or recent_wr < 0.45:
        regime = "NEUTRAL"
    else:
        regime = "GOOD"

    state.market_regime = regime
    state.sol_24h_change = round(sol_change, 1)
    return regime


async def _recent_win_rate(n: int) -> float:
    """Win rate of the last N closed trades (paper trades + real positions)."""
    async with AsyncSessionLocal() as session:
        paper = (await session.execute(
            select(PaperTrade)
            .where(PaperTrade.status == "closed", PaperTrade.paper_pnl_sol.is_not(None))
            .order_by(PaperTrade.closed_at.desc())
            .limit(n)
        )).scalars().all()
        real = (await session.execute(
            select(Position)
            .where(Position.status == "closed", Position.pnl_sol.is_not(None))
            .order_by(Position.closed_at.desc())
            .limit(n)
        )).scalars().all()

    combined = list(paper) + list(real)
    combined.sort(key=lambda t: t.closed_at or datetime.min, reverse=True)
    combined = combined[:n]
    if not combined:
        return 0.5  # neutral if no data
    wins = sum(1 for t in combined if (t.pnl_sol or 0) > 0)
    return wins / len(combined)


async def _alltime_win_rate() -> float:
    """All-time win rate across every closed paper trade + real position.

    Used as an anchor so Agent 6 doesn't overreact to a bad recent streak.
    """
    async with AsyncSessionLocal() as session:
        paper_total = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalar() or 0
        paper_wins = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol > 0,
            )
        )).scalar() or 0
        real_total = (await session.execute(
            select(func.count(Position.id)).where(
                Position.status == "closed",
                Position.pnl_sol.is_not(None),
            )
        )).scalar() or 0
        real_wins = (await session.execute(
            select(func.count(Position.id)).where(
                Position.status == "closed",
                Position.pnl_sol > 0,
            )
        )).scalar() or 0

    total = paper_total + real_total
    if total == 0:
        return 0.5
    return (paper_wins + real_wins) / total


def _compute_effective_wr(recent_wr: float, alltime_wr: float) -> float:
    """
    Blended win rate: weights recent performance 60% / all-time 40%.
    This dampens overreaction to short bad streaks when the long-term
    trajectory is still healthy.
    """
    return (recent_wr * 0.6) + (alltime_wr * 0.4)


def _learning_mode(effective_wr: float) -> tuple[str, int, float]:
    """
    Returns (mode_name, micro_batch, max_weight_shift) for the current cycle.

    Hysteresis band:
      - effective_wr < AGGRESSIVE_WR_ENTER (0.40) → aggressive mode
      - effective_wr ≥ AGGRESSIVE_WR_EXIT  (0.50) → normal mode
      - between 0.40 and 0.50 → stay in whatever mode was active last cycle

    Logs mode transitions so they're visible in Railway. Self-restores to
    normal as soon as WR climbs back through the exit band.
    """
    prev = getattr(state, "learning_loop_mode", "normal")

    if effective_wr < AGGRESSIVE_WR_ENTER:
        mode = "aggressive"
    elif effective_wr >= AGGRESSIVE_WR_EXIT:
        mode = "normal"
    else:
        mode = prev  # inside hysteresis band — keep current mode

    if mode != prev:
        logger.warning(
            "Agent6: learning mode %s → %s (effective_wr=%.2f)",
            prev, mode, effective_wr,
        )

    state.learning_loop_mode = mode

    if mode == "aggressive":
        return "aggressive", AGGRESSIVE_MICRO_BATCH, AGGRESSIVE_MAX_SHIFT
    return "normal", MICRO_BATCH, MAX_WEIGHT_SHIFT


async def _consecutive_sl_count() -> int:
    """Count consecutive SL hits from most recent trades (paper + real)."""
    async with AsyncSessionLocal() as session:
        paper = (await session.execute(
            select(PaperTrade)
            .where(PaperTrade.status == "closed", PaperTrade.paper_pnl_sol.is_not(None))
            .order_by(PaperTrade.closed_at.desc())
            .limit(20)
        )).scalars().all()
        real = (await session.execute(
            select(Position)
            .where(Position.status == "closed", Position.pnl_sol.is_not(None))
            .order_by(Position.closed_at.desc())
            .limit(20)
        )).scalars().all()

    combined = list(paper) + list(real)
    combined.sort(key=lambda t: t.closed_at or datetime.min, reverse=True)

    count = 0
    for t in combined[:20]:
        if t.close_reason == "sl_hit":
            count += 1
        else:
            break
    return count


# ── Threshold adjustment ─────────────────────────────────────────────────────

def _adjust_thresholds(win_rate_20: float, current: dict) -> tuple[dict, list[str]]:
    """
    Adjusts confidence thresholds based on recent win rate.
    Returns (new_thresholds, list_of_changes).
    """
    new = dict(current)
    changes = []

    if win_rate_20 > 0.70:
        # Strong performance → lower thresholds to catch more
        target_full = max(72, current["execute_full"] - 3)
        target_half = max(65, current["execute_half"] - 3)
        reason = "Strong win rate, capturing more opportunities"
    elif win_rate_20 > 0.55:
        # Good performance → slight easing
        target_full = max(75, current["execute_full"] - 1)
        target_half = max(67, current["execute_half"] - 1)
        reason = "Good performance, minor threshold easing"
    elif win_rate_20 < 0.40:
        # Poor performance → raise thresholds
        target_full = min(90, current["execute_full"] + 3)
        target_half = min(82, current["execute_half"] + 3)
        reason = "Poor win rate, tightening entry criteria"
    elif win_rate_20 < 0.50:
        # Below average → slight tightening
        target_full = min(85, current["execute_full"] + 1)
        target_half = min(78, current["execute_half"] + 1)
        reason = "Below average, minor threshold increase"
    else:
        return new, []

    if target_full != current["execute_full"]:
        changes.append(f"execute_full: {current['execute_full']}->{target_full}")
        new["execute_full"] = target_full
    if target_half != current["execute_half"]:
        changes.append(f"execute_half: {current['execute_half']}->{target_half}")
        new["execute_half"] = target_half
    new["monitor"] = max(55, new["execute_half"] - 10)

    if changes:
        changes.append(f"Reason: {reason}")

    return new, changes


# ── Weight adjustment ────────────────────────────────────────────────────────

def _compute_weight_adjustments(
    winners: list[dict],
    losers: list[dict],
    current: dict[str, float],
    max_shift: float = MAX_WEIGHT_SHIFT,
) -> dict[str, float]:
    if not winners and not losers:
        return current

    def _avg(trades):
        if not trades:
            return {k: 50.0 for k in COMPONENT_KEYS}
        totals = {k: 0.0 for k in COMPONENT_KEYS}
        for t in trades:
            for k in COMPONENT_KEYS:
                totals[k] += t.get(SCORE_FIELDS[k], 50.0)
        return {k: totals[k] / len(trades) for k in COMPONENT_KEYS}

    win_avg = _avg(winners)
    loss_avg = _avg(losers)
    new = dict(current)

    for key in COMPONENT_KEYS:
        diff = win_avg[key] - loss_avg[key]
        shift = max(-max_shift, min(max_shift, diff / 1000.0))
        new[key] = max(MIN_WEIGHT, new[key] + shift)

    total = sum(new.values())
    if total > 0:
        new = {k: round(v / total, 4) for k, v in new.items()}
    return new


# ── Wallet tier adjustments ──────────────────────────────────────────────────

async def _adjust_wallet_tiers() -> tuple[int, int]:
    """
    Demotion rules based on recent win rate:
      Tier 1 → Tier 2 if WR < 50%
      Tier 2 → Tier 3 if WR < 35%
      Tier 3 → Ignore if WR < 25%
    Promotion: only if wallet meets full tier criteria (handled by wallet_analyst).
    """
    wallets = await get_tier_wallets(max_tier=3)
    promoted = demoted = 0
    for w in wallets:
        if w.total_trades < 2:
            continue

        # Demotions
        if w.tier == 1 and w.win_rate < 0.50:
            await update_wallet_tier(w.address, 2)
            demoted += 1
            logger.info("Agent6: demoted %s..%s T1→T2 (wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.win_rate * 100)
        elif w.tier == 2 and w.win_rate < 0.35:
            await update_wallet_tier(w.address, 3)
            demoted += 1
            logger.info("Agent6: demoted %s..%s T2→T3 (wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.win_rate * 100)
        elif w.tier == 3 and w.win_rate < 0.25:
            await update_wallet_tier(w.address, 0)
            demoted += 1
            logger.info("Agent6: demoted %s..%s T3→ignore (wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.win_rate * 100)

        # Promotions (must meet full criteria)
        elif w.tier == 2 and w.score >= 80 and w.win_rate >= 0.65 and w.avg_multiple >= 2.0 and w.total_trades >= 5:
            await update_wallet_tier(w.address, 1)
            promoted += 1
            logger.info("Agent6: promoted %s..%s T2→T1 (score=%.0f wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.score, w.win_rate * 100)
        elif w.tier == 3 and w.score >= 60 and w.win_rate >= 0.45 and w.avg_multiple >= 1.5 and w.total_trades >= 3:
            await update_wallet_tier(w.address, 2)
            promoted += 1
            logger.info("Agent6: promoted %s..%s T3→T2 (score=%.0f wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.score, w.win_rate * 100)

    return promoted, demoted


# ── Trade params optimization (per pattern_type, from PaperTrade) ────────────

from bot.agents.trade_profiles import parse_pattern_tags as _parse_pattern_tags


async def _fetch_closed_paper_trades(limit: int = 500) -> list:
    """All closed PaperTrade rows with PnL, newest first."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
            .order_by(PaperTrade.closed_at.desc())
            .limit(limit)
        )).scalars().all()
    return list(rows)


async def _optimize_trade_params(regime: str) -> int:
    """
    Learn per-pattern_type TP and SL from closed PaperTrade outcomes.

    STRATEGY CLOSES ONLY: trades closed via manual_close (the /hub Close
    All button) or reset (/resetbalance) are excluded from learning.
    Those are human decisions and don't represent outcomes the strategy
    could have produced on its own — counting a 14x manual_close as a
    "win" would teach Agent 6 to expect 14x wins that the strategy
    can't actually deliver without human intervention.

    Each surviving closed PaperTrade is assigned to every pattern_type
    it matched at entry (comma-separated pattern_type column → split).
    For each group with >= MIN_SAMPLE_FOR_LEARNING samples we analyze
    four signals and adjust the row in ai_trade_params.

    Signal → action rules (applied independently, clamped per step):
      1. sl_hit_rate > 0.50            → tighten sl_pct by -5 points
      2. peak_exceeds_tp_rate > 0.60   → raise tp_x by +0.5
      3. avg_peak_mult < current_tp*0.80 → lower tp_x by -0.3
      4. avg(mc_1h_after / close_mc) > 1.20 → raise tp_x by +0.3
                                              (price kept running — we
                                              sold too early)

    Hard bounds: tp_x ∈ [1.5, 10.0], sl_pct ∈ [10.0, 50.0].
    """
    raw_all = await _fetch_closed_paper_trades(limit=500)
    if not raw_all:
        return 0
    # Filter out meta closes (manual_close, reset) — human decisions
    # that don't represent strategy outcomes
    from database.models import STRATEGY_CLOSE_REASONS
    all_trades = [
        t for t in raw_all
        if (t.close_reason or "") in STRATEGY_CLOSE_REASONS
    ]
    meta_excluded = len(raw_all) - len(all_trades)
    if meta_excluded:
        logger.info(
            "Agent6 tune: excluded %d meta closes (manual/reset) from learning",
            meta_excluded,
        )
    if not all_trades:
        return 0

    # Bucket trades by pattern_type. One trade contributes to every tag it matched.
    groups: dict[str, list] = {ptype: [] for ptype in PATTERN_TYPES}
    for pt in all_trades:
        for tag in _parse_pattern_tags(pt.pattern_type):
            if tag in groups:
                groups[tag].append(pt)

    updated = 0
    skipped_low_sample: list[tuple[str, int]] = []

    for ptype, trades in groups.items():
        # dead_token is an outcome tag, not an entry tag — handled below
        if ptype == "dead_token":
            continue
        if len(trades) < MIN_SAMPLE_FOR_LEARNING:
            if len(trades) > 0:
                skipped_low_sample.append((ptype, len(trades)))
            continue

        # Load current row so we know what to compare the signals against
        current = await get_trade_params(ptype)
        cur_tp  = float(current.optimal_tp_x)  if current else DEFAULT_TP_X
        cur_sl  = float(current.optimal_sl_pct) if current else DEFAULT_SL_PCT

        n = len(trades)
        sl_hits   = sum(1 for t in trades if t.close_reason == "sl_hit")
        tp_hits   = sum(1 for t in trades if t.close_reason == "tp_hit")
        dead_hits = sum(1 for t in trades if t.close_reason == "dead_token")
        wins    = sum(1 for t in trades if (t.paper_pnl_sol or 0) > 0)
        win_rate = wins / n

        peaks = [float(t.peak_multiple or 1.0) for t in trades]
        avg_peak = sum(peaks) / len(peaks) if peaks else 1.0
        peak_exceeds_tp = sum(1 for p in peaks if p > cur_tp) / n

        sl_hit_rate   = sl_hits / n
        dead_hit_rate = dead_hits / n

        # Sold-too-early signal: across trades with a 1h-after snapshot,
        # did price trend higher than exit?
        runup_samples: list[float] = []
        for t in trades:
            mc_1h = getattr(t, "mc_1h_after", None)
            close_mc = t.peak_mc or t.entry_mc or 0  # approximate close MC
            if mc_1h and close_mc and close_mc > 0:
                runup_samples.append(mc_1h / close_mc)
        avg_runup = sum(runup_samples) / len(runup_samples) if runup_samples else 1.0

        # Full-fat diagnostic log — shows exactly what the calculator
        # saw for this pattern_type group on this cycle
        logger.info(
            "Agent6 tune %s: n=%d wins=%d sl=%d tp=%d dead=%d "
            "sl_rate=%.0f%% dead_rate=%.0f%% avg_peak=%.2fx "
            "peak>tp=%.0f%% 1h_runup=%.2fx cur_tp=%.2fx cur_sl=%.0f%%",
            ptype, n, wins, sl_hits, tp_hits, dead_hits,
            sl_hit_rate * 100, dead_hit_rate * 100, avg_peak,
            peak_exceeds_tp * 100, avg_runup, cur_tp, cur_sl,
        )

        # Apply adjustment rules — thresholds loosened so learning actually
        # fires with real trade data instead of requiring extreme distributions
        new_tp = cur_tp
        new_sl = cur_sl
        reasons: list[str] = []

        # SL getting hit too often — tighten so we exit faster
        if sl_hit_rate > 0.35:
            new_sl = max(SL_FLOOR, cur_sl - 5.0)
            reasons.append(f"sl_hit_rate={sl_hit_rate:.0%}>35% → tighten SL")

        # Dead hits: token data feed died (rug/delist)
        if dead_hit_rate > 0.25:
            new_sl = max(SL_FLOOR, new_sl - 3.0)
            reasons.append(f"dead_hit_rate={dead_hit_rate:.0%}>25% → tighten SL")

        # Peaks exceeding TP — raise TP to capture more upside
        if peak_exceeds_tp > 0.30:
            step = 1.0 if peak_exceeds_tp > 0.50 else 0.5
            new_tp = min(TP_CEILING, new_tp + step)
            reasons.append(f"peak>tp in {peak_exceeds_tp:.0%} → raise TP +{step}")

        # Average peak well below TP — lower TP to actually hit it
        if avg_peak < cur_tp * 0.60:
            new_tp = max(TP_FLOOR, new_tp - 0.5)
            reasons.append(f"avg_peak={avg_peak:.2f}x<tp*0.6 → lower TP")

        # Sold too early — price kept running after we exited
        if avg_runup > 1.15 and runup_samples:
            new_tp = min(TP_CEILING, new_tp + 0.5)
            reasons.append(f"1h_runup={avg_runup:.2f}x (sold early)")

        # Post-close signals: sold_too_early / sold_too_late from paper monitor
        early_count = sum(1 for t in trades if getattr(t, "sold_too_early", False))
        late_count = sum(1 for t in trades if getattr(t, "sold_too_late", False))
        early_rate = early_count / n
        late_rate = late_count / n

        if early_rate > 0.15:
            new_tp = min(TP_CEILING, new_tp + 0.8)
            reasons.append(f"sold_too_early={early_rate:.0%} → raise TP")

        if late_rate > 0.20:
            new_sl = min(SL_CEILING, new_sl + 5.0)
            reasons.append(f"sold_too_late={late_rate:.0%} → widen SL")

        # Clamp
        new_tp = round(max(TP_FLOOR, min(TP_CEILING, new_tp)), 2)
        new_sl = round(max(SL_FLOOR, min(SL_CEILING, new_sl)), 1)

        changed = (abs(new_tp - cur_tp) >= 0.01) or (abs(new_sl - cur_sl) >= 0.1)

        # Position sizing follows win_rate + regime
        if win_rate >= 0.70:
            optimal_pos = 15.0
        elif win_rate >= 0.55:
            optimal_pos = 12.0
        elif win_rate >= 0.40:
            optimal_pos = 10.0
        else:
            optimal_pos = 7.0
        if regime == "BAD":
            optimal_pos = round(optimal_pos * 0.5, 1)
        elif regime == "NEUTRAL":
            optimal_pos = round(optimal_pos * 0.75, 1)

        # avg_multiple from PnL
        mults = []
        for t in trades:
            if t.paper_sol_spent and t.paper_sol_spent > 0 and t.paper_pnl_sol is not None:
                mults.append(max(0.1, 1.0 + t.paper_pnl_sol / max(t.paper_sol_spent, 0.01)))
        avg_multiple = sum(mults) / len(mults) if mults else 1.0
        confidence = min(100.0, n * 2.0)

        # ALWAYS write back — even when rules don't fire. This updates
        # sample_size so the resolver treats the row as "trained" and
        # position sizing / win_rate stay current.
        await upsert_trade_params(
            pattern_type=ptype,
            optimal_tp_x=new_tp,
            optimal_sl_pct=new_sl,
            optimal_position_pct=optimal_pos,
            sample_size=n,
            win_rate=round(win_rate, 4),
            avg_multiple=round(avg_multiple, 2),
            confidence=round(confidence, 1),
        )

        reason_str = "; ".join(reasons) or "no rule fired — stats refreshed"
        if changed:
            # Log param changes for audit trail
            if abs(new_tp - cur_tp) >= 0.01:
                await set_param(
                    f"ai_trade_params.{ptype}.optimal_tp_x", new_tp,
                    reason=f"[{ptype}] {reason_str}", trades=n, win_rate=win_rate,
                )
            if abs(new_sl - cur_sl) >= 0.1:
                await set_param(
                    f"ai_trade_params.{ptype}.optimal_sl_pct", new_sl,
                    reason=f"[{ptype}] {reason_str}", trades=n, win_rate=win_rate,
                )
            logger.info(
                "Agent6 tune %s: tp %.2f->%.2f sl %.1f->%.1f n=%d wr=%.0f%% avg_peak=%.2f [%s]",
                ptype, cur_tp, new_tp, cur_sl, new_sl, n, win_rate * 100, avg_peak, reason_str,
            )
        else:
            logger.info(
                "Agent6 tune %s: stats refreshed (tp=%.2f sl=%.0f n=%d wr=%.0f%% avg_peak=%.2f)",
                ptype, cur_tp, cur_sl, n, win_rate * 100, avg_peak,
            )

        updated += 1

    # Summary of pattern groups that don't have enough samples yet — useful
    # to see "low_mc needs 3 more trades" / "insider_wallet has 12 already"
    # kind of visibility
    if skipped_low_sample:
        summary = ", ".join(f"{p}={n}" for p, n in sorted(skipped_low_sample)[:10])
        logger.info(
            "Agent6 tune: %d groups below MIN_SAMPLE_FOR_LEARNING=%d — %s",
            len(skipped_low_sample), MIN_SAMPLE_FOR_LEARNING, summary,
        )

    # ── dead_token outcome row ──────────────────────────────────────
    # Purely a stats/reporting row: aggregates every paper trade that
    # closed with close_reason="dead_token" regardless of its entry tags.
    # We update sample_size + win_rate + avg_multiple so /params shows
    # the death count. TP/SL are left at defaults — they're not used
    # for entry (resolver never sees dead_token in the matched list).
    dead_trades = [t for t in all_trades if t.close_reason == "dead_token"]
    if dead_trades:
        dn = len(dead_trades)
        dead_wins = sum(1 for t in dead_trades if (t.paper_pnl_sol or 0) > 0)
        dead_mults = []
        for t in dead_trades:
            if t.paper_sol_spent and t.paper_sol_spent > 0 and t.paper_pnl_sol is not None:
                dead_mults.append(max(0.1, 1.0 + t.paper_pnl_sol / max(t.paper_sol_spent, 0.01)))
        dead_avg_mult = sum(dead_mults) / len(dead_mults) if dead_mults else 1.0
        await upsert_trade_params(
            pattern_type="dead_token",
            optimal_tp_x=DEFAULT_TP_X,
            optimal_sl_pct=DEFAULT_SL_PCT,
            optimal_position_pct=DEFAULT_POSITION_PCT,
            sample_size=dn,
            win_rate=round(dead_wins / dn, 4),
            avg_multiple=round(dead_avg_mult, 2),
            confidence=min(100.0, dn * 2.0),
        )
        logger.info(
            "Agent6: dead_token stats updated — n=%d avg_mult=%.2fx",
            dn, dead_avg_mult,
        )

    return updated


# ── Scanner param auto-tuning ────────────────────────────────────────────────

MIN_SCANNER_TUNE_SAMPLE = 10   # need at least N recent strategy closes

async def _optimize_scanner_params(regime: str) -> list[str]:
    """
    Read recent strategy-closed trades and adjust scanner gate params
    (scanner_min_mc, scanner_max_mc, scanner_min_liquidity) based on
    where wins vs losses cluster.

    Runs every learning loop tick (same cadence as _optimize_trade_params).
    Returns list of change description strings for the log.
    """
    from database.models import STRATEGY_CLOSE_REASONS

    raw = await _fetch_closed_paper_trades(limit=200)
    strat = [t for t in raw if (t.close_reason or "") in STRATEGY_CLOSE_REASONS]
    if len(strat) < MIN_SCANNER_TUNE_SAMPLE:
        return []

    changes: list[str] = []
    cur_min_mc  = await get_param("scanner_min_mc")
    cur_max_mc  = await get_param("scanner_max_mc")
    cur_min_liq = await get_param("scanner_min_liquidity")
    n_analyzed  = len(strat)

    # Bucket by entry_mc: low (<50K), mid (50K-500K), high (>500K)
    low = [t for t in strat if (t.entry_mc or 0) < 50_000]
    mid = [t for t in strat if 50_000 <= (t.entry_mc or 0) < 500_000]
    high = [t for t in strat if (t.entry_mc or 0) >= 500_000]

    def wr(bucket):
        if not bucket:
            return 0.0
        wins = sum(1 for t in bucket if (t.paper_pnl_sol or 0) > 0)
        return wins / len(bucket)

    wr_low  = wr(low)
    wr_mid  = wr(mid)
    wr_high = wr(high)
    wr_all  = wr(strat)

    logger.info(
        "Agent6 scanner-tune: n=%d low=%d(%.0f%%) mid=%d(%.0f%%) high=%d(%.0f%%) overall=%.0f%%",
        n_analyzed, len(low), wr_low*100, len(mid), wr_mid*100,
        len(high), wr_high*100, wr_all*100,
    )

    # Rule 1: if low-MC bucket has poor WR and enough samples → raise min_mc
    if len(low) >= 5 and wr_low < 0.15 and wr_low < wr_all * 0.7:
        new_min = min(cur_min_mc * 1.25, 100_000)
        if new_min != cur_min_mc:
            await set_param("scanner_min_mc", round(new_min),
                            f"Low-MC WR {wr_low:.0%} < overall {wr_all:.0%} — raising floor",
                            n_analyzed, wr_all)
            changes.append(f"scanner_min_mc: {cur_min_mc:.0f}→{new_min:.0f}")

    # Rule 2: if low-MC is performing well → lower min_mc to see more
    if len(low) >= 5 and wr_low > 0.35 and wr_low > wr_all * 1.2:
        new_min = max(cur_min_mc * 0.80, 5_000)
        if new_min != cur_min_mc:
            await set_param("scanner_min_mc", round(new_min),
                            f"Low-MC WR {wr_low:.0%} outperforming — widening floor",
                            n_analyzed, wr_all)
            changes.append(f"scanner_min_mc: {cur_min_mc:.0f}→{new_min:.0f}")

    # Rule 3: if high-MC performing well → raise ceiling to see more
    if len(high) >= 3 and wr_high > 0.30:
        new_max = min(cur_max_mc * 1.5, 50_000_000)
        if new_max != cur_max_mc:
            await set_param("scanner_max_mc", round(new_max),
                            f"High-MC WR {wr_high:.0%} — raising ceiling",
                            n_analyzed, wr_all)
            changes.append(f"scanner_max_mc: {cur_max_mc:.0f}→{new_max:.0f}")

    # Rule 4: if most losses come from low-liq tokens → raise min_liquidity
    losses = [t for t in strat if (t.paper_pnl_sol or 0) <= 0]
    if losses:
        loss_mcs = [t.entry_mc or 0 for t in losses]
        avg_loss_mc = sum(loss_mcs) / len(loss_mcs)
        win_trades = [t for t in strat if (t.paper_pnl_sol or 0) > 0]
        win_mcs = [t.entry_mc or 0 for t in win_trades] if win_trades else [0]
        avg_win_mc = sum(win_mcs) / max(len(win_mcs), 1)

        # If losses cluster at much lower MC than wins → raise min
        if avg_loss_mc > 0 and avg_win_mc > 0 and avg_loss_mc < avg_win_mc * 0.5:
            new_liq = min(cur_min_liq * 1.20, 25_000)
            if new_liq != cur_min_liq:
                await set_param("scanner_min_liquidity", round(new_liq),
                                f"Loss avg MC ${avg_loss_mc:.0f} << Win avg MC ${avg_win_mc:.0f}",
                                n_analyzed, wr_all)
                changes.append(f"scanner_min_liq: {cur_min_liq:.0f}→{new_liq:.0f}")

    return changes


# ── Collect scores from batch ────────────────────────────────────────────────

async def _classify_batch(batch) -> tuple[list[dict], list[dict]]:
    """
    Returns (winners_scores, losers_scores).

    Works for both Position and PaperTrade instances (PaperTrade exposes a
    pnl_sol property alias). If a trade has no matching Candidate row
    (common for paper trades), component scores default to 50 so the trade
    still contributes a classification signal rather than being dropped.
    """
    winners, losers = [], []
    for pos in batch:
        candidate = await get_candidate_by_token(pos.token_address)
        if candidate is not None:
            scores = {
                SCORE_FIELDS[k]: getattr(candidate, SCORE_FIELDS[k], None) or 50.0
                for k in COMPONENT_KEYS
            }
        else:
            scores = {SCORE_FIELDS[k]: 50.0 for k in COMPONENT_KEYS}

        pnl = pos.pnl_sol
        if pnl is not None and pnl > 0:
            winners.append(scores)
        else:
            losers.append(scores)
    return winners, losers


# ── Notify Callers HQ ────────────────────────────────────────────────────────

# Minimum deltas that qualify as a "major" adjustment worth announcing
MAJOR_WEIGHT_SHIFT     = 0.05   # absolute weight delta
MAJOR_THRESHOLD_SHIFT  = 3      # integer points on any threshold / conf param
NOTIFY_COOLDOWN_SEC    = 3600   # max 1 notification per hour even if major

# Matches numeric deltas in change strings like
#   "conf threshold: 80→83"   "execute_full: 80->83"   "size_80: 18%→21%"
#   "min_mc: $5000→$4000"
_DELTA_RE = re.compile(r'(\d+(?:\.\d+)?)\s*%?\s*(?:->|\u2192)\s*\$?\s*(\d+(?:\.\d+)?)')


def _is_major_change(
    all_changes: list[str],
    current_weights: dict[str, float],
    new_weights: dict[str, float],
) -> tuple[bool, str]:
    """
    Returns (is_major, reason). Criteria:
      - Any component weight shifted by >= MAJOR_WEIGHT_SHIFT (absolute)
      - Any numeric delta in the change strings with magnitude >= MAJOR_THRESHOLD_SHIFT
      - Autotrade paused (always major)
    """
    for ch in all_changes:
        if "PAUSED" in ch:
            return True, "autotrade paused"

    max_shift = 0.0
    top_key = None
    for k in COMPONENT_KEYS:
        shift = abs(new_weights.get(k, 0.0) - current_weights.get(k, 0.0))
        if shift > max_shift:
            max_shift = shift
            top_key = k
    if max_shift >= MAJOR_WEIGHT_SHIFT:
        return True, f"weight {top_key} shifted {max_shift:.2%}"

    for ch in all_changes:
        # Weight deltas are already evaluated via the dict comparison above —
        # skip their string representation to avoid double-counting
        # (lines that come from the weight section are indented with spaces,
        #  or are the "Weights adjusted:" header itself).
        if ch.startswith(" ") or ch.startswith("Weights adjusted"):
            continue
        m = _DELTA_RE.search(ch)
        if not m:
            continue
        try:
            before = float(m.group(1))
            after = float(m.group(2))
        except ValueError:
            continue
        if abs(after - before) >= MAJOR_THRESHOLD_SHIFT:
            return True, f"numeric delta {abs(after - before):g} in '{ch.strip()}'"

    return False, ""


async def _notify_param_change(bot, changes: list[str], win_rate: float, regime: str):
    """Posts parameter update to Callers HQ. Callers gate this on major + cooldown."""
    text = "\n".join([
        "🧠 AI PARAMETER UPDATE",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Effective win rate: {win_rate * 100:.0f}%",
        f"Market regime: {regime}",
        "",
    ] + changes + [
        "",
        f"Updated {datetime.utcnow().strftime('%H:%M:%S')} UTC",
    ])
    try:
        await bot.send_message(CALLER_GROUP_ID, text)
    except Exception as exc:
        logger.error("Agent6: notify failed: %s", exc)


async def _notify_safety_alert(bot, reason: str):
    text = "\n".join([
        "🚨 AI SAFETY ALERT",
        "━━━━━━━━━━━━━━━━━━━━",
        reason,
        "",
        "Autotrade has been PAUSED.",
        f"Alert time: {datetime.utcnow().strftime('%H:%M:%S')} UTC",
    ])
    try:
        await bot.send_message(CALLER_GROUP_ID, text)
    except Exception as exc:
        logger.error("Agent6: safety alert failed: %s", exc)


# ── Autonomous parameter adjustment ──────────────────────────────────────────

async def _auto_adjust_params(
    recent_wr: float, total_analyzed: int, regime: str,
) -> list[str]:
    """
    Adjusts ALL agent parameters based on outcomes.
    Returns list of change descriptions.
    """
    changes = []

    # ── Confidence thresholds ────────────────────────────────────────
    cur_full = await get_param("conf_full_threshold")
    if recent_wr > 0.65:
        new_full = max(65, cur_full - 2)
        if new_full != cur_full:
            await set_param("conf_full_threshold", new_full,
                            f"WR {recent_wr:.0%} > 65% — lowering", total_analyzed, recent_wr)
            changes.append(f"conf threshold: {cur_full:.0f}→{new_full:.0f} (WR {recent_wr:.0%})")
    elif recent_wr < 0.40:
        new_full = min(90, cur_full + 3)
        if new_full != cur_full:
            await set_param("conf_full_threshold", new_full,
                            f"WR {recent_wr:.0%} < 40% — tightening", total_analyzed, recent_wr)
            changes.append(f"conf threshold: {cur_full:.0f}→{new_full:.0f} (WR {recent_wr:.0%})")

    # ── Scanner filters ──────────────────────────────────────────────
    # Check rejection rate (data_points vs candidates)
    if state.data_points_today > 0 and state.scanner_candidates_today > 0:
        pass_rate = state.data_points_today / max(state.scanner_candidates_today, 1)
        if pass_rate < 0.10:  # >90% rejection
            cur_min_mc = await get_param("scanner_min_mc")
            new_min_mc = max(1000, cur_min_mc * 0.80)
            if new_min_mc != cur_min_mc:
                await set_param("scanner_min_mc", new_min_mc,
                                f"Pass rate {pass_rate:.0%} — loosening min_mc", total_analyzed, recent_wr)
                changes.append(f"min_mc: ${cur_min_mc:.0f}→${new_min_mc:.0f} (pass rate {pass_rate:.0%})")

            cur_min_liq = await get_param("scanner_min_liquidity")
            new_min_liq = max(1000, cur_min_liq * 0.80)
            if new_min_liq != cur_min_liq:
                await set_param("scanner_min_liquidity", new_min_liq,
                                f"Pass rate {pass_rate:.0%} — loosening min_liq", total_analyzed, recent_wr)
                changes.append(f"min_liq: ${cur_min_liq:.0f}→${new_min_liq:.0f}")

    # If paper trades losing too much, tighten scanner
    if recent_wr < 0.35:
        cur_min_mc = await get_param("scanner_min_mc")
        new_min_mc = min(50000, cur_min_mc * 1.20)
        if new_min_mc != cur_min_mc:
            await set_param("scanner_min_mc", new_min_mc,
                            f"WR {recent_wr:.0%} — tightening scanner", total_analyzed, recent_wr)
            changes.append(f"min_mc: ${cur_min_mc:.0f}→${new_min_mc:.0f} (bad WR)")

    # ── MC weight adjustment (most/least predictive) ─────────────────
    # Bump the best component +0.02 and the worst -0.02, then re-normalize
    # the bucket proportionally (sum = 1.0). No hard caps — learned ratios
    # are preserved. _self_heal_weights runs every poll to keep buckets
    # consistent if anything drifts.
    for prefix, label in [("low_mc_", "low"), ("mid_mc_", "mid"), ("high_mc_", "high")]:
        keys = {k: f"{prefix}{k}" for k in COMPONENT_KEYS}
        cur = {}
        for short, full in keys.items():
            cur[short] = await get_param(full)

        best_k = max(cur, key=cur.get)
        worst_k = min(cur, key=cur.get)

        # Bump top signal up, weakest signal down (soft floor at 0.01
        # so we don't accidentally zero a component)
        cur[best_k]  = cur[best_k]  + 0.02
        cur[worst_k] = max(0.01, cur[worst_k] - 0.02)

        # Proportional normalize — preserves ratios learned so far
        total = sum(cur.values())
        if total <= 0:
            continue  # pathological, leave alone
        normalized = {k: round(v / total, 4) for k, v in cur.items()}

        any_change = False
        for short, full in keys.items():
            old = await get_param(full)
            new = normalized[short]
            if abs(new - old) >= 0.0005:
                if short == best_k:
                    reason = f"{label} MC: boosting top signal"
                elif short == worst_k:
                    reason = f"{label} MC: reducing weakest signal"
                else:
                    reason = f"{label} MC: proportional renorm"
                await set_param(full, new, reason, total_analyzed, recent_wr)
                any_change = True

        if any_change:
            bucket_str = " ".join(f"{k}={normalized[k]:.2f}" for k in COMPONENT_KEYS)
            changes.append(f"{label} weights [{bucket_str}]")

    # ── Position sizing ──────────────────────────────────────────────
    if recent_wr > 0.70:
        cur80 = await get_param("size_confidence_80")
        new80 = min(25, cur80 + 1)
        if new80 != cur80:
            await set_param("size_confidence_80", new80,
                            f"High WR {recent_wr:.0%} — increasing high-conf size", total_analyzed, recent_wr)
            changes.append(f"size_80: {cur80:.0f}%→{new80:.0f}%")
    elif recent_wr < 0.50:
        cur80 = await get_param("size_confidence_80")
        new80 = max(10, cur80 - 2)
        if new80 != cur80:
            await set_param("size_confidence_80", new80,
                            f"Low WR {recent_wr:.0%} — decreasing high-conf size", total_analyzed, recent_wr)
            changes.append(f"size_80: {cur80:.0f}%→{new80:.0f}%")

    # Post-close TP/SL learning now lives in _optimize_trade_params which
    # reads sold_too_early directly from the mc_1h_after column. The old
    # tp_adjust_{ptype} write path was dead code — nothing ever read those
    # params — so it has been removed.

    return changes


# ── Self-healing (runs every poll tick, no hard resets) ─────────────────────

# Rule thresholds
NO_TRADE_SOFT_HOURS     = 2      # rule 1: lower threshold by 3
NO_TRADE_HARD_HOURS     = 3      # rule 4: lower threshold by 5
PAPER_THRESHOLD_FLOOR   = 20     # never drop below 20
PAPER_THRESHOLD_CEILING = 50     # never rise above 50
STUCK_WR_TRADES         = 50     # check every N analyzed trades
STUCK_WR_MIN_IMPROVEMENT = 0.02  # 2 points required to count as "improving"
EXPLORATION_SHIFT       = 0.02   # ±0.02 random shift per weight


def _proportional_normalize(weights: dict[str, float]) -> dict[str, float]:
    """
    Divide every weight by the sum so the bucket totals 1.0. Preserves
    relative ratios exactly — the whole point of self-healing per user
    spec. Returns a new dict rounded to 4 decimals.
    """
    # Guard against negative / zero values that would blow up the sum
    positive = {k: max(0.0001, v) for k, v in weights.items()}
    total = sum(positive.values()) or 1.0
    return {k: round(v / total, 4) for k, v in positive.items()}


def _weights_unbalanced(weights: dict[str, float]) -> bool:
    """
    Detection: bucket is unbalanced if sum drifts from 1.0 by >=0.02
    OR any single weight exceeds the drift-high bound OR dips below
    the drift-low bound. These are TRIGGERS not enforcement bounds —
    the heal action is proportional normalize, which may still leave
    values outside [LOW, HIGH] if that's what the learned ratios imply.
    """
    total = sum(weights.values())
    if abs(total - 1.0) >= 0.02:
        return True
    for v in weights.values():
        if v > MC_WEIGHT_DRIFT_HIGH or v < MC_WEIGHT_DRIFT_LOW:
            return True
    return False


async def _self_heal_weights() -> int:
    """
    Rule 2 — detect MC-bucket drift and proportionally normalize each
    bucket so sum == 1.0. Preserves every ratio Agent 6 has learned.
    Returns the number of buckets that were healed this tick.
    """
    healed = 0
    for prefix, label in [("low_mc_", "low"), ("mid_mc_", "mid"), ("high_mc_", "high")]:
        cur = {}
        for k in COMPONENT_KEYS:
            cur[k] = await get_param(f"{prefix}{k}")

        if not _weights_unbalanced(cur):
            continue

        normalized = _proportional_normalize(cur)

        for k in COMPONENT_KEYS:
            old = cur[k]
            new = normalized[k]
            if abs(new - old) >= 0.0005:
                await set_param(
                    f"{prefix}{k}", new,
                    reason=f"{label} self-heal: normalize ratios",
                )
        bucket_str = " ".join(f"{k}={normalized[k]:.2f}" for k in COMPONENT_KEYS)
        logger.info(
            "Weights normalized — kept learned ratios (%s: %s)",
            label, bucket_str,
        )
        healed += 1
    return healed


async def _latest_paper_open_time() -> datetime | None:
    """Return the most recent PaperTrade.opened_at, or None if the table is empty."""
    async with AsyncSessionLocal() as session:
        ts = (await session.execute(
            select(func.max(PaperTrade.opened_at))
        )).scalar()
    return ts


async def _self_heal_threshold() -> None:
    """
    Rules 1 & 4 — if no paper trades have opened in 2+ hours, gently
    lower conf_paper_threshold to explore; if 3+ hours, drop harder.
    Always clamped to [PAPER_THRESHOLD_FLOOR, PAPER_THRESHOLD_CEILING].
    """
    latest = await _latest_paper_open_time()
    now = datetime.utcnow()

    if latest is None:
        # No paper trades yet — count from bot start (best effort: use
        # learning_loop_last_run as a proxy since it advances on boot)
        ref = getattr(state, "learning_loop_last_run", None) or now
        hours_since = (now - ref).total_seconds() / 3600
    else:
        hours_since = (now - latest).total_seconds() / 3600

    if hours_since < NO_TRADE_SOFT_HOURS:
        return

    cur = await get_param("conf_paper_threshold")

    if hours_since >= NO_TRADE_HARD_HOURS:
        # Rule 4: aggressive drop (-5)
        new = max(PAPER_THRESHOLD_FLOOR, min(PAPER_THRESHOLD_CEILING, cur - 5))
        if new != cur:
            await set_param(
                "conf_paper_threshold", new,
                reason=f"no trades {hours_since:.1f}h — aggressive drop",
            )
            logger.info(
                "Agent6: Threshold too high — lowering to %d (was %d, no trades %.1fh)",
                int(new), int(cur), hours_since,
            )
    else:
        # Rule 1: soft drop (-3)
        new = max(PAPER_THRESHOLD_FLOOR, min(PAPER_THRESHOLD_CEILING, cur - 3))
        if new != cur:
            await set_param(
                "conf_paper_threshold", new,
                reason=f"no trades {hours_since:.1f}h — exploring lower threshold",
            )
            logger.info(
                "Agent6: No trades %.1fh — exploring lower threshold %d→%d",
                hours_since, int(cur), int(new),
            )


async def _random_weight_exploration() -> None:
    """
    Apply a random ±EXPLORATION_SHIFT shift to each MC-bucket weight
    and re-normalize proportionally. Explores nearby parameter space
    without wiping learned ratios.
    """
    for prefix in ("low_mc_", "mid_mc_", "high_mc_"):
        cur = {}
        for k in COMPONENT_KEYS:
            cur[k] = await get_param(f"{prefix}{k}")
        shifted = {
            k: v + random.uniform(-EXPLORATION_SHIFT, EXPLORATION_SHIFT)
            for k, v in cur.items()
        }
        normalized = _proportional_normalize(shifted)
        for k in COMPONENT_KEYS:
            await set_param(
                f"{prefix}{k}", normalized[k],
                reason="stuck-wr exploration",
            )


async def _self_heal_stuck_wr() -> None:
    """
    Rule 3 — if trades_analyzed has grown by STUCK_WR_TRADES since the
    last snapshot AND current WR hasn't improved by STUCK_WR_MIN_IMPROVEMENT,
    kick the weights slightly to explore nearby parameter space.

    Snapshots live in agent_params under last_wr_snapshot_at/_wr so we
    survive restarts without special state.
    """
    current_row = await get_current_weights()
    if current_row is None:
        return
    current_analyzed = int(current_row.trades_analyzed or 0)

    last_at = int(await get_param("last_wr_snapshot_at"))
    last_wr = float(await get_param("last_wr_snapshot_wr"))

    if current_analyzed - last_at < STUCK_WR_TRADES:
        return  # not time to check yet

    # Time to compare
    current_wr = await _recent_win_rate(STUCK_WR_TRADES)

    # If no previous snapshot (fresh install), just record this one
    if last_at == 0 and last_wr == 0.0:
        await set_param("last_wr_snapshot_at", float(current_analyzed),
                        reason="initial stuck-wr snapshot")
        await set_param("last_wr_snapshot_wr", current_wr,
                        reason="initial stuck-wr snapshot")
        return

    improvement = current_wr - last_wr
    if improvement < STUCK_WR_MIN_IMPROVEMENT:
        # Stuck — explore
        await _random_weight_exploration()
        logger.info(
            "Exploring new parameters — win rate stuck at %.0f%% "
            "(was %.0f%% after %d more trades)",
            current_wr * 100, last_wr * 100, STUCK_WR_TRADES,
        )

    # Always advance the snapshot so we re-check in another 50 trades
    await set_param("last_wr_snapshot_at", float(current_analyzed),
                    reason="stuck-wr snapshot advance")
    await set_param("last_wr_snapshot_wr", current_wr,
                    reason="stuck-wr snapshot advance")


# ── Main run ─────────────────────────────────────────────────────────────────

async def run_once(bot, force: bool = False) -> bool:
    """
    Tiered analysis:
      - Every 10 trades: micro-adjust weights
      - Every 50 trades: full review (weights + params + wallets)
      - Every 100 trades: major recalibration (thresholds + regime)

    When force=True (admin /agent6force), runs one adjustment cycle on whatever
    unanalyzed trades exist, even if below the normal micro threshold.
    """
    current_row = await get_current_weights()
    last_analyzed = current_row.trades_analyzed if current_row else 0

    current_weights = {
        k: getattr(current_row, f"{k}_weight", DEFAULT_WEIGHTS[k])
        for k in COMPONENT_KEYS
    } if current_row else dict(DEFAULT_WEIGHTS)

    total_closed = await get_total_closed_count()
    pending = total_closed - last_analyzed

    state.learning_loop_last_analyzed = last_analyzed
    state.learning_loop_total_closed = total_closed

    # ── Win rates computed early so learning mode is known before gating ──
    recent_wr = await _recent_win_rate(20)      # short window — circuit breaker
    alltime_wr = await _alltime_win_rate()      # long anchor
    effective_wr = _compute_effective_wr(recent_wr, alltime_wr)
    mode, dyn_micro, dyn_max_shift = _learning_mode(effective_wr)

    logger.info(
        "Agent6: wr_recent=%.2f wr_alltime=%.2f wr_effective=%.2f mode=%s (micro=%d shift=%.2f)",
        recent_wr, alltime_wr, effective_wr, mode, dyn_micro, dyn_max_shift,
    )

    if pending < dyn_micro and not force:
        return False

    # Determine run level (micro threshold is dynamic)
    if pending >= MAJOR_BATCH:
        level = "major"
        batch_size = MAJOR_BATCH
    elif pending >= FULL_BATCH:
        level = "full"
        batch_size = FULL_BATCH
    elif pending >= dyn_micro:
        level = "micro"
        batch_size = dyn_micro
    else:
        level = "forced-micro"
        batch_size = max(1, pending)

    logger.info("Agent6: %d pending trades — running %s adjustment (force=%s, mode=%s)",
                pending, level, force, mode)

    # Fetch batch
    positions = await get_closed_positions_since(0, limit=500)
    batch = positions[last_analyzed:last_analyzed + batch_size]
    if not batch:
        logger.info("Agent6: no unanalyzed trades in fetched window (last_analyzed=%d, fetched=%d)",
                    last_analyzed, len(positions))
        return False
    if len(batch) < dyn_micro and not force:
        return False

    # Classify wins/losses
    winners, losers = await _classify_batch(batch)

    # ── Market regime detection ──────────────────────────────────────
    regime = await _detect_market_regime()

    # ── Safety checks ────────────────────────────────────────────────
    consec_sl = await _consecutive_sl_count()
    all_changes: list[str] = []

    if consec_sl >= 10 and state.autotrade_enabled:
        state.autotrade_enabled = False
        all_changes.append(f"PAUSED: {consec_sl} consecutive SL hits")
        logger.warning("Agent6: PAUSED autotrade — %d consecutive SL", consec_sl)
        await _notify_safety_alert(bot, f"{consec_sl} consecutive stop-loss hits detected.")

    # Safety pause stays on the RECENT window only — this is an emergency
    # circuit breaker, not a tuning signal. Effective_wr would mask a real
    # collapse when all-time history is healthy.
    if recent_wr < 0.40 and total_closed >= 20 and state.autotrade_enabled:
        state.autotrade_enabled = False
        all_changes.append(f"PAUSED: win rate {recent_wr*100:.0f}% < 40% (last 20)")
        logger.warning("Agent6: PAUSED autotrade — win rate %.0f%%", recent_wr * 100)
        await _notify_safety_alert(bot, f"Win rate dropped to {recent_wr*100:.0f}% over last 20 trades.")

    # ── Weight adjustment — shift cap scales with learning mode ─────
    # Micro-level runs use a smaller slice of the cap so individual
    # cycles don't whipsaw weights; full/major use the full cap.
    if level == "micro":
        shift = round(dyn_max_shift * NORMAL_MICRO_SHIFT_RATIO, 4)
    else:
        shift = dyn_max_shift
    new_weights = _compute_weight_adjustments(winners, losers, current_weights, max_shift=shift)

    weight_changes = []
    for k in COMPONENT_KEYS:
        diff = new_weights[k] - current_weights[k]
        if abs(diff) > 0.001:
            weight_changes.append(f"  {k}: {current_weights[k]:.2%}->{new_weights[k]:.2%}")
    if weight_changes:
        all_changes.append("Weights adjusted:")
        all_changes.extend(weight_changes)

    # ── TP/SL refresh — runs EVERY cycle, not just full/major ─────────
    # Previously gated on level in ("full", "major"), which meant
    # _optimize_trade_params never fired because pending rarely reaches
    # FULL_BATCH=5 (trades trickle in 1-2 at a time, micro catches them
    # immediately). After 73 closed trades, the function had been
    # called ZERO times. TP/SL tuning is a global recompute anyway —
    # no reason to batch it.
    params_updated = 0
    try:
        params_updated = await _optimize_trade_params(regime)
        if params_updated:
            all_changes.append(f"Trade params updated for {params_updated} pattern types")
    except Exception as exc:
        logger.error("_optimize_trade_params error: %s", exc)

    # Scanner gate auto-tuning — adjust min_mc, max_mc, min_liquidity
    # based on which MC buckets produce wins vs losses
    try:
        scanner_changes = await _optimize_scanner_params(regime)
        all_changes.extend(scanner_changes)
    except Exception as exc:
        logger.error("_optimize_scanner_params error: %s", exc)

    # Wallet tiers stay batched (expensive Helius calls) — only run at
    # full/major level to avoid hammering the API every 60s.
    promoted = demoted = 0
    if level in ("full", "major"):
        try:
            promoted, demoted = await _adjust_wallet_tiers()
            if promoted or demoted:
                all_changes.append(f"Wallets: +{promoted} promoted, -{demoted} demoted")
        except Exception as exc:
            logger.error("_adjust_wallet_tiers error: %s", exc)

    # ── Save weights ─────────────────────────────────────────────────
    new_analyzed = last_analyzed + len(batch)

    # ── Major: full autonomous parameter adjustment ────────────────────
    # Parameter tuning uses effective_wr (blended 60/40) so that a bad
    # recent streak can't yank thresholds around when the long-term
    # trajectory is still healthy.
    if level in ("full", "major"):
        auto_changes = await _auto_adjust_params(effective_wr, new_analyzed, regime)
        all_changes.extend(auto_changes)
    notes = (
        f"level={level} batch={len(batch)} win={len(winners)} loss={len(losers)} "
        f"regime={regime} wr20={recent_wr:.2f} wr_all={alltime_wr:.2f} wr_eff={effective_wr:.2f}"
    )
    await save_weights(
        fingerprint=new_weights["fingerprint"],
        insider=new_weights["insider"],
        chart=new_weights["chart"],
        rug=new_weights["rug"],
        caller=new_weights["caller"],
        market=new_weights["market"],
        trades_analyzed=new_analyzed,
        notes=notes,
    )

    # Update state
    state.learning_loop_last_analyzed = new_analyzed
    state.learning_loop_total_closed = total_closed
    state.learning_loop_last_run = datetime.utcnow()
    state.learning_loop_weights = new_weights
    state.learning_loop_last_change = "; ".join(all_changes) if all_changes else "No changes"

    await log_agent_run(
        "learning_loop",
        tokens_found=len(batch),
        tokens_saved=len(winners),
        notes=f"{notes} | changes={len(all_changes)}",
    )

    # Notify only on MAJOR changes, and only if we haven't posted in the last hour.
    # Micro-adjustments still get logged to DB + state, but stay silent in chat.
    if all_changes:
        is_major, reason = _is_major_change(all_changes, current_weights, new_weights)
        now = datetime.utcnow()
        last_notify = getattr(state, "learning_loop_last_notify", None)
        cooldown_ok = (
            last_notify is None
            or (now - last_notify).total_seconds() >= NOTIFY_COOLDOWN_SEC
        )
        if is_major and cooldown_ok:
            await _notify_param_change(bot, all_changes, effective_wr, regime)
            state.learning_loop_last_notify = now
            logger.info("Agent6: posted major update to Callers HQ — %s", reason)
        elif is_major and not cooldown_ok:
            remaining = NOTIFY_COOLDOWN_SEC - (now - last_notify).total_seconds()
            logger.info(
                "Agent6: major change suppressed by cooldown (%.0fs left) — %s",
                remaining, reason,
            )
        else:
            logger.info(
                "Agent6: %d changes this cycle but none qualify as major — staying silent",
                len(all_changes),
            )

    logger.info(
        "Agent6[%s]: done — batch=%d win=%d loss=%d regime=%s changes=%d",
        level, len(batch), len(winners), len(losers), regime, len(all_changes),
    )
    return True


# ── Weekly report ────────────────────────────────────────────────────────────

async def _send_weekly_report(bot) -> None:
    """Full weekly performance report to Callers HQ."""
    now = datetime.utcnow()
    week_start = (now - timedelta(days=7)).strftime("%b %d")
    week_end = now.strftime("%b %d, %Y")

    try:
        # ── System activity ──────────────────────────────────────────────
        token_count = await get_token_count()

        cutoff = now - timedelta(days=7)
        async with AsyncSessionLocal() as session:
            candidates_week = (await session.execute(
                select(func.count(Candidate.id)).where(Candidate.created_at >= cutoff)
            )).scalar() or 0

            paper_week = (await session.execute(
                select(func.count(PaperTrade.id)).where(PaperTrade.opened_at >= cutoff)
            )).scalar() or 0

            wallets_week = (await session.execute(
                select(func.count(WalletModel.address)).where(WalletModel.last_updated_at >= cutoff)
            )).scalar() or 0

            patterns_updated = (await session.execute(
                select(func.count(AgentLog.id)).where(
                    AgentLog.agent_name == "pattern_engine", AgentLog.run_at >= cutoff,
                )
            )).scalar() or 0

        # ── Paper trading performance ────────────────────────────────────
        async with AsyncSessionLocal() as session:
            paper_closed = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                )
            )).scalar() or 0

            paper_wins = (await session.execute(
                select(func.count(PaperTrade.id)).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                    PaperTrade.paper_pnl_sol > 0,
                )
            )).scalar() or 0

            paper_pnl = float((await session.execute(
                select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                )
            )).scalar() or 0.0)

            # Best and worst trades
            best_trade = (await session.execute(
                select(PaperTrade).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                ).order_by(PaperTrade.paper_pnl_sol.desc()).limit(1)
            )).scalar_one_or_none()

            worst_trade = (await session.execute(
                select(PaperTrade).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                ).order_by(PaperTrade.paper_pnl_sol.asc()).limit(1)
            )).scalar_one_or_none()

            # Avg win multiple and avg loss %
            win_trades = (await session.execute(
                select(PaperTrade).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                    PaperTrade.paper_pnl_sol > 0,
                )
            )).scalars().all()

            loss_trades = (await session.execute(
                select(PaperTrade).where(
                    PaperTrade.status == "closed", PaperTrade.closed_at >= cutoff,
                    PaperTrade.paper_pnl_sol <= 0,
                )
            )).scalars().all()

        win_rate = round(paper_wins / paper_closed * 100) if paper_closed > 0 else 0
        avg_win_mult = 0.0
        if win_trades:
            mults = [t.peak_multiple or 1.0 for t in win_trades]
            avg_win_mult = sum(mults) / len(mults)
        avg_loss_pct = 0.0
        if loss_trades:
            pcts = [abs(t.paper_pnl_sol or 0) / max(t.paper_sol_spent, 0.01) * 100 for t in loss_trades]
            avg_loss_pct = sum(pcts) / len(pcts)

        best_str = "None"
        if best_trade and best_trade.paper_pnl_sol:
            bn = (best_trade.token_name or "?")[:15]
            best_str = f"{bn} +{best_trade.peak_multiple or 0:.1f}x (+{best_trade.paper_pnl_sol:.4f} SOL)"

        worst_str = "None"
        if worst_trade and worst_trade.paper_pnl_sol:
            wn = (worst_trade.token_name or "?")[:15]
            worst_str = f"{wn} {worst_trade.paper_pnl_sol:.4f} SOL"

        # ── What the AI learned ──────────────────────────────────────────
        weights_row = await get_current_weights()
        if weights_row:
            weight_map = {k: getattr(weights_row, f"{k}_weight") for k in COMPONENT_KEYS}
            best_signal = max(weight_map, key=weight_map.get)
            worst_signal = min(weight_map, key=weight_map.get)
            best_w = weight_map[best_signal]
            worst_w = weight_map[worst_signal]
        else:
            best_signal = worst_signal = "N/A"
            best_w = worst_w = 0

        # Pattern performance
        trade_params = await get_all_trade_params()
        top_pattern = "None"
        worst_pattern = "None"
        if trade_params:
            by_wr = sorted(trade_params, key=lambda p: p.win_rate, reverse=True)
            if by_wr:
                top_pattern = f"{by_wr[0].pattern_type} ({by_wr[0].win_rate * 100:.0f}% WR)"
            if len(by_wr) > 1:
                worst_pattern = f"{by_wr[-1].pattern_type} ({by_wr[-1].win_rate * 100:.0f}% WR)"

        # ── Wallet updates ───────────────────────────────────────────────
        top_wallets = await get_top_wallets(limit=1)
        top_wallet_str = "None yet"
        if top_wallets:
            tw = top_wallets[0]
            top_wallet_str = f"{tw.address[:6]}..{tw.address[-4:]} {tw.win_rate * 100:.0f}% WR T{tw.tier}"

        # ── Parameter changes ────────────────────────────────────────────
        try:
            recent_changes = await get_recent_param_changes(20)
            week_changes = [c for c in recent_changes if c.changed_at and c.changed_at >= cutoff]
        except Exception:
            week_changes = []

        conf_changes = [c for c in week_changes if "conf" in c.param_name]
        scanner_changes = [c for c in week_changes if "scanner" in c.param_name]
        weight_changes = [c for c in week_changes if "mc_" in c.param_name]

        conf_str = "No changes"
        if conf_changes:
            c = conf_changes[0]
            conf_str = f"{c.old_value:g} -> {c.new_value:g} ({c.reason or ''})"[:50]

        # ── Focus for next week ──────────────────────────────────────────
        if win_rate >= 65:
            focus = "Strong performance. Monitoring for consistency before live mode."
            target = "Maintain 65%+ win rate"
        elif win_rate >= 50:
            focus = "Improving signal weights. Tightening entry on weak patterns."
            target = f"Raise win rate from {win_rate}% to 60%+"
        elif win_rate > 0:
            focus = "Adjusting thresholds aggressively. Cutting weak patterns."
            target = f"Raise win rate from {win_rate}% to 50%+"
        else:
            focus = "Collecting data. Need more paper trades for analysis."
            target = "Execute 20+ paper trades for baseline data"

        # ── Build report ─────────────────────────────────────────────────
        text = "\n".join([
            f"📊 WEEKLY AI PERFORMANCE REPORT",
            f"Week of {week_start} — {week_end}",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            "🤖 SYSTEM ACTIVITY",
            f"  Tokens in DB: {token_count}",
            f"  Candidates evaluated: {candidates_week}",
            f"  Paper trades executed: {paper_week}",
            f"  Wallets discovered: {wallets_week}",
            f"  Pattern engine runs: {patterns_updated}",
            "",
            "📈 TRADING PERFORMANCE",
            f"  Paper trades closed: {paper_closed}",
            f"  Win rate: {win_rate}%",
            f"  Avg win multiple: {avg_win_mult:.1f}x",
            f"  Avg loss: {avg_loss_pct:.0f}%",
            f"  Best: {best_str}",
            f"  Worst: {worst_str}",
            f"  Paper P&L: {paper_pnl:+.4f} SOL",
            "",
            "🧠 WHAT THE AI LEARNED",
            f"  Top pattern: {top_pattern}",
            f"  Worst pattern: {worst_pattern}",
            f"  Best signal: {best_signal} ({best_w:.0%} weight)",
            f"  Weakest signal: {worst_signal} ({worst_w:.0%} weight)",
            "",
            "👛 WALLET UPDATES",
            f"  Wallets active: {wallets_week}",
            f"  Top wallet: {top_wallet_str}",
            "",
            "⚙️ PARAMETER CHANGES",
            f"  Confidence: {conf_str}",
            f"  Scanner: {len(scanner_changes)} changes",
            f"  Weights: {len(weight_changes)} changes",
            f"  Total: {len(week_changes)} adjustments this week",
            f"  Run /params to see full list",
            "",
            "🎯 NEXT WEEK FOCUS",
            f"  {focus}",
            f"  Target: {target}",
            "",
            "━━━━━━━━━━━━━━━━━━━━",
            "Keep paper trading. Go live when WR hits 65%+ consistently.",
        ])

        await bot.send_message(CALLER_GROUP_ID, text)
        logger.info("Agent6: weekly report sent to Callers HQ")

    except Exception as exc:
        logger.error("Agent6: weekly report failed: %s", exc)


# ── Background loop ──────────────────────────────────────────────────────────

async def learning_loop(bot) -> None:
    """
    Autonomous learning loop:
    - Every 2 min: check for micro/full/major adjustments
    - Every Monday 9am: weekly report
    - Continuous market regime monitoring
    """
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Learning Loop agent started — autonomous mode, checking every %ds", POLL_INTERVAL)

    last_weekly: datetime | None = None

    # Initialize state
    weights_row = await get_current_weights()
    if weights_row:
        state.learning_loop_weights = {
            k: getattr(weights_row, f"{k}_weight") for k in COMPONENT_KEYS
        }
        state.learning_loop_last_analyzed = weights_row.trades_analyzed
    else:
        state.learning_loop_weights = dict(DEFAULT_WEIGHTS)
        state.learning_loop_last_analyzed = 0

    state.learning_loop_total_closed = await get_total_closed_count()

    # Initialize dynamic state if not set
    if not hasattr(state, "confidence_thresholds") or not state.confidence_thresholds:
        state.confidence_thresholds = dict(DEFAULT_THRESHOLDS)
    if not hasattr(state, "market_regime"):
        state.market_regime = "NEUTRAL"
    if not hasattr(state, "sol_24h_change"):
        state.sol_24h_change = 0.0
    if not hasattr(state, "learning_loop_last_change"):
        state.learning_loop_last_change = "No changes yet"

    while True:
        try:
            # Update last_run on EVERY poll tick, not just when run_once
            # actually produced changes. Previously last_run only advanced
            # inside successful adjustment cycles, so in low-volume periods
            # /hub and /agent_status reported "last run never" forever even
            # though the loop was alive. The heartbeat itself IS a run.
            state.learning_loop_last_run = datetime.utcnow()

            # Heartbeat — confirms the loop is alive in Railway logs
            total_closed = await get_total_closed_count()
            current_row = await get_current_weights()
            analyzed = current_row.trades_analyzed if current_row else 0
            pending = total_closed - analyzed
            hb_mode = getattr(state, "learning_loop_mode", "normal")
            hb_micro = AGGRESSIVE_MICRO_BATCH if hb_mode == "aggressive" else MICRO_BATCH
            logger.info(
                "Agent6 heartbeat: closed_trades=%d analyzed=%d pending=%d mode=%s (micro=%d)",
                total_closed, analyzed, pending, hb_mode, hb_micro,
            )

            # Self-healing — runs BEFORE run_once each tick so learning
            # always operates on a sane state. All three rules preserve
            # whatever Agent 6 has learned; none of them hard-reset.
            try:
                await _self_heal_weights()
            except Exception as exc:
                logger.error("self_heal_weights error: %s", exc)
            try:
                await _self_heal_threshold()
            except Exception as exc:
                logger.error("self_heal_threshold error: %s", exc)
            try:
                await _self_heal_stuck_wr()
            except Exception as exc:
                logger.error("self_heal_stuck_wr error: %s", exc)

            # Market regime check every cycle
            await _detect_market_regime()

            # Run learning
            await run_once(bot)

            # Weekly report
            now = datetime.utcnow()
            if (now.weekday() == 0 and now.hour >= WEEKLY_HOUR
                    and (last_weekly is None or (now - last_weekly).total_seconds() > 82800)):
                await _send_weekly_report(bot)
                last_weekly = now

        except Exception as exc:
            logger.error("Learning loop error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
