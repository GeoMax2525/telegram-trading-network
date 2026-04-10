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
from datetime import datetime, timedelta

import aiohttp

from bot import state
from bot.config import CALLER_GROUP_ID
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
)

logger = logging.getLogger(__name__)

# Intervals
MICRO_BATCH      = 3     # micro-adjust every N trades (chaos: was 10)
FULL_BATCH       = 5     # full review every N trades (chaos: was 50)
MAJOR_BATCH      = 15    # major recalibration every N trades (chaos: was 100)
POLL_INTERVAL    = 60    # check every 1 minute (chaos: was 2 min)
STARTUP_DELAY    = 120
MAX_WEIGHT_SHIFT = 0.10  # aggressive tuning (chaos: was 0.05)
MIN_WEIGHT       = 0.02
WEEKLY_HOUR      = 9

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

PATTERN_TYPES = ["new_launch", "insider_wallet", "volume_spike"]
DEFAULT_TP_X = 3.0
DEFAULT_SL_PCT = 30.0
DEFAULT_POSITION_PCT = 10.0

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
    except Exception:
        pass

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
    """Win rate of the last N closed trades."""
    async with AsyncSessionLocal() as session:
        positions = (await session.execute(
            select(Position)
            .where(Position.status == "closed", Position.pnl_sol.is_not(None))
            .order_by(Position.closed_at.desc())
            .limit(n)
        )).scalars().all()

    if not positions:
        return 0.5  # neutral if no data
    wins = sum(1 for p in positions if p.pnl_sol and p.pnl_sol > 0)
    return wins / len(positions)


async def _consecutive_sl_count() -> int:
    """Count consecutive SL hits from most recent trades."""
    async with AsyncSessionLocal() as session:
        positions = (await session.execute(
            select(Position)
            .where(Position.status == "closed", Position.pnl_sol.is_not(None))
            .order_by(Position.closed_at.desc())
            .limit(20)
        )).scalars().all()

    count = 0
    for p in positions:
        if p.close_reason == "sl_hit":
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


# ── Trade params optimization ────────────────────────────────────────────────

async def _optimize_trade_params(regime: str) -> int:
    updated = 0
    for ptype in PATTERN_TYPES:
        positions = await get_closed_positions_by_source(ptype)
        if not positions:
            continue

        sample_size = len(positions)
        wins = [p for p in positions if p.pnl_sol and p.pnl_sol > 0]
        losses = [p for p in positions if p.pnl_sol and p.pnl_sol <= 0]
        win_rate = len(wins) / sample_size if sample_size > 0 else 0.0

        multiples = []
        for p in positions:
            if p.entry_mc and p.entry_mc > 0 and p.pnl_sol is not None:
                if p.close_reason == "tp_hit" and p.take_profit_x:
                    multiples.append(p.take_profit_x)
                elif p.pnl_sol > 0 and p.amount_sol_spent > 0:
                    multiples.append(1.0 + p.pnl_sol / p.amount_sol_spent)
                else:
                    multiples.append(max(0.1, 1.0 + p.pnl_sol / max(p.amount_sol_spent, 0.01)))

        avg_multiple = sum(multiples) / len(multiples) if multiples else 1.0

        winner_multiples = sorted([m for m in multiples if m > 1.0])
        if len(winner_multiples) >= 3:
            optimal_tp = round(winner_multiples[int(len(winner_multiples) * 0.75)], 1)
        else:
            optimal_tp = DEFAULT_TP_X
        optimal_tp = max(1.5, min(10.0, optimal_tp))

        loss_pcts = []
        for p in losses:
            if p.amount_sol_spent and p.amount_sol_spent > 0 and p.pnl_sol:
                loss_pcts.append(abs(p.pnl_sol / p.amount_sol_spent) * 100)
        if len(loss_pcts) >= 3:
            optimal_sl = round(sorted(loss_pcts)[int(len(loss_pcts) * 0.25)], 0)
        else:
            optimal_sl = DEFAULT_SL_PCT
        optimal_sl = max(10.0, min(50.0, optimal_sl))

        # Position sizing: based on win rate + market regime
        if win_rate >= 0.70:
            optimal_pos = 15.0
        elif win_rate >= 0.55:
            optimal_pos = 12.0
        elif win_rate >= 0.40:
            optimal_pos = 10.0
        else:
            optimal_pos = 7.0

        # Market regime modifier
        if regime == "BAD":
            optimal_pos = round(optimal_pos * 0.5, 1)
        elif regime == "NEUTRAL":
            optimal_pos = round(optimal_pos * 0.75, 1)

        confidence = min(100.0, sample_size * 2.0)

        await upsert_trade_params(
            pattern_type=ptype, optimal_tp_x=optimal_tp,
            optimal_sl_pct=optimal_sl, optimal_position_pct=optimal_pos,
            sample_size=sample_size, win_rate=round(win_rate, 4),
            avg_multiple=round(avg_multiple, 2), confidence=round(confidence, 1),
        )
        updated += 1

    return updated


# ── Collect scores from batch ────────────────────────────────────────────────

async def _classify_batch(batch) -> tuple[list[dict], list[dict]]:
    """Returns (winners_scores, losers_scores)."""
    winners, losers = [], []
    for pos in batch:
        candidate = await get_candidate_by_token(pos.token_address)
        if candidate is None:
            continue
        scores = {
            SCORE_FIELDS[k]: getattr(candidate, SCORE_FIELDS[k], None) or 50.0
            for k in COMPONENT_KEYS
        }
        if pos.pnl_sol and pos.pnl_sol > 0:
            winners.append(scores)
        else:
            losers.append(scores)
    return winners, losers


# ── Notify Callers HQ ────────────────────────────────────────────────────────

async def _notify_param_change(bot, changes: list[str], win_rate: float, regime: str):
    """Posts parameter update to Callers HQ."""
    text = "\n".join([
        "🧠 AI PARAMETER UPDATE",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Win rate (last 20): {win_rate * 100:.0f}%",
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
    for prefix, label in [("low_mc_", "low"), ("mid_mc_", "mid"), ("high_mc_", "high")]:
        keys = {k: f"{prefix}{k}" for k in COMPONENT_KEYS}
        cur = {}
        for short, full in keys.items():
            cur[short] = await get_param(full)

        # Find best/worst by current weight
        best_k = max(cur, key=cur.get)
        worst_k = min(cur, key=cur.get)

        if cur[best_k] < 0.45 and cur[worst_k] > 0.02:
            new_best = min(0.45, cur[best_k] + 0.02)
            new_worst = max(0.02, cur[worst_k] - 0.02)
            if new_best != cur[best_k]:
                await set_param(keys[best_k], new_best,
                                f"{label} MC: boosting top signal", total_analyzed, recent_wr)
                await set_param(keys[worst_k], new_worst,
                                f"{label} MC: reducing weakest signal", total_analyzed, recent_wr)
                changes.append(f"{label} weights: {best_k}↑{new_best:.2f} {worst_k}↓{new_worst:.2f}")

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

    return changes


# ── Main run ─────────────────────────────────────────────────────────────────

async def run_once(bot) -> bool:
    """
    Tiered analysis:
      - Every 10 trades: micro-adjust weights
      - Every 50 trades: full review (weights + params + wallets)
      - Every 100 trades: major recalibration (thresholds + regime)
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

    if pending < MICRO_BATCH:
        return False

    # Determine run level
    if pending >= MAJOR_BATCH:
        level = "major"
        batch_size = MAJOR_BATCH
    elif pending >= FULL_BATCH:
        level = "full"
        batch_size = FULL_BATCH
    else:
        level = "micro"
        batch_size = MICRO_BATCH

    logger.info("Agent6: %d pending trades — running %s adjustment", pending, level)

    # Fetch batch
    positions = await get_closed_positions_since(0, limit=500)
    batch = positions[last_analyzed:last_analyzed + batch_size]
    if len(batch) < MICRO_BATCH:
        return False

    # Classify wins/losses
    winners, losers = await _classify_batch(batch)

    # ── Market regime detection ──────────────────────────────────────
    regime = await _detect_market_regime()

    # ── Safety checks ────────────────────────────────────────────────
    consec_sl = await _consecutive_sl_count()
    recent_wr = await _recent_win_rate(20)
    all_changes: list[str] = []

    if consec_sl >= 10 and state.autotrade_enabled:
        state.autotrade_enabled = False
        all_changes.append(f"PAUSED: {consec_sl} consecutive SL hits")
        logger.warning("Agent6: PAUSED autotrade — %d consecutive SL", consec_sl)
        await _notify_safety_alert(bot, f"{consec_sl} consecutive stop-loss hits detected.")

    if recent_wr < 0.40 and total_closed >= 20 and state.autotrade_enabled:
        state.autotrade_enabled = False
        all_changes.append(f"PAUSED: win rate {recent_wr*100:.0f}% < 40% (last 20)")
        logger.warning("Agent6: PAUSED autotrade — win rate %.0f%%", recent_wr * 100)
        await _notify_safety_alert(bot, f"Win rate dropped to {recent_wr*100:.0f}% over last 20 trades.")

    # ── Micro: weight adjustment ─────────────────────────────────────
    shift = 0.02 if level == "micro" else MAX_WEIGHT_SHIFT
    new_weights = _compute_weight_adjustments(winners, losers, current_weights, max_shift=shift)

    weight_changes = []
    for k in COMPONENT_KEYS:
        diff = new_weights[k] - current_weights[k]
        if abs(diff) > 0.001:
            weight_changes.append(f"  {k}: {current_weights[k]:.2%}->{new_weights[k]:.2%}")
    if weight_changes:
        all_changes.append("Weights adjusted:")
        all_changes.extend(weight_changes)

    # ── Full: TP/SL/position + wallet tiers ──────────────────────────
    params_updated = 0
    promoted = demoted = 0
    if level in ("full", "major"):
        promoted, demoted = await _adjust_wallet_tiers()
        params_updated = await _optimize_trade_params(regime)
        if promoted or demoted:
            all_changes.append(f"Wallets: +{promoted} promoted, -{demoted} demoted")
        if params_updated:
            all_changes.append(f"Trade params updated for {params_updated} pattern types")

    # ── Major: full autonomous parameter adjustment ────────────────────
    if level in ("full", "major"):
        auto_changes = await _auto_adjust_params(recent_wr, new_analyzed, regime)
        all_changes.extend(auto_changes)

    # ── Save weights ─────────────────────────────────────────────────
    new_analyzed = last_analyzed + len(batch)
    notes = (
        f"level={level} batch={len(batch)} win={len(winners)} loss={len(losers)} "
        f"regime={regime} wr20={recent_wr:.2f}"
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

    # Notify if significant changes
    if all_changes and (level in ("full", "major") or threshold_changes):
        await _notify_param_change(bot, all_changes, recent_wr, regime)

    logger.info(
        "Agent6[%s]: done — batch=%d win=%d loss=%d regime=%s changes=%d",
        level, len(batch), len(winners), len(losers), regime, len(all_changes),
    )
    return True


# ── Weekly report ────────────────────────────────────────────────────────────

async def _send_weekly_report(bot) -> None:
    perf = await get_weekly_performance()
    weights_row = await get_current_weights()
    if perf["trades"] == 0:
        return

    regime = state.market_regime
    thresholds = state.confidence_thresholds

    if weights_row:
        weight_map = {k: getattr(weights_row, f"{k}_weight") for k in COMPONENT_KEYS}
        best = max(weight_map, key=weight_map.get)
        worst = min(weight_map, key=weight_map.get)
    else:
        best = worst = "N/A"

    text = "\n".join([
        "📊 WEEKLY AI PERFORMANCE REPORT",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Trades: {perf['trades']} | Win rate: {perf['win_rate']}%",
        f"PnL: {perf['total_pnl']:+.4f} SOL | Avg: {perf['avg_pnl']:+.4f} SOL",
        f"Best signal: {best} | Worst: {worst}",
        f"Market regime: {regime}",
        f"Thresholds: full={thresholds['execute_full']} half={thresholds['execute_half']}",
        "",
        f"Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
    ])

    try:
        await bot.send_message(CALLER_GROUP_ID, text)
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
