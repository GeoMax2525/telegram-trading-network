"""
learning_loop.py — Agent 6: The Learning Loop

Triggers every 50 closed auto-executed trades.
Also posts a weekly performance report every Monday 9am UTC.

What it does each run:
  1. Reviews closed Position outcomes vs. predicted confidence scores
  2. Correlates which component scores were high on winners vs. losers
  3. Adjusts Agent 5 weights (max +/- 0.05 per run to prevent overcorrection)
  4. Demotes wallets with < 40% win rate in last 20 trades (drop one tier)
  5. Promotes wallets with > 70% win rate in last 20 trades (raise one tier)
  6. Saves updated weights to AgentWeights table

Outputs:
  - New row in agent_weights table
  - Wallet tier adjustments
  - Weekly Telegram report to Callers HQ
  - Logs to agent_logs
"""

import asyncio
import logging
from datetime import datetime, timedelta

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
)

logger = logging.getLogger(__name__)

BATCH_SIZE       = 50    # run every N closed trades
POLL_INTERVAL    = 300   # check every 5 minutes
STARTUP_DELAY    = 120   # seconds after bot start
MAX_WEIGHT_SHIFT = 0.05  # max change per component per run
MIN_WEIGHT       = 0.02  # floor for any weight
WEEKLY_HOUR      = 9     # Monday 9am UTC

# Default weights (used when no AgentWeights row exists yet)
DEFAULT_WEIGHTS = {
    "fingerprint": 0.25,
    "insider":     0.25,
    "chart":       0.20,
    "rug":         0.15,
    "caller":      0.10,
    "market":      0.05,
}

COMPONENT_KEYS = ["fingerprint", "insider", "chart", "rug", "caller", "market"]
SCORE_FIELDS   = {
    "fingerprint": "fingerprint_score",
    "insider":     "insider_score",
    "chart":       "chart_score",
    "rug":         "rug_score",
    "caller":      "caller_score",
    "market":      "market_score",
}


# ── Weight adjustment logic ──────────────────────────────────────────────────

def _compute_weight_adjustments(
    winners: list[dict],
    losers: list[dict],
    current: dict[str, float],
) -> dict[str, float]:
    """
    Compares average component scores on winners vs losers.
    Returns new weights dict (sum = 1.0).
    """
    if not winners and not losers:
        return current

    def _avg_scores(trades: list[dict]) -> dict[str, float]:
        if not trades:
            return {k: 50.0 for k in COMPONENT_KEYS}
        totals = {k: 0.0 for k in COMPONENT_KEYS}
        for t in trades:
            for k in COMPONENT_KEYS:
                totals[k] += t.get(SCORE_FIELDS[k], 50.0)
        return {k: totals[k] / len(trades) for k in COMPONENT_KEYS}

    win_avg = _avg_scores(winners)
    loss_avg = _avg_scores(losers)

    new_weights = dict(current)

    for key in COMPONENT_KEYS:
        diff = win_avg[key] - loss_avg[key]
        # Positive diff = this signal was higher on winners → increase weight
        # Negative diff = this signal was higher on losers → decrease weight
        # Scale: 10-point diff → 0.01 shift
        shift = max(-MAX_WEIGHT_SHIFT, min(MAX_WEIGHT_SHIFT, diff / 1000.0))
        new_weights[key] = max(MIN_WEIGHT, new_weights[key] + shift)

    # Normalize to sum = 1.0
    total = sum(new_weights.values())
    if total > 0:
        new_weights = {k: round(v / total, 4) for k, v in new_weights.items()}

    return new_weights


# ── Wallet tier adjustments ──────────────────────────────────────────────────

async def _adjust_wallet_tiers() -> tuple[int, int]:
    """
    Demotes wallets with < 40% win rate (if 20+ trades), promotes > 70%.
    Returns (promoted_count, demoted_count).
    """
    wallets = await get_tier_wallets(max_tier=3)
    promoted = 0
    demoted = 0

    for w in wallets:
        if w.total_trades < 20:
            continue
        if w.win_rate < 0.40 and w.tier > 0:
            await update_wallet_tier(w.address, w.tier - 1)
            demoted += 1
            logger.info("Agent6: demoted wallet %s..%s tier %d→%d (wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.tier, w.tier - 1, w.win_rate * 100)
        elif w.win_rate > 0.70 and w.tier < 3:
            await update_wallet_tier(w.address, w.tier + 1)
            promoted += 1
            logger.info("Agent6: promoted wallet %s..%s tier %d→%d (wr=%.0f%%)",
                        w.address[:4], w.address[-4:], w.tier, w.tier + 1, w.win_rate * 100)

    return promoted, demoted


# ── Trade params optimization ────────────────────────────────────────────────

PATTERN_TYPES = ["new_launch", "insider_wallet", "volume_spike"]

# Defaults to start from
DEFAULT_TP_X          = 3.0
DEFAULT_SL_PCT        = 30.0
DEFAULT_POSITION_PCT  = 10.0


async def _optimize_trade_params() -> int:
    """
    For each pattern type, analyzes closed positions to find optimal TP/SL/size.
    Returns count of pattern types updated.
    """
    updated = 0

    for ptype in PATTERN_TYPES:
        positions = await get_closed_positions_by_source(ptype)
        if not positions:
            continue

        sample_size = len(positions)
        wins = [p for p in positions if p.pnl_sol and p.pnl_sol > 0]
        losses = [p for p in positions if p.pnl_sol and p.pnl_sol <= 0]
        win_rate = len(wins) / sample_size if sample_size > 0 else 0.0

        # Compute average multiple from winners
        multiples = []
        for p in positions:
            if p.entry_mc and p.entry_mc > 0 and p.pnl_sol is not None:
                # Approximate peak multiplier from PnL
                if p.close_reason == "tp_hit" and p.take_profit_x:
                    multiples.append(p.take_profit_x)
                elif p.pnl_sol > 0 and p.amount_sol_spent > 0:
                    multiples.append(1.0 + p.pnl_sol / p.amount_sol_spent)
                else:
                    multiples.append(max(0.1, 1.0 + p.pnl_sol / max(p.amount_sol_spent, 0.01)))

        avg_multiple = sum(multiples) / len(multiples) if multiples else 1.0

        # Optimal TP: analyze winners — what TP would have captured the most value?
        # Use the 75th percentile of winner multiples as optimal TP
        winner_multiples = sorted([m for m in multiples if m > 1.0])
        if len(winner_multiples) >= 3:
            idx_75 = int(len(winner_multiples) * 0.75)
            optimal_tp = round(winner_multiples[idx_75], 1)
        else:
            optimal_tp = DEFAULT_TP_X

        # Clamp TP to reasonable range
        optimal_tp = max(1.5, min(10.0, optimal_tp))

        # Optimal SL: analyze losers — what SL would have prevented the worst losses?
        loss_pcts = []
        for p in losses:
            if p.amount_sol_spent and p.amount_sol_spent > 0 and p.pnl_sol:
                loss_pct = abs(p.pnl_sol / p.amount_sol_spent) * 100
                loss_pcts.append(loss_pct)

        if len(loss_pcts) >= 3:
            # Use 25th percentile — tighter SL on pattern types with sharp drops
            sorted_losses = sorted(loss_pcts)
            idx_25 = int(len(sorted_losses) * 0.25)
            optimal_sl = round(sorted_losses[idx_25], 0)
        else:
            optimal_sl = DEFAULT_SL_PCT

        # Clamp SL to reasonable range
        optimal_sl = max(10.0, min(50.0, optimal_sl))

        # Optimal position size: higher win rate → larger position
        if win_rate >= 0.70:
            optimal_pos = 15.0
        elif win_rate >= 0.55:
            optimal_pos = 12.0
        elif win_rate >= 0.40:
            optimal_pos = 10.0
        else:
            optimal_pos = 7.0

        # Confidence: based on sample size (0-100)
        confidence = min(100.0, sample_size * 2.0)

        await upsert_trade_params(
            pattern_type=ptype,
            optimal_tp_x=optimal_tp,
            optimal_sl_pct=optimal_sl,
            optimal_position_pct=optimal_pos,
            sample_size=sample_size,
            win_rate=round(win_rate, 4),
            avg_multiple=round(avg_multiple, 2),
            confidence=round(confidence, 1),
        )

        logger.info(
            "Agent6: %s params — tp=%.1fx sl=%.0f%% size=%.0f%% "
            "wr=%.0f%% avg=%.2fx samples=%d",
            ptype, optimal_tp, optimal_sl, optimal_pos,
            win_rate * 100, avg_multiple, sample_size,
        )
        updated += 1

    return updated


# ── Main run ─────────────────────────────────────────────────────────────────

async def run_once() -> bool:
    """
    Checks if 50 new closed trades have accumulated since last run.
    If so, analyzes outcomes and updates weights.
    Returns True if a learning run was executed.
    """
    # Get current weights (or defaults)
    current_row = await get_current_weights()
    last_analyzed = current_row.trades_analyzed if current_row else 0

    current = {
        "fingerprint": current_row.fingerprint_weight if current_row else DEFAULT_WEIGHTS["fingerprint"],
        "insider":     current_row.insider_weight     if current_row else DEFAULT_WEIGHTS["insider"],
        "chart":       current_row.chart_weight       if current_row else DEFAULT_WEIGHTS["chart"],
        "rug":         current_row.rug_weight         if current_row else DEFAULT_WEIGHTS["rug"],
        "caller":      current_row.caller_weight      if current_row else DEFAULT_WEIGHTS["caller"],
        "market":      current_row.market_weight      if current_row else DEFAULT_WEIGHTS["market"],
    }

    total_closed = await get_total_closed_count()
    pending = total_closed - last_analyzed

    # Update state for hub display
    state.learning_loop_last_analyzed = last_analyzed
    state.learning_loop_total_closed = total_closed

    if pending < BATCH_SIZE:
        return False

    logger.info("Agent6: %d new closed trades — running learning loop", pending)

    # Fetch closed positions since last run
    # Use position ID as a rough proxy — fetch all we haven't analyzed
    positions = await get_closed_positions_since(0, limit=500)
    # Take only the ones past our last analyzed count
    batch = positions[last_analyzed:last_analyzed + BATCH_SIZE]

    if len(batch) < BATCH_SIZE:
        return False

    # Match positions to their candidate scores
    winners: list[dict] = []
    losers: list[dict] = []

    for pos in batch:
        candidate = await get_candidate_by_token(pos.token_address)
        if candidate is None:
            continue

        scores = {
            "fingerprint_score": candidate.fingerprint_score or 50.0,
            "insider_score":     candidate.insider_score     or 50.0,
            "chart_score":       candidate.chart_score       or 50.0,
            "rug_score":         candidate.rug_score         or 50.0,
            "caller_score":      candidate.caller_score      or 50.0,
            "market_score":      candidate.market_score      or 50.0,
        }

        if pos.pnl_sol and pos.pnl_sol > 0:
            winners.append(scores)
        else:
            losers.append(scores)

    # Compute new weights
    new_weights = _compute_weight_adjustments(winners, losers, current)

    # Determine which signals were most/least predictive
    changes = []
    for k in COMPONENT_KEYS:
        diff = new_weights[k] - current[k]
        if abs(diff) > 0.001:
            direction = "+" if diff > 0 else ""
            changes.append(f"{k}:{direction}{diff:.4f}")

    notes = (
        f"batch={len(batch)} win={len(winners)} loss={len(losers)} "
        f"changes=[{', '.join(changes) if changes else 'none'}]"
    )

    # Save new weights
    new_analyzed = last_analyzed + len(batch)
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

    # Adjust wallet tiers
    promoted, demoted = await _adjust_wallet_tiers()

    # Optimize TP/SL/position params per pattern type
    params_updated = await _optimize_trade_params()

    # Update state
    state.learning_loop_last_analyzed = new_analyzed
    state.learning_loop_total_closed = total_closed
    state.learning_loop_last_run = datetime.utcnow()
    state.learning_loop_weights = new_weights

    await log_agent_run(
        "learning_loop",
        tokens_found=len(batch),
        tokens_saved=len(winners),
        notes=f"{notes} | wallets: +{promoted}/-{demoted} | params: {params_updated} types",
    )

    logger.info(
        "Agent6: done — analyzed=%d winners=%d losers=%d promoted=%d demoted=%d params=%d",
        len(batch), len(winners), len(losers), promoted, demoted, params_updated,
    )

    return True


# ── Weekly report ────────────────────────────────────────────────────────────

async def _send_weekly_report(bot) -> None:
    """Posts weekly AI performance report to Callers HQ."""
    perf = await get_weekly_performance()
    weights_row = await get_current_weights()

    if perf["trades"] == 0:
        return  # nothing to report

    # Determine best and worst signals by current weight
    if weights_row:
        weight_map = {
            "fingerprint": weights_row.fingerprint_weight,
            "insider":     weights_row.insider_weight,
            "chart":       weights_row.chart_weight,
            "rug":         weights_row.rug_weight,
            "caller":      weights_row.caller_weight,
            "market":      weights_row.market_weight,
        }
        best = max(weight_map, key=weight_map.get)
        worst = min(weight_map, key=weight_map.get)
        best_pct = round(weight_map[best] * 100)
        worst_pct = round(weight_map[worst] * 100)
        weights_updated = "Yes"
    else:
        best, worst = "N/A", "N/A"
        best_pct = worst_pct = 0
        weights_updated = "No (no data yet)"

    text = (
        "\U0001f4ca *WEEKLY AI PERFORMANCE REPORT*\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"Trades analyzed: `{perf['trades']}`\n"
        f"Win rate: `{perf['win_rate']}%`\n"
        f"Total PnL: `{perf['total_pnl']:+.4f} SOL`\n"
        f"Avg PnL/trade: `{perf['avg_pnl']:+.4f} SOL`\n"
        f"Best signal: `{best}` ({best_pct}% weight)\n"
        f"Worst signal: `{worst}` ({worst_pct}% weight)\n"
        f"Weights updated: {weights_updated}\n"
        f"\n_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_"
    )

    try:
        await bot.send_message(CALLER_GROUP_ID, text, parse_mode="Markdown")
        logger.info("Agent6: weekly report sent to Callers HQ")
    except Exception as exc:
        logger.error("Agent6: failed to send weekly report: %s", exc)


# ── Background loop ──────────────────────────────────────────────────────────

async def learning_loop(bot) -> None:
    """
    Background loop:
    - Every 5 minutes: checks if 50 new closed trades → runs learning
    - Every Monday 9am UTC: posts weekly performance report
    """
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Learning Loop agent started — checking every %ds", POLL_INTERVAL)

    last_weekly_report: datetime | None = None

    # Initialize state with current weights
    weights_row = await get_current_weights()
    if weights_row:
        state.learning_loop_weights = {
            "fingerprint": weights_row.fingerprint_weight,
            "insider":     weights_row.insider_weight,
            "chart":       weights_row.chart_weight,
            "rug":         weights_row.rug_weight,
            "caller":      weights_row.caller_weight,
            "market":      weights_row.market_weight,
        }
        state.learning_loop_last_analyzed = weights_row.trades_analyzed
    else:
        state.learning_loop_weights = dict(DEFAULT_WEIGHTS)
        state.learning_loop_last_analyzed = 0

    total_closed = await get_total_closed_count()
    state.learning_loop_total_closed = total_closed

    while True:
        try:
            # Check for learning run
            await run_once()

            # Weekly report: Monday 9am UTC
            now = datetime.utcnow()
            if (now.weekday() == 0
                    and now.hour >= WEEKLY_HOUR
                    and (last_weekly_report is None
                         or (now - last_weekly_report).total_seconds() > 82800)):  # ~23h
                await _send_weekly_report(bot)
                last_weekly_report = now

        except Exception as exc:
            logger.error("Learning loop error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)
