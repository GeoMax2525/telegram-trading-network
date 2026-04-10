"""
confidence_engine.py — Agent 5: The Confidence Engine

Called by Agent 4 (Scanner) for every candidate with match_score >= 50.
Produces a single confidence score (0–100) and a trade decision.

Confidence score — 6 weighted components:
  token_fingerprint_score × 0.25  — compare metrics to winning patterns (Agent 3)
  insider_wallet_signal   × 0.25  — is any Tier 1/2 wallet buying this token?
  chart_pattern_score     × 0.20  — Agent 7 chart pattern analysis
  rug_safety_score        × 0.15  — rugcheck score + rug flags
  caller_reliability      × 0.10  — has any caller scanned this token?
  market_conditions       × 0.05  — placeholder 50 until SOL price feed added

Decision thresholds:
  80+   → execute_full  (auto-buy full position if autotrade ON)
  70-79 → execute_half  (auto-buy half position if autotrade ON)
  60-69 → monitor       (log as high interest, no execute)
  <60   → discard       (log and discard)

Hard gates (before any execution):
  - rug_safety_score must be >= 50
  - chart_score must be >= 50
  - token must be < 4 hours old (enforced by scanner)
  - liquidity must be > $5K (enforced by scanner)

All candidates are saved to the candidates table for Agent 6 learning.
No Telegram messages — silent logging only.
"""

import logging

from bot import state
from bot.agents.chart_detector import analyze_chart
from database.models import (
    get_pattern_by_type,
    get_tier_wallets,
    has_caller_scanned,
    save_candidate,
    log_agent_run,
    get_current_weights,
    get_trade_params,
    get_token_by_mint,
)

logger = logging.getLogger(__name__)

# Default weights (used when no learned weights exist in DB)
DEFAULT_WEIGHTS = {
    "fingerprint": 0.25,
    "insider":     0.25,
    "chart":       0.20,
    "rug":         0.15,
    "caller":      0.10,
    "market":      0.05,
}


# ── Component scorers ────────────────────────────────────────────────────────

async def _score_fingerprint(candidate: dict, pattern) -> float:
    """Score 0–100 based on how well metrics match the winner_2x pattern.
    Includes pump.fun bonuses for social links and bonding curve."""
    if pattern is None:
        base = 50.0  # no pattern data — neutral
    else:
        score = 0.0
        checks = 0

        mcap = candidate.get("mcap", 0)
        liquidity = candidate.get("liquidity", 0)
        ai_score = candidate.get("ai_score", 0)

        # MC range
        if pattern.mcap_range_low and pattern.mcap_range_high:
            checks += 1
            low = pattern.mcap_range_low * 0.5
            high = pattern.mcap_range_high * 2.0
            if low <= mcap <= high:
                score += 100.0
            elif mcap < low:
                score += 30.0
            else:
                score += 10.0

        # Liquidity
        if pattern.avg_liquidity and pattern.avg_liquidity > 0:
            checks += 1
            ratio = min(liquidity / pattern.avg_liquidity, 2.0)
            score += ratio * 50.0

        # AI score
        if pattern.avg_ai_score and pattern.avg_ai_score > 0:
            checks += 1
            ratio = min(ai_score / pattern.avg_ai_score, 1.5)
            score += ratio * 66.7

        if checks == 0:
            base = 50.0
        else:
            raw = score / checks
            conf = pattern.confidence_score / 100.0
            base = min(raw * conf + raw * (1 - conf * 0.3), 100)

    # Pump.fun bonuses — look up token data
    mint = candidate.get("mint", "")
    if mint:
        token = await get_token_by_mint(mint)
        if token:
            # Social links bonus: twitter + telegram = +5
            if token.social_links:
                import json as _json
                try:
                    links = _json.loads(token.social_links)
                    if links.get("twitter") and links.get("telegram"):
                        base += 5.0
                except Exception:
                    pass

            # Bonding curve > 50% = +10
            if token.bonding_curve and token.bonding_curve > 50:
                base += 10.0

    return round(min(base, 100.0), 1)


async def _score_insider(candidate: dict) -> float:
    """Score 0–100 based on insider wallet activity for this token."""
    insider_count = candidate.get("insider_count", 0)
    if insider_count >= 3:
        return 100.0
    if insider_count == 2:
        return 80.0
    if insider_count == 1:
        return 60.0

    # No insider data from scanner — check if any tier wallets exist at all
    # (if source was not insider_wallet, count defaults to 0)
    if candidate.get("source") == "insider_wallet":
        return 40.0
    return 30.0  # no insider signal


async def _score_chart(candidate: dict) -> tuple[float, str]:
    """
    Calls Agent 7 (Chart Detector) to analyze chart patterns.
    Returns (score 0-100, pattern_name).
    """
    try:
        result = await analyze_chart(candidate)
        return result["chart_score"], result.get("pattern_name", "none")
    except Exception as exc:
        logger.warning("Agent5: chart analysis failed for %s: %s",
                       candidate.get("mint", "?")[:12], exc)
        return 40.0, "error"


def _score_rug(candidate: dict) -> float:
    """
    Returns 1–100 safety score (higher = safer).

    Uses rugcheck_normalised (1-10 risk scale from API) if available,
    otherwise falls back to raw rugcheck score.
    Inverts risk into safety: safety = 100 - (risk_normalised * 10).
    """
    rc_norm = candidate.get("rugcheck_normalised")
    rc_raw = candidate.get("rugcheck")

    if rc_norm is not None:
        # score_normalised is 1-10, lower = safer
        # Convert: 1 → 90, 5 → 50, 10 → 0  then clamp to 1-100
        safety = max(1.0, min(100.0, 100.0 - (rc_norm * 10.0)))
        logger.info("Rug score: normalised=%s → safety=%.0f", rc_norm, safety)
        return safety

    if rc_raw is not None:
        # Raw score is unbounded risk. Map to 1-100 safety.
        # 0-5 risk → 95, 50 → 70, 100 → 55, 200 → 40, 500+ → 10
        safety = max(1.0, min(100.0, 100.0 - min(rc_raw, 1000) / 10.0))
        logger.info("Rug score: raw=%s → safety=%.0f", rc_raw, safety)
        return safety

    logger.debug("Rug score: no rugcheck data — defaulting to 40")
    return 40.0


async def _score_caller(candidate: dict) -> float:
    """Score 0–100 based on whether any approved caller scanned this token."""
    mint = candidate.get("mint", "")
    if not mint:
        return 0.0
    scanned = await has_caller_scanned(mint)
    return 80.0 if scanned else 20.0


def _score_market() -> float:
    """Placeholder until SOL price feed is integrated."""
    return 50.0


# ── Main scoring function ────────────────────────────────────────────────────

async def _load_weights() -> dict[str, float]:
    """Load learned weights from DB, fall back to defaults."""
    row = await get_current_weights()
    if row:
        return {
            "fingerprint": row.fingerprint_weight,
            "insider":     row.insider_weight,
            "chart":       row.chart_weight,
            "rug":         row.rug_weight,
            "caller":      row.caller_weight,
            "market":      row.market_weight,
        }
    return dict(DEFAULT_WEIGHTS)


async def score_candidate(candidate: dict) -> dict:
    """
    Score a single candidate from Agent 4.
    Returns enriched dict with confidence_score, component scores, and decision.
    All candidates are saved to the database silently.
    """
    pattern = await get_pattern_by_type("winner_2x")
    weights = await _load_weights()

    # Compute all 6 component scores
    fingerprint = await _score_fingerprint(candidate, pattern)
    insider     = await _score_insider(candidate)
    chart, chart_pattern = await _score_chart(candidate)
    rug         = _score_rug(candidate)
    caller      = await _score_caller(candidate)
    market      = _score_market()

    # Weighted confidence score using learned weights
    confidence = round(
        fingerprint * weights["fingerprint"]
        + insider   * weights["insider"]
        + chart     * weights["chart"]
        + rug       * weights["rug"]
        + caller    * weights["caller"]
        + market    * weights["market"],
        1,
    )

    # Hard gates for execution
    rug_gate_pass = rug >= 50
    chart_gate_pass = chart >= 50
    gates_pass = rug_gate_pass and chart_gate_pass

    # Dynamic thresholds (adjusted by Agent 6)
    thresholds = state.confidence_thresholds
    t_full = thresholds.get("execute_full", 80)
    t_half = thresholds.get("execute_half", 70)
    t_monitor = thresholds.get("monitor", 60)

    if confidence >= t_full and gates_pass:
        decision = "execute_full"
    elif confidence >= t_half and gates_pass:
        decision = "execute_half"
    elif confidence >= t_monitor:
        decision = "monitor"
    else:
        decision = "discard"

    # Execution: live mode = real trade, paper mode = paper trade, off = log only
    executed = (
        state.trade_mode == "live"
        and decision in ("execute_full", "execute_half")
    )
    paper_trade = (
        state.trade_mode == "paper"
        and decision in ("execute_full", "execute_half")
    )

    # Look up AI-learned trade params for this source/pattern type
    source = candidate.get("source", "unknown")
    ai_params = await get_trade_params(source)

    if ai_params and ai_params.sample_size >= 10:
        # Enough data — use AI-learned params
        trade_tp_x = ai_params.optimal_tp_x
        trade_sl_pct = ai_params.optimal_sl_pct
        trade_position_pct = ai_params.optimal_position_pct
        params_source = "ai_learned"
    else:
        # Not enough data — use keybot defaults
        trade_tp_x = 3.0
        trade_sl_pct = 30.0
        trade_position_pct = 10.0
        params_source = "keybot_default"

    # Save to database silently — no Telegram messages
    await save_candidate(
        token_address=candidate.get("mint", ""),
        token_name=candidate.get("name"),
        confidence_score=confidence,
        fingerprint_score=fingerprint,
        insider_score=insider,
        chart_score=chart,
        rug_score=rug,
        caller_score=caller,
        market_score=market,
        decision=decision,
        executed=executed,
        source=source,
        chart_pattern=chart_pattern,
    )

    logger.info(
        "Agent5: %s (%s) confidence=%.1f decision=%s executed=%s "
        "chart=%s(%.0f) params=%s [tp=%.1fx sl=%.0f%% size=%.0f%%]",
        candidate.get("name", "?"), candidate.get("mint", "?")[:12],
        confidence, decision, executed,
        chart_pattern, chart,
        params_source, trade_tp_x, trade_sl_pct, trade_position_pct,
    )

    return {
        **candidate,
        "confidence_score":  confidence,
        "fingerprint_score": fingerprint,
        "insider_score":     insider,
        "chart_score":       chart,
        "chart_pattern":     chart_pattern,
        "rug_score":         rug,
        "caller_score":      caller,
        "market_score":      market,
        "decision":          decision,
        "executed":          executed,
        "paper_trade":       paper_trade,
        "trade_tp_x":        trade_tp_x,
        "trade_sl_pct":      trade_sl_pct,
        "trade_position_pct": trade_position_pct,
        "params_source":     params_source,
    }
