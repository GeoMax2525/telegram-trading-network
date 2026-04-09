"""
confidence_engine.py — Agent 5: The Confidence Engine

Called by Agent 4 (Scanner) for every candidate with match_score >= 50.
Produces a single confidence score (0–100) and a trade decision.

Confidence score — 6 weighted components:
  token_fingerprint_score × 0.25  — compare metrics to winning patterns (Agent 3)
  insider_wallet_signal   × 0.25  — is any Tier 1/2 wallet buying this token?
  chart_pattern_score     × 0.20  — placeholder 50 until Agent 7 is built
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
  - token must be < 4 hours old (enforced by scanner)
  - liquidity must be > $5K (enforced by scanner)

All candidates are saved to the candidates table for Agent 6 learning.
No Telegram messages — silent logging only.
"""

import logging

from bot import state
from database.models import (
    get_pattern_by_type,
    get_tier_wallets,
    has_caller_scanned,
    save_candidate,
    log_agent_run,
    get_current_weights,
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

def _score_fingerprint(candidate: dict, pattern) -> float:
    """Score 0–100 based on how well metrics match the winner_2x pattern."""
    if pattern is None:
        return 50.0  # no pattern data — neutral

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
        return 50.0

    raw = score / checks
    # Apply pattern confidence weighting
    conf = pattern.confidence_score / 100.0
    return round(min(raw * conf + raw * (1 - conf * 0.3), 100), 1)


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


def _score_chart() -> float:
    """Placeholder until Agent 7 (Chart Detector) is built."""
    return 50.0


def _score_rug(candidate: dict) -> float:
    """Score 0–100 based on rugcheck score. Higher rugcheck = safer = higher score."""
    rc = candidate.get("rugcheck")
    if rc is None:
        return 40.0  # no data — slightly below neutral

    # Rugcheck scores: 0-1000, 600+ is our minimum filter.
    # Scale 600-1000 to 50-100
    if rc >= 900:
        return 100.0
    if rc >= 800:
        return 85.0
    if rc >= 700:
        return 70.0
    if rc >= 600:
        return 55.0
    return 20.0  # below threshold (shouldn't reach here after scanner filter)


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
    fingerprint = _score_fingerprint(candidate, pattern)
    insider     = await _score_insider(candidate)
    chart       = _score_chart()
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

    # Hard gate: rug safety must be >= 50 for any execution
    rug_gate_pass = rug >= 50

    # Decision thresholds
    if confidence >= 80 and rug_gate_pass:
        decision = "execute_full"
    elif confidence >= 70 and rug_gate_pass:
        decision = "execute_half"
    elif confidence >= 60:
        decision = "monitor"
    else:
        decision = "discard"

    # Only mark as executed if autotrade is on AND decision is execute
    executed = (
        state.autotrade_enabled
        and decision in ("execute_full", "execute_half")
    )

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
    )

    logger.info(
        "Agent5: %s (%s) confidence=%.1f decision=%s executed=%s "
        "[fp=%.0f ins=%.0f chart=%.0f rug=%.0f call=%.0f mkt=%.0f]",
        candidate.get("name", "?"), candidate.get("mint", "?")[:12],
        confidence, decision, executed,
        fingerprint, insider, chart, rug, caller, market,
    )

    return {
        **candidate,
        "confidence_score":  confidence,
        "fingerprint_score": fingerprint,
        "insider_score":     insider,
        "chart_score":       chart,
        "rug_score":         rug,
        "caller_score":      caller,
        "market_score":      market,
        "decision":          decision,
        "executed":          executed,
    }
