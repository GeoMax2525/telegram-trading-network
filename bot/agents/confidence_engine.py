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
    get_token_by_mint,
    get_params,
)
from bot.agents.trade_profiles import match_pattern_types, resolve_trade_params

logger = logging.getLogger(__name__)

# MC-based default weight sets
MC_WEIGHTS = {
    "low": {  # under $100K
        "fingerprint": 0.28, "insider": 0.35, "chart": 0.05,
        "rug": 0.20, "caller": 0.08, "market": 0.04,
    },
    "mid": {  # $100K – $1M
        "fingerprint": 0.25, "insider": 0.30, "chart": 0.15,
        "rug": 0.18, "caller": 0.08, "market": 0.04,
    },
    "high": {  # over $1M
        "fingerprint": 0.20, "insider": 0.20, "chart": 0.30,
        "rug": 0.15, "caller": 0.10, "market": 0.05,
    },
}

# Legacy flat default (used as fallback)
DEFAULT_WEIGHTS = MC_WEIGHTS["mid"]


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

            # GMGN trending bonus = +10
            if getattr(token, "gmgn_trending", None):
                base += 10.0

    return round(min(base, 100.0), 1)


async def _score_insider(candidate: dict) -> float:
    """Score 0–100 based on insider wallet + GMGN smart money activity.

    Cluster boost: when Agent 2 has grouped wallets into coordinated
    clusters, multiple cluster members buying the same token is a much
    stronger signal than uncorrelated insider buys. We multiply the
    insider sub-score by 1.0 / 1.5 / 2.0 depending on how many wallets
    from the strongest matching cluster are on this token.
    """
    insider_count = candidate.get("insider_count", 0)
    gmgn_boost = candidate.get("gmgn_wallet_boost", 0)

    base = 30.0  # default no signal
    if insider_count >= 3:
        base = 100.0
    elif insider_count == 2:
        base = 80.0
    elif insider_count == 1:
        base = 60.0
    elif candidate.get("source") == "insider_wallet":
        base = 40.0

    # Check GMGN top traders for this token
    mint = candidate.get("mint", "")
    if mint and base < 80:
        try:
            from bot.agents.gmgn_agent import gmgn_top_traders
            traders = await gmgn_top_traders(mint)
            smart_count = sum(
                1 for t in traders
                if any(tag in (t.get("tags") or []) for tag in ("smart_degen", "smart_money"))
            )
            if smart_count >= 3:
                gmgn_boost += 25
            elif smart_count >= 1:
                gmgn_boost += 15
        except Exception:
            pass

    combined = min(100.0, base + gmgn_boost)

    # Cluster multiplier — strongest coordinated-group presence wins
    cluster_buy_count = candidate.get("cluster_buy_count", 0) or 0
    cluster_id = candidate.get("cluster_id_hit")
    if cluster_buy_count >= 3:
        boosted = min(100.0, combined * 2.0)
        logger.info(
            "Cluster signal: %s %d wallets → boosted insider score %.0f → %.0f",
            cluster_id or "?", cluster_buy_count, combined, boosted,
        )
        combined = boosted
    elif cluster_buy_count >= 2:
        boosted = min(100.0, combined * 1.5)
        logger.info(
            "Cluster signal: %s %d wallets → boosted insider score %.0f → %.0f",
            cluster_id or "?", cluster_buy_count, combined, boosted,
        )
        combined = boosted

    return combined


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


async def _score_rug(candidate: dict) -> float:
    """
    Returns 1–100 safety score (higher = safer).
    Checks both Rugcheck data and GMGN security.
    """
    # GMGN security check (if available)
    gmgn_bonus = 0
    mint = candidate.get("mint", "")
    if mint:
        try:
            from bot.agents.gmgn_agent import gmgn_token_security
            sec = await gmgn_token_security(mint)
            if sec:
                # GMGN security flags
                if sec.get("renounced_mint") and sec.get("renounced_freeze_account"):
                    gmgn_bonus += 10  # fully renounced = safer
                rug_ratio = float(sec.get("rug_ratio") or 0)
                if rug_ratio > 0.3:
                    gmgn_bonus -= 20  # high rug risk
                elif rug_ratio < 0.1:
                    gmgn_bonus += 5   # low rug risk
        except Exception:
            pass

    rc_norm = candidate.get("rugcheck_normalised")
    rc_raw = candidate.get("rugcheck")

    if rc_norm is not None:
        safety = max(1.0, min(100.0, 100.0 - (rc_norm * 10.0) + gmgn_bonus))
        logger.info("Rug score: normalised=%s gmgn=%+d → safety=%.0f", rc_norm, gmgn_bonus, safety)
        return safety

    if rc_raw is not None:
        safety = max(1.0, min(100.0, 100.0 - min(rc_raw, 1000) / 10.0 + gmgn_bonus))
        logger.info("Rug score: raw=%s → safety=%.0f", rc_raw, safety)
        return safety

    logger.debug("Rug score: no rugcheck data — defaulting to 40 + gmgn=%+d", gmgn_bonus)
    return max(1.0, min(100.0, 40.0 + gmgn_bonus))


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

async def _get_mc_weight_set(mcap: float) -> tuple[dict[str, float], str]:
    """Returns (weights_dict, weight_set_label) from DB params."""
    if mcap < 100_000:
        prefix, label = "low_mc_", "Low MC (<$100K)"
    elif mcap < 1_000_000:
        prefix, label = "mid_mc_", "Mid MC ($100K-$1M)"
    else:
        prefix, label = "high_mc_", "High MC (>$1M)"

    keys = [f"{prefix}{k}" for k in ("insider", "fingerprint", "chart", "rug", "caller", "market")]
    p = await get_params(*keys)

    weights = {
        "insider":     p.get(f"{prefix}insider", 0.25),
        "fingerprint": p.get(f"{prefix}fingerprint", 0.25),
        "chart":       p.get(f"{prefix}chart", 0.15),
        "rug":         p.get(f"{prefix}rug", 0.15),
        "caller":      p.get(f"{prefix}caller", 0.10),
        "market":      p.get(f"{prefix}market", 0.05),
    }

    # Normalize
    total = sum(weights.values())
    if total > 0:
        weights = {k: round(v / total, 4) for k, v in weights.items()}

    return weights, label


async def _load_weights(mcap: float) -> tuple[dict[str, float], str]:
    """
    Load MC-based weights from DB, blend with Agent 6 learned adjustments.
    Returns (weights, weight_set_label).
    """
    base, label = await _get_mc_weight_set(mcap)

    row = await get_current_weights()
    if row:
        learned = {
            "fingerprint": row.fingerprint_weight, "insider": row.insider_weight,
            "chart": row.chart_weight, "rug": row.rug_weight,
            "caller": row.caller_weight, "market": row.market_weight,
        }
        blended = {}
        for k in base:
            blended[k] = round(base[k] * 0.7 + learned[k] * 0.3, 4)
        total = sum(blended.values())
        if total > 0:
            blended = {k: round(v / total, 4) for k, v in blended.items()}
        return blended, label

    return base, label


async def score_candidate(candidate: dict) -> dict:
    """
    Score a single candidate from Agent 4.
    Returns enriched dict with confidence_score, component scores, and decision.
    All candidates are saved to the database silently.
    """
    # Defensive DEX allowlist check — scanner_agent already filters, but
    # this hard-stops any pre-graduation pump.fun bonding-curve token
    # (or any other unsupported DEX) that somehow reached Agent 5.
    from bot.scanner import ALLOWED_DEXES
    dex_id = (candidate.get("dex_id") or "").lower()
    if dex_id and dex_id not in ALLOWED_DEXES:
        logger.info(
            "Agent5: REJECTED %s — unsupported DEX %s (allowed: %s)",
            candidate.get("name", "?")[:20], dex_id, ",".join(sorted(ALLOWED_DEXES)),
        )
        return {
            "mint": candidate.get("mint"),
            "confidence_score": 0,
            "decision": "discard",
            "executed": False,
            "paper_trade": False,
            "reason": f"unsupported_dex:{dex_id}",
        }

    pattern = await get_pattern_by_type("winner_2x")
    mcap = candidate.get("mcap", 0) or 0
    weights, weight_set = await _load_weights(mcap)

    # Compute all 6 component scores
    fingerprint = await _score_fingerprint(candidate, pattern)
    insider     = await _score_insider(candidate)
    chart, chart_pattern = await _score_chart(candidate)
    rug         = await _score_rug(candidate)
    caller      = await _score_caller(candidate)
    market      = _score_market()

    # Weighted confidence score using MC-adjusted weights
    confidence = round(
        fingerprint * weights["fingerprint"]
        + insider   * weights["insider"]
        + chart     * weights["chart"]
        + rug       * weights["rug"]
        + caller    * weights["caller"]
        + market    * weights["market"],
        1,
    )

    # Name quality penalty — obvious scam/low-effort names get dinged
    _SPAM_KEYWORDS = {
        "psyop", "elon", "inu", "420", "cum", "porn", "nude", "nsfw",
        "rugpull", "scam", "honeypot", "fakeai", "ponzi",
    }
    token_name = (candidate.get("name") or "").lower()
    token_symbol = (candidate.get("symbol") or "").lower()
    name_combined = token_name + " " + token_symbol
    spam_hits = sum(1 for kw in _SPAM_KEYWORDS if kw in name_combined)
    if spam_hits >= 2:
        confidence = max(0, confidence - 20)
        logger.info("Agent5: name penalty -20 on %s (hits=%d)", token_name[:20], spam_hits)
    elif spam_hits == 1:
        confidence = max(0, confidence - 10)
        logger.info("Agent5: name penalty -10 on %s (hits=%d)", token_name[:20], spam_hits)

    # Hard gates for LIVE execution (both rug + chart must pass)
    rug_gate_pass = rug >= 50
    chart_gate_pass = chart >= 50
    live_gates_pass = rug_gate_pass and chart_gate_pass

    logger.info(
        "Agent5: %s mode=%s weights=%s conf=%.1f rug=%.0f(%s) chart=%.0f(%s)",
        candidate.get("name", "?")[:20], state.trade_mode, weight_set,
        confidence, rug, rug_gate_pass, chart, chart_gate_pass,
    )

    # Decision thresholds from DB (Agent 6 adjustable)
    tp = await get_params("conf_full_threshold", "conf_half_threshold", "conf_paper_threshold")
    t_full = tp["conf_full_threshold"]
    t_half = tp["conf_half_threshold"]
    t_paper = tp["conf_paper_threshold"]
    t_monitor = t_half - 10  # monitor = 10 below half

    # LIVE decision: requires chart + rug hard gates + high threshold
    if confidence >= t_full and live_gates_pass:
        decision = "execute_full"
    elif confidence >= t_half and live_gates_pass:
        decision = "execute_half"
    elif confidence >= t_monitor:
        decision = "monitor"
    else:
        decision = "discard"

    # LIVE execution
    executed = (
        state.trade_mode == "live"
        and decision in ("execute_full", "execute_half")
    )

    # PAPER execution: DB-driven confidence threshold + rug floor.
    # Previously had no hard gates, which let garbage tokens through
    # whenever the weighted score happened to land above the (low)
    # paper threshold. Rug floor at 40 keeps obvious scams out of
    # the learning corpus without being as strict as the live gate.
    paper_rug_floor_pass = rug >= 40
    paper_trade = (
        state.trade_mode == "paper"
        and confidence >= t_paper
        and paper_rug_floor_pass
    )

    logger.info(
        "Agent5: PAPER CHECK — %s mode=%s conf=%.1f threshold=%.0f rug=%.0f(floor=40) result=%s",
        candidate.get("name", "?")[:20], state.trade_mode, confidence, t_paper,
        rug,
        "TRIGGER" if paper_trade else f"SKIP(mode={state.trade_mode},conf={confidence:.1f},rug_ok={paper_rug_floor_pass})",
    )

    if paper_trade:
        logger.info(
            "PAPER TRADE ATTEMPT: %s score:%.1f mode:%s",
            candidate.get("name", "?"), confidence, state.trade_mode,
        )

    # Match every pattern_type this candidate belongs to, then resolve
    # combined TP/SL/trailing from ai_trade_params. The matcher also
    # reads insider_score/chart_score/caller_score from the candidate dict
    # so we merge the just-computed scores in before matching.
    matchable = {
        **candidate,
        "insider_score": insider,
        "chart_score":   chart,
        "caller_score":  caller,
    }
    pattern_tags = match_pattern_types(matchable)
    resolved = await resolve_trade_params(pattern_tags)

    trade_tp_x         = resolved["tp_x"]
    trade_sl_pct       = resolved["sl_pct"]
    trade_position_pct = resolved.get("position_pct", 10.0)  # learned per pattern_type
    trail_enabled      = resolved["trail_enabled"]
    trail_trigger      = resolved["trail_trigger"]
    trail_pct          = resolved["trail_pct"]
    profile_tag_csv    = ",".join(pattern_tags)
    params_source = (
        f"ai_learned({resolved['matched_rows']}/{len(pattern_tags)})"
        if resolved["matched_rows"] > 0
        else "defaults_fallback"
    )
    source = candidate.get("source", "unknown")

    # Diagnostic: show exactly which ai_trade_params rows fed the
    # final tp/sl. Used to verify learning is actually reaching the
    # opener — if tp/sl stays at baseline forever, this log shows it.
    logger.info(
        "Agent5 resolve [%s]: tags=%s matched=%s → tp=%.2fx sl=%.0f%%",
        candidate.get("name", "?")[:20],
        ",".join(pattern_tags) or "-",
        ",".join(resolved.get("matched_types") or []) or "none",
        trade_tp_x, trade_sl_pct,
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
        "weight_set":        weight_set,
        "trade_tp_x":        trade_tp_x,
        "trade_sl_pct":      trade_sl_pct,
        "trade_position_pct": trade_position_pct,
        "params_source":     params_source,
        "pattern_tags":      pattern_tags,
        "profile_tag":       profile_tag_csv,
        "trail_enabled":     trail_enabled,
        "trail_trigger":     trail_trigger,
        "trail_pct":         trail_pct,
    }
