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
    """Score 0–100 based on how well metrics match learned pattern buckets.
    Uses winner_2x as the primary signal, winner_5x as a bonus, and the
    rug fingerprint as a penalty — so the system learns from both wins AND
    losses. Includes pump.fun bonuses for social links and bonding curve."""
    mcap = candidate.get("mcap", 0)
    liquidity = candidate.get("liquidity", 0)
    ai_score = candidate.get("ai_score", 0)

    def _match_pattern(pat) -> float | None:
        if pat is None:
            return None
        sc = 0.0
        n = 0
        if pat.mcap_range_low and pat.mcap_range_high:
            n += 1
            low = pat.mcap_range_low * 0.5
            high = pat.mcap_range_high * 2.0
            if low <= mcap <= high:
                sc += 100.0
            elif mcap < low:
                sc += 30.0
            else:
                sc += 10.0
        if pat.avg_liquidity and pat.avg_liquidity > 0:
            n += 1
            ratio = min(liquidity / pat.avg_liquidity, 2.0)
            sc += ratio * 50.0
        if pat.avg_ai_score and pat.avg_ai_score > 0:
            n += 1
            ratio = min(ai_score / pat.avg_ai_score, 1.5)
            sc += ratio * 66.7
        if n == 0:
            return None
        raw = sc / n
        conf = (pat.confidence_score or 0) / 100.0
        return min(raw * conf + raw * (1 - conf * 0.3), 100)

    # Primary: winner_2x
    if pattern is None:
        base = 50.0
    else:
        w2x = _match_pattern(pattern)
        base = w2x if w2x is not None else 50.0

    # Bonus: winner_5x — high-conviction fingerprint match
    try:
        pat_5x = await get_pattern_by_type("winner_5x")
        w5x = _match_pattern(pat_5x)
        if w5x is not None and w5x > 70:
            base = min(base + 10.0, 100.0)
    except Exception:
        pass

    # Bonus: winner_10x — moonshot fingerprint (rare but very high signal)
    try:
        pat_10x = await get_pattern_by_type("winner_10x")
        w10x = _match_pattern(pat_10x)
        if w10x is not None and w10x > 75:
            base = min(base + 15.0, 100.0)
    except Exception:
        pass

    # Penalty: rug fingerprint — avoid repeating past trap shapes
    try:
        pat_rug = await get_pattern_by_type("rug")
        rug_match = _match_pattern(pat_rug)
        if rug_match is not None and rug_match > 70:
            base = max(base - 15.0, 0.0)
    except Exception:
        pass

    # Bonus: best_time — if current hour/day matches historically
    # profitable trading windows from the best_time pattern
    try:
        import json as _json2
        pat_time = await get_pattern_by_type("best_time")
        if pat_time and pat_time.best_hours:
            from datetime import datetime as _dt
            now_hour = _dt.utcnow().hour
            best_hours = _json2.loads(pat_time.best_hours)
            if now_hour in best_hours[:3]:
                base = min(base + 5.0, 100.0)
    except Exception:
        pass

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
    """Score 0–100 based on insider wallet activity, tier quality, recency,
    cluster coordination, and GMGN smart money.

    Tier weighting: tier-1 buys count 2x because those wallets have a
    proven track record of early-entry winners.

    Recency bonus: a buy in the last 5 minutes is a much stronger signal
    than one 25 minutes ago (which may already be priced in).

    Cluster boost: coordinated wallet groups buying the same token.
    """
    t1 = candidate.get("insider_tier_1_count", 0) or 0
    t2 = candidate.get("insider_tier_2_count", 0) or 0
    t3 = candidate.get("insider_tier_3_count", 0) or 0
    gmgn_boost = candidate.get("gmgn_wallet_boost", 0)
    buy_age_s = candidate.get("insider_buy_age_s")

    # Weighted insider strength: T1=2x, T2=1x, T3=0.5x
    # T3 includes most GMGN smart-money wallets — weaker signal
    # but still valuable since they have proven trading records.
    weighted = t1 * 2 + t2 + t3 * 0.5
    if weighted >= 5:
        base = 100.0
    elif weighted >= 3:
        base = 85.0
    elif weighted >= 2:
        base = 70.0
    elif weighted >= 1:
        base = 55.0
    elif candidate.get("source") == "insider_wallet":
        base = 40.0
    else:
        base = 30.0

    # Recency bonus — fresher buys are stronger signals
    if buy_age_s is not None and weighted > 0:
        if buy_age_s < 300:       # < 5 min
            base = min(base + 20, 100)
        elif buy_age_s < 900:     # < 15 min
            base = min(base + 10, 100)

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


CHART_MIN_MC = 500_000  # skip chart patterns below this MC — pure noise

async def _score_chart(candidate: dict) -> tuple[float, str]:
    """
    Calls Agent 7 (Chart Detector) to analyze chart patterns, then layers
    a volume-acceleration bonus on top. Returns (score 0-100, pattern_name).

    For tokens under $500K MC, chart scoring is skipped entirely — there
    isn't enough price history for patterns to be meaningful. Returns a
    neutral 50 + volume bonus only.
    """
    mcap = candidate.get("mcap", 0) or 0

    # Volume acceleration bonus applies at ALL MC levels — it's a
    # momentum signal, not a chart-structure signal.
    vol_bonus = 0
    vol_m5 = float(candidate.get("volume_m5") or 0)
    vol_h1 = float(candidate.get("volume_h1") or 0)
    if vol_m5 > 0 and vol_h1 > 0:
        pace_ratio = (vol_m5 * 12) / vol_h1
        if pace_ratio >= 3.0:
            vol_bonus = 15
        elif pace_ratio >= 2.0:
            vol_bonus = 8

    if mcap < CHART_MIN_MC:
        return min(50.0 + vol_bonus, 100.0), "skipped_low_mc"

    try:
        result = await analyze_chart(candidate)
        chart_score = result["chart_score"]
        pattern_name = result.get("pattern_name", "none")
        return min(chart_score + vol_bonus, 100.0), pattern_name
    except Exception as exc:
        logger.warning("Agent5: chart analysis failed for %s: %s",
                       candidate.get("mint", "?")[:12], exc)
        return 40.0, "error"


async def _score_rug(candidate: dict) -> float:
    """
    Returns 1–100 safety score (higher = safer).
    Deep checks via both Rugcheck and GMGN security + token info.
    Catches bundled tokens, botted volume, entrapment, and rug setups.
    """
    gmgn_bonus = 0
    gmgn_flags: list[str] = []
    mint = candidate.get("mint", "")
    if mint:
        try:
            from bot.agents.gmgn_agent import gmgn_token_security, gmgn_token_info

            # Deep security check
            sec = await gmgn_token_security(mint)
            if sec:
                if sec.get("renounced_mint") and sec.get("renounced_freeze_account"):
                    gmgn_bonus += 10
                    gmgn_flags.append("renounced")

                rug_ratio = float(sec.get("rug_ratio") or 0)
                if rug_ratio > 0.3:
                    gmgn_bonus -= 25
                    gmgn_flags.append(f"rug_ratio={rug_ratio:.0%}")
                elif rug_ratio < 0.1:
                    gmgn_bonus += 5

                # Bundler / bot detection
                bot_ratio = float(sec.get("bot_ratio") or sec.get("bluechip_ratio") or 0)
                if bot_ratio > 0.5:
                    gmgn_bonus -= 15
                    gmgn_flags.append(f"bot_ratio={bot_ratio:.0%}")

                # Entrapment ratio (honeypot-like behavior)
                entrap = float(sec.get("entrapment_ratio") or 0)
                if entrap > 0.2:
                    gmgn_bonus -= 20
                    gmgn_flags.append(f"entrapment={entrap:.0%}")

            # Deep token info — KOL presence, holder quality
            info = await gmgn_token_info(mint)
            if info:
                # KOL (Key Opinion Leader) buying is a positive signal
                kol_count = int(info.get("kol_count") or info.get("smart_buy_24h") or 0)
                if kol_count >= 3:
                    gmgn_bonus += 10
                    gmgn_flags.append(f"kol={kol_count}")
                elif kol_count >= 1:
                    gmgn_bonus += 5

                # Wash trading detection
                wash = float(info.get("wash_trading_ratio") or 0)
                if wash > 0.3:
                    gmgn_bonus -= 15
                    gmgn_flags.append(f"wash={wash:.0%}")

            if gmgn_flags:
                logger.info("Agent5 GMGN deep: %s → %s bonus=%+d",
                            mint[:12], ", ".join(gmgn_flags), gmgn_bonus)

        except Exception as exc:
            logger.warning("Agent5: GMGN deep check failed for %s: %s",
                           mint[:12], exc)

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

    logger.debug("Rug score: no rugcheck data — defaulting to 50 + gmgn=%+d", gmgn_bonus)
    return max(1.0, min(100.0, 50.0 + gmgn_bonus))


async def _score_caller(candidate: dict) -> float:
    """Score 0–100 based on whether any approved caller scanned this token."""
    mint = candidate.get("mint", "")
    if not mint:
        return 0.0
    scanned = await has_caller_scanned(mint)
    return 80.0 if scanned else 20.0


def _score_market() -> float:
    """Score 0-100 based on SOL 24h change and market regime."""
    sol_change = getattr(state, "sol_24h_change", 0.0) or 0.0
    regime = getattr(state, "market_regime", "NEUTRAL")

    if sol_change > 5.0:
        base = 80.0
    elif sol_change > 2.0:
        base = 65.0
    elif sol_change > -2.0:
        base = 50.0
    elif sol_change > -5.0:
        base = 35.0
    else:
        base = 20.0

    # Regime adjustment from Agent 6
    if regime == "GOOD":
        base = min(100, base + 10)
    elif regime == "BAD":
        base = max(0, base - 10)

    return round(base, 1)


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

    # TG signal boost — if a signal channel flagged this token, boost confidence
    tg_boost = 0.0
    mint = candidate.get("mint", "")
    if mint:
        try:
            from database.models import get_recent_tg_signal
            tg_sig = await get_recent_tg_signal(mint, within_minutes=30)
            if tg_sig:
                tg_params = await get_params(
                    "tg_signal_confidence_boost",
                    "tg_signal_volume_boost",
                    "tg_signal_clean_dev_boost",
                )
                tg_boost += tg_params.get("tg_signal_confidence_boost", 10.0)
                # Volume boost: high volume from signal
                if tg_sig.volume and tg_sig.volume > 50_000:
                    tg_boost += tg_params.get("tg_signal_volume_boost", 5.0)
                # Clean dev boost: 0% dev
                if tg_sig.dev_pct is not None and tg_sig.dev_pct == 0:
                    tg_boost += tg_params.get("tg_signal_clean_dev_boost", 5.0)
                confidence = min(100, confidence + tg_boost)
                logger.info(
                    "Agent5: TG signal boost +%.0f on %s (channel=%s)",
                    tg_boost, candidate.get("name", "?")[:20], tg_sig.channel,
                )
        except Exception as exc:
            logger.debug("Agent5: TG signal check failed: %s", exc)

    # Hard gates for LIVE execution (both rug + chart must pass)
    rug_gate_pass = rug >= 50
    chart_gate_pass = chart >= 50
    live_gates_pass = rug_gate_pass and chart_gate_pass

    logger.info(
        "Agent5: %s mode=%s weights=%s conf=%.1f rug=%.0f(%s) chart=%.0f(%s) tg=%+.0f",
        candidate.get("name", "?")[:20], state.trade_mode, weight_set,
        confidence, rug, rug_gate_pass, chart, chart_gate_pass, tg_boost,
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

    base_tp  = resolved["tp_x"]
    base_sl  = resolved["sl_pct"]
    trade_position_pct = resolved.get("position_pct", 10.0)
    trail_enabled      = resolved["trail_enabled"]
    trail_trigger      = resolved["trail_trigger"]
    trail_pct          = resolved["trail_pct"]

    # ── Per-trade adaptive TP/SL ─────────────────────────────────────
    # Override base TP/SL based on THIS trade's characteristics.
    # MC size: smaller tokens have more room to run but also dump harder.
    # Confidence: higher confidence = wider SL (give it room), tighter on low conf.
    # Momentum: already pumped = lower TP (less room), fresh = higher TP.

    pct_24h = candidate.get("price_change_24h", 0) or 0

    # MC-based TP/SL — memecoins need WIDE stops and REALISTIC targets.
    # A 20% SL on a memecoin is just selling normal volatility.
    # The trailing stop at 2x is the real exit, TP is the dream target.
    if mcap < 50_000:
        mc_tp = 5.0    # micro cap — high upside but TP must be reachable
        mc_sl = 35.0   # wide stop — these tokens wick 30% and recover
    elif mcap < 200_000:
        mc_tp = 4.0    # small cap
        mc_sl = 35.0   # still wide — normal volatility range
    elif mcap < 500_000:
        mc_tp = 3.0    # mid
        mc_sl = 30.0
    elif mcap < 2_000_000:
        mc_tp = 2.5    # larger
        mc_sl = 28.0
    else:
        mc_tp = 2.0    # big cap for memecoin
        mc_sl = 25.0

    # Momentum adjustment — already pumped = lower TP
    if pct_24h > 500:
        mc_tp = min(mc_tp, 2.0)
    elif pct_24h > 200:
        mc_tp = min(mc_tp, 3.0)

    # Confidence adjustment
    if confidence >= 75:
        mc_sl = min(mc_sl + 5, 40.0)   # strong conviction, very wide stop
        trail_trigger = 1.5
    elif confidence < 50:
        mc_sl = max(mc_sl - 5, 25.0)   # weak conviction but still reasonable
        mc_tp = min(mc_tp, 3.0)

    # Blend: weighted average of pattern-learned and per-trade adaptive
    # 40% learned (Agent 6 history), 60% adaptive (this trade's characteristics)
    trade_tp_x  = round(base_tp * 0.4 + mc_tp * 0.6, 2)
    trade_sl_pct = round(base_sl * 0.4 + mc_sl * 0.6, 1)

    # Enable trailing stop earlier for high-confidence trades
    if confidence >= 70 and not trail_enabled:
        trail_enabled = True
        trail_trigger = 1.5
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
