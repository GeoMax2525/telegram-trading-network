"""
pattern_engine.py — Agent 3: Pattern Engine

Runs every 24 hours. Analyzes all closed scans with known outcomes to
derive statistical fingerprints of winning tokens vs rugs.

Buckets analyzed:
  winner_2x   — peak_multiplier >= 2
  winner_5x   — peak_multiplier >= 5
  winner_10x  — peak_multiplier >= 10
  rug         — close_reason in (rug_mc, rug_liquidity)
  best_time   — hour/day-of-week win-rate across all closed scans

Metrics computed per bucket (from available DB data):
  avg/min/max entry market cap
  avg/min entry liquidity
  average AI score
  average Rugcheck score (joined from Tokens table)
  best hours of day  (UTC, top-3 by win frequency)
  best days of week  (0=Mon, top-3 by win frequency)
  confidence score   (capped at 100, grows with sample count)

Caller reliability is computed and stored in the AgentLog notes.

Patterns are upserted by pattern_type so each run refreshes the data.
"""

import asyncio
import json
import logging
from collections import Counter, defaultdict
from statistics import mean, median

from database.models import (
    get_closed_scans_for_analysis,
    get_tokens_by_mints,
    upsert_pattern,
    log_agent_run,
    get_last_agent_run,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 86_400   # 24 hours
STARTUP_DELAY = 120      # seconds after bot start
MIN_SAMPLES   = 3        # minimum scans needed to emit a pattern


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_mean(values: list) -> float | None:
    cleaned = [v for v in values if v is not None]
    return round(mean(cleaned), 2) if cleaned else None


def _safe_min(values: list) -> float | None:
    cleaned = [v for v in values if v is not None]
    return round(min(cleaned), 2) if cleaned else None


def _safe_max(values: list) -> float | None:
    cleaned = [v for v in values if v is not None]
    return round(max(cleaned), 2) if cleaned else None


def _confidence(sample_count: int) -> float:
    """
    Confidence grows with sample count, capped at 100.
    < 5 samples → low confidence (0-40)
    5-20 samples → medium (40-80)
    > 20 samples → high (80-100)
    """
    return round(min(sample_count * 5, 100), 1)


def _best_n(counter: Counter, n: int = 3) -> list[int]:
    return [k for k, _ in counter.most_common(n)]


# ── Fingerprint computation ───────────────────────────────────────────────────

def _compute_fingerprint(
    pattern_type: str,
    outcome_threshold: str,
    scans: list,
    token_map: dict,
) -> dict:
    """Compute statistical fingerprint for a group of scans."""
    entry_mcaps = [s.entry_price     for s in scans if s.entry_price]
    liquidities = [s.entry_liquidity for s in scans if s.entry_liquidity]
    ai_scores   = [s.ai_score        for s in scans if s.ai_score]
    rc_scores   = [
        token_map[s.contract_address].rugcheck_score
        for s in scans
        if s.contract_address in token_map
        and token_map[s.contract_address].rugcheck_score is not None
    ]

    hour_counter = Counter(
        s.scanned_at.hour for s in scans if s.scanned_at
    )
    day_counter  = Counter(
        s.scanned_at.weekday() for s in scans if s.scanned_at
    )

    return {
        "pattern_type":      pattern_type,
        "outcome_threshold": outcome_threshold,
        "sample_count":      len(scans),
        "avg_entry_mcap":    _safe_mean(entry_mcaps),
        "mcap_range_low":    _safe_min(entry_mcaps),
        "mcap_range_high":   _safe_max(entry_mcaps),
        "avg_liquidity":     _safe_mean(liquidities),
        "min_liquidity":     _safe_min(liquidities),
        "avg_ai_score":      _safe_mean(ai_scores),
        "avg_rugcheck_score":_safe_mean(rc_scores),
        "best_hours":        json.dumps(_best_n(hour_counter, 3)),
        "best_days":         json.dumps(_best_n(day_counter, 3)),
        "confidence_score":  _confidence(len(scans)),
    }


def _compute_best_time_pattern(all_scans: list) -> dict | None:
    """
    Finds which hours/days have the highest win rate (peak_mult >= 2).
    Returns a fingerprint dict or None if insufficient data.
    """
    if len(all_scans) < MIN_SAMPLES:
        return None

    # Hour-level win rates
    hour_wins  = defaultdict(int)
    hour_total = defaultdict(int)
    day_wins   = defaultdict(int)
    day_total  = defaultdict(int)

    for s in all_scans:
        if not s.scanned_at:
            continue
        h = s.scanned_at.hour
        d = s.scanned_at.weekday()
        is_win = (s.peak_multiplier or 0) >= 2

        hour_total[h] += 1
        day_total[d]  += 1
        if is_win:
            hour_wins[h] += 1
            day_wins[d]  += 1

    overall_wr = sum(hour_wins.values()) / max(sum(hour_total.values()), 1)

    # Hours with above-average win rate, sorted by win rate desc
    hour_rates  = {
        h: hour_wins[h] / hour_total[h]
        for h in hour_total
        if hour_total[h] >= 2
    }
    best_hours = sorted(hour_rates, key=lambda h: hour_rates[h], reverse=True)[:5]

    day_rates = {
        d: day_wins[d] / day_total[d]
        for d in day_total
        if day_total[d] >= 2
    }
    best_days = sorted(day_rates, key=lambda d: day_rates[d], reverse=True)[:3]

    return {
        "pattern_type":      "best_time",
        "outcome_threshold": "2x",
        "sample_count":      len(all_scans),
        "avg_entry_mcap":    None,
        "mcap_range_low":    None,
        "mcap_range_high":   None,
        "avg_liquidity":     None,
        "min_liquidity":     None,
        "avg_ai_score":      None,
        "avg_rugcheck_score":None,
        "best_hours":        json.dumps(best_hours),
        "best_days":         json.dumps(best_days),
        "confidence_score":  _confidence(len(all_scans)),
    }


# ── Caller reliability ────────────────────────────────────────────────────────

def _compute_caller_reliability(scans: list) -> str:
    """
    Returns a short summary string of caller performance for AgentLog notes.
    Top 5 callers by total points, with win rate and avg multiple.
    """
    caller_stats: dict[str, dict] = defaultdict(
        lambda: {"wins": 0, "total": 0, "points": 0.0}
    )
    for s in scans:
        caller = s.scanned_by or "unknown"
        caller_stats[caller]["total"] += 1
        if (s.peak_multiplier or 0) >= 2:
            caller_stats[caller]["wins"] += 1
        caller_stats[caller]["points"] += s.peak_multiplier or 1.0

    ranked = sorted(
        caller_stats.items(),
        key=lambda x: x[1]["points"],
        reverse=True,
    )[:5]

    lines = []
    for caller, stats in ranked:
        total = stats["total"]
        wins  = stats["wins"]
        wr    = round(wins / total * 100) if total else 0
        pts   = round(stats["points"], 1)
        lines.append(f"@{caller}: {total} calls, {wr}% win rate, {pts} pts")

    return " | ".join(lines) if lines else "no data"


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single engine tick. Returns (scans_analyzed, patterns_saved).
    """
    scans = await get_closed_scans_for_analysis()
    if not scans:
        await log_agent_run(
            "pattern_engine", tokens_found=0, tokens_saved=0,
            notes="no closed scans with outcome data",
        )
        logger.info("Pattern Engine: no closed scans to analyze")
        return 0, 0

    logger.info("Pattern Engine: analyzing %d closed scan(s)", len(scans))

    # Join with Token table for rugcheck scores
    mints     = [s.contract_address for s in scans]
    token_map = await get_tokens_by_mints(mints)

    # Bucket scans by outcome
    winners_2x  = [s for s in scans if (s.peak_multiplier or 0) >= 2]
    winners_5x  = [s for s in scans if (s.peak_multiplier or 0) >= 5]
    winners_10x = [s for s in scans if (s.peak_multiplier or 0) >= 10]
    rugs        = [
        s for s in scans
        if s.close_reason in ("rug_mc", "rug_liquidity")
    ]

    patterns_saved = 0

    buckets = [
        ("winner_2x",  "2x",  winners_2x),
        ("winner_5x",  "5x",  winners_5x),
        ("winner_10x", "10x", winners_10x),
        ("rug",        "rug", rugs),
    ]

    for ptype, threshold, bucket in buckets:
        if len(bucket) < MIN_SAMPLES:
            logger.info(
                "Pattern Engine: skipping %s — only %d samples (min %d)",
                ptype, len(bucket), MIN_SAMPLES,
            )
            continue

        fp = _compute_fingerprint(ptype, threshold, bucket, token_map)
        await upsert_pattern(**fp)
        patterns_saved += 1
        logger.info(
            "Pattern Engine: %s pattern saved — %d samples, confidence=%.0f",
            ptype, fp["sample_count"], fp["confidence_score"],
        )

    # Best-time pattern across all scans
    time_fp = _compute_best_time_pattern(scans)
    if time_fp and len(scans) >= MIN_SAMPLES:
        await upsert_pattern(**time_fp)
        patterns_saved += 1
        logger.info("Pattern Engine: best_time pattern saved")

    # Caller reliability (logged as notes, not stored as a pattern)
    caller_summary = _compute_caller_reliability(scans)

    await log_agent_run(
        "pattern_engine",
        tokens_found=len(scans),
        tokens_saved=patterns_saved,
        notes=f"callers: {caller_summary}",
    )

    logger.info(
        "Pattern Engine: done — scans=%d patterns_saved=%d",
        len(scans), patterns_saved,
    )
    return len(scans), patterns_saved


# ── Background loop ───────────────────────────────────────────────────────────

async def pattern_engine_loop() -> None:
    """Runs the pattern engine once per 24 hours."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Pattern Engine agent started — running every %dh", POLL_INTERVAL // 3600)
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Pattern Engine loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
