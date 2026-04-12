"""
trade_profiles.py — pattern_type matcher and resolver

Single source of truth for "which pattern_types does this candidate match"
and "given a list of matched pattern_types, what TP/SL/trail should we use".

No new concepts: everything routes through the existing pattern_type string
and the existing ai_trade_params table. A candidate can match multiple
pattern_types simultaneously — we combine rows with max(TP) / min(SL) so
the strongest signal drives the upside target and the most protective
signal drives the downside.

Pattern types
  Legacy (scanner source):
    new_launch, insider_wallet, volume_spike
  MC bucket (exactly one matches):
    low_mc   — under $50K
    mid_mc   — $50K to $500K
    high_mc  — $500K and up
  Signal quality (zero or more):
    high_chart   — chart_score > 75
    high_caller  — caller_score >= 70 (effectively "any approved caller scanned")
    insider_wallet is ALSO matched here when insider_score >= 60
"""

import logging

from database.models import (
    AsyncSessionLocal,
    AITradeParams,
    select,
    get_params,
)

logger = logging.getLogger(__name__)

# Thresholds — single source of truth for every consumer
MC_LOW_CUTOFF   = 50_000
MC_HIGH_CUTOFF  = 500_000
CHART_MIN       = 75       # "pattern score > 75"
CALLER_MIN      = 70       # caller_score is binary 20/80; 70 means "scanned"
INSIDER_MIN     = 60       # "any tier insider" — insider_score 60+ == 1+ insiders

# Complete list of known pattern_types — used for seeding ai_trade_params
ALL_PATTERN_TYPES = [
    "new_launch",
    "insider_wallet",
    "volume_spike",
    "low_mc",
    "mid_mc",
    "high_mc",
    "high_chart",
    "high_caller",
]

# Default rows seeded on first boot. Tuned per user spec.
DEFAULT_AI_TRADE_PARAMS = {
    "new_launch":     {"tp_x": 3.0, "sl_pct": 30.0, "trail_trigger": 0.50, "trail_on": 0},
    "insider_wallet": {"tp_x": 4.0, "sl_pct": 25.0, "trail_trigger": 0.50, "trail_on": 0},
    "volume_spike":   {"tp_x": 3.0, "sl_pct": 30.0, "trail_trigger": 0.50, "trail_on": 0},
    "low_mc":         {"tp_x": 3.0, "sl_pct": 30.0, "trail_trigger": 0.50, "trail_on": 0},
    "mid_mc":         {"tp_x": 3.0, "sl_pct": 30.0, "trail_trigger": 0.50, "trail_on": 0},
    "high_mc":        {"tp_x": 3.0, "sl_pct": 25.0, "trail_trigger": 0.50, "trail_on": 0},
    "high_chart":     {"tp_x": 3.5, "sl_pct": 30.0, "trail_trigger": 0.50, "trail_on": 0},
    "high_caller":    {"tp_x": 3.5, "sl_pct": 25.0, "trail_trigger": 0.50, "trail_on": 0},
}


def match_pattern_types(candidate: dict) -> list[str]:
    """
    Returns the list of pattern_types this candidate matches.

    Always returns at least one MC bucket. Additive signal-quality tags
    stack on top. The candidate's scanner `source` is added as a legacy tag.
    """
    tags: list[str] = []

    # Legacy source tag (scanner assigns exactly one of the 3)
    source = (candidate.get("source") or "").lower()
    if source in ("new_launch", "insider_wallet", "volume_spike"):
        tags.append(source)

    # MC bucket — exactly one
    mcap = candidate.get("mcap") or candidate.get("entry_mc") or 0
    if mcap < MC_LOW_CUTOFF:
        tags.append("low_mc")
    elif mcap < MC_HIGH_CUTOFF:
        tags.append("mid_mc")
    else:
        tags.append("high_mc")

    # Signal-quality additives
    if (candidate.get("chart_score") or 0) > CHART_MIN:
        tags.append("high_chart")
    if (candidate.get("caller_score") or 0) >= CALLER_MIN:
        tags.append("high_caller")

    # Also tag insider_wallet when the insider signal is strong, even if
    # the scanner source wasn't "insider_wallet" — lets a volume_spike
    # with multiple insider buyers get the insider_wallet params too.
    if "insider_wallet" not in tags and (candidate.get("insider_score") or 0) >= INSIDER_MIN:
        tags.append("insider_wallet")

    return tags


async def _fetch_rows(pattern_types: list[str]) -> dict:
    """Load ai_trade_params rows for the given pattern_types."""
    if not pattern_types:
        return {}
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AITradeParams).where(AITradeParams.pattern_type.in_(pattern_types))
        )
        rows = result.scalars().all()
    return {r.pattern_type: r for r in rows}


async def resolve_trade_params(pattern_types: list[str]) -> dict:
    """
    Look up every matched pattern_type in ai_trade_params and combine:
        TP    = max(optimal_tp_x)    across matched rows  — most aggressive
        SL    = min(optimal_sl_pct)  across matched rows  — most protective
        TRAIL = enabled if global kill switch ON AND any matched row
                has trail_sl_enabled=1; trigger = min across those rows
                (earliest activation = most protective)

    Returns a dict with keys:
        tp_x            — float (e.g. 4.0)
        sl_pct          — float in percent (e.g. 25.0)
        trail_enabled   — bool
        trail_trigger   — float (e.g. 0.50 = 50% above entry)
        trail_pct       — float (e.g. 0.20 = 20% below peak)
        matched_rows    — int  (how many rows contributed)
        matched_types   — list[str] (the rows we actually found)
    """
    rows = await _fetch_rows(pattern_types)

    tp_candidates: list[float] = []
    sl_candidates: list[float] = []
    trail_triggers: list[float] = []

    for pt in pattern_types:
        row = rows.get(pt)
        if row is None:
            continue
        tp_candidates.append(float(row.optimal_tp_x or 3.0))
        sl_candidates.append(float(row.optimal_sl_pct or 30.0))
        if int(getattr(row, "trail_sl_enabled", 0) or 0) == 1:
            trail_triggers.append(float(getattr(row, "trail_sl_trigger_pct", 0.50) or 0.50))

    # Globals — kill switch and trail distance
    g = await get_params("trail_sl_enabled", "trail_sl_pct")
    global_trail_on = g.get("trail_sl_enabled", 0.0) >= 0.5

    tp_x  = max(tp_candidates) if tp_candidates else 3.0
    sl_pct = min(sl_candidates) if sl_candidates else 30.0

    trail_enabled = bool(global_trail_on and trail_triggers)
    trail_trigger = min(trail_triggers) if trail_triggers else 0.50
    trail_pct     = float(g.get("trail_sl_pct", 0.20))

    return {
        "tp_x":          tp_x,
        "sl_pct":        sl_pct,
        "trail_enabled": trail_enabled,
        "trail_trigger": trail_trigger,
        "trail_pct":     trail_pct,
        "matched_rows":  len(tp_candidates),
        "matched_types": list(rows.keys()),
    }
