"""
trade_profiles.py — pattern_type matcher and resolver

Single source of truth for "which pattern_types does this candidate match"
and "given a list of matched pattern_types, what TP/SL/trail should we use".

No new concepts: everything routes through the existing pattern_type string
and the existing ai_trade_params table. A candidate can match multiple
pattern_types simultaneously — we combine rows with max(TP) / min(SL) so
the strongest signal drives the upside target and the most protective
signal drives the downside.

Pattern type catalog
  Scanner source (exactly one):
    new_launch, insider_wallet, volume_spike
  MC bucket (exactly one):
    low_mc   — under $50K
    mid_mc   — $50K to $500K
    high_mc  — $500K and up
  Signal quality (zero or more):
    high_chart   — chart_score > 75
    high_caller  — caller_score >= 70
  Timing (zero or more):
    early_entry  — token < 30 min old at scan
    late_entry   — token > 4 h old at scan
    weekend_trade, asia_hours, us_hours
  Momentum:
    accelerating_volume — 5m volume pace > 3x hourly average
  Wallet:
    tier1_insider, tier2_insider, multi_insider
  Social/signal:
    trending_gmgn, smart_money_gmgn
  Safety (rugcheck /report):
    lp_burned, low_dev, high_holders, low_concentration
  Market:
    sol_up, sol_down
  Chart patterns (exact string match on chart_detector output):
    bull_flag_confirmed, launchpad_setup, insider_accumulation,
    fakeout_recovery, double_bottom, ascending_triangle
"""

import logging
from datetime import datetime

from bot import state as app_state
from database.models import (
    AsyncSessionLocal,
    AITradeParams,
    select,
    get_params,
)

logger = logging.getLogger(__name__)

# ── Thresholds ──────────────────────────────────────────────────────────────
MC_LOW_CUTOFF        = 50_000
MC_HIGH_CUTOFF       = 500_000
CHART_MIN            = 75      # pattern score > 75
CALLER_MIN           = 70      # caller_score binary 20/80; 70 means scanned
INSIDER_MIN          = 60      # insider_score 60+ == 1+ insiders

# Timing
EARLY_ENTRY_MINUTES  = 30      # token < 30 min old
LATE_ENTRY_MINUTES   = 240     # token > 4 h old
ASIA_HOUR_START      = 0       # UTC
ASIA_HOUR_END        = 8       # exclusive
US_HOUR_START        = 13
US_HOUR_END          = 21      # exclusive

# Market regime (SOL 24h change thresholds in PERCENT)
SOL_UP_PCT           = 3.0
SOL_DOWN_PCT         = -3.0

# Momentum
VOL_ACCEL_RATIO      = 3.0     # 5min pace vs hourly average

# Safety (rugcheck)
LOW_DEV_PCT          = 2.0     # dev wallet under 2% of supply
HIGH_HOLDERS_MIN     = 100     # 100+ unique holders
LOW_CONCENTRATION    = 30.0    # top-10 holders under 30%

# Chart pattern name → pattern_type key
# (chart_detector emits the left side; the right side is what lands in
# ai_trade_params. Note "bull_flag_confirmed" has a _confirmed suffix in
# the pattern_type name per the user spec but the chart detector emits
# the plain name — we map here.)
CHART_PATTERN_MAP = {
    "bull_flag":            "bull_flag_confirmed",
    "launchpad_setup":      "launchpad_setup",
    "insider_accumulation": "insider_accumulation",
    "fakeout_recovery":     "fakeout_recovery",
    "double_bottom":        "double_bottom",
    "ascending_triangle":   "ascending_triangle",
}

# ── Full pattern_type catalog ───────────────────────────────────────────────
ALL_PATTERN_TYPES = [
    # Scanner source
    "new_launch", "insider_wallet", "volume_spike",
    # MC bucket
    "low_mc", "mid_mc", "high_mc",
    # Signal quality
    "high_chart", "high_caller",
    # Timing
    "early_entry", "late_entry", "weekend_trade", "asia_hours", "us_hours",
    # Momentum
    "accelerating_volume",
    # Wallet
    "tier1_insider", "tier2_insider", "multi_insider",
    # Social/signal
    "trending_gmgn", "smart_money_gmgn",
    # Safety
    "lp_burned", "low_dev", "high_holders", "low_concentration",
    # Market
    "sol_up", "sol_down",
    # Chart patterns (mapped from chart_detector output)
    "bull_flag_confirmed", "launchpad_setup", "insider_accumulation",
    "fakeout_recovery", "double_bottom", "ascending_triangle",
]

# Default rows seeded on first boot. Baseline tp=3.0 / sl=30.0 / trail off
# for all new types — Agent 6 learns per-type from outcomes over time.
# The earlier 8 retain their user-tuned baselines.
def _default(tp=3.0, sl=30.0, trig=0.50, on=0):
    return {"tp_x": tp, "sl_pct": sl, "trail_trigger": trig, "trail_on": on}

DEFAULT_AI_TRADE_PARAMS = {
    # Originals (baseline values from user's first spec)
    "new_launch":     _default(3.0, 30.0),
    "insider_wallet": _default(4.0, 25.0),
    "volume_spike":   _default(3.0, 30.0),
    "low_mc":         _default(3.0, 30.0),
    "mid_mc":         _default(3.0, 30.0),
    "high_mc":        _default(3.0, 25.0),
    "high_chart":     _default(3.5, 30.0),
    "high_caller":    _default(3.5, 25.0),
    # New rows — uniform baselines, learned from outcomes
    "early_entry":            _default(),
    "late_entry":             _default(),
    "weekend_trade":          _default(),
    "asia_hours":             _default(),
    "us_hours":               _default(),
    "accelerating_volume":    _default(),
    "tier1_insider":          _default(),
    "tier2_insider":          _default(),
    "multi_insider":          _default(),
    "trending_gmgn":          _default(),
    "smart_money_gmgn":       _default(),
    "lp_burned":              _default(),
    "low_dev":                _default(),
    "high_holders":           _default(),
    "low_concentration":      _default(),
    "sol_up":                 _default(),
    "sol_down":               _default(),
    "bull_flag_confirmed":    _default(),
    "launchpad_setup":        _default(),
    "insider_accumulation":   _default(),
    "fakeout_recovery":       _default(),
    "double_bottom":          _default(),
    "ascending_triangle":     _default(),
}


def match_pattern_types(candidate: dict, now: datetime | None = None) -> list[str]:
    """
    Returns the list of pattern_types this candidate matches at scan time.

    Every tag evaluated here must correspond to a signal that actually
    lives on the candidate dict (set by scanner_agent._evaluate_candidate)
    or module-level state. Signals that aren't wired yet (Group C) are
    NOT evaluated — they'd produce dead matchers.

    `now` is injected for testability; defaults to datetime.utcnow().
    """
    tags: list[str] = []
    now = now or datetime.utcnow()

    # ── Scanner source ───────────────────────────────────────────────
    source = (candidate.get("source") or "").lower()
    if source in ("new_launch", "insider_wallet", "volume_spike"):
        tags.append(source)

    # ── MC bucket (exactly one) ──────────────────────────────────────
    mcap = candidate.get("mcap") or candidate.get("entry_mc") or 0
    if mcap < MC_LOW_CUTOFF:
        tags.append("low_mc")
    elif mcap < MC_HIGH_CUTOFF:
        tags.append("mid_mc")
    else:
        tags.append("high_mc")

    # ── Signal quality ───────────────────────────────────────────────
    chart_score = candidate.get("chart_score") or 0
    caller_score = candidate.get("caller_score") or 0
    insider_score = candidate.get("insider_score") or 0
    if chart_score > CHART_MIN:
        tags.append("high_chart")
    if caller_score >= CALLER_MIN:
        tags.append("high_caller")
    if "insider_wallet" not in tags and insider_score >= INSIDER_MIN:
        tags.append("insider_wallet")

    # ── Timing ───────────────────────────────────────────────────────
    age_minutes = candidate.get("age_minutes")
    if isinstance(age_minutes, (int, float)):
        if age_minutes < EARLY_ENTRY_MINUTES:
            tags.append("early_entry")
        elif age_minutes > LATE_ENTRY_MINUTES:
            tags.append("late_entry")

    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        tags.append("weekend_trade")
    if ASIA_HOUR_START <= now.hour < ASIA_HOUR_END:
        tags.append("asia_hours")
    if US_HOUR_START <= now.hour < US_HOUR_END:
        tags.append("us_hours")

    # ── Momentum: accelerating_volume ────────────────────────────────
    # 5-minute pace × 12 ≥ 1-hour average × VOL_ACCEL_RATIO
    # (mirrors the volume_spike source heuristic from scanner_agent)
    v5 = candidate.get("volume_m5") or 0
    vh = candidate.get("volume_h1") or 0
    if v5 and vh and (v5 * 12) >= (vh * VOL_ACCEL_RATIO):
        tags.append("accelerating_volume")

    # ── Wallet ───────────────────────────────────────────────────────
    t1 = candidate.get("insider_tier_1_count") or 0
    t2 = candidate.get("insider_tier_2_count") or 0
    insider_count = candidate.get("insider_count") or 0
    if t1 >= 1:
        tags.append("tier1_insider")
    if t2 >= 1:
        tags.append("tier2_insider")
    if insider_count >= 2 or (t1 + t2) >= 2:
        tags.append("multi_insider")

    # ── Social/signal: GMGN flags ────────────────────────────────────
    if candidate.get("gmgn_trending"):
        tags.append("trending_gmgn")
    if candidate.get("gmgn_smart_money"):
        tags.append("smart_money_gmgn")

    # ── Safety ───────────────────────────────────────────────────────
    if candidate.get("lp_burned") is True:
        tags.append("lp_burned")
    dev_pct = candidate.get("dev_wallet_pct")
    if isinstance(dev_pct, (int, float)) and dev_pct < LOW_DEV_PCT:
        tags.append("low_dev")
    holder_count = candidate.get("holder_count")
    if isinstance(holder_count, (int, float)) and holder_count >= HIGH_HOLDERS_MIN:
        tags.append("high_holders")
    concentration = candidate.get("top_10_concentration")
    if isinstance(concentration, (int, float)) and concentration < LOW_CONCENTRATION:
        tags.append("low_concentration")

    # ── Market regime (global SOL 24h change from learning_loop) ─────
    sol_change = getattr(app_state, "sol_24h_change", 0.0) or 0.0
    if sol_change > SOL_UP_PCT:
        tags.append("sol_up")
    elif sol_change < SOL_DOWN_PCT:
        tags.append("sol_down")

    # ── Chart patterns (mapped from chart_detector emission) ─────────
    chart_pattern = candidate.get("chart_pattern")
    if chart_pattern and chart_pattern in CHART_PATTERN_MAP:
        mapped = CHART_PATTERN_MAP[chart_pattern]
        if mapped not in tags:
            tags.append(mapped)

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
