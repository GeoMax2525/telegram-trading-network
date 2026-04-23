"""
candidate_tracker.py — Tracks ALL candidates (not just traded ones).

Two jobs:
1. Confirmation gate: tokens must be seen 2+ times across scanner ticks
   before they can open a paper trade. Prevents instant pump-and-dumps.
2. Missed trade journal: tracks rejected candidates and checks if they
   pumped, so Agent 6 can learn from what it skipped.

The confirmation cache is in-memory (resets on restart).
The journal persists to DB via the candidates table.
"""

import logging
import time

logger = logging.getLogger(__name__)

# ── Confirmation gate ────────────────────────────────────────────────────────
# mint -> {"first_seen": timestamp, "count": int, "best_confidence": float}
_seen: dict[str, dict] = {}
_SEEN_TTL = 600  # forget tokens not seen in 10 minutes
MIN_SIGHTINGS = 2  # must be seen at least 2 ticks before trading
MIN_AGE_SECONDS = 30  # must have been first seen at least 30s ago


def record_sighting(mint: str, confidence: float) -> None:
    """Record that the scanner evaluated this token this tick."""
    now = time.time()
    entry = _seen.get(mint)
    if entry is None:
        _seen[mint] = {
            "first_seen": now,
            "count": 1,
            "best_confidence": confidence,
        }
    else:
        entry["count"] += 1
        entry["best_confidence"] = max(entry["best_confidence"], confidence)

    # Evict stale entries
    if len(_seen) > 1000:
        cutoff = now - _SEEN_TTL
        stale = [k for k, v in _seen.items() if v["first_seen"] < cutoff]
        for k in stale:
            del _seen[k]


def is_confirmed(mint: str) -> bool:
    """Has this token been seen enough times to allow a trade?"""
    entry = _seen.get(mint)
    if entry is None:
        return False
    now = time.time()
    age = now - entry["first_seen"]
    return entry["count"] >= MIN_SIGHTINGS and age >= MIN_AGE_SECONDS


def get_sighting_info(mint: str) -> dict | None:
    """Returns sighting info for logging."""
    return _seen.get(mint)
