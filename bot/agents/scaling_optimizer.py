"""
scaling_optimizer.py — the LEARNING loop for the SmartScalingExitManager.

Periodically reads closed paper trades per canonical type (high_conviction /
conservative / bundle) and nudges that type's runner trail width based on what
actually happened, using data we already collect:

  • peak_after_close — did the token make a NEW high AFTER we exited? If we keep
    exiting before the top, WIDEN the trail (let runners ride longer).
  • capture vs peak — if we ride up but give back too much before exiting
    (captured << peak) and it did NOT run after, TIGHTEN the trail.

Bounded, slow, and gated: small steps, a minimum sample per type, and hard
clamps so it can never run away. Tuning is OFF unless scaling_learn_enabled=1.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from bot.smart_scaling_exit import MIN_TRAIL_PCT, MAX_TRAIL_PCT

logger = logging.getLogger(__name__)

STEP = 0.03                 # how much to nudge the trail per cycle
EARLY_EXIT_THRESHOLD = 0.30  # >30% of runners made a new high after we left → widen
GIVEBACK_RATIO = 0.50        # captured < 50% of peak (and didn't run after) → tighten


def _canonical_type(pattern: str) -> str:
    p = (pattern or "").lower()
    if "bundle" in p:
        return "bundle"
    if "tg_signal" in p or "algo:" in p:
        return "high_conviction"
    return "conservative"


async def _tune_once() -> None:
    from database.models import (
        AsyncSessionLocal, select, PaperTrade, get_params,
        get_scaling_config, update_scaling_config,
    )
    cfg = await get_params(
        "scaling_learn_enabled", "scaling_optimizer_min_trades",
    )
    if float(cfg.get("scaling_learn_enabled", 0.0) or 0) < 0.5:
        return
    min_trades = int(float(cfg.get("scaling_optimizer_min_trades", 20.0) or 20.0))

    cutoff = datetime.utcnow() - timedelta(days=14)
    async with AsyncSessionLocal() as s:
        trades = list((await s.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.closed_at >= cutoff,
                PaperTrade.peak_multiple >= 2.0,   # only runners inform the trail
            )
        )).scalars().all())

    # Bucket runners by canonical type.
    buckets: dict = {"high_conviction": [], "conservative": [], "bundle": []}
    for t in trades:
        buckets[_canonical_type(t.pattern_type)].append(t)

    for stype, rows in buckets.items():
        if len(rows) < min_trades:
            logger.info("scaling_optimizer: %s — %d runners (< %d), skip",
                        stype, len(rows), min_trades)
            continue

        n = len(rows)
        caps = [1.0 + (t.paper_pnl_sol or 0) / (t.paper_sol_spent or 1.0) for t in rows]
        peaks = [t.peak_multiple or 1.0 for t in rows]
        avg_cap = sum(caps) / n
        avg_peak = sum(peaks) / n
        # Fraction that made a NEW high after we exited (we left money on table).
        early = sum(1 for t in rows
                    if (t.peak_after_close or 0) > (t.peak_mc or 0) * 1.1) / n
        ratio = (avg_cap / avg_peak) if avg_peak > 0 else 1.0

        current = await get_scaling_config(stype)
        cur_trail = float(current["runner_trail_pct"])

        new_trail = cur_trail
        reason = "no change"
        if early > EARLY_EXIT_THRESHOLD:
            new_trail = min(cur_trail + STEP, MAX_TRAIL_PCT)
            reason = f"early-exit {early:.0%} > {EARLY_EXIT_THRESHOLD:.0%} → widen"
        elif ratio < GIVEBACK_RATIO:
            new_trail = max(cur_trail - STEP, MIN_TRAIL_PCT)
            reason = f"capture {ratio:.0%} of peak (giveback) → tighten"

        if abs(new_trail - cur_trail) > 1e-9:
            await update_scaling_config(stype, runner_trail_pct=round(new_trail, 4))
            logger.info(
                "scaling_optimizer: %s trail %.2f → %.2f (%s) "
                "[n=%d avg_cap=%.1fx avg_peak=%.1fx]",
                stype, cur_trail, new_trail, reason, n, avg_cap, avg_peak,
            )
        else:
            logger.info(
                "scaling_optimizer: %s held trail %.2f "
                "[n=%d early=%.0f%% capture=%.0f%% of peak]",
                stype, cur_trail, n, early * 100, ratio * 100,
            )


async def scaling_optimizer_loop() -> None:
    """Background loop — re-tunes the scaling configs on the configured interval."""
    await asyncio.sleep(300)   # let the bot settle after boot
    logger.info("scaling_optimizer: started")
    while True:
        try:
            from database.models import get_params
            cfg = await get_params("scaling_optimizer_interval_hours")
            interval_h = float(cfg.get("scaling_optimizer_interval_hours", 24.0) or 24.0)
        except Exception:
            interval_h = 24.0
        try:
            await _tune_once()
        except Exception as exc:
            logger.error("scaling_optimizer: tune failed: %s", exc)
        await asyncio.sleep(max(interval_h, 1.0) * 3600.0)
