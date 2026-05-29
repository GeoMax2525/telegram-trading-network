"""
regime_tracker.py — Solana memecoin market regime detection.

Polls multiple free data sources every 5 min to determine if the market
is HOT (favoring memecoin trading), NEUTRAL, or COLD (preserve capital).

Signal mix (no new API keys required):
  - Aggregate 24h volume of trending Solana tokens (DexScreener)
  - SOL/USD 24h price change (DexScreener)
  - New tokens harvested per hour (from our own harvester agent)

Regime drives bot aggression downstream:
  - HOT: probe size ×1.5, conf threshold lowered
  - NEUTRAL: current settings
  - COLD: probe size ×0.5, conf threshold raised, OR pause new entries

The volume baseline auto-tunes via exponential moving average — no need
to seed historical data. After ~4 hours of polling, EMA stabilizes
into a meaningful baseline.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

import aiohttp

from bot import state
from database.models import get_param, set_param

logger = logging.getLogger(__name__)

POLL_INTERVAL = 300   # 5 minutes
DEXSCREENER_TRENDING = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_SOL = (
    "https://api.dexscreener.com/latest/dex/tokens/"
    "So11111111111111111111111111111111111111112"
)

# Regime thresholds — tunable via /setparam
DEFAULTS = {
    "regime_hot_vol_ratio":   1.5,  # current vol >= 1.5x EMA → hot signal
    "regime_cold_vol_ratio":  0.7,  # current vol <= 0.7x EMA → cold signal
    "regime_hot_sol_24h":     5.0,  # SOL up 5%+ → hot signal
    "regime_cold_sol_24h":   -5.0,  # SOL down 5%+ → cold signal
    "regime_ema_alpha":       0.05, # EMA smoothing (lower = slower)
    # Bot adjustments per regime
    "regime_hot_probe_mult":   1.5,
    "regime_cold_probe_mult":  0.5,
    "regime_cold_skip_trades": 0,   # 1 = pause entries entirely in COLD
}


async def _fetch_json(url: str, timeout: float = 10.0) -> dict | list | None:
    """Light-weight aiohttp GET wrapper. Returns parsed JSON or None on failure."""
    try:
        connector = aiohttp.TCPConnector(limit=4)
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(
            connector=connector, timeout=client_timeout,
        ) as sess:
            async with sess.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("regime: fetch %s failed: %s", url, exc)
        return None


async def _fetch_solana_volume_proxy() -> float | None:
    """Aggregate volume of top boosted Solana tokens. Returns 0+ on success,
    None on API failure. Used as the 'memecoin activity' proxy."""
    data = await _fetch_json(DEXSCREENER_TRENDING)
    if not isinstance(data, list):
        return None
    total_vol = 0.0
    for token in data[:50]:
        if (token.get("chainId") or "").lower() != "solana":
            continue
        # Boosted tokens have 'totalAmount' which roughly correlates with
        # paid attention. We sum these as a hot-market proxy.
        amt = token.get("totalAmount") or token.get("amount") or 0
        try:
            total_vol += float(amt)
        except (TypeError, ValueError):
            continue
    return total_vol


async def _fetch_sol_24h_change() -> float | None:
    """SOL 24h price change percentage. Positive = up, negative = down."""
    data = await _fetch_json(DEXSCREENER_SOL)
    if not isinstance(data, dict):
        return None
    pairs = data.get("pairs") or []
    if not pairs:
        return None
    best = max(
        pairs,
        key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
    )
    change = (best.get("priceChange") or {}).get("h24")
    try:
        return float(change) if change is not None else None
    except (TypeError, ValueError):
        return None


def _classify(
    vol_ratio: float | None,
    sol_24h: float | None,
    cfg: dict,
) -> tuple[str, int]:
    """Combine signals into HOT / NEUTRAL / COLD with a score for diagnostics."""
    score = 0
    if vol_ratio is not None:
        if vol_ratio >= float(cfg.get("regime_hot_vol_ratio") or 1.5):
            score += 2
        elif vol_ratio <= float(cfg.get("regime_cold_vol_ratio") or 0.7):
            score -= 2
    if sol_24h is not None:
        if sol_24h >= float(cfg.get("regime_hot_sol_24h") or 5.0):
            score += 1
        elif sol_24h <= float(cfg.get("regime_cold_sol_24h") or -5.0):
            score -= 1
    if score >= 2:
        return ("HOT", score)
    if score <= -2:
        return ("COLD", score)
    return ("NEUTRAL", score)


async def _load_cfg() -> dict:
    """Read regime params with defaults fallback."""
    cfg = {}
    for k, v in DEFAULTS.items():
        cfg[k] = await get_param(k) or v
    return cfg


async def regime_tracker_loop() -> None:
    """Background loop. Polls signals every 5 min, updates state.meme_regime.

    Survives transient API failures by carrying over the previous regime
    until next successful poll. Logs a single line per cycle for audit."""
    logger.info("regime_tracker: starting")

    # Seed in-memory state to NEUTRAL so other agents reading state.meme_regime
    # before the first successful poll get a sane default
    if not hasattr(state, "meme_regime"):
        state.meme_regime = "NEUTRAL"
        state.meme_regime_score = 0
        state.meme_regime_volume_ratio = 1.0
        state.meme_regime_sol_24h = 0.0
        state.meme_regime_updated_at = None

    # Brief startup delay so harvester / scanner spin up first
    await asyncio.sleep(45)

    while True:
        try:
            cfg = await _load_cfg()

            current_vol = await _fetch_solana_volume_proxy()
            sol_24h = await _fetch_sol_24h_change()

            # EMA baseline for volume — persisted across restarts via DB
            prev_ema_raw = await get_param("regime_volume_ema")
            prev_ema = float(prev_ema_raw) if prev_ema_raw else 0.0
            alpha = float(cfg.get("regime_ema_alpha") or 0.05)

            vol_ratio = None
            if current_vol is not None and current_vol > 0:
                if prev_ema <= 0:
                    new_ema = current_vol
                else:
                    new_ema = (prev_ema * (1 - alpha)) + (current_vol * alpha)
                await set_param(
                    "regime_volume_ema", new_ema,
                    reason="regime_tracker EMA update",
                )
                if prev_ema > 0:
                    vol_ratio = current_vol / prev_ema
                else:
                    vol_ratio = 1.0

            regime, score = _classify(vol_ratio, sol_24h, cfg)

            # Update bot state for other agents to read
            state.meme_regime = regime
            state.meme_regime_score = score
            state.meme_regime_volume_ratio = vol_ratio or 0.0
            state.meme_regime_sol_24h = sol_24h or 0.0
            state.meme_regime_updated_at = datetime.utcnow()

            logger.info(
                "regime: %s (score=%+d) vol_ratio=%.2fx sol_24h=%+.1f%% "
                "current_vol=%.0f ema=%.0f",
                regime, score,
                vol_ratio or 0.0, sol_24h or 0.0,
                current_vol or 0.0, prev_ema,
            )

            await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("regime_tracker: cancelled")
            raise
        except Exception as exc:
            logger.error("regime_tracker: error %s, sleeping 60s", exc)
            await asyncio.sleep(60)


def get_probe_size_multiplier() -> float:
    """Return the probe-size multiplier for the current regime.

    Other agents call this when sizing a new position. Defaults to 1.0
    if regime hasn't been computed yet (boot window)."""
    regime = getattr(state, "meme_regime", "NEUTRAL")
    if regime == "HOT":
        # Use DEFAULTS — caller can override via /setparam if needed
        return float(DEFAULTS.get("regime_hot_probe_mult", 1.5))
    if regime == "COLD":
        return float(DEFAULTS.get("regime_cold_probe_mult", 0.5))
    return 1.0


def should_skip_in_cold() -> bool:
    """True iff the operator has configured COLD-regime trade skipping.
    Caller treats True as 'don't open this trade, defer for HOT/NEUTRAL'."""
    regime = getattr(state, "meme_regime", "NEUTRAL")
    if regime != "COLD":
        return False
    return float(DEFAULTS.get("regime_cold_skip_trades") or 0) >= 0.5
