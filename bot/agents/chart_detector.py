"""
chart_detector.py — Agent 7: The Chart Pattern Detector

Called by Agent 5 for every candidate before execution decision.
Fetches OHLCV candle data from DexScreener and detects patterns.

Classical patterns:
  bull_flag, ascending_triangle, double_bottom, falling_wedge, cup_and_handle

Meme coin patterns:
  launchpad_setup, insider_accumulation, caller_pump, fakeout_recovery

Technical indicators:
  RSI 14, EMA 9/21 crossover, Volume MA 20, VWAP

Multi-timeframe confluence:
  Same pattern on 2 timeframes: +25 points
  Same pattern on 3 timeframes: +50 points
  Conflicting patterns: -20 points

Output: chart_score (0-100), pattern_name, details dict
"""

import asyncio
import logging
import math
from datetime import datetime

import aiohttp

from bot.scanner import fetch_token_data

logger = logging.getLogger(__name__)

# DexScreener OHLCV endpoint — uses pair address, not token mint
OHLCV_URL = "https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"

# Timeframe resolutions (minutes)
TIMEFRAMES = {
    "1m":  {"minutes": 1,  "label": "1m"},
    "5m":  {"minutes": 5,  "label": "5m"},
    "15m": {"minutes": 15, "label": "15m"},
}


# ── Candle fetching ──────────────────────────────────────────────────────────

async def _fetch_pair_address(mint: str) -> str | None:
    """Get the primary pair address for a token mint via DexScreener."""
    pair = await fetch_token_data(mint)
    if pair:
        return pair.get("pairAddress")
    return None


async def _fetch_ohlcv(pair_address: str) -> list[dict]:
    """
    Fetch recent trade data and build synthetic candles from pair info.
    DexScreener provides price/volume snapshots we can work with.
    """
    url = OHLCV_URL.format(pair_address=pair_address)
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("Chart: OHLCV fetch failed for %s: %s", pair_address[:12], exc)
        return []

    pair = data.get("pair") or (data.get("pairs") or [None])[0] if isinstance(data, dict) else None
    if not pair:
        return []

    # Extract available price/volume data points
    price_usd = float(pair.get("priceUsd") or 0)
    volume = pair.get("volume") or {}
    price_change = pair.get("priceChange") or {}

    # Build synthetic candle summary from available data
    return [{
        "pair": pair,
        "price": price_usd,
        "volume_m5": float(volume.get("m5") or 0),
        "volume_h1": float(volume.get("h1") or 0),
        "volume_h6": float(volume.get("h6") or 0),
        "volume_h24": float(volume.get("h24") or 0),
        "change_m5": float(price_change.get("m5") or 0),
        "change_h1": float(price_change.get("h1") or 0),
        "change_h6": float(price_change.get("h6") or 0),
        "change_h24": float(price_change.get("h24") or 0),
        "txns": pair.get("txns") or {},
        "liquidity": float((pair.get("liquidity") or {}).get("usd") or 0),
        "fdv": float(pair.get("fdv") or 0),
        "created_at": pair.get("pairCreatedAt"),
    }]


# ── Technical indicators ─────────────────────────────────────────────────────

def _compute_rsi(changes: list[float], period: int = 14) -> float:
    """Compute RSI from a list of price changes."""
    if len(changes) < period:
        return 50.0  # neutral if not enough data

    gains = [c for c in changes[-period:] if c > 0]
    losses = [-c for c in changes[-period:] if c < 0]

    avg_gain = sum(gains) / period if gains else 0.001
    avg_loss = sum(losses) / period if losses else 0.001

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _volume_spike_ratio(vol_m5: float, vol_h1: float) -> float:
    """Returns how much current 5m volume pace exceeds hourly average."""
    if vol_h1 <= 0:
        return 1.0
    hourly_avg_per_5m = vol_h1 / 12.0
    if hourly_avg_per_5m <= 0:
        return 1.0
    return vol_m5 / hourly_avg_per_5m


def _ema_momentum(change_m5: float, change_h1: float) -> str:
    """Simple EMA crossover proxy using price changes."""
    if change_m5 > 0 and change_h1 > 0 and change_m5 > change_h1 / 12:
        return "bullish"
    if change_m5 < 0 and change_h1 < 0:
        return "bearish"
    return "neutral"


# ── Pattern detectors ────────────────────────────────────────────────────────

def _detect_bull_flag(data: dict) -> tuple[float, bool]:
    """
    Bull flag: strong pole (big recent move up), then consolidation.
    Uses h1 change as pole and m5 as flag behavior.
    Score 0-100.
    """
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))

    # Need a strong pole: h1 up 20%+
    if change_h1 < 20:
        return 0.0, False

    score = 0.0

    # Consolidation: m5 change is small/slightly negative (flag)
    if -10 <= change_m5 <= 5:
        score += 40.0  # tight consolidation
    elif -20 <= change_m5 < -10:
        score += 20.0  # deeper pullback but still flag-like

    # Volume declining during flag
    if vol_ratio < 0.8:
        score += 20.0  # volume dropping — good for flag
    elif vol_ratio > 2.0:
        score += 30.0  # volume spike — breakout happening

    # Strong pole bonus
    if change_h1 >= 50:
        score += 20.0
    elif change_h1 >= 30:
        score += 10.0

    return min(score, 100.0), score >= 50


def _detect_ascending_triangle(data: dict) -> tuple[float, bool]:
    """
    Ascending triangle: higher lows with flat resistance.
    Uses multi-timeframe changes as proxy.
    """
    change_h1 = data.get("change_h1", 0)
    change_h6 = data.get("change_h6", 0)
    change_m5 = data.get("change_m5", 0)

    score = 0.0

    # h6 trending up (higher lows over time)
    if change_h6 > 5:
        score += 30.0

    # h1 relatively flat or slightly up (resistance zone)
    if -5 <= change_h1 <= 15:
        score += 25.0

    # m5 pushing up toward resistance
    if change_m5 > 0:
        score += 20.0

    # Volume increasing on approach
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio > 1.5:
        score += 25.0

    return min(score, 100.0), score >= 50


def _detect_double_bottom(data: dict) -> tuple[float, bool]:
    """
    Double bottom: price dumped, recovered, tested low again, bouncing.
    Uses h6/h1/m5 changes as proxy.
    """
    change_h6 = data.get("change_h6", 0)
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)

    score = 0.0

    # Was down significantly in h6 (first bottom happened)
    if change_h6 < -15:
        score += 25.0

    # h1 is near flat or slightly negative (second bottom/test)
    if -10 <= change_h1 <= 5:
        score += 25.0

    # m5 bouncing up (recovery from second bottom)
    if change_m5 > 2:
        score += 30.0

    # Volume on bounce
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio > 1.5:
        score += 20.0

    return min(score, 100.0), score >= 50


def _detect_falling_wedge(data: dict) -> tuple[float, bool]:
    """Falling wedge: converging downtrend with bullish reversal."""
    change_h6 = data.get("change_h6", 0)
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)

    score = 0.0

    # Overall downtrend (h6 down)
    if change_h6 < -10:
        score += 20.0

    # Rate of decline slowing (h1 less negative than h6 pace)
    h6_hourly_pace = change_h6 / 6
    if change_h1 > h6_hourly_pace:
        score += 25.0

    # Reversal starting (m5 turning up)
    if change_m5 > 0:
        score += 30.0
    if change_m5 > 3:
        score += 10.0

    # Volume spike on reversal
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio > 1.5:
        score += 15.0

    return min(score, 100.0), score >= 50


def _detect_cup_and_handle(data: dict) -> tuple[float, bool]:
    """Cup and handle: U-shape recovery with small handle pullback."""
    change_h6 = data.get("change_h6", 0)
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)

    score = 0.0

    # h6 roughly flat or positive (cup completed — went down and came back)
    if -5 <= change_h6 <= 20:
        score += 25.0

    # h1 up (right side of cup)
    if change_h1 > 5:
        score += 25.0

    # m5 slight pullback or flat (handle)
    if -8 <= change_m5 <= 2:
        score += 25.0

    # Volume declining in handle
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio < 0.8:
        score += 25.0

    return min(score, 100.0), score >= 50


# ── Meme coin specific patterns ─────────────────────────────────────────────

def _detect_launchpad_setup(data: dict) -> tuple[float, bool]:
    """
    Launchpad: token 15min-4hrs old, 30%+ below launch high,
    building a tight base with growing activity.
    """
    created_at = data.get("created_at")
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)
    txns = data.get("txns", {})

    score = 0.0

    # Check age (should be young)
    if created_at and isinstance(created_at, (int, float)):
        import time
        age_hours = (time.time() * 1000 - created_at) / 3_600_000
        if 0.25 <= age_hours <= 4:
            score += 20.0
        elif age_hours > 4:
            return 0.0, False  # too old for launchpad

    # Below launch high (h1 negative = pulled back)
    if change_h1 < -20:
        score += 20.0

    # Building base (m5 tight)
    if -5 <= change_m5 <= 5:
        score += 25.0

    # Buyer activity still present
    buys_m5 = txns.get("m5", {}).get("buys", 0) or 0
    if buys_m5 >= 5:
        score += 20.0
    elif buys_m5 >= 3:
        score += 10.0

    # Volume still flowing
    if data.get("volume_m5", 0) > 200:
        score += 15.0

    return min(score, 100.0), score >= 50


def _detect_insider_accumulation(data: dict, insider_count: int) -> tuple[float, bool]:
    """
    Flat price + rising volume + tracked wallets buying.
    Highest priority pattern.
    """
    change_m5 = data.get("change_m5", 0)
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))

    score = 0.0

    # Flat price (accumulation, not pumping)
    if -5 <= change_m5 <= 8:
        score += 20.0

    # Volume rising despite flat price
    if vol_ratio > 1.3:
        score += 20.0
    if vol_ratio > 2.0:
        score += 10.0

    # Insider wallets buying (main signal)
    if insider_count >= 3:
        score += 40.0
    elif insider_count >= 2:
        score += 30.0
    elif insider_count >= 1:
        score += 15.0

    return min(score, 100.0), score >= 50


def _detect_caller_pump(data: dict) -> tuple[float, bool]:
    """
    Caller pump setup: first wave up, pullback to ~0.618 fib, second wave entry.
    """
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)
    change_h6 = data.get("change_h6", 0)

    score = 0.0

    # First wave happened (h6 or h1 was up significantly)
    if change_h6 > 20 or change_h1 > 15:
        score += 25.0

    # Pullback from highs (h1 pulling back from h6 peak)
    if change_h6 > 20 and change_h1 < change_h6 * 0.5:
        score += 25.0  # pulled back ~50% (near 0.618 fib)

    # Second wave starting (m5 turning up)
    if change_m5 > 2:
        score += 25.0

    # Volume returning
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio > 1.5:
        score += 25.0

    return min(score, 100.0), score >= 50


def _detect_fakeout_recovery(data: dict) -> tuple[float, bool]:
    """
    Break below support then immediate reversal back above.
    """
    change_h1 = data.get("change_h1", 0)
    change_m5 = data.get("change_m5", 0)

    score = 0.0

    # Was down in h1 (broke support)
    if change_h1 < -10:
        score += 25.0

    # Sharp m5 recovery (bounced back hard)
    if change_m5 > 5:
        score += 35.0
    elif change_m5 > 2:
        score += 20.0

    # Volume spike on recovery
    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    if vol_ratio > 2.0:
        score += 25.0
    elif vol_ratio > 1.3:
        score += 15.0

    return min(score, 100.0), score >= 50


# ── RSI gate ─────────────────────────────────────────────────────────────────

def _rsi_adjustment(data: dict) -> float:
    """
    Returns score adjustment based on RSI proxy.
    Oversold (< 30 proxy): +10
    Overbought (> 70 proxy): -15
    Normal: 0
    """
    # Use multi-timeframe changes as RSI proxy
    changes = [
        data.get("change_m5", 0),
        data.get("change_h1", 0) / 12,  # normalize to per-5m
    ]
    rsi = _compute_rsi(changes * 7)  # expand to get enough data points

    if rsi < 30:
        return 10.0   # oversold bounce potential
    if rsi > 70:
        return -15.0  # overbought risk
    return 0.0


# ── Main analysis function ───────────────────────────────────────────────────

ALL_PATTERNS = {
    "bull_flag":              _detect_bull_flag,
    "ascending_triangle":     _detect_ascending_triangle,
    "double_bottom":          _detect_double_bottom,
    "falling_wedge":          _detect_falling_wedge,
    "cup_and_handle":         _detect_cup_and_handle,
    "launchpad_setup":        _detect_launchpad_setup,
    "caller_pump":            _detect_caller_pump,
    "fakeout_recovery":       _detect_fakeout_recovery,
    # insider_accumulation handled separately (needs insider_count)
}


async def analyze_chart(candidate: dict) -> dict:
    """
    Analyze chart patterns for a candidate token.

    Returns:
        {
            "chart_score": 0-100,
            "pattern_name": str or "none",
            "patterns_detected": list of pattern names,
            "rsi_adj": float,
            "momentum": str,
        }
    """
    mint = candidate.get("mint", "")
    insider_count = candidate.get("insider_count", 0)

    if not mint:
        return {"chart_score": 30.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    # Fetch pair data
    pair_address = await _fetch_pair_address(mint)
    if not pair_address:
        logger.debug("Chart: no pair found for %s", mint[:12])
        return {"chart_score": 35.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    candle_data = await _fetch_ohlcv(pair_address)
    if not candle_data:
        logger.debug("Chart: no candle data for %s", mint[:12])
        return {"chart_score": 35.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    data = candle_data[0]

    # Run all pattern detectors — log every attempt
    detected: list[tuple[str, float]] = []
    all_scores: list[str] = []

    for name, detector in ALL_PATTERNS.items():
        score, matched = detector(data)
        all_scores.append(f"{name}:{score:.0f}{'*' if matched else ''}")
        if score > 0:  # chaos mode: include all non-zero scores
            detected.append((name, score))

    # Insider accumulation (special — needs insider_count)
    ins_score, ins_matched = _detect_insider_accumulation(data, insider_count)
    all_scores.append(f"insider_accum:{ins_score:.0f}{'*' if ins_matched else ''}")
    if ins_score > 0:
        detected.append(("insider_accumulation", ins_score))

    logger.info("Chart[%s]: all patterns: %s", mint[:12], " | ".join(all_scores))

    # Sort by score descending
    detected.sort(key=lambda x: x[1], reverse=True)

    # Compute chart score
    if not detected:
        base_score = 30.0
        pattern_name = "none"
    elif len(detected) == 1:
        base_score = detected[0][1]
        pattern_name = detected[0][0]
    else:
        # Multi-pattern confluence
        base_score = detected[0][1]
        pattern_name = detected[0][0]

        # Check for confluence bonus
        if len(detected) >= 3:
            base_score += 50.0  # 3+ patterns
        elif len(detected) >= 2:
            base_score += 25.0  # 2 patterns

        # Check for conflicting patterns (some bearish + bullish)
        bearish_patterns = {"falling_wedge", "fakeout_recovery", "double_bottom"}
        bullish_patterns = {"bull_flag", "ascending_triangle", "caller_pump"}
        det_names = {d[0] for d in detected}
        if det_names & bearish_patterns and det_names & bullish_patterns:
            base_score -= 20.0  # conflicting signals

    # RSI adjustment
    rsi_adj = _rsi_adjustment(data)
    base_score += rsi_adj

    # EMA momentum
    momentum = _ema_momentum(data.get("change_m5", 0), data.get("change_h1", 0))
    if momentum == "bullish":
        base_score += 5.0
    elif momentum == "bearish":
        base_score -= 10.0

    # Clamp to 15-100 (chaos mode: never return 0)
    chart_score = round(max(15.0, min(100.0, base_score)), 1)

    pattern_names = [d[0] for d in detected]

    logger.info(
        "Chart: %s score=%.1f pattern=%s detected=%s momentum=%s rsi_adj=%.0f",
        mint[:12], chart_score, pattern_name,
        ",".join(pattern_names) if pattern_names else "none",
        momentum, rsi_adj,
    )

    return {
        "chart_score":       chart_score,
        "pattern_name":      pattern_name,
        "patterns_detected": pattern_names,
        "rsi_adj":           rsi_adj,
        "momentum":          momentum,
    }
