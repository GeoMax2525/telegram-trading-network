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
    Fetch real OHLCV candles from GeckoTerminal (free, no API key).
    Returns list of candle dicts with open, high, low, close, volume.
    Falls back to DexScreener synthetic data if GeckoTerminal fails.
    """
    # Try GeckoTerminal first — real 5-minute OHLCV candles
    gecko_url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pair_address}/ohlcv/minute?aggregate=5&limit=50"
    )
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.get(gecko_url, headers={"Accept": "application/json"}) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    ohlcv_list = (data.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
                    if ohlcv_list and len(ohlcv_list) >= 5:
                        candles = []
                        for c in ohlcv_list:
                            # GeckoTerminal format: [timestamp, open, high, low, close, volume]
                            if len(c) >= 6:
                                candles.append({
                                    "timestamp": int(c[0]),
                                    "open": float(c[1]),
                                    "high": float(c[2]),
                                    "low": float(c[3]),
                                    "close": float(c[4]),
                                    "volume": float(c[5]),
                                })
                        if candles:
                            # Sort oldest first
                            candles.sort(key=lambda x: x["timestamp"])
                            logger.debug("Chart: %d real candles from GeckoTerminal for %s",
                                         len(candles), pair_address[:12])
                            return candles
    except Exception as exc:
        logger.debug("Chart: GeckoTerminal failed for %s: %s", pair_address[:12], exc)

    # Fallback: DexScreener synthetic candle
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
        logger.debug("Chart: DexScreener fallback failed for %s: %s", pair_address[:12], exc)
        return []

    pair = data.get("pair") or (data.get("pairs") or [None])[0] if isinstance(data, dict) else None
    if not pair:
        return []

    price_usd = float(pair.get("priceUsd") or 0)
    volume = pair.get("volume") or {}
    price_change = pair.get("priceChange") or {}

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
        # Use available data if at least 5 candles
        if len(changes) >= 5:
            period = len(changes)
        else:
            return 50.0  # neutral if not enough data

    gains = [c for c in changes[-period:] if c > 0]
    losses = [-c for c in changes[-period:] if c < 0]

    avg_gain = sum(gains) / period if gains else 0.001
    avg_loss = sum(losses) / period if losses else 0.001

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _compute_ema(prices: list[float], period: int) -> list[float]:
    """Compute EMA from a list of prices."""
    if len(prices) < period:
        return prices[:]
    multiplier = 2 / (period + 1)
    ema = [sum(prices[:period]) / period]
    for price in prices[period:]:
        ema.append(price * multiplier + ema[-1] * (1 - multiplier))
    return ema


def _candles_to_signals(candles: list[dict]) -> dict:
    """
    Convert real OHLCV candles into trading signals.
    Returns a dict compatible with the pattern detectors.
    """
    if not candles or len(candles) < 3:
        return {}

    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    current = closes[-1]
    prev = closes[-2]

    # Price changes
    changes = [(closes[i] - closes[i-1]) / closes[i-1] * 100
               for i in range(1, len(closes)) if closes[i-1] > 0]

    # RSI from real data
    rsi = _compute_rsi(changes)

    # EMA 9/21 from real data
    ema9 = _compute_ema(closes, 9)
    ema21 = _compute_ema(closes, 21)
    ema_bullish = len(ema9) > 0 and len(ema21) > 0 and ema9[-1] > ema21[-1]

    # Volume analysis
    avg_vol = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
    current_vol = volumes[-1]
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

    # Recent price action
    last_5 = closes[-5:] if len(closes) >= 5 else closes
    change_recent = ((last_5[-1] - last_5[0]) / last_5[0] * 100) if last_5[0] > 0 else 0

    last_12 = closes[-12:] if len(closes) >= 12 else closes
    change_h1 = ((last_12[-1] - last_12[0]) / last_12[0] * 100) if last_12[0] > 0 else 0

    # Support/resistance from highs and lows
    recent_high = max(highs[-10:]) if len(highs) >= 10 else max(highs)
    recent_low = min(lows[-10:]) if len(lows) >= 10 else min(lows)
    range_pct = ((recent_high - recent_low) / recent_low * 100) if recent_low > 0 else 0

    # Higher lows detection (bullish)
    higher_lows = True
    low_list = lows[-6:] if len(lows) >= 6 else lows
    for i in range(1, len(low_list)):
        if low_list[i] < low_list[i-1] * 0.98:
            higher_lows = False
            break

    return {
        "has_real_candles": True,
        "rsi": rsi,
        "ema_bullish": ema_bullish,
        "vol_ratio": vol_ratio,
        "change_m5": change_recent,
        "change_h1": change_h1,
        "volume_m5": current_vol,
        "volume_h1": sum(volumes[-12:]) if len(volumes) >= 12 else sum(volumes),
        "range_pct": range_pct,
        "higher_lows": higher_lows,
        "near_high": current >= recent_high * 0.95,
        "near_low": current <= recent_low * 1.05,
        "candle_count": len(candles),
    }


def _volume_spike_ratio(vol_m5: float, vol_h1: float) -> float:
    """Returns how much current 5m volume pace exceeds hourly average."""
    if vol_h1 <= 0:
        return 1.0
    hourly_avg_per_5m = vol_h1 / 12.0
    if hourly_avg_per_5m <= 0:
        return 1.0
    return vol_m5 / hourly_avg_per_5m


def _ema_momentum(change_m5: float, change_h1: float, data: dict | None = None) -> str:
    """EMA crossover — real EMA 9/21 when candles available, proxy otherwise."""
    if data and data.get("has_real_candles") and "ema_bullish" in data:
        if data["ema_bullish"] and data.get("vol_ratio", 1) > 1.2:
            return "bullish"
        if not data["ema_bullish"]:
            return "bearish"
        return "neutral"

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
    volume_m5 = data.get("volume_m5", 0) or 0
    volume_h1 = data.get("volume_h1", 0) or 0

    # Age gate: must be 15min-4hr old
    if created_at and isinstance(created_at, (int, float)):
        import time
        age_hours = (time.time() * 1000 - created_at) / 3_600_000
        if age_hours > 4 or age_hours < 0.25:
            return 0.0, False

    # Hard activity gate — no free points for age alone.
    # Require either real volume acceleration or real buyer flow.
    buys_m5 = txns.get("m5", {}).get("buys", 0) or 0
    # Volume increasing: current 5m pace exceeds h1 average pace by 50%+
    volume_accelerating = volume_m5 * 12 >= volume_h1 * 1.5 and volume_m5 > 0
    # Buyer count >20 in last 15min proxy: m5 buys * 3 >= 20 → buys_m5 >= 7
    buyers_strong = buys_m5 >= 7
    if not (volume_accelerating or buyers_strong):
        return 0.0, False

    score = 20.0  # base for passing age + activity gate

    # Below launch high (h1 negative = pulled back)
    if change_h1 < -20:
        score += 20.0

    # Building base (m5 tight)
    if -5 <= change_m5 <= 5:
        score += 25.0

    # Buyer activity bonus
    if buys_m5 >= 15:
        score += 20.0
    elif buys_m5 >= 7:
        score += 10.0

    # Volume still flowing
    if volume_m5 > 200:
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
    Returns score adjustment based on RSI.
    Uses real RSI from candles when available, proxy otherwise.
    """
    # Real RSI from GeckoTerminal candles
    if data.get("has_real_candles") and "rsi" in data:
        rsi = data["rsi"]
        # More granular with real data
        if rsi < 25:
            return 15.0    # deeply oversold — strong bounce
        if rsi < 35:
            return 8.0     # oversold
        if rsi > 80:
            return -20.0   # extremely overbought
        if rsi > 70:
            return -10.0   # overbought
        return 0.0

    # Proxy RSI from DexScreener changes
    changes = [
        data.get("change_m5", 0),
        data.get("change_h1", 0) / 12,
    ]
    rsi = _compute_rsi(changes * 7)

    if rsi < 30:
        return 10.0
    if rsi > 70:
        return -15.0
    return 0.0


# ── Meme Strength Score — indicator-based, no classical patterns ──────────────
#
# Classical chart patterns (bull flag, head & shoulders, etc.) don't work
# on memecoins. What real meme traders use:
#   1. RSI on 5m with degen thresholds (buy <30, scale out >75)
#   2. Volume spike / delta (the #1 entry signal)
#   3. EMA slope for momentum direction
#   4. Higher highs / lows (trend confirmation)
#   5. Buy/sell pressure ratio
#
# This replaces ALL_PATTERNS with pure indicator confluence scoring.


def _meme_strength_from_candles(signals: dict) -> tuple[float, str, list[str]]:
    """
    Score a token's meme strength from real candle indicators.
    Returns (score 0-100, primary_signal, list of active signals).
    """
    score = 50.0  # neutral start
    active_signals: list[str] = []
    primary = "neutral"

    rsi = signals.get("rsi", 50)
    vol_ratio = signals.get("vol_ratio", 1.0)
    ema_bull = signals.get("ema_bullish", False)
    higher_lows = signals.get("higher_lows", False)
    near_high = signals.get("near_high", False)
    change_m5 = signals.get("change_m5", 0)
    change_h1 = signals.get("change_h1", 0)

    # RSI — the most important indicator for meme entry/exit
    if rsi < 28:
        score += 25     # deeply oversold — strong buy zone
        active_signals.append(f"RSI oversold ({rsi:.0f})")
        primary = "oversold_bounce"
    elif rsi < 35:
        score += 15     # approaching oversold
        active_signals.append(f"RSI low ({rsi:.0f})")
        primary = "dip_buy"
    elif rsi > 80:
        score -= 20     # extremely overbought — exit zone
        active_signals.append(f"RSI overbought ({rsi:.0f})")
        primary = "overbought"
    elif rsi > 72:
        score -= 10     # hot — caution
        active_signals.append(f"RSI hot ({rsi:.0f})")

    # Volume spike — #1 entry signal for memes
    if vol_ratio >= 5.0:
        score += 25     # massive volume surge
        active_signals.append(f"volume explosion ({vol_ratio:.1f}x avg)")
        if primary == "neutral":
            primary = "volume_surge"
    elif vol_ratio >= 3.0:
        score += 18     # strong volume
        active_signals.append(f"high volume ({vol_ratio:.1f}x avg)")
        if primary == "neutral":
            primary = "volume_spike"
    elif vol_ratio >= 1.5:
        score += 8      # above average
        active_signals.append(f"active volume ({vol_ratio:.1f}x)")
    elif vol_ratio < 0.5:
        score -= 10     # dead volume
        active_signals.append("low volume")

    # EMA momentum — trend direction
    if ema_bull and change_m5 > 0:
        score += 10
        active_signals.append("bullish momentum")
    elif not ema_bull and change_m5 < -5:
        score -= 10
        active_signals.append("bearish momentum")

    # Higher lows — trend confirmation
    if higher_lows:
        score += 8
        active_signals.append("higher lows (uptrend)")

    # Near recent high — breakout potential
    if near_high and vol_ratio >= 1.5:
        score += 10
        active_signals.append("testing highs with volume")
        if primary in ("neutral", "volume_spike"):
            primary = "breakout"

    # RSI divergence detection (price up but RSI dropping = exhaustion)
    if change_h1 > 20 and rsi < 50:
        score += 5      # hidden bullish divergence
        active_signals.append("bullish divergence")
    elif change_h1 > 50 and rsi > 70:
        score -= 8      # bearish divergence — price extended
        active_signals.append("bearish divergence")

    return max(10.0, min(100.0, score)), primary, active_signals


def _meme_strength_from_synthetic(data: dict) -> tuple[float, str, list[str]]:
    """
    Score from DexScreener synthetic data when no real candles available.
    Uses volume ratio and price changes as proxy indicators.
    """
    score = 50.0
    active_signals: list[str] = []
    primary = "neutral"

    vol_ratio = _volume_spike_ratio(data.get("volume_m5", 0), data.get("volume_h1", 0))
    change_m5 = data.get("change_m5", 0)
    change_h1 = data.get("change_h1", 0)

    # Volume spike
    if vol_ratio >= 3.0:
        score += 20
        active_signals.append(f"volume surge ({vol_ratio:.1f}x)")
        primary = "volume_spike"
    elif vol_ratio >= 1.5:
        score += 8
        active_signals.append(f"active volume ({vol_ratio:.1f}x)")

    # Momentum proxy
    momentum = _ema_momentum(change_m5, change_h1)
    if momentum == "bullish":
        score += 10
        active_signals.append("bullish")
    elif momentum == "bearish":
        score -= 10
        active_signals.append("bearish")

    # RSI proxy
    rsi_adj = _rsi_adjustment(data)
    score += rsi_adj
    if rsi_adj > 0:
        active_signals.append("oversold bounce")
    elif rsi_adj < 0:
        active_signals.append("overbought")

    return max(10.0, min(100.0, score)), primary, active_signals


async def analyze_chart(candidate: dict) -> dict:
    """
    Meme Strength Analyzer — replaces classical pattern detection.

    Uses RSI, volume spikes, EMA momentum, and price action to score
    how strong a token's momentum is RIGHT NOW. No bull flags or cup
    and handles — those don't work on memecoins.

    Returns:
        {
            "chart_score": 0-100,
            "pattern_name": str (primary signal),
            "patterns_detected": list of active signals,
            "rsi_adj": float,
            "momentum": str,
        }
    """
    mint = candidate.get("mint", "")

    if not mint:
        return {"chart_score": 30.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    pair_address = await _fetch_pair_address(mint)
    if not pair_address:
        return {"chart_score": 35.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    candle_data = await _fetch_ohlcv(pair_address)
    if not candle_data:
        return {"chart_score": 35.0, "pattern_name": "none", "patterns_detected": [], "rsi_adj": 0, "momentum": "neutral"}

    # Use real candles when available, synthetic fallback otherwise
    if len(candle_data) >= 5 and "open" in candle_data[0]:
        signals = _candles_to_signals(candle_data)
        score, primary, active = _meme_strength_from_candles(signals)
        rsi = signals.get("rsi", 50)
        momentum = "bullish" if signals.get("ema_bullish") else "bearish" if signals.get("change_m5", 0) < -5 else "neutral"
        logger.info(
            "Chart[%s]: MEME STRENGTH %.0f | RSI=%.0f | vol=%.1fx | %s | signals: %s",
            mint[:12], score, rsi, signals.get("vol_ratio", 1),
            primary, ", ".join(active[:4]) if active else "none",
        )
    else:
        data = candle_data[0]
        score, primary, active = _meme_strength_from_synthetic(data)
        rsi = 50
        momentum = _ema_momentum(data.get("change_m5", 0), data.get("change_h1", 0))
        logger.info(
            "Chart[%s]: MEME STRENGTH %.0f (synthetic) | %s | signals: %s",
            mint[:12], score, primary, ", ".join(active[:4]) if active else "none",
        )

    rsi_adj = _rsi_adjustment({"rsi": rsi, "has_real_candles": len(candle_data) >= 5} if len(candle_data) >= 5 else candle_data[0] if candle_data else {})

    return {
        "chart_score":       round(score, 1),
        "pattern_name":      primary,
        "patterns_detected": active,
        "rsi_adj":           rsi_adj,
        "momentum":          momentum,
    }
