"""
scanner.py — Fetches live token data from the DexScreener public API
(no API key required) and runs the AI scoring algorithm.

Scoring categories (total 100 pts):
  Liquidity            20 pts
  Volume (24h)         20 pts
  Momentum (price %)   20 pts
  Holder Distribution  15 pts
  Contract Safety      15 pts
  Deployer Reputation  10 pts
"""

import aiohttp
import asyncio
from typing import Optional
from bot.config import DEXSCREENER_URL, SCORE_WEIGHTS, VERDICT_THRESHOLDS


# ── Data Fetching ─────────────────────────────────────────────────────────────

async def fetch_token_data(address: str) -> Optional[dict]:
    """
    Calls the DexScreener API for the given contract address.
    Returns the first pair found, or None if the request fails.
    """
    url = DEXSCREENER_URL.format(address=address)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None

    pairs = data.get("pairs")
    if not pairs:
        return None

    # Pick the pair with the highest liquidity (most reliable data)
    pairs.sort(key=lambda p: p.get("liquidity", {}).get("usd", 0), reverse=True)
    return pairs[0]


def parse_token_metrics(pair: dict) -> dict:
    """
    Extracts and normalises the fields we care about from a DexScreener pair object.
    """
    base = pair.get("baseToken", {})
    liquidity = pair.get("liquidity", {})
    volume = pair.get("volume", {})
    price_change = pair.get("priceChange", {})
    txns = pair.get("txns", {}).get("h24", {})

    # Estimate unique holders from buy/sell tx counts (rough heuristic)
    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)
    estimated_holders = max(buys, sells, 1)

    return {
        "address":        base.get("address", ""),
        "name":           base.get("name", "Unknown"),
        "symbol":         base.get("symbol", "???"),
        "price_usd":      float(pair.get("priceUsd") or 0),
        "market_cap":     float(pair.get("marketCap") or pair.get("fdv") or 0),
        "liquidity_usd":  float(liquidity.get("usd") or 0),
        "volume_24h":     float(volume.get("h24") or 0),
        "price_change_24h": float(price_change.get("h24") or 0),
        "estimated_holders": estimated_holders,
        "dex_url":        pair.get("url", ""),
        "chain":          pair.get("chainId", "unknown"),
    }


# ── AI Scoring ────────────────────────────────────────────────────────────────

def _score_liquidity(liquidity_usd: float) -> float:
    """
    Full 20 pts for ≥ $500k liquidity. Scales linearly down to 0 for < $5k.
    """
    if liquidity_usd >= 500_000:
        return 20.0
    if liquidity_usd < 5_000:
        return 0.0
    return round((liquidity_usd / 500_000) * 20, 2)


def _score_volume(volume_24h: float) -> float:
    """
    Full 20 pts for ≥ $1M daily volume. Scales linearly down to 0 for < $10k.
    """
    if volume_24h >= 1_000_000:
        return 20.0
    if volume_24h < 10_000:
        return 0.0
    return round((volume_24h / 1_000_000) * 20, 2)


def _score_momentum(price_change_24h: float) -> float:
    """
    Rewards positive momentum, penalises steep drops.
    +20 pts at ≥ +50 %, 0 pts at -30 % or worse.
    """
    pct = price_change_24h
    if pct >= 50:
        return 20.0
    if pct <= -30:
        return 0.0
    # Map [-30, +50] → [0, 20]
    return round(((pct + 30) / 80) * 20, 2)


def _score_holder_distribution(estimated_holders: int) -> float:
    """
    More unique tx participants = better distribution.
    Full 15 pts for ≥ 500 unique participants.
    """
    if estimated_holders >= 500:
        return 15.0
    if estimated_holders < 10:
        return 0.0
    return round((estimated_holders / 500) * 15, 2)


def _score_contract_safety(metrics: dict) -> float:
    """
    Heuristic safety score (0–15 pts) based on observable signals:
    - Liquidity-to-MarketCap ratio  (low ratio = honeypot risk)
    - Volume-to-Liquidity ratio     (very high = wash trading risk)
    """
    score = 15.0
    liq = metrics["liquidity_usd"]
    mc = metrics["market_cap"] or 1
    vol = metrics["volume_24h"]

    liq_mc_ratio = liq / mc
    # If liquidity is < 1 % of MC, subtract 7 pts
    if liq_mc_ratio < 0.01:
        score -= 7
    # If volume is > 20× liquidity, likely wash trading: subtract 5 pts
    if liq > 0 and (vol / liq) > 20:
        score -= 5

    return max(0.0, round(score, 2))


def _score_deployer_reputation(metrics: dict) -> float:
    """
    Deployer reputation heuristic (0–10 pts).
    Without on-chain wallet history access we use proxy signals:
    - Age of the pair on DexScreener (not directly available, so we use
      volume / liquidity stability as a proxy).
    Currently returns a conservative mid-score of 6 to avoid over-awarding.
    Extend this with a wallet-analysis API call when available.
    """
    # Conservative base: 6/10 (neutral — no negative signals found)
    base = 6.0

    # Slight bonus if volume is healthy relative to liquidity
    liq = metrics["liquidity_usd"]
    vol = metrics["volume_24h"]
    if liq > 0:
        ratio = vol / liq
        if 0.5 <= ratio <= 10:   # healthy range
            base = min(10.0, base + 2)
        elif ratio > 20:          # suspicious wash-trade signal
            base = max(0.0, base - 4)

    return round(base, 2)


def calculate_ai_score(metrics: dict) -> dict:
    """
    Runs all scoring functions, sums them, and assigns a human verdict.
    Returns a dict with individual component scores and the total.
    """
    components = {
        "liquidity":            _score_liquidity(metrics["liquidity_usd"]),
        "volume":               _score_volume(metrics["volume_24h"]),
        "momentum":             _score_momentum(metrics["price_change_24h"]),
        "holder_distribution":  _score_holder_distribution(metrics["estimated_holders"]),
        "contract_safety":      _score_contract_safety(metrics),
        "deployer_reputation":  _score_deployer_reputation(metrics),
    }

    total = round(sum(components.values()), 1)

    # Determine verdict based on thresholds (highest match wins)
    verdict = "AVOID"
    for label, threshold in VERDICT_THRESHOLDS.items():
        if total >= threshold:
            verdict = label
            break

    return {
        "components": components,
        "total":      total,
        "verdict":    verdict,
    }


# ── Public entry points ───────────────────────────────────────────────────────

async def fetch_current_market_cap(address: str) -> Optional[float]:
    """Returns the current market cap (USD) for an address, or None on failure.
    Falls back to fdv if marketCap is null (common on pump.fun tokens)."""
    pair = await fetch_token_data(address)
    if pair is None:
        return None
    return float(pair.get("marketCap") or pair.get("fdv") or 0) or None


async def fetch_live_data(address: str) -> Optional[dict]:
    """
    Returns live token data dict, or None on failure.
    Keys: market_cap, liquidity_usd, price_usd, symbol, price_changes{m5,h1,h6,h24}
    """
    pair = await fetch_token_data(address)
    if pair is None:
        return None
    mc     = float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = float((pair.get("liquidity") or {}).get("usd") or 0)
    price  = float(pair.get("priceUsd") or 0)
    pc     = pair.get("priceChange") or {}
    symbol = (pair.get("baseToken") or {}).get("symbol", "???")
    return {
        "market_cap":    mc,
        "liquidity_usd": liq,
        "price_usd":     price,
        "symbol":        symbol,
        "price_changes": {
            "m5":  float(pc.get("m5")  or 0),
            "h1":  float(pc.get("h1")  or 0),
            "h6":  float(pc.get("h6")  or 0),
            "h24": float(pc.get("h24") or 0),
        },
    }


async def fetch_sol_price_usd() -> float:
    """Returns current SOL/USD price from Jupiter price API, or 0 on failure."""
    url = "https://api.jup.ag/price/v2?ids=So11111111111111111111111111111111111111112"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url) as resp:
                data = await resp.json()
                return float(
                    data["data"]["So11111111111111111111111111111111111111112"]["price"]
                )
    except Exception:
        return 0.0


async def scan_token(address: str) -> Optional[dict]:
    """
    High-level function called by handlers.
    Returns a combined dict with metrics + AI score, or None on failure.
    """
    pair = await fetch_token_data(address)
    if pair is None:
        return None

    metrics = parse_token_metrics(pair)
    score_data = calculate_ai_score(metrics)

    return {**metrics, **score_data}
