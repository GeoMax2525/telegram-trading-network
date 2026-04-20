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

import logging
import aiohttp
import asyncio
from typing import Optional
from bot.config import DEXSCREENER_URL, SCORE_WEIGHTS, VERDICT_THRESHOLDS

logger = logging.getLogger(__name__)


# ── DEX allowlist ────────────────────────────────────────────────────────────
#
# Only tokens trading on these DEXes are allowed through the pipeline. Any
# pre-graduation pump.fun bonding-curve pair reports dexId="pumpfun" (or a
# close variant) and will be dropped at the source by fetch_token_data().
#
# Per-DEX minimum liquidity floors are enforced downstream in
# bot/agents/scanner_agent.py::_evaluate_candidate.
ALLOWED_DEXES = {"raydium", "pumpswap", "orca", "meteora", "meteoradbc"}

# Per-DEX min liquidity floors (USD) used by scanner_agent._evaluate_candidate.
MIN_LIQUIDITY_BY_DEX = {
    "raydium":    10_000,
    "pumpswap":    5_000,
    "orca":       10_000,
    "meteora":    10_000,
    "meteoradbc": 10_000,
}


# Ecosystem allowlist — mint address MUST end with one of these suffixes.
# The on-chain factory appends a fixed suffix per launchpad:
#   pump.fun  -> *pump
#   bonk      -> *bonk
#   bags      -> *bags
# Any mint without one of these suffixes is rejected at the very first
# gate in scanner_agent._evaluate_candidate, harvester._save_graduated_token,
# and gmgn_agent before save. Non-learning hard-coded constant — Agent 6
# cannot touch this list.
ALLOWED_MINT_SUFFIXES = ("pump", "bonk", "bags")


def mint_suffix_ok(mint: str | None) -> bool:
    """Return True iff the mint ends with an allowed launchpad suffix."""
    if not mint:
        return False
    lower = mint.lower()
    return any(lower.endswith(suf) for suf in ALLOWED_MINT_SUFFIXES)


def _pair_dex_id(pair: dict) -> str:
    return (pair.get("dexId") or "").lower()


# ── Data Fetching ─────────────────────────────────────────────────────────────

async def fetch_token_data(
    address: str,
    allow_any_dex: bool = False,
) -> Optional[dict]:
    """
    Calls the DexScreener API for the given contract address.

    Default mode (allow_any_dex=False) enforces the DEX allowlist so
    new candidates coming from Agent 1 / Agent 4 never leak a pump.fun
    bonding-curve or meteora pair into the trading pipeline.

    allow_any_dex=True bypasses the allowlist and just returns the
    highest-liquidity pair. Used by the paper monitor and live-data
    helpers so trades already open on now-unsupported DEXes can still
    be tracked to exit — filtering them there would orphan the
    position with current_mc=0 forever.
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

    if allow_any_dex:
        pairs.sort(key=lambda p: p.get("liquidity", {}).get("usd", 0), reverse=True)
        return pairs[0]

    # DEX allowlist — drop unsupported pairs before picking best
    allowed = [p for p in pairs if _pair_dex_id(p) in ALLOWED_DEXES]
    if not allowed:
        seen_dexes = sorted({_pair_dex_id(p) or "unknown" for p in pairs})
        logger.info(
            "Rejected %s: unsupported DEX(es) %s (allowed: %s)",
            address[:12], ",".join(seen_dexes), ",".join(sorted(ALLOWED_DEXES)),
        )
        return None

    # Pick the pair with the highest liquidity (most reliable data)
    allowed.sort(key=lambda p: p.get("liquidity", {}).get("usd", 0), reverse=True)
    return allowed[0]


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
        "dex_id":         _pair_dex_id(pair),
    }


# ── AI Scoring ────────────────────────────────────────────────────────────────

def _score_liquidity(metrics: dict) -> float:
    """
    Liquidity health (0-20 pts). Uses liquidity-to-MC ratio instead of
    raw USD — a $50K MC token with $10K liquidity is healthier than a
    $5M MC token with $50K liquidity.
    """
    liq = metrics["liquidity_usd"]
    mc = metrics["market_cap"] or 1

    if liq < 3_000:
        return 0.0

    ratio = liq / mc
    # Best: 5-15% liq/mc ratio = healthy pool depth
    if ratio >= 0.10:
        score = 20.0
    elif ratio >= 0.05:
        score = 16.0
    elif ratio >= 0.03:
        score = 12.0
    elif ratio >= 0.01:
        score = 8.0
    else:
        score = 4.0

    # Bonus for absolute liquidity depth
    if liq >= 100_000:
        score = min(20.0, score + 2.0)

    return round(score, 1)


def _score_volume(metrics: dict) -> float:
    """
    Volume velocity (0-20 pts). Measures volume relative to liquidity
    (turnover rate) and 5-min pace vs hourly average, not just raw 24h.
    High turnover = active interest. Pace acceleration = early momentum.
    """
    vol_24h = metrics["volume_24h"]
    liq = metrics["liquidity_usd"]

    if vol_24h < 1_000:
        return 0.0

    score = 0.0

    # Turnover rate: vol/liq ratio (healthy = 1-10x per day)
    if liq > 0:
        turnover = vol_24h / liq
        if turnover >= 5.0:
            score += 12.0
        elif turnover >= 2.0:
            score += 9.0
        elif turnover >= 1.0:
            score += 6.0
        elif turnover >= 0.3:
            score += 3.0

    # Raw volume floor bonus (shows real interest exists)
    if vol_24h >= 500_000:
        score += 8.0
    elif vol_24h >= 100_000:
        score += 6.0
    elif vol_24h >= 25_000:
        score += 4.0
    elif vol_24h >= 5_000:
        score += 2.0

    return round(min(20.0, score), 1)


def _score_momentum(metrics: dict) -> float:
    """
    Price momentum (0-20 pts). Rewards positive movement at any level.
    Memecoins routinely do 200-1000% in a day — penalizing big moves
    misses runners. Negative momentum is the real red flag.
    """
    pct = metrics["price_change_24h"]

    # Strong positive momentum at any level
    if pct >= 200:
        return 20.0   # mooning
    if pct >= 50:
        return 20.0   # strong runner
    if pct >= 10:
        return 18.0   # healthy momentum
    if pct >= 2:
        return 14.0   # early momentum
    # Flat / consolidating: -5% to +2%
    if pct >= -5:
        return 10.0   # neutral, could break either way
    # Dipping: -5% to -15%
    if pct >= -15:
        return 6.0
    # Dropping: -15% to -30%
    if pct >= -30:
        return 3.0
    # Dumping hard: below -30%
    return 0.0


def _score_holder_distribution(metrics: dict) -> float:
    """
    Holder activity (0-15 pts). Uses buy/sell transaction count as a
    proxy for distribution quality. High buy count with balanced
    buy/sell ratio = organic activity.
    """
    holders = metrics["estimated_holders"]

    if holders < 10:
        return 0.0

    # Base score from unique participants
    if holders >= 1000:
        score = 12.0
    elif holders >= 500:
        score = 10.0
    elif holders >= 200:
        score = 8.0
    elif holders >= 100:
        score = 6.0
    elif holders >= 50:
        score = 4.0
    else:
        score = 2.0

    # Buy/sell balance bonus (both sides active = organic market)
    buys = metrics.get("buys_24h", holders)
    sells = metrics.get("sells_24h", 0)
    if buys > 0 and sells > 0:
        ratio = min(buys, sells) / max(buys, sells)
        if ratio >= 0.3:
            score += 3.0  # healthy two-sided market

    return round(min(15.0, score), 1)


def _score_contract_safety(metrics: dict) -> float:
    """
    Contract safety (0-15 pts). Checks liquidity depth relative to MC,
    volume wash-trading signals, and basic health indicators.
    """
    score = 15.0
    liq = metrics["liquidity_usd"]
    mc = metrics["market_cap"] or 1
    vol = metrics["volume_24h"]

    liq_mc_ratio = liq / mc

    # Dangerously low liquidity relative to MC = honeypot risk
    if liq_mc_ratio < 0.005:
        score -= 8
    elif liq_mc_ratio < 0.01:
        score -= 5

    # Extreme volume/liquidity = wash trading
    if liq > 0 and (vol / liq) > 30:
        score -= 5
    elif liq > 0 and (vol / liq) > 15:
        score -= 3

    # Very low absolute liquidity
    if liq < 5_000:
        score -= 3

    return max(0.0, round(score, 1))


def _score_market_strength(metrics: dict) -> float:
    """
    Market strength (0-10 pts). Combines multiple signals into an
    overall health indicator: MC size, volume presence, and momentum
    direction working together.
    """
    mc = metrics["market_cap"] or 0
    vol = metrics["volume_24h"]
    pct = metrics["price_change_24h"]

    score = 0.0

    # MC in productive range ($10K-$5M for memecoins)
    if 50_000 <= mc <= 2_000_000:
        score += 4.0  # sweet spot
    elif 10_000 <= mc <= 5_000_000:
        score += 3.0  # acceptable
    elif mc > 5_000_000:
        score += 2.0  # established but less upside
    else:
        score += 1.0

    # Volume confirms price action (volume + momentum aligned)
    if vol > 10_000 and pct > 0:
        score += 3.0  # green volume
    elif vol > 10_000:
        score += 1.5  # volume exists but price down

    # Not in freefall
    if pct > -10:
        score += 3.0
    elif pct > -25:
        score += 1.5

    return round(min(10.0, score), 1)


def _score_opportunity(metrics: dict) -> tuple[int, str]:
    """
    Opportunity score (0-100) — how much upside is likely left.
    Separate from health score. A token can be perfectly healthy (89/100)
    but have zero opportunity left because it already did 60x.

    Returns (score, label).
    """
    mc = metrics["market_cap"] or 0
    pct = metrics["price_change_24h"]
    vol = metrics["volume_24h"]
    liq = metrics["liquidity_usd"]

    score = 50  # neutral start

    # MC size — lower MC = more room to run
    if mc < 50_000:
        score += 25       # micro cap, massive upside potential
    elif mc < 200_000:
        score += 20       # small cap, strong upside
    elif mc < 500_000:
        score += 15       # mid entry
    elif mc < 1_000_000:
        score += 10       # getting larger
    elif mc < 5_000_000:
        score += 5        # established, moderate upside
    else:
        score -= 5        # large cap for a memecoin, limited upside

    # Already pumped — the bigger the move, the less opportunity remains
    if pct > 1000:
        score -= 30       # 10x+ already happened
    elif pct > 500:
        score -= 20       # 5x already happened
    elif pct > 200:
        score -= 12       # 2-5x already happened
    elif pct > 100:
        score -= 5        # doubled, some move priced in
    elif 10 <= pct <= 100:
        score += 5        # sweet spot — momentum started, room left
    elif 0 <= pct < 10:
        score += 10       # early — hasn't moved much yet

    # Volume relative to MC — high vol/MC = price hasn't caught up yet
    if mc > 0:
        vol_mc = vol / mc
        if vol_mc > 2.0:
            score += 10   # volume way ahead of MC — price lag
        elif vol_mc > 1.0:
            score += 5
        elif vol_mc < 0.1:
            score -= 5    # no volume interest

    score = max(0, min(100, score))

    if score >= 75:
        label = "High Opportunity"
    elif score >= 50:
        label = "Moderate Opportunity"
    elif score >= 30:
        label = "Low Opportunity"
    else:
        label = "Late Entry"

    return score, label


def calculate_ai_score(metrics: dict) -> dict:
    """
    Runs all scoring functions, sums them, and assigns a human verdict.
    Also calculates a separate opportunity score (how much upside is left).

    Components (total 100 pts):
      Liquidity Health      0-20  (liq/MC ratio, not raw USD)
      Volume Velocity       0-20  (turnover + pace, not raw 24h)
      Momentum              0-20  (rewards sweet spot, penalizes late entries)
      Holder Distribution   0-15  (activity count + buy/sell balance)
      Contract Safety       0-15  (honeypot/wash detection)
      Market Strength       0-10  (MC range + volume-momentum alignment)
    """
    components = {
        "liquidity":            _score_liquidity(metrics),
        "volume":               _score_volume(metrics),
        "momentum":             _score_momentum(metrics),
        "holder_distribution":  _score_holder_distribution(metrics),
        "contract_safety":      _score_contract_safety(metrics),
        "market_strength":      _score_market_strength(metrics),
    }

    total = round(sum(components.values()), 1)

    # Determine verdict based on thresholds (highest match wins)
    verdict = "AVOID"
    for label, threshold in VERDICT_THRESHOLDS.items():
        if total >= threshold:
            verdict = label
            break

    opp_score, opp_label = _score_opportunity(metrics)

    return {
        "components":       components,
        "total":            total,
        "verdict":          verdict,
        "opportunity":      opp_score,
        "opportunity_label": opp_label,
    }


# ── Public entry points ───────────────────────────────────────────────────────

async def fetch_current_market_cap(address: str) -> Optional[float]:
    """Returns the current market cap (USD) for an address, or None on failure.
    Falls back to fdv if marketCap is null (common on pump.fun tokens).

    Uses allow_any_dex=True so trades already open on now-unsupported
    DEXes can still be tracked to exit; the DEX allowlist only gates
    the scanner's open path.
    """
    pair = await fetch_token_data(address, allow_any_dex=True)
    if pair is None:
        return None
    return float(pair.get("marketCap") or pair.get("fdv") or 0) or None


async def fetch_live_data(address: str) -> Optional[dict]:
    """
    Returns live token data dict, or None on failure.
    Keys: market_cap, liquidity_usd, price_usd, symbol, price_changes, volume

    Uses allow_any_dex=True so the paper monitor can track trades
    opened before the filter (or against DEXes that later dropped off
    the allowlist) all the way to their TP/SL/expired exit.
    """
    pair = await fetch_token_data(address, allow_any_dex=True)
    if pair is None:
        return None
    mc     = float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = float((pair.get("liquidity") or {}).get("usd") or 0)
    price  = float(pair.get("priceUsd") or 0)
    pc     = pair.get("priceChange") or {}
    vol    = pair.get("volume") or {}
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
        "volume": {
            "m5":  float(vol.get("m5")  or 0),
            "h1":  float(vol.get("h1")  or 0),
            "h6":  float(vol.get("h6")  or 0),
            "h24": float(vol.get("h24") or 0),
        },
    }


async def fetch_sol_price_usd() -> float:
    """Returns current SOL/USD price from DexScreener, or 0 on failure."""
    url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url) as resp:
                data = await resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return 0.0
        # Use the pair with the highest liquidity for the most accurate price
        pairs.sort(key=lambda p: p.get("liquidity", {}).get("usd", 0), reverse=True)
        return float(pairs[0].get("priceUsd") or 0)
    except Exception:
        return 0.0


async def scan_token(address: str, allow_any_dex: bool = False) -> Optional[dict]:
    """
    High-level function called by handlers.
    Returns a combined dict with metrics + AI score, or None on failure.
    allow_any_dex=True for manual scans (paste CA) so users can scan
    any token regardless of DEX. The auto-trading pipeline uses the default
    (False) to enforce the allowlist.
    """
    pair = await fetch_token_data(address, allow_any_dex=allow_any_dex)
    if pair is None:
        return None

    metrics = parse_token_metrics(pair)
    score_data = calculate_ai_score(metrics)

    return {**metrics, **score_data}
