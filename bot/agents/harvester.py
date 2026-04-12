"""
harvester.py — Agent 1: Token Harvester (Graduated Tokens Only)

Tracks only tokens that have graduated to a DEX (Raydium, PumpSwap).
Bonding curve tokens are ignored — no MC data available from DexScreener.

Sources:
  1. DexScreener new Solana pairs (every 60s)
  2. DexScreener PumpSwap search (every 60s)
  3. GMGN trending tokens (handled by gmgn_agent.py)

Every token saved has real MC, liquidity, and volume from day one.
"""

import asyncio
import json
import logging

import aiohttp

from bot import state
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import token_exists, save_token, log_agent_run

logger = logging.getLogger(__name__)

PROFILES_URL     = "https://api.dexscreener.com/token-profiles/latest/v1"
RUGCHECK_URL     = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
DEXSCREENER_NEW  = "https://api.dexscreener.com/latest/dex/pairs/solana?sort=pairAge&order=asc"

POLL_INTERVAL    = 60   # seconds
STARTUP_DELAY    = 20


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _fetch_rugcheck(mint: str) -> dict | None:
    try:
        url = RUGCHECK_URL.format(mint=mint)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception:
        return None


async def _save_graduated_token(mint: str, pair: dict, source: str) -> bool:
    """Save a graduated token with full DexScreener data. Returns True if new."""
    if not mint or await token_exists(mint):
        return False

    metrics = parse_token_metrics(pair)
    mc = metrics.get("market_cap", 0) or 0
    liq = metrics.get("liquidity_usd", 0) or 0

    # Only save tokens with real MC data
    if mc <= 0:
        return False

    name = metrics.get("name", "Unknown")
    symbol = metrics.get("symbol", "???")

    # Rugcheck
    rugcheck_score = None
    rugcheck_risks = None
    rc = await _fetch_rugcheck(mint)
    if rc:
        raw_score = rc.get("score")
        rugcheck_score = int(raw_score) if raw_score is not None else None
        risks = rc.get("risks") or []
        if risks:
            risk_names = [r.get("name", "") for r in risks if r.get("name")]
            rugcheck_risks = json.dumps(risk_names[:10])

    await save_token(
        mint=mint, name=name, symbol=symbol,
        price_usd=metrics.get("price_usd"),
        market_cap=mc,
        liquidity_usd=liq,
        volume_24h=metrics.get("volume_24h"),
        rugcheck_score=rugcheck_score,
        rugcheck_risks=rugcheck_risks,
        source=source,
    )

    logger.info(
        "Harvester[%s]: %s (%s) MC=$%s liq=$%s",
        source, symbol, mint[:12],
        f"{mc/1000:.0f}K" if mc < 1e6 else f"{mc/1e6:.1f}M",
        f"{liq/1000:.0f}K" if liq else "?",
    )
    return True


# ── Source 1: DexScreener Profiles (new Solana tokens) ───────────────────────

async def _harvest_dexscreener_profiles() -> tuple[int, int]:
    """Fetch latest token profiles from DexScreener."""
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PROFILES_URL) as resp:
                if resp.status != 200:
                    return 0, 0
                data = await resp.json(content_type=None)
                profiles = data if isinstance(data, list) else []
    except Exception as exc:
        logger.debug("Harvester: DexScreener profiles failed: %s", exc)
        return 0, 0

    sol_profiles = [
        p for p in profiles
        if p.get("chainId") == "solana"
        and (p.get("tokenAddress") or p.get("address"))
    ]

    found = len(sol_profiles)
    saved = 0

    for profile in sol_profiles:
        mint = profile.get("tokenAddress") or profile.get("address")
        if not mint or await token_exists(mint):
            continue

        pair = await fetch_token_data(mint)
        if not pair:
            continue

        if await _save_graduated_token(mint, pair, "dexscreener"):
            saved += 1
            state.harvester_poll_tokens_today += 1

    return found, saved


# ── Source 2: DexScreener New Pairs Search ───────────────────────────────────

async def _harvest_new_pairs() -> tuple[int, int]:
    """Search DexScreener for brand new Solana pairs."""
    found = saved = 0

    # Search for new pairs on common DEXes
    for query in ["pumpswap", "raydium"]:
        try:
            url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
        except Exception:
            continue

        pairs = data.get("pairs") or []
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        found += len(sol_pairs)

        for pair in sol_pairs[:20]:  # cap per search
            mint = pair.get("baseToken", {}).get("address")
            if not mint:
                continue

            if await _save_graduated_token(mint, pair, "dex_new"):
                saved += 1
                state.harvester_poll_tokens_today += 1

    return found, saved


# ── Main polling loop ────────────────────────────────────────────────────────

async def _poll_once() -> None:
    """Single polling tick — all sources."""
    # DexScreener profiles
    prof_found, prof_saved = await _harvest_dexscreener_profiles()

    # New pairs search
    new_found, new_saved = await _harvest_new_pairs()

    total_found = prof_found + new_found
    total_saved = prof_saved + new_saved

    if total_saved > 0:
        await log_agent_run(
            agent_name="harvester",
            tokens_found=total_found,
            tokens_saved=total_saved,
            notes=f"profiles={prof_found}/{prof_saved} new_pairs={new_found}/{new_saved}",
        )
        logger.info(
            "Harvester: profiles %d/%d | new_pairs %d/%d",
            prof_found, prof_saved, new_found, new_saved,
        )


async def harvester_loop() -> None:
    """Background loop: polls DexScreener every 60 seconds for graduated tokens."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Harvester started — graduated tokens only, polling every %ds", POLL_INTERVAL)

    while True:
        try:
            await _poll_once()
        except Exception as exc:
            logger.error("Harvester error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
