"""
harvester.py — Agent 1: Token Harvester

Polls DexScreener every 60 seconds for the latest Solana token profiles.
For each token not already in the database:
  1. Fetches full market data (MC, liquidity, volume, price) via DexScreener
  2. Checks Rugcheck for a risk score and risk labels
  3. Saves to the Tokens table in PostgreSQL

Logs every run to AgentLogs: tokens_found, tokens_saved, run_at.
"""

import asyncio
import json
import logging

import aiohttp

from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import token_exists, save_token, log_agent_run

logger = logging.getLogger(__name__)

PROFILES_URL  = "https://api.dexscreener.com/token-profiles/latest/v1"
RUGCHECK_URL  = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
POLL_INTERVAL = 60   # seconds
STARTUP_DELAY = 20   # seconds after bot start


async def _fetch_profiles() -> list[dict]:
    """Fetches the latest token profiles from DexScreener. Returns a list."""
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PROFILES_URL) as resp:
                if resp.status != 200:
                    logger.warning(
                        "Harvester: DexScreener profiles returned HTTP %d", resp.status
                    )
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.error("Harvester: failed to fetch profiles: %s", exc)
        return []


async def _fetch_rugcheck(mint: str) -> dict | None:
    """Fetches Rugcheck summary for a mint. Returns parsed JSON or None."""
    try:
        url = RUGCHECK_URL.format(mint=mint)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("Harvester: rugcheck failed for %s: %s", mint, exc)
        return None


async def run_once() -> tuple[int, int]:
    """
    Single harvester tick.
    Returns (tokens_found, tokens_saved).
    """
    profiles = await _fetch_profiles()

    # Keep only Solana tokens with a mint address
    sol_profiles = [
        p for p in profiles
        if p.get("chainId") == "solana"
        and (p.get("tokenAddress") or p.get("address"))
    ]
    tokens_found = len(sol_profiles)
    tokens_saved = 0

    for profile in sol_profiles:
        mint = profile.get("tokenAddress") or profile.get("address")
        if not mint:
            continue

        # Skip tokens already in DB
        if await token_exists(mint):
            continue

        # Fetch full market data from DexScreener
        pair    = await fetch_token_data(mint)
        metrics = parse_token_metrics(pair) if pair else {}

        name          = metrics.get("name")   or profile.get("name")   or "Unknown"
        symbol        = metrics.get("symbol") or profile.get("symbol") or "???"
        price_usd     = metrics.get("price_usd")     or None
        market_cap    = metrics.get("market_cap")    or None
        liquidity_usd = metrics.get("liquidity_usd") or None
        volume_24h    = metrics.get("volume_24h")    or None

        # Fetch Rugcheck (non-blocking — skip gracefully on failure)
        rugcheck_score = None
        rugcheck_risks = None
        rc = await _fetch_rugcheck(mint)
        if rc:
            raw_score = rc.get("score")
            rugcheck_score = int(raw_score) if raw_score is not None else None
            risks = rc.get("risks") or []
            if risks:
                risk_names = [
                    r.get("name", "") for r in risks if r.get("name")
                ]
                rugcheck_risks = json.dumps(risk_names[:10])

        await save_token(
            mint=mint,
            name=name,
            symbol=symbol,
            price_usd=price_usd,
            market_cap=market_cap,
            liquidity_usd=liquidity_usd,
            volume_24h=volume_24h,
            rugcheck_score=rugcheck_score,
            rugcheck_risks=rugcheck_risks,
        )
        tokens_saved += 1
        logger.info(
            "Harvester: saved %s (%s) MC=%s liq=%s rugcheck=%s",
            symbol, mint[:12],
            f"${market_cap/1000:.0f}K" if market_cap else "?",
            f"${liquidity_usd/1000:.0f}K" if liquidity_usd else "?",
            rugcheck_score,
        )

    await log_agent_run(
        agent_name="harvester",
        tokens_found=tokens_found,
        tokens_saved=tokens_saved,
    )
    logger.info(
        "Harvester tick complete — found=%d saved=%d", tokens_found, tokens_saved
    )
    return tokens_found, tokens_saved


async def harvester_loop() -> None:
    """Background loop: runs the harvester every POLL_INTERVAL seconds."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info(
        "Harvester agent started — polling DexScreener every %ds", POLL_INTERVAL
    )
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Harvester loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
