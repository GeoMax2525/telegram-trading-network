"""
harvester.py — Agent 1: Token Harvester

Polls DexScreener and Pump.fun every 60 seconds for the latest Solana tokens.
For each token not already in the database:
  1. Fetches full market data (MC, liquidity, volume, price)
  2. Checks Rugcheck for a risk score and risk labels
  3. Saves to the Tokens table in PostgreSQL

Sources:
  - DexScreener: token profiles (established tokens)
  - Pump.fun: new token launches (bonding curve, social data)

Logs every run to AgentLogs: tokens_found, tokens_saved, run_at.
"""

import asyncio
import json
import logging

import aiohttp

from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import token_exists, save_token, log_agent_run

logger = logging.getLogger(__name__)

PROFILES_URL   = "https://api.dexscreener.com/token-profiles/latest/v1"
PUMPFUN_URL    = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
PUMPFUN_DETAIL = "https://frontend-api.pump.fun/coins/{mint}"
RUGCHECK_URL   = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
POLL_INTERVAL  = 60   # seconds
STARTUP_DELAY  = 20   # seconds after bot start


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


async def _fetch_pumpfun_tokens() -> list[dict]:
    """Fetches the latest tokens from Pump.fun API."""
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PUMPFUN_URL) as resp:
                if resp.status != 200:
                    logger.debug("Harvester: Pump.fun returned HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.debug("Harvester: Pump.fun fetch failed: %s", exc)
        return []


async def _fetch_pumpfun_detail(mint: str) -> dict | None:
    """Fetches detailed token data from Pump.fun."""
    try:
        url = PUMPFUN_DETAIL.format(mint=mint)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("Harvester: Pump.fun detail failed for %s: %s", mint[:12], exc)
        return None


def _parse_pumpfun_social(token: dict) -> str:
    """Returns JSON string of social link presence."""
    links = {
        "twitter":  bool(token.get("twitter")),
        "telegram": bool(token.get("telegram")),
        "website":  bool(token.get("website")),
    }
    return json.dumps(links)


async def _harvest_dexscreener() -> tuple[int, int]:
    """Harvest tokens from DexScreener. Returns (found, saved)."""
    profiles = await _fetch_profiles()

    sol_profiles = [
        p for p in profiles
        if p.get("chainId") == "solana"
        and (p.get("tokenAddress") or p.get("address"))
    ]
    found = len(sol_profiles)
    saved = 0

    for profile in sol_profiles:
        mint = profile.get("tokenAddress") or profile.get("address")
        if not mint:
            continue
        if await token_exists(mint):
            continue

        pair    = await fetch_token_data(mint)
        metrics = parse_token_metrics(pair) if pair else {}

        name          = metrics.get("name")   or profile.get("name")   or "Unknown"
        symbol        = metrics.get("symbol") or profile.get("symbol") or "???"
        price_usd     = metrics.get("price_usd")     or None
        market_cap    = metrics.get("market_cap")    or None
        liquidity_usd = metrics.get("liquidity_usd") or None
        volume_24h    = metrics.get("volume_24h")    or None

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
            price_usd=price_usd, market_cap=market_cap,
            liquidity_usd=liquidity_usd, volume_24h=volume_24h,
            rugcheck_score=rugcheck_score, rugcheck_risks=rugcheck_risks,
            source="dexscreener",
        )
        saved += 1
        logger.info(
            "Harvester[dex]: saved %s (%s) MC=%s liq=%s",
            symbol, mint[:12],
            f"${market_cap/1000:.0f}K" if market_cap else "?",
            f"${liquidity_usd/1000:.0f}K" if liquidity_usd else "?",
        )

    return found, saved


async def _harvest_pumpfun() -> tuple[int, int]:
    """Harvest tokens from Pump.fun. Returns (found, saved)."""
    tokens = await _fetch_pumpfun_tokens()
    found = len(tokens)
    saved = 0

    for token in tokens:
        mint = token.get("mint")
        if not mint:
            continue
        if await token_exists(mint):
            continue

        # Fetch detailed data
        detail = await _fetch_pumpfun_detail(mint)
        if detail:
            token = detail  # use richer data

        name      = token.get("name") or "Unknown"
        symbol    = token.get("symbol") or "???"
        market_cap = float(token.get("usd_market_cap") or 0) or None

        # Compute price from reserves if available
        vsol = float(token.get("virtual_sol_reserves") or 0)
        vtok = float(token.get("virtual_token_reserves") or 0)
        price_usd = None
        if vsol > 0 and vtok > 0:
            # Rough estimate — would need SOL price for accuracy
            price_usd = (vsol / vtok) if vtok > 0 else None

        bonding_curve = float(token.get("king_of_the_hill_progress") or 0)
        reply_count   = int(token.get("reply_count") or 0)
        graduated     = bool(token.get("raydium_pool"))
        social_links  = _parse_pumpfun_social(token)

        # Try to get rugcheck for pump.fun tokens too
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

        # Try DexScreener for liquidity/volume if graduated
        liquidity_usd = None
        volume_24h    = None
        if graduated:
            pair = await fetch_token_data(mint)
            if pair:
                metrics = parse_token_metrics(pair)
                liquidity_usd = metrics.get("liquidity_usd")
                volume_24h    = metrics.get("volume_24h")
                if not market_cap:
                    market_cap = metrics.get("market_cap")
                if not price_usd:
                    price_usd = metrics.get("price_usd")

        await save_token(
            mint=mint, name=name, symbol=symbol,
            price_usd=price_usd, market_cap=market_cap,
            liquidity_usd=liquidity_usd, volume_24h=volume_24h,
            rugcheck_score=rugcheck_score, rugcheck_risks=rugcheck_risks,
            source="pumpfun",
            bonding_curve=bonding_curve,
            social_links=social_links,
            graduated=graduated,
            reply_count=reply_count,
        )
        saved += 1
        logger.info(
            "Harvester[pump]: saved %s (%s) MC=%s curve=%.0f%% grad=%s replies=%d",
            symbol, mint[:12],
            f"${market_cap/1000:.0f}K" if market_cap else "?",
            bonding_curve, graduated, reply_count,
        )

    return found, saved


async def run_once() -> tuple[int, int]:
    """
    Single harvester tick — fetches from DexScreener and Pump.fun.
    Returns (tokens_found, tokens_saved).
    """
    # Run both sources concurrently
    (dex_found, dex_saved), (pump_found, pump_saved) = await asyncio.gather(
        _harvest_dexscreener(),
        _harvest_pumpfun(),
        return_exceptions=False,
    )

    tokens_found = dex_found + pump_found
    tokens_saved = dex_saved + pump_saved

    await log_agent_run(
        agent_name="harvester",
        tokens_found=tokens_found,
        tokens_saved=tokens_saved,
        notes=f"dex={dex_found}/{dex_saved} pump={pump_found}/{pump_saved}",
    )
    logger.info(
        "Harvester tick — dex: %d/%d pump: %d/%d total saved=%d",
        dex_found, dex_saved, pump_found, pump_saved, tokens_saved,
    )
    return tokens_found, tokens_saved


async def harvester_loop() -> None:
    """Background loop: runs the harvester every POLL_INTERVAL seconds."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info(
        "Harvester agent started — polling DexScreener + Pump.fun every %ds", POLL_INTERVAL
    )
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Harvester loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
