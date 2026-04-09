"""
harvester.py — Agent 1: Token Harvester

WebSocket cascade for real-time new token detection:
  1. pumpdev.io (primary)
  2. PumpPortal (fallback 1)
  3. Helius Enhanced WS (fallback 2)
  4. HTTP polling (final fallback)

Also polls DexScreener every 60 seconds for established tokens.

Logs every run to AgentLogs: tokens_found, tokens_saved, run_at.
"""

import asyncio
import json
import logging
import time

import aiohttp

from bot import state
from bot.config import HELIUS_RPC_URL
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import token_exists, save_token, log_agent_run

logger = logging.getLogger(__name__)

PROFILES_URL     = "https://api.dexscreener.com/token-profiles/latest/v1"
PUMPFUN_URL      = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
PUMPFUN_DETAIL   = "https://frontend-api.pump.fun/coins/{mint}"
RUGCHECK_URL     = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"

# WebSocket endpoints — tried in order
PUMPDEV_WS       = "wss://pumpdev.io/ws"
PUMPPORTAL_WS    = "wss://pumpportal.fun/api/data"
PUMP_PROGRAM_ID  = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

POLL_INTERVAL    = 60   # seconds
STARTUP_DELAY    = 20   # seconds after bot start
WS_RECONNECT     = 30   # seconds before reconnect attempt
WS_CONNECT_TIMEOUT = 10  # seconds to try each WS source


# ── Shared helpers ───────────────────────────────────────────────────────────

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
    except Exception as exc:
        logger.debug("Harvester: rugcheck failed for %s: %s", mint[:12], exc)
        return None


def _parse_pumpfun_social(token: dict) -> str:
    links = {
        "twitter":  bool(token.get("twitter")),
        "telegram": bool(token.get("telegram")),
        "website":  bool(token.get("website")),
    }
    return json.dumps(links)


async def _save_pumpfun_token(token: dict, via: str) -> bool:
    """
    Saves a pump.fun token to the database.
    Returns True if saved (new), False if skipped (already exists).
    """
    mint = token.get("mint")
    if not mint:
        return False
    if await token_exists(mint):
        return False

    name       = token.get("name") or "Unknown"
    symbol     = token.get("symbol") or "???"
    market_cap = float(token.get("usd_market_cap") or 0) or None

    vsol = float(token.get("virtual_sol_reserves") or 0)
    vtok = float(token.get("virtual_token_reserves") or 0)
    price_usd = (vsol / vtok) if vsol > 0 and vtok > 0 else None

    bonding_curve = float(token.get("king_of_the_hill_progress") or 0)
    reply_count   = int(token.get("reply_count") or 0)
    graduated     = bool(token.get("raydium_pool"))
    social_links  = _parse_pumpfun_social(token)

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

    logger.info(
        "Harvester[%s]: saved %s (%s) MC=%s curve=%.0f%%",
        via, symbol, mint[:12],
        f"${market_cap/1000:.0f}K" if market_cap else "?",
        bonding_curve,
    )
    return True


# ── WebSocket source 1: pumpdev.io ───────────────────────────────────────────

async def _ws_pumpdev(session: aiohttp.ClientSession) -> None:
    """
    pumpdev.io — plain WebSocket, sends JSON messages with new token data.
    """
    logger.info("Harvester WS: trying pumpdev.io...")
    async with session.ws_connect(
        PUMPDEV_WS,
        timeout=aiohttp.ClientTimeout(total=WS_CONNECT_TIMEOUT),
        heartbeat=25,
    ) as ws:
        state.harvester_ws_connected = True
        state.harvester_ws_source = "pumpdev"
        logger.info("Harvester WS: connected to pumpdev.io")

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    if isinstance(data, dict) and data.get("mint"):
                        saved = await _save_pumpfun_token(data, via="pumpdev")
                        if saved:
                            state.harvester_ws_tokens_today += 1
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and item.get("mint"):
                                saved = await _save_pumpfun_token(item, via="pumpdev")
                                if saved:
                                    state.harvester_ws_tokens_today += 1
                except (json.JSONDecodeError, TypeError):
                    pass
            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                break


# ── WebSocket source 2: PumpPortal ──────────────────────────────────────────

async def _ws_pumpportal(session: aiohttp.ClientSession) -> None:
    """
    PumpPortal — JSON WebSocket, requires subscription message.
    """
    logger.info("Harvester WS: trying PumpPortal...")
    async with session.ws_connect(
        PUMPPORTAL_WS,
        timeout=aiohttp.ClientTimeout(total=WS_CONNECT_TIMEOUT),
        heartbeat=25,
    ) as ws:
        # Subscribe to new token events
        await ws.send_json({"method": "subscribeNewToken"})

        state.harvester_ws_connected = True
        state.harvester_ws_source = "pumpportal"
        logger.info("Harvester WS: connected to PumpPortal")

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    if isinstance(data, dict):
                        # PumpPortal sends token data directly or wrapped
                        token_data = data
                        if "mint" not in token_data and "token" in token_data:
                            token_data = token_data["token"]
                        if token_data.get("mint"):
                            saved = await _save_pumpfun_token(token_data, via="pumpportal")
                            if saved:
                                state.harvester_ws_tokens_today += 1
                except (json.JSONDecodeError, TypeError):
                    pass
            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                break


# ── WebSocket source 3: Helius Enhanced WS ──────────────────────────────────

def _helius_ws_url() -> str:
    """Convert HELIUS_RPC_URL (https) to wss for WebSocket."""
    url = HELIUS_RPC_URL.replace("https://", "wss://").replace("http://", "ws://")
    return url


async def _ws_helius(session: aiohttp.ClientSession) -> None:
    """
    Helius Enhanced WebSocket — subscribe to pump.fun program logs
    to detect new token mints in real time.
    """
    ws_url = _helius_ws_url()
    logger.info("Harvester WS: trying Helius at %s...", ws_url[:40])

    async with session.ws_connect(
        ws_url,
        timeout=aiohttp.ClientTimeout(total=WS_CONNECT_TIMEOUT),
        heartbeat=30,
    ) as ws:
        # Subscribe to pump.fun program logs
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [PUMP_PROGRAM_ID]},
                {"commitment": "confirmed"},
            ],
        }
        await ws.send_json(subscribe_msg)

        state.harvester_ws_connected = True
        state.harvester_ws_source = "helius"
        logger.info("Harvester WS: connected to Helius (pump.fun program logs)")

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    result = data.get("params", {}).get("result", {})
                    value = result.get("value", {})
                    logs = value.get("logs") or []
                    signature = value.get("signature")

                    if not logs or not signature:
                        continue

                    # Look for "InitializeMint" or token creation logs
                    is_new_token = any(
                        "InitializeMint" in log or "create" in log.lower()
                        for log in logs
                    )
                    if not is_new_token:
                        continue

                    # Extract mint address from logs
                    # Logs typically contain the mint address after "Program log: "
                    mint = None
                    for log in logs:
                        # Look for base58 addresses in program logs
                        if "Program log: " in log:
                            parts = log.split()
                            for part in parts:
                                # Solana addresses are 32-44 chars, base58
                                if 32 <= len(part) <= 44 and part.isalnum():
                                    mint = part
                                    break
                        if mint:
                            break

                    if mint and not await token_exists(mint):
                        # Fetch token details from pump.fun REST
                        try:
                            detail_url = PUMPFUN_DETAIL.format(mint=mint)
                            async with aiohttp.ClientSession(
                                timeout=aiohttp.ClientTimeout(total=5)
                            ) as detail_session:
                                async with detail_session.get(detail_url) as resp:
                                    if resp.status == 200:
                                        token_data = await resp.json(content_type=None)
                                        if token_data and token_data.get("mint"):
                                            saved = await _save_pumpfun_token(token_data, via="helius")
                                            if saved:
                                                state.harvester_ws_tokens_today += 1
                        except Exception as exc:
                            logger.debug("Harvester WS[helius]: detail fetch failed for %s: %s",
                                         mint[:12], exc)

                except (json.JSONDecodeError, TypeError, KeyError):
                    pass
            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                break


# ── WebSocket cascade ────────────────────────────────────────────────────────

WS_SOURCES = [
    ("pumpdev",    _ws_pumpdev),
    ("pumpportal", _ws_pumpportal),
    ("helius",     _ws_helius),
]


async def _websocket_loop() -> None:
    """
    Tries WebSocket sources in cascade order.
    If one fails, tries the next. After all fail, waits and restarts.
    """
    while True:
        for source_name, ws_func in WS_SOURCES:
            state.harvester_ws_connected = False
            state.harvester_ws_source = "none"
            try:
                async with aiohttp.ClientSession() as session:
                    await ws_func(session)
            except asyncio.CancelledError:
                state.harvester_ws_connected = False
                state.harvester_ws_source = "none"
                return
            except Exception as exc:
                logger.warning("Harvester WS[%s]: failed — %s", source_name, exc)

            state.harvester_ws_connected = False
            state.harvester_ws_source = "none"

            # Brief pause before trying next source
            await asyncio.sleep(3)

        # All sources failed — wait before full retry
        logger.info("Harvester WS: all sources failed, retrying in %ds...", WS_RECONNECT)
        await asyncio.sleep(WS_RECONNECT)


# ── DexScreener polling ──────────────────────────────────────────────────────

async def _fetch_profiles() -> list[dict]:
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PROFILES_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.error("Harvester: DexScreener fetch failed: %s", exc)
        return []


async def _harvest_dexscreener() -> tuple[int, int]:
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
        if not mint or await token_exists(mint):
            continue

        pair    = await fetch_token_data(mint)
        metrics = parse_token_metrics(pair) if pair else {}

        name          = metrics.get("name")   or profile.get("name")   or "Unknown"
        symbol        = metrics.get("symbol") or profile.get("symbol") or "???"
        price_usd     = metrics.get("price_usd")
        market_cap    = metrics.get("market_cap")
        liquidity_usd = metrics.get("liquidity_usd")
        volume_24h    = metrics.get("volume_24h")

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

    return found, saved


# ── Pump.fun polling fallback ────────────────────────────────────────────────

async def _fetch_pumpfun_tokens() -> list[dict]:
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PUMPFUN_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.debug("Harvester: Pump.fun poll failed: %s", exc)
        return []


async def _harvest_pumpfun_poll() -> tuple[int, int]:
    """Polling fallback — only runs when WebSocket is disconnected."""
    tokens = await _fetch_pumpfun_tokens()
    found = len(tokens)
    saved = 0

    for token in tokens:
        detail_mint = token.get("mint")
        if not detail_mint:
            continue

        # Fetch detail for richer data
        try:
            url = PUMPFUN_DETAIL.format(mint=detail_mint)
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            ) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        detail = await resp.json(content_type=None)
                        if detail:
                            token = detail
        except Exception:
            pass

        if await _save_pumpfun_token(token, via="poll"):
            saved += 1
            state.harvester_poll_tokens_today += 1

    return found, saved


# ── Polling loop (DexScreener + Pump.fun fallback) ───────────────────────────

async def _polling_loop() -> None:
    """
    Runs every 60 seconds:
    - Always polls DexScreener
    - Only polls Pump.fun if WebSocket is disconnected
    """
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Harvester polling started — every %ds", POLL_INTERVAL)

    while True:
        try:
            dex_found, dex_saved = await _harvest_dexscreener()

            pump_found, pump_saved = 0, 0
            pump_source = "ws"
            if not state.harvester_ws_connected:
                pump_found, pump_saved = await _harvest_pumpfun_poll()
                pump_source = "poll"

            total_found = dex_found + pump_found
            total_saved = dex_saved + pump_saved

            ws_status = "connected" if state.harvester_ws_connected else "disconnected"
            await log_agent_run(
                agent_name="harvester",
                tokens_found=total_found,
                tokens_saved=total_saved,
                notes=f"dex={dex_found}/{dex_saved} pump({pump_source})={pump_found}/{pump_saved} ws={ws_status}",
            )

            if total_saved > 0:
                logger.info(
                    "Harvester poll — dex:%d/%d pump(%s):%d/%d ws=%s",
                    dex_found, dex_saved, pump_source, pump_found, pump_saved, ws_status,
                )

        except Exception as exc:
            logger.error("Harvester poll error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)


# ── Main entry point ─────────────────────────────────────────────────────────

async def harvester_loop() -> None:
    """
    Starts both the WebSocket cascade and the polling loop concurrently.
    WebSocket cascade: pumpdev.io → PumpPortal → Helius (auto-failover).
    Polling: DexScreener always + Pump.fun REST when WS is down.
    """
    logger.info("Harvester agent starting — WebSocket cascade + polling mode")

    await asyncio.gather(
        _websocket_loop(),
        _polling_loop(),
        return_exceptions=True,
    )
