"""
harvester.py — Agent 1: Token Harvester

Primary: Pump.fun WebSocket for real-time new token detection.
Fallback: HTTP polling every 60 seconds if WebSocket disconnects.
Also polls DexScreener every 60 seconds for established tokens.

Sources:
  - Pump.fun WebSocket (wss://frontend-api.pump.fun/socket.io/) — instant
  - Pump.fun REST API — fallback polling
  - DexScreener profiles — established tokens

Logs every run to AgentLogs: tokens_found, tokens_saved, run_at.
"""

import asyncio
import json
import logging
import time

import aiohttp

from bot import state
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import token_exists, save_token, log_agent_run

logger = logging.getLogger(__name__)

PROFILES_URL   = "https://api.dexscreener.com/token-profiles/latest/v1"
PUMPFUN_URL    = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
PUMPFUN_DETAIL = "https://frontend-api.pump.fun/coins/{mint}"
PUMPFUN_WS_URL = "wss://frontend-api.pump.fun/socket.io/?EIO=4&transport=websocket"
RUGCHECK_URL   = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
POLL_INTERVAL  = 60   # seconds
STARTUP_DELAY  = 20   # seconds after bot start
WS_RECONNECT   = 30   # seconds before reconnect attempt


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


# ── Pump.fun WebSocket ───────────────────────────────────────────────────────

async def _pumpfun_websocket_loop() -> None:
    """
    Connects to Pump.fun WebSocket for real-time new token events.
    Socket.IO over WebSocket (EIO=4 protocol):
      - Server sends "0{...}" on connect (open)
      - Client sends "40" to connect to default namespace
      - Server sends "40{...}" to confirm namespace
      - Server sends "2" as ping, client replies "3" as pong
      - Events come as "42[event_name, data]"
      - We subscribe to "newToken" events
    Auto-reconnects on disconnect.
    """
    while True:
        state.harvester_ws_connected = False
        try:
            async with aiohttp.ClientSession() as session:
                logger.info("Harvester WS: connecting to Pump.fun...")
                async with session.ws_connect(
                    PUMPFUN_WS_URL,
                    timeout=aiohttp.ClientTimeout(total=30),
                    heartbeat=25,
                ) as ws:
                    state.harvester_ws_connected = True
                    logger.info("Harvester WS: connected to Pump.fun")

                    # Wait for EIO open packet ("0{...}")
                    # Then send Socket.IO connect to default namespace
                    namespace_connected = False

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.data

                            # EIO open packet
                            if data.startswith("0"):
                                await ws.send_str("40")
                                continue

                            # Namespace connect confirmation
                            if data.startswith("40"):
                                namespace_connected = True
                                logger.info("Harvester WS: namespace connected, subscribing...")
                                # Subscribe to new token events
                                await ws.send_str('42["subscribeNewToken"]')
                                continue

                            # Ping from server — respond with pong
                            if data == "2":
                                await ws.send_str("3")
                                continue

                            # Event message: "42[event_name, data]"
                            if data.startswith("42"):
                                try:
                                    payload = json.loads(data[2:])
                                    if isinstance(payload, list) and len(payload) >= 2:
                                        event_name = payload[0]
                                        event_data = payload[1]

                                        if event_name == "newToken" and isinstance(event_data, dict):
                                            saved = await _save_pumpfun_token(event_data, via="ws")
                                            if saved:
                                                state.harvester_ws_tokens_today += 1
                                except (json.JSONDecodeError, IndexError, TypeError) as exc:
                                    logger.debug("Harvester WS: parse error: %s", exc)
                                continue

                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            logger.warning("Harvester WS: connection closed/error")
                            break

        except asyncio.CancelledError:
            logger.info("Harvester WS: cancelled")
            state.harvester_ws_connected = False
            return
        except Exception as exc:
            logger.warning("Harvester WS: error — %s", exc)

        state.harvester_ws_connected = False
        logger.info("Harvester WS: reconnecting in %ds...", WS_RECONNECT)
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
    Starts both the WebSocket listener and the polling loop concurrently.
    WebSocket handles real-time Pump.fun tokens.
    Polling handles DexScreener and acts as Pump.fun fallback.
    """
    logger.info("Harvester agent starting — WebSocket + polling mode")

    # Run both concurrently — if one crashes, the other keeps going
    await asyncio.gather(
        _pumpfun_websocket_loop(),
        _polling_loop(),
        return_exceptions=True,
    )
