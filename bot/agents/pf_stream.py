"""
pf_stream.py — Real-time Pump.fun / Bonk launch discovery via PumpPortal.

Discovers tokens the MOMENT they're created on-chain (block 0), earlier
than DexScreener can index them, by streaming PumpPortal's free
new-token feed:

  wss://pumpportal.fun/api/data   ->  {"method": "subscribeNewToken"}

Design (MVP, experiment-flagged):
  1. Stream every new launch. Validate the mint suffix (pump/bonk/bags).
  2. Hold each discovered mint in a short-lived watchlist.
  3. Re-inject watchlisted mints into state.pending_candidates every
     few seconds until they age out. The scanner re-fetches DexScreener
     each tick, so a token enters the normal evaluation pipeline
     (confidence_engine + entry momentum gate + open_paper_trade) the
     moment DexScreener can price it AND it shows momentum — instead of
     us waiting for DexScreener's trending list to surface it.

Nothing downstream changes: pf_stream is just another discovery source,
like laserstream / tg_scraper. It is GATED OFF by default
(pf_stream_enabled=0) so it can't contaminate the Profit Protection v2
measurement until we deliberately A/B it. Toggle live, no redeploy:

  /setparam pf_stream_enabled 1

PumpPortal new-token feed is free (rate-limited). We do NOT subscribe to
per-token trade streams (those are metered).
"""

import asyncio
import json
import logging

import aiohttp

from bot import state
from bot.scanner import mint_suffix_ok

logger = logging.getLogger(__name__)

WS_URL = "wss://pumpportal.fun/api/data"
STARTUP_DELAY = 50          # let the rest of the bot boot first
RECONNECT_DELAY = 5
REINJECT_INTERVAL = 20      # re-offer watchlisted mints to the scanner
ENABLE_RECHECK_SEC = 60     # how often to re-read the on/off flag when idle

# Watchlist guardrails. subscribeNewToken is a firehose (many launches/sec);
# these keep us from flooding the scanner. Downstream gates
# (max_open_paper_trades, confidence, momentum gate) cap actual opens, but
# we still don't want to evaluate thousands of dead mints per tick.
MAX_WATCH = 200             # max mints tracked at once
MAX_AGE_SEC = 300           # evict a mint after 5 min if still unindexed
MAX_PENDING_FROM_PF = 25    # cap pf_stream's share of pending_candidates

# mint -> first_seen monotonic timestamp
_watchlist: dict[str, float] = {}
# mint -> {name, symbol} hint from the PumpPortal payload
_meta: dict[str, dict] = {}


async def _pf_enabled() -> bool:
    try:
        from database.models import get_param
        val = await get_param("pf_stream_enabled")
        return bool(val and val >= 0.5)
    except Exception:
        return False


def _now() -> float:
    return asyncio.get_event_loop().time()


def _handle_new_token(data: dict) -> None:
    """Add a freshly-created mint to the watchlist (if room + valid suffix)."""
    mint = data.get("mint")
    if not mint or not mint_suffix_ok(mint):
        return
    if mint in _watchlist:
        return
    if len(_watchlist) >= MAX_WATCH:
        return  # firehose backpressure — drop until watchlist drains
    _watchlist[mint] = _now()
    _meta[mint] = {
        "name": data.get("name"),
        "symbol": data.get("symbol"),
    }
    logger.info(
        "pf_stream: new launch %s (%s) — watching",
        mint[:12], (data.get("symbol") or "?"),
    )


async def _reinject_loop() -> None:
    """Re-offer watchlisted mints to the scanner until they age out.

    The scanner clears pending_candidates each tick, so a token must be
    re-injected to keep getting evaluated while we wait for DexScreener to
    index it. Evicts mints older than MAX_AGE_SEC.
    """
    while True:
        await asyncio.sleep(REINJECT_INTERVAL)
        if not _watchlist:
            continue
        if not await _pf_enabled():
            # Gate flipped off — drop the watchlist so it can't leak in later.
            _watchlist.clear()
            _meta.clear()
            continue

        now = _now()
        # Evict aged-out mints
        expired = [m for m, ts in _watchlist.items() if now - ts > MAX_AGE_SEC]
        for m in expired:
            _watchlist.pop(m, None)
            _meta.pop(m, None)

        try:
            existing = {c.get("mint") for c in state.pending_candidates}
            pf_in_pending = sum(
                1 for c in state.pending_candidates if c.get("source") == "pf_stream"
            )
            injected = 0
            for mint in list(_watchlist.keys()):
                if pf_in_pending + injected >= MAX_PENDING_FROM_PF:
                    break
                if mint in existing:
                    continue
                meta = _meta.get(mint) or {}
                state.pending_candidates.append({
                    "mint": mint,
                    "name": meta.get("name"),
                    "symbol": meta.get("symbol"),
                    "mcap": None,
                    "liquidity": None,
                    "source": "pf_stream",
                })
                state.data_points_today += 1
                injected += 1
            if injected:
                logger.info(
                    "pf_stream: re-injected %d/%d watched mints into scanner",
                    injected, len(_watchlist),
                )
        except Exception as exc:
            logger.debug("pf_stream: reinject error: %s", exc)


async def pf_stream_loop() -> None:
    """Stream PumpPortal new-token events. Auto-reconnects. Gated off by
    pf_stream_enabled (default 0) — idles until toggled on via /setparam."""
    await asyncio.sleep(STARTUP_DELAY)
    asyncio.create_task(_reinject_loop())
    logger.info("pf_stream: started (gated by pf_stream_enabled)")

    while True:
        if not await _pf_enabled():
            await asyncio.sleep(ENABLE_RECHECK_SEC)
            continue

        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                    await ws.send_json({"method": "subscribeNewToken"})
                    logger.info("pf_stream: connected — subscribed to new tokens")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            # Cheap re-check so flipping the flag off stops
                            # ingestion within one message instead of one
                            # whole reconnect cycle.
                            if not await _pf_enabled():
                                logger.info("pf_stream: disabled — closing stream")
                                break
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue
                            # Subscription ack / status messages have no mint.
                            if isinstance(data, dict) and data.get("mint"):
                                _handle_new_token(data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            logger.warning("pf_stream: connection closed")
                            break

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("pf_stream: connection error: %s — reconnecting in %ds", exc, RECONNECT_DELAY)
        except Exception as exc:
            logger.error("pf_stream: unexpected error: %s — reconnecting in %ds", exc, RECONNECT_DELAY)

        await asyncio.sleep(RECONNECT_DELAY)
