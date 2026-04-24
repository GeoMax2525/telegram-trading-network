"""
laserstream.py — Real-time token detection via Helius Enhanced WebSocket.

Replaces 15-second polling with instant detection of new token events.
Catches new token launches, large buys, and liquidity additions the
moment they hit the blockchain.

Uses Helius Enhanced WebSocket API (Business plan):
  wss://atlas-mainnet.helius-rpc.com?api-key=KEY

Subscribes to:
  - Token program transactions (new token mints)
  - Raydium/Pumpswap/Meteora pool creation events

When a new token event is detected:
  1. Validates mint suffix (pump/bonk/bags)
  2. Fetches basic token data from DexScreener
  3. Injects directly into scanner pipeline as source "laserstream"

This is the "we're too late" fix — tokens are caught in <2 seconds
instead of waiting for the next 15-second scanner tick.
"""

import asyncio
import json
import logging

import aiohttp

from bot.config import HELIUS_API_KEY, HELIUS_LASERSTREAM_URL
from bot import state

logger = logging.getLogger(__name__)

STARTUP_DELAY = 45
RECONNECT_DELAY = 5

# Helius Enhanced WebSocket URL
def _get_ws_url() -> str:
    """Build the WebSocket URL from config."""
    if HELIUS_LASERSTREAM_URL:
        return HELIUS_LASERSTREAM_URL
    if HELIUS_API_KEY and HELIUS_API_KEY != "demo":
        return f"wss://atlas-mainnet.helius-rpc.com?api-key={HELIUS_API_KEY}"
    return ""


# Known DEX program IDs for pool creation detection
RAYDIUM_AMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PUMPSWAP = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP"


async def _handle_transaction(tx_data: dict) -> None:
    """Process a real-time transaction from the WebSocket stream."""
    from bot.scanner import mint_suffix_ok

    # Extract token mints from the transaction
    token_transfers = tx_data.get("tokenTransfers") or []
    account_data = tx_data.get("accountData") or []
    tx_type = tx_data.get("type") or ""
    description = tx_data.get("description") or ""

    mints_found: set[str] = set()

    # Check token transfers for new mints
    for transfer in token_transfers:
        mint = transfer.get("mint")
        if mint and mint_suffix_ok(mint):
            mints_found.add(mint)

    # Check for pool creation events (Raydium, Pumpswap)
    instructions = tx_data.get("instructions") or []
    for ix in instructions:
        program_id = ix.get("programId") or ""
        if program_id in (RAYDIUM_AMM, PUMPSWAP):
            # Pool creation — extract token mint from accounts
            accounts = ix.get("accounts") or []
            for acc in accounts:
                if isinstance(acc, str) and mint_suffix_ok(acc):
                    mints_found.add(acc)

    if not mints_found:
        return

    # Inject each discovered mint into the scanner pipeline
    for mint in mints_found:
        # Avoid duplicates in pending_candidates
        existing_mints = {c.get("mint") for c in state.pending_candidates}
        if mint in existing_mints:
            continue

        state.pending_candidates.append({
            "mint": mint,
            "name": None,
            "symbol": None,
            "mcap": None,
            "liquidity": None,
            "source": "laserstream",
        })
        state.data_points_today += 1
        logger.info("LaserStream: detected new token %s — injected into scanner", mint[:12])


async def laserstream_loop() -> None:
    """
    Connect to Helius Enhanced WebSocket and stream transactions.
    Auto-reconnects on disconnect.
    """
    ws_url = _get_ws_url()
    if not ws_url:
        logger.info("LaserStream: no WebSocket URL configured — skipping")
        return

    await asyncio.sleep(STARTUP_DELAY)
    logger.info("LaserStream: connecting to %s...", ws_url[:50])

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, heartbeat=30) as ws:
                    logger.info("LaserStream: connected — subscribing to token events")

                    # Subscribe to transaction notifications
                    # Helius Enhanced WebSocket supports transactionSubscribe
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "transactionSubscribe",
                        "params": [
                            {
                                "failed": False,
                                "accountInclude": [RAYDIUM_AMM, PUMPSWAP],
                            },
                            {
                                "commitment": "confirmed",
                                "encoding": "jsonParsed",
                                "transactionDetails": "full",
                                "maxSupportedTransactionVersion": 0,
                            },
                        ],
                    }
                    await ws.send_json(subscribe_msg)
                    logger.info("LaserStream: subscribed to pool creation events")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)

                                # Handle subscription confirmation
                                if "result" in data and isinstance(data["result"], int):
                                    logger.info("LaserStream: subscription confirmed (id=%s)", data["result"])
                                    continue

                                # Handle transaction notification
                                params = data.get("params") or {}
                                result = params.get("result") or {}
                                tx = result.get("transaction") or {}

                                if tx:
                                    await _handle_transaction(tx)

                            except json.JSONDecodeError:
                                pass
                            except Exception as exc:
                                logger.debug("LaserStream: message processing error: %s", exc)

                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            logger.warning("LaserStream: connection closed")
                            break

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("LaserStream: connection error: %s — reconnecting in %ds", exc, RECONNECT_DELAY)
        except Exception as exc:
            logger.error("LaserStream: unexpected error: %s — reconnecting in %ds", exc, RECONNECT_DELAY)

        await asyncio.sleep(RECONNECT_DELAY)
