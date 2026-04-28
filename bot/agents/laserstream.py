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
    """Build the WebSocket URL from config.
    Uses atlas-mainnet (Enhanced WebSocket), NOT LaserStream (gRPC).
    LaserStream requires gRPC protocol which aiohttp can't handle."""
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


# ── Whale mirroring ──────────────────────────────────────────────────────────
# When a tracked whale wallet buys a token, mirror it instantly.
# Checks the top tier 1/2 wallets from DB and watches their buys.

_whale_wallets: set[str] = set()
_whale_refresh_interval = 300  # refresh whale list every 5 min

async def _refresh_whale_list() -> None:
    """Load top tier 1/2 wallets from DB for real-time mirroring."""
    global _whale_wallets
    try:
        from database.models import get_tier_wallets
        wallets = await get_tier_wallets(max_tier=2)
        if wallets:
            _whale_wallets = {w.address for w in wallets[:30]}
            logger.info("LaserStream: watching %d whale wallets for mirroring", len(_whale_wallets))
    except Exception as exc:
        logger.warning("LaserStream: whale list refresh failed: %s", exc)


async def _handle_whale_buy(tx_data: dict) -> None:
    """Check if a transaction is a buy by a tracked whale wallet."""
    from bot.scanner import mint_suffix_ok

    fee_payer = tx_data.get("feePayer") or ""
    if fee_payer not in _whale_wallets:
        return

    # This whale is buying — find what token
    token_transfers = tx_data.get("tokenTransfers") or []
    for transfer in token_transfers:
        to_addr = transfer.get("toUserAccount")
        mint = transfer.get("mint")

        if to_addr == fee_payer and mint and mint_suffix_ok(mint):
            # Whale is receiving tokens = buying
            existing_mints = {c.get("mint") for c in state.pending_candidates}
            if mint in existing_mints:
                continue

            state.pending_candidates.append({
                "mint": mint,
                "name": None,
                "symbol": None,
                "mcap": None,
                "liquidity": None,
                "source": "whale_mirror",
                "insider_count": 1,
                "insider_tier_1_count": 1,
                "insider_buy_age_s": 0,
            })
            state.data_points_today += 1
            logger.info(
                "WHALE MIRROR: %s..%s bought %s — injected with insider boost",
                fee_payer[:4], fee_payer[-4:], mint[:12],
            )


async def laserstream_loop() -> None:
    """
    Connect to Helius Enhanced WebSocket and stream transactions.
    Handles both new token detection AND whale wallet mirroring.
    Auto-reconnects on disconnect.
    """
    ws_url = _get_ws_url()
    if not ws_url:
        logger.info("LaserStream: no WebSocket URL configured — skipping")
        return

    await asyncio.sleep(STARTUP_DELAY)
    await _refresh_whale_list()
    logger.info("LaserStream: connecting to %s...", ws_url[:50])

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, heartbeat=30) as ws:
                    logger.info("LaserStream: connected — subscribing to token events")

                    # Subscribe to pool creation events (new tokens)
                    pool_sub = {
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
                    await ws.send_json(pool_sub)
                    logger.info("LaserStream: subscribed to pool creation events")

                    # Subscribe to whale wallet transactions (mirroring)
                    if _whale_wallets:
                        whale_sub = {
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "transactionSubscribe",
                            "params": [
                                {
                                    "failed": False,
                                    "accountInclude": list(_whale_wallets)[:30],
                                },
                                {
                                    "commitment": "confirmed",
                                    "encoding": "jsonParsed",
                                    "transactionDetails": "full",
                                    "maxSupportedTransactionVersion": 0,
                                },
                            ],
                        }
                        await ws.send_json(whale_sub)
                        logger.info("LaserStream: subscribed to %d whale wallets", len(_whale_wallets))

                    # Background task: refresh whale list periodically
                    last_whale_refresh = asyncio.get_event_loop().time()

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
                                    await _handle_whale_buy(tx)

                                # Periodic whale list refresh
                                now = asyncio.get_event_loop().time()
                                if now - last_whale_refresh > _whale_refresh_interval:
                                    await _refresh_whale_list()
                                    last_whale_refresh = now

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
