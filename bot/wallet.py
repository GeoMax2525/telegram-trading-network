"""
wallet.py — Solana wallet utilities.

Loads the bot's trading wallet from WALLET_PRIVATE_KEY env var.
Supports both base58-encoded secret key and JSON byte-array format
(as exported by Phantom / Solana CLI).
"""

import json
import logging
import os
from typing import Optional

import aiohttp

from bot.config import HELIUS_RPC_URL

logger = logging.getLogger(__name__)

# Module-level keypair cache — loaded once on first access
_keypair = None
_loaded: bool = False


def _load() -> Optional[object]:
    """Loads and caches the Keypair from WALLET_PRIVATE_KEY. Returns None if unset/invalid."""
    global _keypair, _loaded
    if _loaded:
        return _keypair

    _loaded = True
    raw = os.getenv("WALLET_PRIVATE_KEY", "").strip()
    if not raw:
        logger.info("WALLET_PRIVATE_KEY not set — wallet features disabled.")
        return None

    try:
        from solders.keypair import Keypair  # type: ignore

        if raw.startswith("["):
            _keypair = Keypair.from_bytes(bytes(json.loads(raw)))
        else:
            _keypair = Keypair.from_base58_string(raw)

        logger.info("Wallet loaded: %s", str(_keypair.pubkey()))
        return _keypair

    except Exception as exc:
        logger.error("Failed to load wallet from WALLET_PRIVATE_KEY: %s", exc)
        return None


def get_keypair() -> Optional[object]:
    """Returns the Keypair for signing transactions, or None if not configured."""
    return _load()


def get_wallet_address() -> Optional[str]:
    """Returns the public address as a base58 string, or None if not configured."""
    kp = _load()
    return str(kp.pubkey()) if kp else None


async def get_sol_balance(address: str) -> Optional[float]:
    """
    Fetches the SOL balance for *address* via Solana mainnet RPC.
    Returns balance in SOL (not lamports), or None on error.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [address],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                HELIUS_RPC_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                lamports = data["result"]["value"]
                return round(lamports / 1_000_000_000, 4)
    except Exception as exc:
        logger.error("Failed to fetch SOL balance for %s: %s", address, exc)
        return None
