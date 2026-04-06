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

logger = logging.getLogger(__name__)

SOLANA_RPC = "https://api.mainnet-beta.solana.com"

# Cached public address derived from the private key at startup
_wallet_address: Optional[str] = None
_loaded: bool = False


def get_wallet_address() -> Optional[str]:
    """
    Returns the public address derived from WALLET_PRIVATE_KEY, or None if
    the env var is not set or the key is invalid.  Result is cached after the
    first call.
    """
    global _wallet_address, _loaded
    if _loaded:
        return _wallet_address

    _loaded = True
    raw = os.getenv("WALLET_PRIVATE_KEY", "").strip()
    if not raw:
        logger.info("WALLET_PRIVATE_KEY not set — wallet auto-populate disabled.")
        return None

    try:
        from solders.keypair import Keypair  # type: ignore

        if raw.startswith("["):
            # JSON byte-array format: [1, 2, 3, ...]
            key_bytes = bytes(json.loads(raw))
            kp = Keypair.from_bytes(key_bytes)
        else:
            # Base58-encoded 64-byte secret key
            kp = Keypair.from_base58_string(raw)

        _wallet_address = str(kp.pubkey())
        logger.info("Wallet loaded: %s", _wallet_address)
        return _wallet_address

    except Exception as exc:
        logger.error("Failed to load wallet from WALLET_PRIVATE_KEY: %s", exc)
        return None


async def get_sol_balance(address: str) -> Optional[float]:
    """
    Fetches the SOL balance (in SOL, not lamports) for *address* using the
    Solana mainnet JSON-RPC endpoint.  Returns None on any error.
    """
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [address],
            }
            async with session.post(
                SOLANA_RPC,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                lamports = data["result"]["value"]
                return round(lamports / 1_000_000_000, 4)
    except Exception as exc:
        logger.error("Failed to fetch SOL balance for %s: %s", address, exc)
        return None
