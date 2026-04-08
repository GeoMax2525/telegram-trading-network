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


async def get_holder_info(wallet_address: str, mint: str) -> Optional[dict]:
    """
    Uses Helius RPC (single batch request) to get holder rank, token balance,
    and % of total supply for wallet_address.

    Batch calls:
      1. getTokenAccountsByOwner  → our token account pubkey + UI balance
      2. getTokenLargestAccounts  → top-20 holders list (rank within top 20)
      3. getTokenSupply           → total supply for % calculation

    Returns:
      {"rank": int|None, "balance": float, "pct_supply": float}
      rank is 1-20 if wallet is a top-20 holder, None otherwise (shown as ">20").
    Returns None on failure or if the wallet holds 0 tokens.
    """
    payload = [
        {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [wallet_address, {"mint": mint}, {"encoding": "jsonParsed"}],
        },
        {
            "jsonrpc": "2.0", "id": 2,
            "method": "getTokenLargestAccounts",
            "params": [mint],
        },
        {
            "jsonrpc": "2.0", "id": 3,
            "method": "getTokenSupply",
            "params": [mint],
        },
    ]
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.post(
                HELIUS_RPC_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    logger.warning("Helius holder info returned HTTP %d", resp.status)
                    return None
                results = await resp.json()

        by_id = {r["id"]: r for r in results}

        # 1. Our token accounts for this mint
        our_accounts = ((by_id.get(1) or {}).get("result") or {}).get("value", [])
        if not our_accounts:
            return None  # wallet holds none of this token

        our_pubkey  = our_accounts[0]["pubkey"]
        token_info  = our_accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
        our_raw     = int(token_info["amount"])
        our_balance = float(token_info.get("uiAmount") or 0)

        if our_raw == 0:
            return None

        # 2. Top-20 holders — match our token account pubkey to find rank
        largest = ((by_id.get(2) or {}).get("result") or {}).get("value", [])
        rank = None
        for idx, holder in enumerate(largest, 1):
            if holder.get("address") == our_pubkey:
                rank = idx
                break

        # 3. Total supply for % calculation
        supply_val   = ((by_id.get(3) or {}).get("result") or {}).get("value", {})
        total_supply = int(supply_val.get("amount", 0))
        pct_supply   = (our_raw / total_supply * 100) if total_supply > 0 else 0.0

        return {
            "rank":       rank,        # int 1-20, or None if not in top 20
            "balance":    our_balance, # UI amount (e.g. 1_250_000.0)
            "pct_supply": pct_supply,  # e.g. 0.125
        }

    except Exception as exc:
        logger.error("get_holder_info failed for %s mint=%s: %s", wallet_address, mint, exc)
        return None


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
