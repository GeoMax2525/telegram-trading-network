"""
wallet.py — Solana wallet utilities.

Loads the bot's trading wallet from WALLET_PRIVATE_KEY env var.
Supports both base58-encoded secret key and JSON byte-array format
(as exported by Phantom / Solana CLI).
"""

import asyncio
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


_SOLSCAN_BASE    = "https://public-api.solscan.io"
_SOLSCAN_HEADERS = {"accept": "application/json", "User-Agent": "Mozilla/5.0"}
_HOLDER_LIMIT    = 50    # results per page
_HOLDER_MAX_RANK = 500   # stop searching after this rank


async def _solscan_get(session: aiohttp.ClientSession, url: str) -> Optional[dict]:
    """GET a Solscan endpoint, return parsed JSON or None on error."""
    try:
        async with session.get(url, headers=_SOLSCAN_HEADERS) as resp:
            if resp.status != 200:
                logger.warning("Solscan %s returned HTTP %d", url, resp.status)
                return None
            return await resp.json(content_type=None)
    except Exception as exc:
        logger.error("Solscan request failed (%s): %s", url, exc)
        return None


async def get_holder_info(wallet_address: str, mint: str) -> Optional[dict]:
    """
    Uses Solscan public API to find holder rank, UI balance, and % of supply.

    Strategy:
      - Fetch token meta + first holder page concurrently.
      - Paginate holder pages (50/page) until wallet_address is found as `owner`.
      - Cap search at _HOLDER_MAX_RANK (500). Returns None if not found.

    Returns:
      {"rank": int, "balance": float, "pct_supply": float}
    Returns None on API failure or if wallet is not in the top 500 holders.
    """
    meta_url  = f"{_SOLSCAN_BASE}/token/meta?tokenAddress={mint}"
    page0_url = (
        f"{_SOLSCAN_BASE}/token/holders"
        f"?tokenAddress={mint}&limit={_HOLDER_LIMIT}&offset=0"
    )

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Page 0 and token meta in parallel
            meta_data, page0_data = await asyncio.gather(
                _solscan_get(session, meta_url),
                _solscan_get(session, page0_url),
            )

            if not page0_data:
                return None

            # Token decimals + total supply (raw integer string)
            decimals     = int((meta_data or {}).get("decimals", 0))
            supply_raw   = int((meta_data or {}).get("supply", 0) or 0)
            total_supply = supply_raw  # raw units

            holders      = page0_data.get("data", [])
            total_listed = page0_data.get("total", 0)
            pages_needed = min(
                (_HOLDER_MAX_RANK + _HOLDER_LIMIT - 1) // _HOLDER_LIMIT,
                (total_listed  + _HOLDER_LIMIT - 1) // _HOLDER_LIMIT,
            )

            for page in range(pages_needed):
                if page > 0:
                    offset = page * _HOLDER_LIMIT
                    url    = (
                        f"{_SOLSCAN_BASE}/token/holders"
                        f"?tokenAddress={mint}&limit={_HOLDER_LIMIT}&offset={offset}"
                    )
                    data = await _solscan_get(session, url)
                    if not data:
                        break
                    holders = data.get("data", [])

                for idx, holder in enumerate(holders):
                    if holder.get("owner") == wallet_address:
                        rank      = page * _HOLDER_LIMIT + idx + 1
                        raw_amt   = int(holder.get("amount", 0) or 0)
                        ui_amount = raw_amt / (10 ** decimals) if decimals >= 0 else float(raw_amt)
                        pct       = (raw_amt / total_supply * 100) if total_supply > 0 else 0.0
                        return {
                            "rank":       rank,
                            "balance":    ui_amount,
                            "pct_supply": pct,
                        }

        # Wallet not in top _HOLDER_MAX_RANK holders
        return None

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
