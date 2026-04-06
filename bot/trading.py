"""
trading.py — Jupiter swap integration.

get_token_balance()  — fetches SPL token balance from Solana RPC
get_jupiter_quote()  — fetches best swap route from Jupiter API v6
execute_swap()       — signs the Jupiter transaction and broadcasts it

Slippage default: 1% (100 bps).
SOL mint:  So11111111111111111111111111111111111111112
"""

import base64
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SOL_MINT          = "So11111111111111111111111111111111111111112"
JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL  = "https://quote-api.jup.ag/v6/swap"
DEFAULT_SLIPPAGE  = 100   # bps  (1 %)

# RPC endpoints tried in order — first success wins
SOLANA_RPC_URLS = [
    "https://solana-mainnet.g.alchemy.com/v2/demo",
    "https://rpc.ankr.com/solana",
]


async def _rpc_post(payload: dict) -> dict:
    """POST *payload* to Solana RPC, trying each endpoint until one succeeds."""
    last_exc: Exception = RuntimeError("No RPC endpoints configured.")
    async with httpx.AsyncClient(timeout=30) as client:
        for url in SOLANA_RPC_URLS:
            try:
                resp = await client.post(url, json=payload)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                logger.warning("RPC %s failed: %s — trying next endpoint", url, exc)
                last_exc = exc
    raise last_exc


async def get_token_balance(wallet_address: str, token_mint: str) -> int:
    """
    Returns the raw (integer) SPL token balance held by *wallet_address*
    for the given *token_mint*.  Returns 0 if no account exists or on error.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            wallet_address,
            {"mint": token_mint},
            {"encoding": "jsonParsed"},
        ],
    }
    try:
        data = await _rpc_post(payload)
        accounts = data.get("result", {}).get("value", [])
        if not accounts:
            return 0

        # Sum across all token accounts for this mint (usually just one)
        total = 0
        for acct in accounts:
            amount_str = (
                acct.get("account", {})
                    .get("data", {})
                    .get("parsed", {})
                    .get("info", {})
                    .get("tokenAmount", {})
                    .get("amount", "0")
            )
            total += int(amount_str)
        return total

    except Exception as exc:
        logger.error("Failed to fetch token balance for %s: %s", token_mint, exc)
        return 0


async def get_jupiter_quote(
    output_mint: str,
    amount: int,
    slippage_bps: int = DEFAULT_SLIPPAGE,
    input_mint: str = SOL_MINT,
) -> dict:
    """
    Returns the best swap quote for *input_mint* → *output_mint*.

    Defaults to SOL → token (buy).  Pass input_mint=token, output_mint=SOL_MINT
    for a sell.

    :param output_mint:  Destination token mint address.
    :param amount:       Amount of input token in its smallest unit.
    :param slippage_bps: Max acceptable slippage in basis points.
    :param input_mint:   Source token mint (default: SOL).
    :raises RuntimeError: if Jupiter returns no routes.
    """
    params = {
        "inputMint":        input_mint,
        "outputMint":       output_mint,
        "amount":           str(amount),
        "slippageBps":      slippage_bps,
        "onlyDirectRoutes": False,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(JUPITER_QUOTE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    if not data.get("outAmount"):
        raise RuntimeError("Jupiter returned no routes for this token.")

    return data


async def execute_swap(quote_response: dict, keypair) -> str:
    """
    Builds the swap transaction via Jupiter, signs it with *keypair*,
    submits it to the Solana network, and returns the transaction signature.

    :param quote_response: The dict returned by get_jupiter_quote().
    :param keypair:        solders.keypair.Keypair for the trading wallet.
    :returns: Base58 transaction signature string.
    :raises RuntimeError: on any failure (Jupiter build, RPC error, etc.).
    """
    from solders.transaction import VersionedTransaction  # type: ignore

    # ── Step 1: Ask Jupiter to build the serialised transaction ──────────────
    swap_payload = {
        "quoteResponse":             quote_response,
        "userPublicKey":             str(keypair.pubkey()),
        "wrapAndUnwrapSol":          True,
        "dynamicComputeUnitLimit":   True,
        "prioritizationFeeLamports": "auto",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(JUPITER_SWAP_URL, json=swap_payload)
        resp.raise_for_status()
        swap_data = resp.json()

    if "swapTransaction" not in swap_data:
        raise RuntimeError(f"Jupiter swap build failed: {swap_data}")

    # ── Step 2: Deserialise → sign with our keypair ───────────────────────────
    tx_bytes  = base64.b64decode(swap_data["swapTransaction"])
    tx        = VersionedTransaction.from_bytes(tx_bytes)
    signed_tx = VersionedTransaction(tx.message, [keypair])

    # ── Step 3: Broadcast via Solana JSON-RPC ─────────────────────────────────
    raw_b64 = base64.b64encode(bytes(signed_tx)).decode()
    rpc_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendTransaction",
        "params": [
            raw_b64,
            {"encoding": "base64", "preflightCommitment": "confirmed"},
        ],
    }
    rpc_data = await _rpc_post(rpc_payload)

    if "error" in rpc_data:
        raise RuntimeError(f"RPC error: {rpc_data['error'].get('message', rpc_data['error'])}")

    return rpc_data["result"]   # base58 transaction signature
