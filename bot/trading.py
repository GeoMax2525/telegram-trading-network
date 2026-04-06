"""
trading.py — Jupiter swap integration.

get_jupiter_quote()  — fetches best SOL→token route from Jupiter API v6
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
SOLANA_RPC_URL    = "https://api.mainnet-beta.solana.com"
DEFAULT_SLIPPAGE  = 100   # bps  (1 %)


async def get_jupiter_quote(
    output_mint: str,
    amount_lamports: int,
    slippage_bps: int = DEFAULT_SLIPPAGE,
) -> dict:
    """
    Returns the best swap quote for SOL → *output_mint*.

    :param output_mint:    Contract address of the token to buy.
    :param amount_lamports: Amount of SOL to spend, in lamports (1 SOL = 1e9).
    :param slippage_bps:   Max acceptable slippage in basis points.
    :raises httpx.HTTPStatusError: on non-2xx response from Jupiter.
    :raises RuntimeError: if Jupiter returns no routes.
    """
    params = {
        "inputMint":      SOL_MINT,
        "outputMint":     output_mint,
        "amount":         str(amount_lamports),
        "slippageBps":    slippage_bps,
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
    async with httpx.AsyncClient(timeout=30) as client:
        rpc_resp = await client.post(SOLANA_RPC_URL, json=rpc_payload)
        rpc_resp.raise_for_status()
        rpc_data = rpc_resp.json()

    if "error" in rpc_data:
        raise RuntimeError(f"RPC error: {rpc_data['error'].get('message', rpc_data['error'])}")

    return rpc_data["result"]   # base58 transaction signature
