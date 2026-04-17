"""
trading.py — Jupiter Ultra API integration.

get_token_balance()    — fetches SPL token balance from Solana RPC
get_ultra_order()      — fetches a signed-ready order from Jupiter Ultra /order
execute_ultra_order()  — signs the order transaction and submits it to Jupiter /execute

Jupiter Ultra API:
  Order:   GET  https://api.jup.ag/ultra/v1/order
  Execute: POST https://api.jup.ag/ultra/v1/execute

SOL mint: So11111111111111111111111111111111111111112
RPC:      Helius (set HELIUS_RPC_URL in Railway env vars)
"""

import base64
import logging

import aiohttp

from bot.helius import rpc_call

logger = logging.getLogger(__name__)

SOL_MINT           = "So11111111111111111111111111111111111111112"
ULTRA_ORDER_URL    = "https://api.jup.ag/ultra/v1/order"
ULTRA_EXECUTE_URL  = "https://api.jup.ag/ultra/v1/execute"

_TIMEOUT      = aiohttp.ClientTimeout(total=30)
_JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


# ── Token balance ─────────────────────────────────────────────────────────────

async def get_token_balance(wallet_address: str, token_mint: str) -> int:
    """
    Returns the raw (integer) SPL token balance held by *wallet_address*
    for the given *token_mint*.  Returns 0 if no account exists or on error.
    """
    try:
        data     = await rpc_call(
            "getTokenAccountsByOwner",
            [wallet_address, {"mint": token_mint}, {"encoding": "jsonParsed"}],
            label="trading_balance",
        )
        if data is None:
            return 0
        accounts = data.get("result", {}).get("value", [])
        if not accounts:
            return 0

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


# ── Jupiter Ultra order ───────────────────────────────────────────────────────

async def get_ultra_order(
    output_mint: str,
    amount: int,
    wallet_address: str,
    input_mint: str = SOL_MINT,
) -> dict:
    """
    Fetches a Jupiter Ultra order for *input_mint* → *output_mint*.

    Returns the full order dict which includes:
      - transaction   (base64 tx ready to sign)
      - requestId     (required for /execute)
      - outAmount     (expected output tokens)
      - priceImpactPct
    """
    params = {
        "inputMint":  input_mint,
        "outputMint": output_mint,
        "amount":     str(amount),
        "taker":      wallet_address,
    }
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.get(
            ULTRA_ORDER_URL,
            params=params,
            headers=_JSON_HEADERS,
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Jupiter Ultra /order returned {resp.status}: {body[:300]}")
            data = await resp.json()

    if "transaction" not in data:
        raise RuntimeError(f"Jupiter Ultra /order returned no transaction: {data}")

    logger.info(
        "Ultra order: %s → %s  amount=%s  outAmount=%s",
        input_mint[:6], output_mint[:6], amount, data.get("outAmount"),
    )
    return data


# ── Jupiter Ultra execute ─────────────────────────────────────────────────────

async def execute_ultra_order(order: dict, keypair) -> str:
    """
    Signs the transaction in *order* with *keypair* and submits it to
    Jupiter Ultra /execute.  Returns the base58 transaction signature.

    Raises RuntimeError with a human-readable message on failure.
    """
    from solders.transaction import VersionedTransaction  # type: ignore

    # ── Step 1: Sign the transaction ─────────────────────────────────────────
    tx_bytes  = base64.b64decode(order["transaction"])
    tx        = VersionedTransaction.from_bytes(tx_bytes)
    signed_tx = VersionedTransaction(tx.message, [keypair])
    signed_b64 = base64.b64encode(bytes(signed_tx)).decode()

    # ── Step 2: Submit to Jupiter Ultra /execute ──────────────────────────────
    payload = {
        "signedTransaction": signed_b64,
        "requestId":         order["requestId"],
    }
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
        async with session.post(
            ULTRA_EXECUTE_URL,
            json=payload,
            headers=_JSON_HEADERS,
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(f"Jupiter Ultra /execute returned {resp.status}: {body[:300]}")
            result = await resp.json()

    status    = result.get("status", "")
    signature = result.get("signature", "")

    if status != "Success" or not signature:
        error   = result.get("error", "")
        code    = result.get("code", "")
        details = f"status={status}"
        if error:
            details += f", error={error}"
        if code:
            details += f", code={code}"
        raise RuntimeError(f"Swap failed — {details}")

    logger.info("Ultra execute success: %s", signature)
    return signature
