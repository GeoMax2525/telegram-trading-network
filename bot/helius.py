"""
helius.py — Shared Helius API client.

Single reusable aiohttp session with:
  - Connection pooling (persistent session, not per-request)
  - Exponential backoff retry on 429 / 5xx errors
  - Rate-limit aware (200 req/s on Business plan)
  - Both RPC and Enhanced API endpoints
  - Proper error logging (no more silent failures)

All agents import from here instead of building their own sessions.
"""

import asyncio
import logging
import time

import aiohttp

from bot.config import HELIUS_RPC_URL, HELIUS_API_KEY

logger = logging.getLogger(__name__)

# ── Rate limit config ────────────────────────────────────────────────────────
MAX_CONCURRENT = 150        # Business plan: 200 req/s capacity
MAX_RETRIES    = 4          # retry count for transient errors
BASE_DELAY     = 0.3        # initial backoff delay (seconds)

_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_session: aiohttp.ClientSession | None = None

# ── Enhanced API base URLs ───────────────────────────────────────────────────
ENHANCED_TX_URL   = "https://api.helius.xyz/v0/transactions"
ENHANCED_ADDR_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions"


async def _get_session() -> aiohttp.ClientSession:
    """Lazy-init a persistent connection-pooled session."""
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, keepalive_timeout=30)
        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"Content-Type": "application/json"},
        )
    return _session


async def close_session() -> None:
    """Call on shutdown to close the persistent session."""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


# ── Core request with retry + backoff ────────────────────────────────────────

async def _request_with_retry(
    method: str,
    url: str,
    *,
    json: dict | list | None = None,
    retries: int = MAX_RETRIES,
    label: str = "helius",
) -> dict | list | None:
    """
    Makes an HTTP request with exponential backoff on 429/5xx.
    Returns parsed JSON or None on persistent failure.
    """
    delay = BASE_DELAY
    session = await _get_session()

    for attempt in range(retries):
        try:
            async with _semaphore:
                if method == "POST":
                    async with session.post(url, json=json) as resp:
                        if resp.status == 200:
                            return await resp.json(content_type=None)
                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", delay))
                            logger.warning(
                                "%s: 429 rate limited (attempt %d/%d), backing off %.1fs",
                                label, attempt + 1, retries, retry_after,
                            )
                            await asyncio.sleep(retry_after)
                            delay = min(delay * 2, 10.0)
                            continue
                        if resp.status >= 500:
                            logger.warning(
                                "%s: HTTP %d (attempt %d/%d), retrying in %.1fs",
                                label, resp.status, attempt + 1, retries, delay,
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 10.0)
                            continue
                        # 4xx (not 429) — log and don't retry
                        body = await resp.text()
                        logger.error(
                            "%s: HTTP %d — %s", label, resp.status, body[:200],
                        )
                        return None
                else:  # GET
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            return await resp.json(content_type=None)
                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", delay))
                            logger.warning(
                                "%s: 429 rate limited, backing off %.1fs",
                                label, retry_after,
                            )
                            await asyncio.sleep(retry_after)
                            delay = min(delay * 2, 10.0)
                            continue
                        if resp.status >= 500:
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 10.0)
                            continue
                        body = await resp.text()
                        logger.error("%s: HTTP %d — %s", label, resp.status, body[:200])
                        return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "%s: connection error (attempt %d/%d): %s",
                label, attempt + 1, retries, exc,
            )
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
            continue

    logger.error("%s: all %d retries exhausted", label, retries)
    return None


# ── RPC calls (Solana JSON-RPC via Helius) ───────────────────────────────────

async def rpc_call(method: str, params: list, label: str = "rpc") -> dict | None:
    """
    Solana JSON-RPC call via Helius RPC endpoint.
    Returns the full JSON response (caller checks .get("result")).
    Retries on 429/5xx with exponential backoff.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    data = await _request_with_retry("POST", HELIUS_RPC_URL, json=payload, label=label)

    if data is None:
        return None

    # Helius sometimes returns HTTP 200 with JSON-RPC error inside body
    if isinstance(data, dict) and "error" in data:
        err = data["error"]
        logger.error(
            "%s: JSON-RPC error code=%s msg=%s",
            label, err.get("code"), err.get("message", ""),
        )
        return None

    return data


async def rpc_batch(payloads: list[dict], label: str = "rpc_batch") -> list[dict]:
    """
    Batch multiple JSON-RPC calls in one HTTP request.
    Returns list of response objects (same order as input).
    """
    data = await _request_with_retry("POST", HELIUS_RPC_URL, json=payloads, label=label)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return [data] if isinstance(data, dict) else []


# ── Enhanced Transactions API ────────────────────────────────────────────────

async def get_address_transactions(
    address: str,
    limit: int = 100,
    before: str | None = None,
    label: str = "enhanced_addr_tx",
) -> list[dict]:
    """
    Helius Enhanced Transactions API — get parsed transactions for an address.
    POST https://api.helius.xyz/v0/addresses/{address}/transactions?api-key=KEY

    Returns enriched transaction objects with tokenTransfers, nativeTransfers,
    type, description, etc. Much richer than raw getSignaturesForAddress.

    Args:
        address: Solana address (wallet or token mint)
        limit: Max transactions to return (1-100)
        before: Pagination cursor — signature to start before
        label: Log label for debugging
    """
    url = f"{ENHANCED_ADDR_URL.format(address=address)}?api-key={HELIUS_API_KEY}"
    body: dict = {"limit": min(limit, 100)}
    if before:
        body["before"] = before

    data = await _request_with_retry("POST", url, json=body, label=label)
    if isinstance(data, list):
        return data
    if data is None:
        logger.warning("%s: no data for %s", label, address[:12])
    return []


async def get_address_transactions_all(
    address: str,
    max_txns: int = 500,
    label: str = "enhanced_addr_tx_all",
) -> list[dict]:
    """
    Paginated fetch — pulls up to max_txns transactions for an address
    using the Enhanced API with cursor-based pagination.
    """
    all_txns: list[dict] = []
    before: str | None = None

    while len(all_txns) < max_txns:
        batch_size = min(100, max_txns - len(all_txns))
        batch = await get_address_transactions(
            address, limit=batch_size, before=before, label=label,
        )
        if not batch:
            break
        all_txns.extend(batch)
        # Last signature becomes the pagination cursor
        last_sig = batch[-1].get("signature")
        if not last_sig or len(batch) < batch_size:
            break
        before = last_sig

    return all_txns


async def parse_transactions(signatures: list[str], label: str = "parse_tx") -> list[dict]:
    """
    Helius Enhanced Transactions API — batch parse up to 100 signatures.
    POST https://api.helius.xyz/v0/transactions?api-key=KEY
    """
    if not signatures:
        return []
    url = f"{ENHANCED_TX_URL}?api-key={HELIUS_API_KEY}"
    data = await _request_with_retry(
        "POST", url, json={"transactions": signatures[:100]}, label=label,
    )
    if isinstance(data, list):
        return data
    return []


# ── Batch helpers (concurrent with rate limiting) ────────────────────────────

async def get_signatures_for_address(
    address: str, limit: int = 200, label: str = "get_sigs",
) -> list[dict]:
    """
    getSignaturesForAddress via Helius RPC with retry.
    Returns list of signature objects with blockTime, signature, etc.
    """
    data = await rpc_call(
        "getSignaturesForAddress",
        [address, {"limit": limit, "commitment": "confirmed"}],
        label=label,
    )
    if data is None:
        return []
    return data.get("result") or []


async def get_multiple_address_transactions(
    addresses: list[str],
    limit: int = 100,
    label: str = "batch_addr_tx",
) -> dict[str, list[dict]]:
    """
    Fetch transactions for multiple addresses concurrently.
    Returns {address: [transactions]} dict.
    Respects the semaphore to stay under rate limits.
    """
    results: dict[str, list[dict]] = {}

    async def _fetch_one(addr: str):
        txns = await get_address_transactions(
            addr, limit=limit, label=f"{label}:{addr[:8]}",
        )
        results[addr] = txns

    await asyncio.gather(
        *[_fetch_one(a) for a in addresses],
        return_exceptions=True,
    )
    return results
