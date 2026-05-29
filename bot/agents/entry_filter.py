"""
entry_filter.py — Phase 4 on-chain entry gates for 4am tg_signal trades.

Scanner trades already get heavy filtering via the Confidence Engine
(rugcheck, top10 concentration, liquidity floor). 4am tg_signal trades
bypass all that — they auto-buy from a Telegram signal with no scoring.

This module bridges that gap. Before any 4am trade opens, we check:

  1. Liquidity floor — DexScreener data, fast (already cached)
  2. Top 10 holder concentration — Helius getTokenLargestAccounts
  3. Mint authority — Helius getAccountInfo (defer to v2 if slow)

Returns (passed, reason). False with reason = reject the trade. The
reason string is logged for diagnostics and surfaced in /4amreport's
'rejected by filter' aggregate.

Every threshold is tunable via /setparam so the operator can dial in
aggression without a deploy. Defaults come from research consensus
across Nansen, DEXTools, Odinbot, and multiple memecoin writeups.

The filter is opt-in via `entry_filter_enabled` agent_param. Defaults
to 1 (on) — set to 0 to bypass entirely for an A/B comparison.
"""

import asyncio
import logging
from typing import Optional

import aiohttp

from bot.config import HELIUS_RPC_URL
from database.models import get_params

logger = logging.getLogger(__name__)

# Default thresholds (tunable via /setparam)
DEFAULTS = {
    "entry_filter_enabled":        1.0,
    "entry_min_liquidity_usd":  8000.0,
    "entry_max_top10_pct":        30.0,
    "entry_check_mint_authority":  1.0,
    "entry_check_top_holders":     1.0,
}

# Network knobs
_HELIUS_TIMEOUT = 4.0  # seconds, per call. Keeps hot path tight.


async def _fetch_top_holders(mint: str) -> tuple[float | None, list[float]]:
    """Get top-10 holder concentration via Helius getTokenLargestAccounts.

    Returns (top10_percent_of_supply, raw_amounts_list).
    top10_percent_of_supply is None when we can't compute (no supply data).
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint],
    }
    try:
        timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(HELIUS_RPC_URL, json=payload) as resp:
                if resp.status != 200:
                    return None, []
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("entry_filter: getTokenLargestAccounts %s failed: %s",
                     mint[:12], exc)
        return None, []

    accounts = (data or {}).get("result", {}).get("value") or []
    if not accounts:
        return None, []

    # uiAmount is human-readable token amount; amount is base units string
    amounts = []
    for a in accounts[:10]:
        ua = a.get("uiAmount")
        if isinstance(ua, (int, float)):
            amounts.append(float(ua))

    if not amounts:
        return None, []

    # We don't have total supply from this endpoint, so we need it
    # separately. Fall back to comparing top10 vs everything returned.
    # Helius returns top 20 by default — using all 20 as a denominator
    # gives a useful concentration proxy even without true supply.
    top20 = []
    for a in accounts[:20]:
        ua = a.get("uiAmount")
        if isinstance(ua, (int, float)):
            top20.append(float(ua))

    if not top20:
        return None, amounts

    total_top20 = sum(top20)
    if total_top20 <= 0:
        return None, amounts

    top10_pct = (sum(amounts) / total_top20) * 100.0
    return top10_pct, amounts


async def _fetch_mint_authority_active(mint: str) -> bool | None:
    """True iff mint authority is still active (token can be minted more).
    None on API failure.

    Uses RPC getAccountInfo. SPL token's mint authority field at byte
    offset 4-36 in parsed data. We use jsonParsed encoding for safety.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint, {"encoding": "jsonParsed"}],
    }
    try:
        timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(HELIUS_RPC_URL, json=payload) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("entry_filter: getAccountInfo %s failed: %s",
                     mint[:12], exc)
        return None

    info = (
        (data or {}).get("result", {}).get("value", {})
        .get("data", {}).get("parsed", {}).get("info", {})
    )
    if not info:
        return None
    mint_auth = info.get("mintAuthority")
    # None or empty string means no mint authority (it's been disabled/burned)
    return bool(mint_auth)


async def check_entry_filters(
    mint: str,
    token_data: dict,
) -> tuple[bool, str | None]:
    """Run all enabled on-chain entry filters for a 4am tg_signal trade.

    Args:
        mint: the token mint address
        token_data: DexScreener-style dict with 'liquidity' field

    Returns:
        (passed, reason). passed=True means trade can open. False means
        reject with the given reason string for logging / diagnostics.
    """
    cfg = await get_params(
        "entry_filter_enabled",
        "entry_min_liquidity_usd",
        "entry_max_top10_pct",
        "entry_check_mint_authority",
        "entry_check_top_holders",
    )

    # Master kill — operator can disable all filters in one toggle
    if float(cfg.get("entry_filter_enabled") or 0.0) < 0.5:
        return True, None

    min_liq = float(cfg.get("entry_min_liquidity_usd") or DEFAULTS["entry_min_liquidity_usd"])
    max_top10 = float(cfg.get("entry_max_top10_pct") or DEFAULTS["entry_max_top10_pct"])

    # 1. Liquidity floor — fast, uses already-fetched DexScreener data
    liq = float((token_data.get("liquidity") or {}).get("usd") or 0)
    if liq <= 0:
        return False, "no_liquidity_data"
    if liq < min_liq:
        return False, f"liquidity ${liq:.0f} < ${min_liq:.0f}"

    # The rest of the checks make Helius calls in parallel. Keeps
    # added latency to ~max(check_a, check_b) instead of sum.
    tasks = []
    do_holders = float(cfg.get("entry_check_top_holders") or 1.0) >= 0.5
    do_mint = float(cfg.get("entry_check_mint_authority") or 1.0) >= 0.5

    if do_holders:
        tasks.append(("holders", _fetch_top_holders(mint)))
    if do_mint:
        tasks.append(("mint_auth", _fetch_mint_authority_active(mint)))

    if not tasks:
        return True, None

    results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)
    by_name = {name: res for (name, _), res in zip(tasks, results)}

    # 2. Top holder concentration
    if do_holders:
        h_res = by_name.get("holders")
        if isinstance(h_res, tuple):
            top10_pct, _ = h_res
            if top10_pct is not None and top10_pct > max_top10:
                return False, f"top10 holders {top10_pct:.0f}% > {max_top10:.0f}%"

    # 3. Mint authority still active
    if do_mint:
        m_res = by_name.get("mint_auth")
        if m_res is True:  # explicit True = still active = bad
            return False, "mint_authority_active"

    return True, None
