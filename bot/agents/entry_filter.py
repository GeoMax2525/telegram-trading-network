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
#
# IMPORTANT: entry_filter_enabled defaults to 0 (OFF) for tg_signal trades.
# The original 4am architecture was built to TRUST the source — the channel
# does its own vetting before posting, and the bot snipes fast based on
# that trust. Adding on-chain filters here would reject legitimate fresh
# launches (high early concentration is normal, mint authority often still
# active for first few minutes, liquidity builds over time).
#
# Operator opts in if they want to test: /setparam entry_filter_enabled 1
# This makes Phase 4 an A/B test, not a default behavior change.
DEFAULTS = {
    "entry_filter_enabled":        0.0,  # OFF by default — preserve 4am trust
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


async def detect_bundle(mint: str) -> tuple[bool, float | None]:
    """Detect a bundled launch via top-holder concentration (Helius).

    Bundles (many wallets buying in coordination at launch) DO pump but dump
    fast — research: 73% collapse below 40% within 20 min. So we don't skip
    them, we trade them with tighter rules (faster TP, tighter SL, 15-min
    time-exit). Concentration is the cheap proxy: a clean launch spreads supply,
    a bundle clusters it in the top holders.

    Returns (is_bundle, top10_concentration_pct). top10 is None on API failure
    (treated as NOT a bundle — fail open, trade normally).
    """
    cfg = await get_params("bundle_detect_enabled", "bundle_top10_pct_threshold")
    if float(cfg.get("bundle_detect_enabled") or 1.0) < 0.5:
        return False, None
    threshold = float(cfg.get("bundle_top10_pct_threshold") or 60.0)
    top10_pct, _amounts = await _fetch_top_holders(mint)
    if top10_pct is None:
        return False, None  # fail open — no data, trade as normal
    is_bundle = top10_pct >= threshold
    if is_bundle:
        logger.info("entry_filter: BUNDLE detected %s — top10=%.0f%% >= %.0f%%",
                    mint[:12], top10_pct, threshold)
    return is_bundle, top10_pct


async def fetch_bundle_wallets(mint: str, top_n: int = 8) -> list[str]:
    """Resolve the top-N holder TOKEN accounts to their OWNER wallet addresses
    (the actual wallets behind the bundle). getTokenLargestAccounts returns
    token accounts; one getAccountInfo each resolves the owner. Capped + gated
    so the Helius cost stays bounded. Returns [] on any failure."""
    cfg = await get_params("bundle_track_wallets_enabled")
    if float(cfg.get("bundle_track_wallets_enabled") or 1.0) < 0.5:
        return []
    # 1. Top holder token accounts
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint]}
    try:
        timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(HELIUS_RPC_URL, json=payload) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("fetch_bundle_wallets: largest accounts %s failed: %s", mint[:12], exc)
        return []
    accts = [(a or {}).get("address") for a in
             ((data or {}).get("result", {}).get("value") or [])[:top_n]]
    accts = [a for a in accts if a]
    if not accts:
        return []

    # 2. Resolve each token account → owner wallet (parallel, bounded)
    async def _owner(tok_acct: str) -> str | None:
        p = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
             "params": [tok_acct, {"encoding": "jsonParsed"}]}
        try:
            timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.post(HELIUS_RPC_URL, json=p) as resp:
                    if resp.status != 200:
                        return None
                    d = await resp.json(content_type=None)
            return (d or {}).get("result", {}).get("value", {}).get("data", {}) \
                .get("parsed", {}).get("info", {}).get("owner")
        except Exception:
            return None

    owners = await asyncio.gather(*[_owner(a) for a in accts], return_exceptions=True)
    out = []
    for o in owners:
        if isinstance(o, str) and o and o not in out:
            out.append(o)
    return out


async def detect_launch_bundle(mint: str) -> dict:
    """REAL bundle detection: analyse the launch block. A bundle = many distinct
    wallets receiving the token in the SAME launch slot(s) — the dev's coordinated
    Jito-bundle snipe at block 0. This is the true definition, not a concentration
    proxy.

    Returns {determinable, is_bundle, wallet_count, wallets, launch_slot}. If the
    token is too old/busy to page back to its launch within the cost cap,
    determinable=False (caller falls back to the concentration check).
    """
    cfg = await get_params("bundle_min_launch_wallets")
    min_wallets = int(float(cfg.get("bundle_min_launch_wallets") or 4))

    async def _rpc(method, params):
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(HELIUS_RPC_URL, json=payload) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)

    # 1. Page back to the OLDEST signatures (the launch). Cap pages so an old,
    #    high-volume token doesn't blow the cost budget.
    sigs: list[dict] = []
    before = None
    MAX_PAGES = 2
    reached_oldest = False
    try:
        for _ in range(MAX_PAGES):
            params = [mint, {"limit": 1000}]
            if before:
                params[1]["before"] = before
            data = await _rpc("getSignaturesForAddress", params)
            batch = (data or {}).get("result") or []
            if not batch:
                reached_oldest = True
                break
            sigs.extend(batch)
            before = batch[-1].get("signature")
            if len(batch) < 1000:
                reached_oldest = True
                break
    except Exception as exc:
        logger.debug("detect_launch_bundle: sig paging %s failed: %s", mint[:12], exc)
        return {"determinable": False}

    if not reached_oldest or not sigs:
        return {"determinable": False}  # couldn't see the launch — defer

    launch_slot = min(int(s.get("slot") or 0) for s in sigs if s.get("slot"))
    if launch_slot <= 0:
        return {"determinable": False}

    # 2. The launch-window signatures (creation slot + a slot or two). Cap parse.
    WINDOW_SLOTS = 2
    MAX_PARSE = 20
    launch_sigs = [s for s in sigs
                   if 0 < int(s.get("slot") or 0) <= launch_slot + WINDOW_SLOTS]
    launch_sigs = launch_sigs[:MAX_PARSE]

    # 3. Parse each launch tx; collect each wallet's launch-block token balance
    #    (max post-balance seen) so we can compute the % of supply they sniped.
    holdings: dict[str, float] = {}
    async def _buyers_in(sig: str):
        try:
            d = await _rpc("getTransaction",
                           [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
            meta = (d or {}).get("result", {}).get("meta", {}) or {}
            for tb in (meta.get("postTokenBalances") or []):
                if tb.get("mint") == mint and tb.get("owner"):
                    amt = ((tb.get("uiTokenAmount") or {}).get("uiAmount")) or 0
                    owner = tb["owner"]
                    if amt and amt > holdings.get(owner, 0):
                        holdings[owner] = float(amt)
                    holdings.setdefault(owner, 0.0)
        except Exception:
            pass
    try:
        await asyncio.gather(*[_buyers_in(s["signature"]) for s in launch_sigs
                               if s.get("signature")])
    except Exception as exc:
        logger.debug("detect_launch_bundle: tx parse %s failed: %s", mint[:12], exc)
        return {"determinable": False}

    wallets = list(holdings.keys())
    is_bundle = len(wallets) >= min_wallets

    # % of supply sniped = sum of bundle-wallet launch holdings / total supply.
    sniped_pct = None
    try:
        sup = await _rpc("getTokenSupply", [mint])
        total = float(((sup or {}).get("result", {}).get("value", {}) or {}).get("uiAmount") or 0)
        if total > 0:
            sniped_pct = round(sum(holdings.values()) / total * 100.0, 1)
    except Exception:
        pass

    if is_bundle:
        logger.info("detect_launch_bundle: BUNDLE %s — %d wallets sniped %.1f%% in launch slot %d",
                    mint[:12], len(wallets), sniped_pct or 0, launch_slot)
    return {
        "determinable": True, "is_bundle": is_bundle,
        "wallet_count": len(wallets), "wallets": wallets,
        "sniped_pct": sniped_pct, "launch_slot": launch_slot,
    }


async def classify_launch(mint: str) -> dict:
    """Unified entry classifier. Tries REAL launch-bundle detection first; if it
    can't see the launch, falls back to the holder-concentration proxy. Returns
    {kind, label, wallets, detail} where kind is 'bundle' | 'concentration' | None.
    Both kinds get the tighter bundle trade rules; only the LABEL differs so the
    entry card is honest about what we actually detected."""
    # Real launch bundle (authoritative)
    try:
        lb = await detect_launch_bundle(mint)
        if lb.get("determinable") and lb.get("is_bundle"):
            pct = lb.get("sniped_pct")
            pct_txt = f" sniped {pct:.0f}% of supply" if pct else ""
            return {
                "kind": "bundle",
                "label": f"BUNDLED — {lb['wallet_count']} wallets{pct_txt} at launch",
                "wallets": lb.get("wallets") or [],
                "detail": f"{lb['wallet_count']} wallets bought in the launch block{pct_txt}",
                "sniped_pct": pct,
            }
    except Exception as exc:
        logger.debug("classify_launch: launch-bundle %s err: %s", mint[:12], exc)

    # Concentration proxy (fallback / additional signal)
    try:
        is_conc, top10 = await detect_bundle(mint)
        if is_conc:
            return {
                "kind": "concentration",
                "label": f"HIGH CONCENTRATION — top-10 hold {top10:.0f}% of supply",
                "wallets": [],
                "detail": f"top-10 hold {top10:.0f}% of supply",
            }
    except Exception as exc:
        logger.debug("classify_launch: concentration %s err: %s", mint[:12], exc)

    return {"kind": None, "label": "", "wallets": [], "detail": ""}


def bundle_trade_params(clean_tp: float, clean_sl: float) -> tuple[float, float]:
    """Tighter exits for a bundle: lower TP (take the fast pop), tighter SL
    (the dump is sudden). Research consensus: 2x TP / 35% SL for bundles vs
    the wider clean-launch targets. Caps so we never widen a clean trade."""
    bundle_tp = min(clean_tp, 2.0)    # cap TP at 2x — grab the pop before the dump
    bundle_sl = min(clean_sl, 35.0)   # tighter stop — dumps are fast & deep
    return bundle_tp, bundle_sl


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
