"""
scanner_agent.py — Agent 4: The Scanner

Runs every 30 seconds ALWAYS, regardless of autotrade status.
Silent learning mode: logs all candidates to database for Agent 6.
Autotrade toggle only controls whether Agent 5 can execute buys.

Sources:
  1. New Launches    — DexScreener profiles filtered by age < 4h,
                       mcap $10K–$5M, liquidity > $5K, m5 buys > 5
  2. Insider Wallets — Helius: mints bought by Tier 1/2 wallets
                       in the last 30 minutes
  3. Volume Spikes   — Recent DB tokens where m5_vol × 12 ≥ h1_vol × 3

Rug filters (all sources):
  - Rugcheck score < 600              → skip
  - Liquidity < $5 K                  → skip
  - Market cap outside $10K–$5M       → skip
  - High-risk flags (freeze/mint auth) → skip

Pattern fingerprint matching (winner_2x pattern if available):
  - Scores each candidate against the winner fingerprint
  - Only candidates with match_score ≥ 50 are queued for Agent 5

Outputs:
  - Appends to state.pending_candidates
  - Updates state.scanner_last_run, state.scanner_candidates_today
  - Logs each run to AgentLogs
  - Stores all candidates in database silently (no Telegram alerts)
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta

import aiohttp

from bot import state
from bot.config import HELIUS_RPC_URL, HELIUS_API_KEY
from bot.scanner import fetch_token_data, parse_token_metrics, scan_token
from bot.agents.confidence_engine import score_candidate
from database.models import (
    log_agent_run,
    get_tier_wallets,
    get_pattern_by_type,
    token_exists,
    get_token_count,
    open_paper_trade,
    has_open_paper_trade,
    has_recent_manual_close,
    get_params,
    compute_paper_balance,
    AsyncSessionLocal,
    select,
    Token,
)

logger = logging.getLogger(__name__)

PROFILES_URL    = "https://api.dexscreener.com/token-profiles/latest/v1"
RUGCHECK_URL    = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
HELIUS_ENHANCED = "https://api.helius.xyz/v0/transactions"

POLL_INTERVAL   = 15     # seconds (chaos mode: was 30)
STARTUP_DELAY   = 60     # seconds after bot start
MAX_CANDIDATES  = 10     # max new candidates queued per tick (was 5)

# Filters
MIN_MCAP        = 10_000
MAX_MCAP        = 5_000_000
MIN_LIQUIDITY   = 5_000
MAX_RUGCHECK_RISK = 500   # reject tokens with rugcheck risk score above this
MIN_AI_SCORE    = 40
MAX_AGE_HOURS   = 4
MIN_BUYERS_M5   = 5
INSIDER_WINDOW  = 1_800   # 30 min in seconds

HIGH_RISK_FLAGS = {
    "freeze_authority_enabled",
    "mint_authority_enabled",
    "honeypot",
    "rugged",
    "high_sell_tax",
    "high_buy_tax",
}


# ── Rug check ─────────────────────────────────────────────────────────────────

async def _fetch_rugcheck(mint: str) -> dict | None:
    try:
        url = RUGCHECK_URL.format(mint=mint)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("Scanner rugcheck failed for %s: %s", mint[:12], exc)
        return None


def _extract_rugcheck_safety(rc: dict | None) -> dict:
    """
    Pull the 4 safety signals used by Agent 6 pattern matching out of the
    rugcheck /report response. Every field is defensively None if we
    can't find it — matchers treat None as a non-match (no false positives).

    Expected (best-effort) schema from rugcheck.xyz /v1/tokens/{mint}/report:
      - markets[].lp.lpLockedPct    → LP burn ratio (0–100)
      - creator (address) + topHolders[].address + topHolders[].pct
                                    → dev wallet % of supply
      - totalHolders                 → holder count
      - topHolders[].pct             → top-10 concentration (sum of first 10)
    """
    if not isinstance(rc, dict):
        return {
            "lp_burned": None, "dev_wallet_pct": None,
            "holder_count": None, "top_10_concentration": None,
        }

    # LP burn — any market with >= 99% LP locked counts as "burned"
    lp_burned = None
    markets = rc.get("markets")
    if isinstance(markets, list) and markets:
        try:
            lp_pcts = []
            for m in markets:
                if not isinstance(m, dict):
                    continue
                lp_obj = m.get("lp") or {}
                pct = lp_obj.get("lpLockedPct")
                if pct is None:
                    continue
                lp_pcts.append(float(pct))
            if lp_pcts:
                lp_burned = max(lp_pcts) >= 99.0
        except Exception:
            lp_burned = None

    # Holder count (total wallets holding the token)
    holder_count = None
    tc = rc.get("totalHolders")
    if isinstance(tc, (int, float)):
        holder_count = int(tc)

    # Top 10 concentration — sum of top 10 holder percentages
    top_10_concentration = None
    top_holders = rc.get("topHolders")
    if isinstance(top_holders, list) and top_holders:
        try:
            pcts = []
            for h in top_holders[:10]:
                if isinstance(h, dict) and "pct" in h:
                    pcts.append(float(h["pct"]))
            if pcts:
                # Rugcheck returns percentage as 0–100 in newer API versions
                total = sum(pcts)
                # Normalize 0–1 → 0–100 if we get a fractional value
                if total <= 1.0:
                    total = total * 100
                top_10_concentration = round(total, 2)
        except Exception:
            top_10_concentration = None

    # Dev wallet % — find creator address in topHolders
    dev_wallet_pct = None
    creator = rc.get("creator") or rc.get("creatorAddress")
    if creator and isinstance(top_holders, list):
        try:
            for h in top_holders:
                if isinstance(h, dict) and h.get("address") == creator:
                    pct = float(h.get("pct") or 0)
                    if pct <= 1.0:
                        pct = pct * 100
                    dev_wallet_pct = round(pct, 2)
                    break
        except Exception:
            dev_wallet_pct = None

    return {
        "lp_burned": lp_burned,
        "dev_wallet_pct": dev_wallet_pct,
        "holder_count": holder_count,
        "top_10_concentration": top_10_concentration,
    }


def _passes_rug_filter(rc_data: dict | None, mcap: float, liquidity: float) -> bool:
    """Returns True if the token passes all rug/safety filters."""
    if not (MIN_MCAP <= mcap <= MAX_MCAP):
        return False
    if liquidity < MIN_LIQUIDITY:
        return False
    if rc_data is None:
        return True   # no data — allow through; AI score will handle it

    # Rugcheck score = RISK score (lower = safer, higher = more risky)
    risk_score = rc_data.get("score", 0)
    if risk_score > MAX_RUGCHECK_RISK:
        return False

    risks = {r.get("name", "").lower() for r in (rc_data.get("risks") or [])}
    if risks & HIGH_RISK_FLAGS:
        return False

    return True


# ── Pattern fingerprint match ─────────────────────────────────────────────────

def _fingerprint_match(
    pattern,
    mcap: float,
    liquidity: float,
    ai_score: float,
) -> float:
    """
    Returns a match score 0–100 against the winner_2x pattern fingerprint.
    100 = perfect match on every dimension.
    """
    if pattern is None:
        return 50.0   # no pattern data yet — neutral pass

    score = 0.0
    checks = 0

    # MC range check
    if pattern.mcap_range_low and pattern.mcap_range_high:
        checks += 1
        low  = pattern.mcap_range_low  * 0.5
        high = pattern.mcap_range_high * 2.0
        if low <= mcap <= high:
            score += 30.0
        elif mcap < low:
            score += 10.0   # smaller = riskier but not impossible

    # Liquidity check
    if pattern.avg_liquidity and pattern.avg_liquidity > 0:
        checks += 1
        ratio = min(liquidity / pattern.avg_liquidity, 2.0)
        score += ratio * 20.0

    # AI score check
    if pattern.avg_ai_score and pattern.avg_ai_score > 0:
        checks += 1
        ratio = min(ai_score / pattern.avg_ai_score, 1.5)
        score += ratio * 30.0

    # Confidence weighting
    confidence_weight = pattern.confidence_score / 100.0
    base_score = score / max(checks, 1)
    return round(min(base_score * confidence_weight + base_score * (1 - confidence_weight * 0.3), 100), 1)


# ── Source 1: New Launches ────────────────────────────────────────────────────

async def _source1_new_launches() -> list[dict]:
    """
    Fetches DexScreener profiles, filters to new Solana tokens < 4h old,
    returns list of {mint, name, symbol, mcap, liquidity, buys_m5, pair_age_h}.
    """
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PROFILES_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                profiles = data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("Scanner source1 fetch failed: %s", exc)
        return []

    sol_profiles = [
        p for p in profiles
        if p.get("chainId") == "solana"
        and (p.get("tokenAddress") or p.get("address"))
    ]

    candidates = []
    now_ms = int(time.time() * 1000)
    age_cutoff_ms = MAX_AGE_HOURS * 3_600_000

    async def _check_profile(profile: dict) -> dict | None:
        mint = profile.get("tokenAddress") or profile.get("address")
        if not mint:
            return None

        pair = await fetch_token_data(mint)
        if pair is None:
            return None

        # Age filter
        created_at = pair.get("pairCreatedAt")
        if created_at and isinstance(created_at, (int, float)):
            age_ms = now_ms - created_at
            if age_ms > age_cutoff_ms:
                return None

        metrics = parse_token_metrics(pair)
        mcap      = metrics.get("market_cap", 0) or 0
        liquidity = metrics.get("liquidity_usd", 0) or 0

        if not (MIN_MCAP <= mcap <= MAX_MCAP):
            return None
        if liquidity < MIN_LIQUIDITY:
            return None

        # 5-min buyer activity check
        buys_m5 = pair.get("txns", {}).get("m5", {}).get("buys", 0) or 0
        if buys_m5 < MIN_BUYERS_M5:
            return None

        return {
            "mint":      mint,
            "name":      metrics.get("name", "Unknown"),
            "symbol":    metrics.get("symbol", "???"),
            "mcap":      mcap,
            "liquidity": liquidity,
            "buys_m5":   buys_m5,
            "source":    "new_launch",
        }

    # Run up to 10 profile checks concurrently
    tasks = [_check_profile(p) for p in sol_profiles[:15]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, dict):
            candidates.append(r)

    return candidates


# ── Source 2: Insider Wallet Activity ────────────────────────────────────────

async def _get_wallet_sigs(wallet: str, since_ts: int) -> list[str]:
    """Returns recent signature strings for a wallet since since_ts (Unix seconds)."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignaturesForAddress",
        "params": [wallet, {"limit": 20, "commitment": "confirmed"}],
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.post(
                HELIUS_RPC_URL, json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                sigs = data.get("result") or []
                return [
                    s["signature"] for s in sigs
                    if s.get("blockTime") and s["blockTime"] >= since_ts
                    and not s.get("err")
                ]
    except Exception as exc:
        logger.debug("Scanner wallet sigs failed for %s: %s", wallet[:12], exc)
        return []


async def _parse_transactions(sigs: list[str]) -> list[dict]:
    if not sigs:
        return []
    url = f"{HELIUS_ENHANCED}?api-key={HELIUS_API_KEY}"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            async with session.post(
                url, json={"transactions": sigs[:50]},
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.debug("Scanner parse_transactions failed: %s", exc)
        return []


async def _source2_insider_wallets() -> list[dict]:
    """
    Checks recent buys by Tier 1/2 wallets via Helius.
    Returns list of {mint, source, insider_count, insider_tier_1_count, insider_tier_2_count}.
    """
    wallets = await get_tier_wallets(max_tier=2)
    if not wallets:
        logger.info("Scanner source2: no tier1/tier2 wallets in DB — source will return empty until Agent 2 promotes some")
        return []
    logger.info("Scanner source2: checking %d tier1/2 wallets for recent buys", len(wallets[:15]))

    since_ts = int(time.time()) - INSIDER_WINDOW
    # mint -> {1: count, 2: count}
    mint_tier_counts: dict[str, dict[int, int]] = {}
    # mint -> list of (wallet_addr, cluster_id_or_None) for cluster grouping
    mint_wallet_clusters: dict[str, list[tuple[str, str | None]]] = {}

    async def _check_wallet(wallet_addr: str, wallet_tier: int, cluster_id: str | None) -> tuple[int, list[str], str, str | None]:
        sigs = await _get_wallet_sigs(wallet_addr, since_ts)
        if not sigs:
            return wallet_tier, [], wallet_addr, cluster_id
        parsed = await _parse_transactions(sigs)
        found_mints = []
        for tx in parsed:
            for transfer in (tx.get("tokenTransfers") or []):
                if transfer.get("toUserAccount") == wallet_addr:
                    mint = transfer.get("mint")
                    if mint:
                        found_mints.append(mint)
        return wallet_tier, found_mints, wallet_addr, cluster_id

    tasks = [
        _check_wallet(w.address, int(w.tier or 2), getattr(w, "cluster_id", None))
        for w in wallets[:15]
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if not isinstance(res, tuple):
            continue
        tier, mints, addr, cid = res
        for m in mints:
            bucket = mint_tier_counts.setdefault(m, {1: 0, 2: 0})
            bucket[tier] = bucket.get(tier, 0) + 1
            mint_wallet_clusters.setdefault(m, []).append((addr, cid))

    candidates = []
    for mint, tc in mint_tier_counts.items():
        t1 = tc.get(1, 0)
        t2 = tc.get(2, 0)

        # Cluster-aware buy count: how many distinct clusters bought
        # this token, AND the max wallets from a single cluster
        entries = mint_wallet_clusters.get(mint, [])
        cluster_counts: dict[str, int] = {}
        for _, cid in entries:
            if cid:
                cluster_counts[cid] = cluster_counts.get(cid, 0) + 1
        # The strongest signal: highest wallet count from a single cluster
        cluster_buy_count = max(cluster_counts.values()) if cluster_counts else 0
        cluster_id_hit = (
            max(cluster_counts.items(), key=lambda x: x[1])[0]
            if cluster_counts else None
        )

        candidates.append({
            "mint": mint,
            "source": "insider_wallet",
            "insider_count": t1 + t2,
            "insider_tier_1_count": t1,
            "insider_tier_2_count": t2,
            "cluster_buy_count": cluster_buy_count,
            "cluster_id_hit": cluster_id_hit,
        })

    candidates.sort(key=lambda x: x["insider_count"], reverse=True)
    return candidates[:10]


# ── Source 3: Volume Spikes ───────────────────────────────────────────────────

async def _source3_volume_spikes() -> list[dict]:
    """
    Checks recently harvested tokens for 5-min volume anomalies.
    m5_vol × 12 ≥ h1_vol × 3 (current 5-min pace is ≥ 3× hourly average).
    """
    # Use DexScreener profiles to get fresh token list and their volume data
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(PROFILES_URL) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                profiles = data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("Scanner source3 fetch failed: %s", exc)
        return []

    sol_profiles = [
        p for p in profiles
        if p.get("chainId") == "solana"
        and (p.get("tokenAddress") or p.get("address"))
    ]

    candidates = []

    async def _check_volume(profile: dict) -> dict | None:
        mint = profile.get("tokenAddress") or profile.get("address")
        if not mint:
            return None
        pair = await fetch_token_data(mint)
        if pair is None:
            return None

        vol = pair.get("volume") or {}
        m5  = float(vol.get("m5")  or 0)
        h1  = float(vol.get("h1")  or 0)

        # Volume spike: 5-min pace is at least 3× the hourly average
        if h1 > 0 and m5 * 12 >= h1 * 3 and m5 > 500:
            metrics   = parse_token_metrics(pair)
            mcap      = metrics.get("market_cap",    0) or 0
            liquidity = metrics.get("liquidity_usd", 0) or 0
            if not (MIN_MCAP <= mcap <= MAX_MCAP):
                return None
            if liquidity < MIN_LIQUIDITY:
                return None
            return {
                "mint":      mint,
                "name":      metrics.get("name",   "Unknown"),
                "symbol":    metrics.get("symbol", "???"),
                "mcap":      mcap,
                "liquidity": liquidity,
                "m5_vol":    m5,
                "h1_vol":    h1,
                "source":    "volume_spike",
            }
        return None

    tasks = [_check_volume(p) for p in sol_profiles[:12]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, dict):
            candidates.append(r)
    return candidates


# ── Source 4: GMGN-flagged tokens (trending + smart money) ─────────────────

async def _source4_gmgn_flagged() -> list[dict]:
    """
    Pulls tokens from the local Token table that gmgn_agent has flagged
    as either trending on GMGN or touched by smart money. Filters to
    recent (last 6h) tokens within the scanner's mcap/liquidity bands,
    and tags them with source="gmgn_smart" so trade_profiles matches
    the `trending_gmgn` / `smart_money_gmgn` pattern_types.

    This is the ONLY source that actually reads the Token table —
    source 1 and source 3 both just hit the DexScreener profiles
    endpoint so they'd only pick up GMGN tokens by coincidence.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(hours=6)
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(Token)
                .where(
                    (Token.gmgn_trending == True) | (Token.gmgn_smart_money == True),
                    Token.first_seen_at >= cutoff,
                    Token.source != "dead",
                )
                .order_by(Token.last_updated_at.desc())
                .limit(25)
            )).scalars().all()
    except Exception as exc:
        logger.warning("Scanner source4 DB fetch failed: %s", exc)
        return []

    if not rows:
        return []

    candidates = []
    for tok in rows:
        mcap = float(tok.market_cap or 0) if tok.market_cap else 0
        liquidity = float(tok.liquidity_usd or 0) if tok.liquidity_usd else 0
        # Soft mcap window — in paper mode this isn't enforced anyway,
        # but don't ship tokens with zero data
        if mcap <= 0:
            continue
        candidates.append({
            "mint":      tok.mint,
            "name":      tok.name or "Unknown",
            "symbol":    tok.symbol or "???",
            "mcap":      mcap,
            "liquidity": liquidity,
            "source":    "gmgn_smart" if tok.gmgn_smart_money else "new_launch",
        })
    return candidates


# ── Full candidate evaluation ─────────────────────────────────────────────────

async def _evaluate_candidate(
    raw: dict,
    pattern,
) -> dict | None:
    """
    Fetches fresh data (if not already in raw), runs rug filter,
    AI scoring, and pattern matching. Reads filter params from DB.
    Returns enriched candidate dict or None if filtered out.
    """
    # Load dynamic filter params from DB
    p = await get_params(
        "scanner_min_mc", "scanner_max_mc", "scanner_min_liquidity",
        "scanner_min_ai_score", "scanner_rugcheck_max_risk",
    )
    mint = raw["mint"]

    # Fetch full data if not already present
    mcap      = raw.get("mcap")
    liquidity = raw.get("liquidity")
    name      = raw.get("name",   "Unknown")
    symbol    = raw.get("symbol", "???")

    is_paper = state.trade_mode == "paper"

    # Always fetch pair so age_minutes + volume_m5/h1 are available for
    # every candidate regardless of source. pair = None falls back to
    # raw-dict values below.
    pair = await fetch_token_data(mint)
    if pair is None and (not mcap or not liquidity):
        logger.info("Scanner REJECTED %s (%s): DexScreener returned no data", name, mint[:12])
        return None
    if pair is not None:
        metrics   = parse_token_metrics(pair)
        mcap      = mcap      or metrics.get("market_cap",    0) or 0
        liquidity = liquidity or metrics.get("liquidity_usd", 0) or 0
        name      = metrics.get("name",   name)
        symbol    = metrics.get("symbol", symbol)

    # Rugcheck
    rc_data = await _fetch_rugcheck(mint)

    _min_mc = p["scanner_min_mc"]
    _max_mc = p["scanner_max_mc"]
    _min_liq = p["scanner_min_liquidity"]
    _min_ai = p["scanner_min_ai_score"]
    _max_rug = p["scanner_rugcheck_max_risk"]

    if is_paper:
        # CHAOS MODE: skip all filters, just log
        if not mcap:
            logger.info("Scanner[paper] %s: no MC data, passing anyway", name)
    else:
        # LIVE MODE: enforce DB-driven filters
        if not mcap:
            logger.info("Scanner REJECTED %s (%s): no market cap data", name, mint[:12])
            return None
        if not (_min_mc <= mcap <= _max_mc):
            logger.info("Scanner REJECTED %s: mcap $%.0f outside $%.0f–$%.0f", name, mcap, _min_mc, _max_mc)
            return None
        if liquidity < _min_liq:
            logger.info("Scanner REJECTED %s: liquidity $%.0f < $%.0f", name, liquidity, _min_liq)
            return None
        if rc_data:
            rc_score_val = rc_data.get("score", 0)
            risks = {r.get("name", "").lower() for r in (rc_data.get("risks") or [])}
            if rc_score_val > _max_rug:
                logger.info("Scanner REJECTED %s: rugcheck risk %d > %.0f", name, rc_score_val, _max_rug)
                return None
            if risks & HIGH_RISK_FLAGS:
                logger.info("Scanner REJECTED %s: rug flags %s", name, risks & HIGH_RISK_FLAGS)
                return None

    # AI score via full scan_token
    scan_data = await scan_token(mint)
    ai_score = 0
    if scan_data is None:
        if is_paper:
            logger.info("Scanner[paper] %s: scan_token failed, using ai_score=0", name)
        else:
            logger.info("Scanner REJECTED %s: scan_token returned None (API fail)", name)
            return None
    else:
        ai_score = scan_data.get("total", 0)

    if not is_paper and ai_score < _min_ai:
        logger.info("Scanner REJECTED %s: AI score %.0f < %.0f", name, ai_score, _min_ai)
        return None

    # Pattern match
    match_score = _fingerprint_match(pattern, mcap, liquidity, ai_score)

    rc_score = (rc_data or {}).get("score")
    rc_norm = (rc_data or {}).get("score_normalised")
    safety = _extract_rugcheck_safety(rc_data)

    # Age in minutes from DexScreener pairCreatedAt (ms epoch)
    age_minutes: float | None = None
    if pair is not None:
        created_ms = pair.get("pairCreatedAt")
        if isinstance(created_ms, (int, float)) and created_ms > 0:
            now_ms = datetime.utcnow().timestamp() * 1000
            age_minutes = max(0.0, (now_ms - float(created_ms)) / 60_000.0)

    # Volume buckets — universalize across all sources so accelerating_volume
    # matcher can see them. raw dict takes precedence (source 3 populates
    # these already); fall back to pair data.
    volume_m5 = raw.get("volume_m5")
    volume_h1 = raw.get("volume_h1")
    if pair is not None:
        vol = pair.get("volume") or {}
        if volume_m5 is None:
            volume_m5 = vol.get("m5") or 0.0
        if volume_h1 is None:
            volume_h1 = vol.get("h1") or 0.0

    # GMGN flags from the Token table (set by gmgn_agent background tasks)
    gmgn_trending = False
    gmgn_smart_money = False
    try:
        async with AsyncSessionLocal() as session:
            tok_row = (await session.execute(
                select(Token).where(Token.mint == mint)
            )).scalar_one_or_none()
            if tok_row is not None:
                gmgn_trending = bool(tok_row.gmgn_trending)
                gmgn_smart_money = bool(getattr(tok_row, "gmgn_smart_money", False))
    except Exception as exc:
        logger.debug("Scanner: Token flag lookup failed for %s: %s", mint[:12], exc)

    return {
        "mint":                  mint,
        "name":                  name,
        "symbol":                symbol,
        "source":                raw.get("source", "unknown"),
        "ai_score":              round(ai_score, 1),
        "match_score":           match_score,
        "mcap":                  mcap,
        "liquidity":             liquidity,
        "rugcheck":              rc_score,
        "rugcheck_normalised":   rc_norm,
        "found_at":              datetime.utcnow().isoformat(),
        "insider_count":         raw.get("insider_count", 0),
        # New pattern-matcher inputs
        "age_minutes":           age_minutes,
        "volume_m5":             volume_m5,
        "volume_h1":             volume_h1,
        "insider_tier_1_count":  raw.get("insider_tier_1_count", 0),
        "insider_tier_2_count":  raw.get("insider_tier_2_count", 0),
        "cluster_buy_count":     raw.get("cluster_buy_count", 0),
        "cluster_id_hit":        raw.get("cluster_id_hit"),
        "gmgn_trending":         gmgn_trending,
        "gmgn_smart_money":      gmgn_smart_money,
        "lp_burned":             safety["lp_burned"],
        "dev_wallet_pct":        safety["dev_wallet_pct"],
        "holder_count":          safety["holder_count"],
        "top_10_concentration":  safety["top_10_concentration"],
    }


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single scanner tick — runs ALWAYS regardless of autotrade status.
    Returns (candidates_found, candidates_queued).
    """
    state.scanner_status = "running"
    # Refresh paper_balance at the top of every tick so the in-memory
    # cache stays close to DB reality. Previously this only refreshed
    # inside the trade-opening loop which meant ticks with no new
    # candidates left state.paper_balance stale — user saw a 904 SOL
    # divergence when lots of trades closed between ticks.
    try:
        state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
    except Exception as exc:
        logger.debug("Scanner balance refresh failed: %s", exc)

    # Clear cross-tick dedupe list. This was an append-only cache that
    # grew forever — any mint evaluated once was permanently excluded
    # from future ticks. When user toggled trade_mode between off/paper,
    # candidates scored during "off" windows were appended to pending
    # but never reached the open path, and couldn't be re-evaluated
    # once mode flipped back to paper. Cross-tick dedup is now handled
    # by DB gates (has_open_paper_trade + has_recent_manual_close)
    # which are correct and respect the actual trade state.
    state.pending_candidates.clear()

    pattern = await get_pattern_by_type("winner_2x")

    # Gather raw candidates from all 4 sources concurrently. Source 1 and
    # Source 3 both hit DexScreener PROFILES_URL (historically); Source 2
    # uses Helius wallet-history; Source 4 reads the local Token table
    # for GMGN-flagged tokens (trending / smart money).
    s1, s2, s3, s4 = await asyncio.gather(
        _source1_new_launches(),
        _source2_insider_wallets(),
        _source3_volume_spikes(),
        _source4_gmgn_flagged(),
        return_exceptions=True,
    )

    raw_candidates: list[dict] = []
    per_source_counts = {"s1_new_launch": 0, "s2_insider": 0, "s3_volume": 0, "s4_gmgn": 0}
    for label, result in zip(
        ("s1_new_launch", "s2_insider", "s3_volume", "s4_gmgn"),
        (s1, s2, s3, s4),
    ):
        if isinstance(result, list):
            per_source_counts[label] = len(result)
            raw_candidates.extend(result)
        elif isinstance(result, Exception):
            logger.warning("Scanner %s error: %s", label, result)

    # Per-source visibility: see at a glance which sources are pulling weight
    logger.info(
        "Scanner sources: s1=%d s2=%d s3=%d s4=%d raw_total=%d",
        per_source_counts["s1_new_launch"],
        per_source_counts["s2_insider"],
        per_source_counts["s3_volume"],
        per_source_counts["s4_gmgn"],
        len(raw_candidates),
    )

    if not raw_candidates:
        state.scanner_status = "idle"
        await log_agent_run(
            "scanner", tokens_found=0, tokens_saved=0,
            notes=(
                f"s1={per_source_counts['s1_new_launch']} "
                f"s2={per_source_counts['s2_insider']} "
                f"s3={per_source_counts['s3_volume']} "
                f"s4={per_source_counts['s4_gmgn']}"
            ),
        )
        return 0, 0

    logger.info("Scanner: %d raw candidates from all sources", len(raw_candidates))

    # Deduplicate by mint
    seen: set[str] = set()
    deduped = []
    for c in raw_candidates:
        if c["mint"] not in seen:
            seen.add(c["mint"])
            deduped.append(c)

    # Filter out mints already queued
    already_queued = {c["mint"] for c in state.pending_candidates}
    deduped = [c for c in deduped if c["mint"] not in already_queued]

    candidates_found = len(deduped)

    # Evaluate (rug filter + AI score + pattern match) with concurrency cap
    semaphore = asyncio.Semaphore(3)

    async def _bounded_eval(raw):
        async with semaphore:
            return await _evaluate_candidate(raw, pattern)

    evaluated = await asyncio.gather(
        *[_bounded_eval(c) for c in deduped[:20]],
        return_exceptions=True,
    )

    # Log evaluation results summary
    eval_none = sum(1 for r in evaluated if r is None)
    eval_exc = sum(1 for r in evaluated if isinstance(r, Exception))
    eval_ok = sum(1 for r in evaluated if isinstance(r, dict))
    logger.info(
        "Scanner: evaluation results — ok=%d rejected=%d errors=%d",
        eval_ok, eval_none, eval_exc,
    )
    for r in evaluated:
        if isinstance(r, Exception):
            logger.warning("Scanner: evaluation exception: %s", r)

    queued = 0
    is_paper_mode = state.trade_mode == "paper"
    for result in evaluated:
        if not isinstance(result, dict):
            continue
        if not is_paper_mode and result["match_score"] < 30:
            logger.info(
                "Scanner REJECTED %s: match_score=%.0f < 30",
                result.get("name", result["mint"][:12]), result["match_score"],
            )
            continue
        state.data_points_today += 1

        # Pass every qualifying candidate to Agent 5 for confidence scoring
        try:
            scored = await score_candidate(result)
            logger.info(
                "Scanner→Agent5: %s (%s) confidence=%.0f decision=%s",
                scored.get("name", "?"), scored.get("mint", "?")[:12],
                scored.get("confidence_score", 0), scored.get("decision", "?"),
            )
        except Exception as exc:
            logger.error("Scanner: Agent5 scoring failed for %s: %s", result["mint"][:12], exc)
            scored = result  # fall back to unscored candidate

        # Open paper trade if Agent 5 flagged it
        if scored.get("paper_trade"):
            # Check for duplicate — one position per token
            mint_addr = scored.get("mint", "")
            if mint_addr and await has_open_paper_trade(mint_addr):
                logger.info("Scanner: skip paper trade %s — already have open position",
                            scored.get("name", "?")[:20])
                continue

            # "Rage quit" cooldown — if the user recently manually closed
            # this token, don't reopen it just because DexScreener still
            # has it on trending. Tunable via agent_params.
            if mint_addr:
                cooldown_cfg = await get_params("manual_close_cooldown_hours")
                cooldown_hours = float(cooldown_cfg.get("manual_close_cooldown_hours") or 24.0)
                if cooldown_hours > 0:
                    try:
                        if await has_recent_manual_close(mint_addr, within_hours=cooldown_hours):
                            logger.info(
                                "Scanner: skip paper trade %s — manual_close cooldown (%.1fh)",
                                scored.get("name", "?")[:20], cooldown_hours,
                            )
                            # Also add to pending_candidates so this tick's
                            # in-memory dedupe holds across ticks until the
                            # cooldown elapses
                            state.pending_candidates.append(scored)
                            continue
                    except Exception as exc:
                        logger.debug("Scanner: manual_close cooldown check failed: %s", exc)

            # Compute true balance from DB
            state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)

            if state.paper_balance < 0.05:
                logger.info("Scanner: paper balance depleted (%.4f SOL), skipping", state.paper_balance)
                continue

            # Position sizing is now learned per pattern_type by Agent 6:
            # confidence_engine's resolver returns position_pct as the
            # mean of trained ai_trade_params.optimal_position_pct across
            # matched rows. Agent 6 updates that per pattern_type based
            # on win_rate + market regime.
            #
            # Hardcoded confidence tiers (size_confidence_*) are now
            # ignored. The two remaining caps are:
            #   - 20% of balance (tail risk cap)
            #   - max_position_sol (absolute cap to block the runaway
            #     compounding loop when balance inflates)
            sp = await get_params("max_position_sol")
            max_abs = float(sp.get("max_position_sol") or 5.0)

            learned_pct = float(scored.get("trade_position_pct", 10.0)) / 100.0
            paper_sol = round(
                min(
                    state.paper_balance * learned_pct,
                    state.paper_balance * 0.20,
                    max_abs,
                ),
                4,
            )

            logger.info(
                "Scanner: PAPER TRADE %s conf=%.0f size=%.0f%% sol=%.4f bal=%.4f",
                scored.get("name", "?")[:20], conf, size_pct * 100,
                paper_sol, state.paper_balance,
            )
            try:
                # pattern_type now stores the COMMA-SEPARATED list of matched
                # ai_trade_params keys so Agent 6 can learn per-bucket from a
                # single PaperTrade row. See trade_profiles.match_pattern_types.
                pt = await open_paper_trade(
                    token_address=scored.get("mint", ""),
                    token_name=scored.get("name"),
                    entry_mc=scored.get("mcap"),
                    entry_price=scored.get("mcap"),
                    paper_sol=paper_sol,
                    confidence=scored.get("confidence_score", 0),
                    pattern_type=scored.get("profile_tag") or scored.get("source"),
                    tp_x=scored.get("trade_tp_x", 3.0),
                    sl_pct=scored.get("trade_sl_pct", 30.0),
                )
                # Recompute balance from DB (trade deducted via open_paper_trade)
                state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                state.paper_trades_today += 1
                logger.info("Scanner: paper trade id=%s bal=%.4f SOL", pt.id, state.paper_balance)
            except Exception as exc:
                logger.error("Scanner: paper trade DB failed for %s: %s", scored.get("mint", "?")[:12], exc)

        state.pending_candidates.append(scored)
        state.scanner_candidates_today += 1
        queued += 1

        if queued >= MAX_CANDIDATES:
            break

    state.scanner_last_run = datetime.utcnow()
    state.scanner_status   = "idle"

    await log_agent_run(
        "scanner",
        tokens_found=candidates_found,
        tokens_saved=queued,
        notes=(
            f"s1={per_source_counts['s1_new_launch']} "
            f"s2={per_source_counts['s2_insider']} "
            f"s3={per_source_counts['s3_volume']} "
            f"s4={per_source_counts['s4_gmgn']} "
            f"queued={queued}"
        ),
    )

    logger.info(
        "Scanner: done — raw=%d evaluated=%d queued=%d",
        candidates_found, len([r for r in evaluated if isinstance(r, dict)]), queued,
    )
    return candidates_found, queued


# ── Background loop ───────────────────────────────────────────────────────────

async def scanner_agent_loop() -> None:
    """Runs the scanner every 30 seconds ALWAYS — autotrade only controls execution."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Scanner agent started — polling every %ds (always-on mode)", POLL_INTERVAL)
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Scanner loop error: %s", exc)
            state.scanner_status = "idle"
        await asyncio.sleep(POLL_INTERVAL)
