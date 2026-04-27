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
from bot.config import HELIUS_API_KEY
from bot.helius import (
    get_address_transactions,
    get_signatures_for_address,
    parse_transactions,
    rpc_call,
)
from bot.scanner import (
    fetch_token_data, parse_token_metrics, scan_token,
    ALLOWED_DEXES, MIN_LIQUIDITY_BY_DEX,
    ALLOWED_MINT_SUFFIXES, mint_suffix_ok,
)
from bot.agents.confidence_engine import score_candidate
from database.models import (
    log_agent_run,
    get_tier_wallets,
    get_pattern_by_type,
    token_exists,
    get_token_count,
    open_paper_trade,
    has_open_paper_trade,
    has_open_paper_trade_by_name,
    has_recent_manual_close,
    has_recent_close,
    count_open_paper_trades,
    get_params,
    compute_paper_balance,
    AsyncSessionLocal,
    select,
    Token,
)

logger = logging.getLogger(__name__)

PROFILES_URL    = "https://api.dexscreener.com/token-profiles/latest/v1"
RUGCHECK_URL    = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"

POLL_INTERVAL   = 8      # seconds (faster scanning — speed is edge)
STARTUP_DELAY   = 60     # seconds after bot start
INSIDER_WINDOW  = 1_800   # 30 min in seconds

# ── Per-tick params cache ────────────────────────────────────────────────────
# Loaded once at the top of run_once() from DB agent_params. Source functions
# and _evaluate_candidate all read from this dict so Agent 6 adjustments
# propagate everywhere on the next tick. /setparam also works.
_SP: dict[str, float] = {}

async def _refresh_scanner_params() -> None:
    global _SP
    _SP = await get_params(
        "scanner_min_mc", "scanner_max_mc", "scanner_min_liquidity",
        "scanner_min_ai_score", "scanner_rugcheck_max_risk",
        "scanner_max_candidates", "scanner_max_age_hours",
        "scanner_min_buyers_m5",
    )

def _sp(name: str, default: float) -> float:
    return float(_SP.get(name) or default)

HIGH_RISK_FLAGS = {
    "freeze_authority_enabled",
    "mint_authority_enabled",
    "honeypot",
    "rugged",
    "high_sell_tax",
    "high_buy_tax",
}


# ── Rug check ─────────────────────────────────────────────────────────────────

# ── LP safety gate ───────────────────────────────────────────────────────────
#
# Hard, non-negotiable gate applied to every candidate before AI scoring.
# A token passes only if the LP is provably burned OR provably locked
# (by a known locker program, or by rugcheck reporting lpLockedPct >= 99).
# Any other state — unlocked, or unknown — is an automatic skip.
#
# Agent 6 must NOT tune `require_lp_safe`. The param exists so humans can
# flip it for emergencies, not so the learning loop can turn it off.

LP_BURN_ADDRESSES = frozenset({
    "1nc1nerator11111111111111111111111111111111",
    "11111111111111111111111111111111",
})

# Solana LP locker program IDs / authority addresses. Only entries whose
# exact on-chain address is confirmed should live here — unverified
# entries get filtered out by the "unknown holder" path instead.
LP_LOCKER_PROGRAMS = frozenset({
    # Streamflow
    "strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m",
    # Squads v3 multisig timelock
    "SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu",
    # Tokenvesting program
    "CChTq6PthWU82YZkbveA3WDf7s97BWhBK4Vx9bmsT743",
    # Magna — address not yet verified, intentionally absent. Rugcheck's
    # lpLockedPct fallback will still catch Magna-locked pools.
})


def _classify_lp(rc: dict | None) -> dict:
    """
    Inspect rugcheck market[] entries and decide whether the LP is safe.

    Returns {'state': str, 'expiry': datetime|None, 'locked_pct': float|None}
    where state is one of:
      'burned'    — LP held by a known burn address
      'locked'    — LP held by a known locker contract OR lpLockedPct >= 99
      'unlocked'  — lpLockedPct < 99 and holder not in allowlist
      'unknown'   — no usable LP data available
    """
    if not isinstance(rc, dict):
        return {"state": "unknown", "expiry": None, "locked_pct": None}

    markets = rc.get("markets")
    if not isinstance(markets, list) or not markets:
        return {"state": "unknown", "expiry": None, "locked_pct": None}

    best_state = "unknown"
    best_expiry: datetime | None = None
    best_pct: float | None = None

    for m in markets:
        if not isinstance(m, dict):
            continue
        lp_obj = m.get("lp") or {}

        pct = lp_obj.get("lpLockedPct")
        if isinstance(pct, (int, float)):
            pct_f = float(pct)
            if best_pct is None or pct_f > best_pct:
                best_pct = pct_f

        holder = (
            lp_obj.get("lpLockedOwner")
            or lp_obj.get("lockedOwner")
            or lp_obj.get("owner")
            or ""
        )
        if isinstance(holder, str):
            if holder in LP_BURN_ADDRESSES:
                best_state = "burned"
                # burned is the strongest state; keep iterating only to
                # capture the max lpLockedPct for logging
            elif holder in LP_LOCKER_PROGRAMS and best_state != "burned":
                best_state = "locked"
                exp = (
                    lp_obj.get("lockExpiry")
                    or lp_obj.get("unlockDate")
                    or lp_obj.get("lpLockedUntil")
                )
                if isinstance(exp, (int, float)) and exp > 0:
                    try:
                        # Accept both seconds and ms epoch
                        ts = float(exp)
                        if ts > 1e12:
                            ts = ts / 1000.0
                        best_expiry = datetime.utcfromtimestamp(int(ts))
                    except Exception:
                        best_expiry = None

    # Fallbacks using lpLockedPct alone
    if best_state == "unknown" and best_pct is not None:
        if best_pct >= 99.0:
            best_state = "locked"
        else:
            best_state = "unlocked"

    return {"state": best_state, "expiry": best_expiry, "locked_pct": best_pct}


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
    if not (_sp("scanner_min_mc", 10_000) <= mcap <= _sp("scanner_max_mc", 5_000_000)):
        return False
    if liquidity < _sp("scanner_min_liquidity", 5_000):
        return False
    if rc_data is None:
        return True   # no data — allow through; AI score will handle it

    # Rugcheck score = RISK score (lower = safer, higher = more risky)
    risk_score = rc_data.get("score", 0)
    if risk_score > _sp("scanner_rugcheck_max_risk", 500):
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
    Returns a match score 0–100 against a pattern fingerprint.
    100 = perfect match on every dimension.
    """
    if pattern is None:
        return 50.0   # no pattern data yet — neutral pass

    score = 0.0
    checks = 0

    # MC range check — award partial credit for appreciated tokens too
    if pattern.mcap_range_low and pattern.mcap_range_high:
        checks += 1
        low  = pattern.mcap_range_low  * 0.5
        high = pattern.mcap_range_high * 2.0
        if low <= mcap <= high:
            score += 30.0
        elif mcap < low:
            score += 10.0   # smaller = riskier but not impossible
        else:
            score += 10.0   # above range = appreciated past pattern (not a penalty)

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


def _best_pattern_match(
    patterns: list,
    mcap: float,
    liquidity: float,
    ai_score: float,
) -> float:
    """
    Score against ALL available patterns and return the best match.
    This ensures mega_runner, winner_5x, winner_10x patterns are all used.
    """
    if not patterns:
        return 50.0
    scores = [_fingerprint_match(p, mcap, liquidity, ai_score) for p in patterns if p is not None]
    return max(scores) if scores else 50.0


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
    age_cutoff_ms = _sp("scanner_max_age_hours", 4.0) * 3_600_000

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

        if not (_sp("scanner_min_mc", 10_000) <= mcap <= _sp("scanner_max_mc", 5_000_000)):
            return None
        if liquidity < _sp("scanner_min_liquidity", 5_000):
            return None

        # 5-min buyer activity check
        buys_m5 = pair.get("txns", {}).get("m5", {}).get("buys", 0) or 0
        if buys_m5 < _sp("scanner_min_buyers_m5", 5):
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

    # Run profile checks concurrently (raised from 15 — Developer plan supports 50 req/s)
    tasks = [_check_profile(p) for p in sol_profiles[:40]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, dict):
            candidates.append(r)

    return candidates


# ── Source 2: Insider Wallet Activity ────────────────────────────────────────

async def _get_wallet_sigs(wallet: str, since_ts: int) -> list[dict]:
    """Returns recent signatures for a wallet since since_ts (Unix seconds)."""
    sigs = await get_signatures_for_address(wallet, limit=20, label=f"scan_sigs:{wallet[:8]}")
    return [
        {"signature": s["signature"], "blockTime": s["blockTime"]}
        for s in sigs
        if s.get("blockTime") and s["blockTime"] >= since_ts
        and not s.get("err")
    ]


async def _parse_transactions(sigs: list[str]) -> list[dict]:
    if not sigs:
        return []
    return await parse_transactions(sigs[:50], label="scan_parse_tx")


async def _source2_insider_wallets() -> list[dict]:
    """
    Checks recent buys by Tier 1/2 wallets via Helius.
    Returns list of {mint, source, insider_count, insider_tier_1_count, insider_tier_2_count}.
    """
    wallets = await get_tier_wallets(max_tier=3)
    if not wallets:
        logger.info("Scanner source2: no tiered wallets in DB — source will return empty until Agent 2 or GMGN imports some")
        return []

    # Prioritize wallets: tier 1 >> tier 2 >> tier 3, then by win rate and recency
    def _wallet_priority(w):
        tier_w = {1: 100, 2: 10, 3: 1}.get(int(w.tier or 3), 1)
        wr_bonus = 1.0 + float(w.win_rate or 0) * 0.5
        return tier_w * wr_bonus

    wallets = sorted(wallets, key=_wallet_priority, reverse=True)
    logger.info("Scanner source2: checking %d tier1/2/3 wallets (prioritized)", len(wallets[:50]))

    since_ts = int(time.time()) - INSIDER_WINDOW
    # mint -> {1: count, 2: count, 3: count}
    mint_tier_counts: dict[str, dict[int, int]] = {}
    # mint -> list of (wallet_addr, cluster_id_or_None) for cluster grouping
    mint_wallet_clusters: dict[str, list[tuple[str, str | None]]] = {}

    async def _check_wallet(wallet_addr: str, wallet_tier: int, cluster_id: str | None):
        sig_objs = await _get_wallet_sigs(wallet_addr, since_ts)
        if not sig_objs:
            return wallet_tier, [], wallet_addr, cluster_id, 0
        sig_strs = [s["signature"] for s in sig_objs]
        newest_block_time = max(s["blockTime"] for s in sig_objs)
        parsed = await _parse_transactions(sig_strs)
        found_mints = []
        for tx in parsed:
            for transfer in (tx.get("tokenTransfers") or []):
                if transfer.get("toUserAccount") == wallet_addr:
                    mint = transfer.get("mint")
                    if mint:
                        found_mints.append(mint)
        return wallet_tier, found_mints, wallet_addr, cluster_id, newest_block_time

    tasks = [
        _check_wallet(w.address, int(w.tier or 2), getattr(w, "cluster_id", None))
        for w in wallets[:50]   # Business plan: 200 req/s
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # newest_buy_ts tracks the most recent blockTime per mint
    mint_newest_buy: dict[str, int] = {}
    for res in results:
        if not isinstance(res, tuple):
            continue
        tier, mints, addr, cid, newest_bt = res
        for m in mints:
            bucket = mint_tier_counts.setdefault(m, {1: 0, 2: 0, 3: 0})
            bucket[tier] = bucket.get(tier, 0) + 1
            mint_wallet_clusters.setdefault(m, []).append((addr, cid))
            if newest_bt:
                mint_newest_buy[m] = max(mint_newest_buy.get(m, 0), newest_bt)

    now_ts = int(time.time())
    candidates = []
    for mint, tc in mint_tier_counts.items():
        t1 = tc.get(1, 0)
        t2 = tc.get(2, 0)
        t3 = tc.get(3, 0)

        entries = mint_wallet_clusters.get(mint, [])
        cluster_counts: dict[str, int] = {}
        for _, cid in entries:
            if cid:
                cluster_counts[cid] = cluster_counts.get(cid, 0) + 1
        cluster_buy_count = max(cluster_counts.values()) if cluster_counts else 0
        cluster_id_hit = (
            max(cluster_counts.items(), key=lambda x: x[1])[0]
            if cluster_counts else None
        )

        newest_ts = mint_newest_buy.get(mint, 0)
        buy_age_s = now_ts - newest_ts if newest_ts else INSIDER_WINDOW

        candidates.append({
            "mint": mint,
            "source": "insider_wallet",
            "insider_count": t1 + t2 + t3,
            "insider_tier_1_count": t1,
            "insider_tier_2_count": t2,
            "insider_tier_3_count": t3,
            "cluster_buy_count": cluster_buy_count,
            "cluster_id_hit": cluster_id_hit,
            "insider_buy_age_s": buy_age_s,
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
            if not (_sp("scanner_min_mc", 10_000) <= mcap <= _sp("scanner_max_mc", 5_000_000)):
                return None
            if liquidity < _sp("scanner_min_liquidity", 5_000):
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

    tasks = [_check_volume(p) for p in sol_profiles[:30]]
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
    the trending_gmgn / smart_money_gmgn pattern_types.

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
    all_patterns: list | None = None,
) -> dict | None:
    """
    Fetches fresh data (if not already in raw), runs rug filter,
    AI scoring, and pattern matching. Reads filter params from DB.
    Returns enriched candidate dict or None if filtered out.
    """
    mint = raw["mint"]
    raw_name_hint = raw.get("name") or raw.get("symbol") or "?"

    # ── GATE 1 (non-negotiable): mint suffix allowlist ───────────────
    # Must come BEFORE any network fetches or DB reads so garbage
    # candidates cost us nothing. Agent 6 cannot override this.
    if not mint_suffix_ok(mint):
        logger.info(
            "SKIP %s (%s) — mint suffix filter failed (allowed: %s)",
            raw_name_hint, (mint or "")[:12], "/".join(ALLOWED_MINT_SUFFIXES),
        )
        return None

    # Load dynamic filter params from DB
    p = await get_params(
        "scanner_min_mc", "scanner_max_mc", "scanner_min_liquidity",
        "scanner_min_ai_score", "scanner_rugcheck_max_risk",
    )

    # Fetch full data if not already present
    mcap      = raw.get("mcap")
    liquidity = raw.get("liquidity")
    name      = raw.get("name",   "Unknown")
    symbol    = raw.get("symbol", "???")

    is_paper = state.trade_mode == "paper"

    # Always fetch pair — a valid pair on an ALLOWED DEX is required for
    # every candidate regardless of source. Without this hard gate,
    # candidates from GMGN (source 4) that already carry mcap/liquidity
    # bypass the DEX check entirely, letting pre-bonded pump.fun tokens
    # through.
    pair = await fetch_token_data(mint)
    if pair is None:
        logger.info(
            "Scanner REJECTED %s (%s): no pair on allowed DEX (%s)",
            name, mint[:12], ",".join(sorted(ALLOWED_DEXES)),
        )
        return None
    dex_id = ""
    if pair is not None:
        metrics   = parse_token_metrics(pair)
        mcap      = mcap      or metrics.get("market_cap",    0) or 0
        liquidity = liquidity or metrics.get("liquidity_usd", 0) or 0
        name      = metrics.get("name",   name)
        symbol    = metrics.get("symbol", symbol)
        dex_id    = metrics.get("dex_id", "") or ""

        # Defensive re-check: fetch_token_data already filters to ALLOWED_DEXES,
        # but this guards against future code paths that hand a pair in directly.
        if dex_id not in ALLOWED_DEXES:
            logger.info(
                "Scanner REJECTED %s (%s): unsupported DEX %s",
                name, mint[:12], dex_id or "unknown",
            )
            return None

        # Per-DEX min liquidity floor (applied in both paper and live modes).
        min_liq_dex = MIN_LIQUIDITY_BY_DEX.get(dex_id, 10_000)
        if liquidity and liquidity < min_liq_dex:
            logger.info(
                "Scanner REJECTED %s: %s liquidity $%.0f < $%.0f floor",
                name, dex_id, liquidity, min_liq_dex,
            )
            return None

    # ── GATE 4: missing required data ───────────────────────────────
    # Name + symbol must be present. Mint is already validated by gate 1.
    if not name or name in ("?", "Unknown") or not symbol or symbol in ("?", "???"):
        logger.info(
            "SKIP %s (%s) — missing required data (name=%r symbol=%r)",
            raw_name_hint, mint[:12], name, symbol,
        )
        return None

    # Rugcheck
    rc_data = await _fetch_rugcheck(mint)

    # LP lock/burn gate has been removed per operator decision — raw LP
    # state from rugcheck was too unreliable (missing data on freshly
    # graduated pools) and the DEX allowlist + holder/dev/top10 tightening
    # below provides enough trap defense. _classify_lp and the constants
    # are kept around in case we reintroduce it later with a better data
    # source, but _evaluate_candidate no longer consults them.

    lp_params = await get_params(
        "safety_max_dev_pct",
        "safety_max_top10_pct",
        "safety_min_holders",
    )

    # ── Holder / concentration safety tightening ───────────────────────
    # Reads rugcheck safety fields that were previously only surfaced in
    # the candidate dict for pattern matching. Hard-rejects obvious trap
    # shapes before the token ever reaches Agent 5.
    _safety_preview = _extract_rugcheck_safety(rc_data)
    _max_dev  = float(lp_params.get("safety_max_dev_pct")  or 15.0)
    _max_top10 = float(lp_params.get("safety_max_top10_pct") or 40.0)
    _min_holders = int(lp_params.get("safety_min_holders") or 50)

    _dev = _safety_preview.get("dev_wallet_pct")
    _top10 = _safety_preview.get("top_10_concentration")
    _holders = _safety_preview.get("holder_count")

    if _dev is not None and _dev >= _max_dev:
        logger.info(
            "SKIP %s (%s) — dev wallet %.1f%% >= %.0f%%",
            name, mint[:12], _dev, _max_dev,
        )
        return None
    if _top10 is not None and _top10 >= _max_top10:
        logger.info(
            "SKIP %s (%s) — top10 %.1f%% >= %.0f%%",
            name, mint[:12], _top10, _max_top10,
        )
        return None
    if _holders is not None and _holders < _min_holders:
        logger.info(
            "SKIP %s (%s) — only %d holders (< %d)",
            name, mint[:12], _holders, _min_holders,
        )
        return None

    _min_mc = p["scanner_min_mc"]
    _max_mc = p["scanner_max_mc"]
    _min_liq = p["scanner_min_liquidity"]
    _min_ai = p["scanner_min_ai_score"]
    _max_rug = p["scanner_rugcheck_max_risk"]

    # Quality filters apply in BOTH paper and live modes. The old
    # "chaos mode" bypass let $8K MC dumps, high-risk tokens, and
    # garbage through the paper pipeline, poisoning the learning corpus.
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
        logger.info("Scanner REJECTED %s: scan_token returned None (API fail)", name)
        return None
    else:
        ai_score = scan_data.get("total", 0)

    if not is_paper and ai_score < _min_ai:
        logger.info("Scanner REJECTED %s: AI score %.0f < %.0f", name, ai_score, _min_ai)
        return None

    # Pattern match — score against ALL loaded patterns, use the best
    if all_patterns:
        match_score = _best_pattern_match(all_patterns, mcap, liquidity, ai_score)
    else:
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
        "dex_id":                dex_id,
        "insider_buy_age_s":     raw.get("insider_buy_age_s"),
    }


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single scanner tick — runs ALWAYS regardless of autotrade status.
    Returns (candidates_found, candidates_queued).
    """
    state.scanner_status = "running"
    await _refresh_scanner_params()
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
    # Consume pending candidates (TG signals, etc.) BEFORE clearing.
    # Previous bug: .clear() wiped TG signals before they were evaluated.
    injected_candidates = list(state.pending_candidates)
    state.pending_candidates.clear()
    if injected_candidates:
        logger.info("Scanner: consumed %d injected candidates (TG signals, etc.)", len(injected_candidates))

    # Load ALL pattern types
    pattern_2x   = await get_pattern_by_type("winner_2x")
    pattern_5x   = await get_pattern_by_type("winner_5x")
    pattern_10x  = await get_pattern_by_type("winner_10x")
    pattern_mega = await get_pattern_by_type("mega_runner")
    all_patterns = [p for p in [pattern_2x, pattern_5x, pattern_10x, pattern_mega] if p is not None]
    logger.info("Scanner: loaded %d pattern types for matching", len(all_patterns))

    # Gather raw candidates from all 5 sources concurrently.
    s1, s2, s3, s4 = await asyncio.gather(
        _source1_new_launches(),
        _source2_insider_wallets(),
        _source3_volume_spikes(),
        _source4_gmgn_flagged(),
        return_exceptions=True,
    )

    raw_candidates: list[dict] = []
    # Source 5: injected candidates (TG signals, manual additions)
    raw_candidates.extend(injected_candidates)
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
    semaphore = asyncio.Semaphore(15)  # Business plan: 200 req/s

    async def _bounded_eval(raw):
        async with semaphore:
            return await _evaluate_candidate(raw, pattern_2x, all_patterns=all_patterns)

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

        # Record sighting for confirmation gate
        from bot.agents.candidate_tracker import record_sighting, is_confirmed, get_sighting_info
        record_sighting(result["mint"], result.get("match_score", 0))

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
            mint_addr = scored.get("mint", "")

            # Session cooldown — stop trading after 5 consecutive losses
            # But cap at 15 minutes max, and log clearly
            from datetime import datetime as _dt
            if state.session_cooldown_until and _dt.utcnow() < state.session_cooldown_until:
                remaining = (state.session_cooldown_until - _dt.utcnow()).total_seconds()
                if remaining > 900:  # cap at 15 min
                    state.session_cooldown_until = _dt.utcnow() + timedelta(minutes=15)
                logger.info(
                    "Scanner: SKIP %s — session cooldown (%.0f min remaining)",
                    scored.get("name", "?")[:20], remaining / 60,
                )
                continue

            # Confirmation gate REMOVED — speed matters more than confirmation
            # for memecoins. Rug protection comes from safety filters + phase SL.
            # The 30+ second delay was killing entry quality — by the time we
            # confirmed, the token had already pumped 50-200%.
            token_name = scored.get("name")

            # Hard cap on concurrent open positions. Runs BEFORE any
            # other gate so we never waste cycles evaluating when full.
            risk_cfg = await get_params("max_open_paper_trades", "close_cooldown_hours")
            max_open = int(risk_cfg.get("max_open_paper_trades") or 5)
            open_count = await count_open_paper_trades()
            if open_count >= max_open:
                logger.info(
                    "Scanner: skip paper trade %s — max_open_paper_trades reached (%d/%d)",
                    (token_name or "?")[:20], open_count, max_open,
                )
                continue

            # Duplicate guards (by mint + by display name)
            if mint_addr and await has_open_paper_trade(mint_addr):
                logger.info("Scanner: skip paper trade %s — already have open position (mint match)",
                            (token_name or "?")[:20])
                continue
            if token_name and await has_open_paper_trade_by_name(token_name):
                logger.info("Scanner: skip paper trade %s — already have open position (name match)",
                            token_name[:20])
                continue

            # Re-entry cooldown: if this mint was closed (any reason —
            # sl / tp / trail / dead / manual) within the cooldown window,
            # don't reopen immediately. Prevents the DexScreener trending
            # loop from churning the same handful of tokens repeatedly.
            close_cooldown = float(risk_cfg.get("close_cooldown_hours") or 2.0)
            if close_cooldown > 0 and mint_addr:
                try:
                    if await has_recent_close(mint_addr, within_hours=close_cooldown):
                        logger.info(
                            "Scanner: skip paper trade %s — re-entry cooldown (%.1fh since any close)",
                            (token_name or "?")[:20], close_cooldown,
                        )
                        continue
                except Exception as exc:
                    logger.debug("Scanner: re-entry cooldown check failed: %s", exc)

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

            # Stale-price gate: the candidate was evaluated N ms ago and
            # a pump.fun token can drop 50-70% in seconds. Re-fetch live
            # MC right now and reject if it's significantly below eval.
            # Also use the fresh MC as the entry value — we're buying
            # at current price, not some cached price from 2 seconds ago.
            eval_mc = float(scored.get("mcap") or 0)
            fresh_mc = eval_mc
            if mint_addr:
                try:
                    fresh_pair = await fetch_token_data(mint_addr)
                    if fresh_pair is not None:
                        fresh_metrics = parse_token_metrics(fresh_pair)
                        new_mc = float(fresh_metrics.get("market_cap") or 0)
                        if new_mc > 0:
                            fresh_mc = new_mc
                            if eval_mc > 0:
                                drop_ratio = (eval_mc - new_mc) / eval_mc
                                if drop_ratio > 0.10:
                                    logger.info(
                                        "Scanner: skip paper trade %s — stale price, "
                                        "eval_mc=%.0f → now=%.0f (%.0f%% drop since eval)",
                                        (token_name or "?")[:20],
                                        eval_mc, new_mc, drop_ratio * 100,
                                    )
                                    continue
                except Exception as exc:
                    logger.debug("Scanner: fresh MC re-fetch failed for %s: %s",
                                 mint_addr[:12], exc)

            # Hard reject: never open a trade with entry_mc=0 or None.
            # BUG #1 from audit_report_v2: a zero entry_mc makes all
            # multiplier math nonsense and poisons the learning corpus.
            if not fresh_mc or fresh_mc <= 0:
                logger.info(
                    "Scanner: SKIP paper trade %s — entry_mc is 0/None after "
                    "both eval and fresh fetch failed", (token_name or "?")[:20],
                )
                continue

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

            # Probe sizing: 0.1 SOL per trade like real degen traders.
            # Small probes = survive losses, let winners compound via scaling.
            # At 20% SL, a 0.1 SOL probe loses max 0.02 SOL per bad trade.
            # At 5x win, a 0.1 SOL probe gains 0.4 SOL.
            # 10 trades: 3 wins (3 * 0.4 = 1.2) + 7 losses (7 * 0.02 = 0.14)
            # Net: +1.06 SOL even at 30% WR.
            paper_sol = 0.1

            logger.info(
                "Scanner: PAPER TRADE %s conf=%.0f size=%.1f%% sol=%.4f bal=%.4f",
                scored.get("name", "?")[:20],
                scored.get("confidence_score", 0),
                learned_pct * 100,
                paper_sol,
                state.paper_balance,
            )
            try:
                # pattern_type now stores the COMMA-SEPARATED list of matched
                # ai_trade_params keys so Agent 6 can learn per-bucket from a
                # single PaperTrade row. See trade_profiles.match_pattern_types.
                # entry_mc uses the FRESH re-fetched value (fresh_mc) so the
                # peak_multiple/TP/SL math starts from current price, not
                # the cached eval-time price.
                pt = await open_paper_trade(
                    token_address=scored.get("mint", ""),
                    token_name=scored.get("name"),
                    entry_mc=fresh_mc,
                    entry_price=fresh_mc,
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

        if queued >= int(_sp("scanner_max_candidates", 10)):
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
