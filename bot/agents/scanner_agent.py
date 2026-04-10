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
    get_params,
    compute_paper_balance,
)

logger = logging.getLogger(__name__)

PROFILES_URL    = "https://api.dexscreener.com/token-profiles/latest/v1"
RUGCHECK_URL    = "https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
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
    Returns list of {mint, source, insider_count}.
    """
    wallets = await get_tier_wallets(max_tier=2)
    if not wallets:
        return []

    since_ts = int(time.time()) - INSIDER_WINDOW
    mint_counts: dict[str, int] = {}

    # Check up to 15 wallets
    async def _check_wallet(wallet_addr: str) -> list[str]:
        sigs = await _get_wallet_sigs(wallet_addr, since_ts)
        if not sigs:
            return []
        parsed = await _parse_transactions(sigs)
        found_mints = []
        for tx in parsed:
            for transfer in (tx.get("tokenTransfers") or []):
                if transfer.get("toUserAccount") == wallet_addr:
                    mint = transfer.get("mint")
                    if mint:
                        found_mints.append(mint)
        return found_mints

    tasks = [_check_wallet(w.address) for w in wallets[:15]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for mints in results:
        if isinstance(mints, list):
            for m in mints:
                mint_counts[m] = mint_counts.get(m, 0) + 1

    candidates = []
    for mint, count in mint_counts.items():
        candidates.append({"mint": mint, "source": "insider_wallet", "insider_count": count})

    # Sort by most insiders first
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

    if not mcap or not liquidity:
        pair = await fetch_token_data(mint)
        if pair is None:
            logger.info("Scanner REJECTED %s (%s): DexScreener returned no data", name, mint[:12])
            return None
        metrics   = parse_token_metrics(pair)
        mcap      = metrics.get("market_cap",    0) or 0
        liquidity = metrics.get("liquidity_usd", 0) or 0
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
    }


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single scanner tick — runs ALWAYS regardless of autotrade status.
    Returns (candidates_found, candidates_queued).
    """
    state.scanner_status = "running"
    pattern = await get_pattern_by_type("winner_2x")

    # Gather raw candidates from all 3 sources concurrently
    s1, s2, s3 = await asyncio.gather(
        _source1_new_launches(),
        _source2_insider_wallets(),
        _source3_volume_spikes(),
        return_exceptions=True,
    )

    raw_candidates: list[dict] = []
    for result in (s1, s2, s3):
        if isinstance(result, list):
            raw_candidates.extend(result)
        elif isinstance(result, Exception):
            logger.warning("Scanner source error: %s", result)

    if not raw_candidates:
        state.scanner_status = "idle"
        await log_agent_run("scanner", tokens_found=0, tokens_saved=0, notes="no raw candidates")
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
            # Compute true balance from DB
            state.paper_balance = await compute_paper_balance(state.PAPER_STARTING_BALANCE)

            if state.paper_balance < 0.05:
                logger.info("Scanner: paper balance depleted (%.4f SOL), skipping", state.paper_balance)
                continue

            # Confidence-based position sizing from DB params (% of balance)
            sp = await get_params(
                "size_confidence_20", "size_confidence_50",
                "size_confidence_70", "size_confidence_80",
            )
            conf = scored.get("confidence_score", 0)
            if conf >= 80:
                size_pct = sp["size_confidence_80"] / 100.0
            elif conf >= 70:
                size_pct = sp["size_confidence_70"] / 100.0
            elif conf >= 50:
                size_pct = sp["size_confidence_50"] / 100.0
            else:
                size_pct = sp["size_confidence_20"] / 100.0

            paper_sol = round(min(state.paper_balance * size_pct, state.paper_balance * 0.20), 4)

            logger.info(
                "Scanner: PAPER TRADE %s conf=%.0f size=%.0f%% sol=%.4f bal=%.4f",
                scored.get("name", "?")[:20], conf, size_pct * 100,
                paper_sol, state.paper_balance,
            )
            try:
                pt = await open_paper_trade(
                    token_address=scored.get("mint", ""),
                    token_name=scored.get("name"),
                    entry_mc=scored.get("mcap"),
                    entry_price=scored.get("mcap"),
                    paper_sol=paper_sol,
                    confidence=scored.get("confidence_score", 0),
                    pattern_type=scored.get("chart_pattern") or scored.get("source"),
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
            f"sources: s1={len(s1) if isinstance(s1, list) else 0} "
            f"s2={len(s2) if isinstance(s2, list) else 0} "
            f"s3={len(s3) if isinstance(s3, list) else 0} "
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
