"""
wallet_analyst.py — Agent 2: Wallet Analyst

Runs every hour as a background task.

For each winning token (peak_multiplier >= 2) found since the last run:
  1. Fetches transaction signatures for the token mint via Helius RPC
  2. Identifies the earliest block time (= token launch)
  3. Filters to transactions within the first 10 minutes (early buyers)
  4. Parses those transactions via Helius Enhanced API to extract wallets
  5. Aggregates per-wallet stats across all winning tokens:
       wins, avg_multiple, avg_entry_mcap
  6. Scores each wallet:
       win_rate × 35
       + avg_multiple/10 × 25  (capped at 10x)
       + early_entry_bonus × 20  (avg_entry_mcap < $200K)
       + consistency_bonus × 10  (wins >= 3)
       + volume_bonus × 10       (wins >= 5)
  7. Assigns tier: 1 (80-100), 2 (60-79), 3 (40-59), ignored (<40)
  8. Upserts to Wallets table, logs run to AgentLogs
"""

import asyncio
import logging
from collections import defaultdict

import aiohttp

from bot.config import HELIUS_RPC_URL, HELIUS_API_KEY
from database.models import (
    get_last_agent_run, log_agent_run,
    get_winning_scans, upsert_wallet,
)

logger = logging.getLogger(__name__)

HELIUS_ENHANCED_URL = "https://api.helius.xyz/v0/transactions"
POLL_INTERVAL       = 3600   # 1 hour
STARTUP_DELAY       = 90     # seconds after bot start


# ── Helius helpers ────────────────────────────────────────────────────────────

async def _get_signatures(mint: str, limit: int = 500) -> list[dict]:
    """
    Standard JSON-RPC getSignaturesForAddress on the token's mint.
    Returns a list of signature objects with 'signature' and 'blockTime'.
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignaturesForAddress",
        "params": [mint, {"limit": limit, "commitment": "confirmed"}],
    }
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.post(
                HELIUS_RPC_URL, json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return data.get("result") or []
    except Exception as exc:
        logger.warning("getSignaturesForAddress failed for %s: %s", mint[:12], exc)
        return []


async def _parse_transactions(signatures: list[str]) -> list[dict]:
    """
    Helius Enhanced Transactions API — batch parse up to 100 signatures.
    Returns a list of enriched transaction objects with feePayer, type, etc.
    """
    if not signatures:
        return []
    url = f"{HELIUS_ENHANCED_URL}?api-key={HELIUS_API_KEY}"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            async with session.post(
                url,
                json={"transactions": signatures[:100]},
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        "Helius parse_transactions HTTP %d for %d sigs",
                        resp.status, len(signatures),
                    )
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("parse_transactions failed: %s", exc)
        return []


# ── Early buyer detection ─────────────────────────────────────────────────────

async def _get_early_buyers(mint: str, window_minutes: int = 10) -> list[str]:
    """
    Returns a deduplicated list of wallet addresses (fee payers) that
    transacted on this token's mint within the first `window_minutes`.
    """
    sigs = await _get_signatures(mint, limit=500)
    if not sigs:
        return []

    # Filter out failed transactions and those without a timestamp
    valid = [s for s in sigs if s.get("blockTime") and not s.get("err")]
    if not valid:
        return []

    # Oldest transaction ≈ token launch time
    launch_time = min(s["blockTime"] for s in valid)
    cutoff      = launch_time + window_minutes * 60

    early_sigs = [s["signature"] for s in valid if s["blockTime"] <= cutoff]
    if not early_sigs:
        return []

    logger.debug(
        "Token %s: %d total sigs, %d within first %dm",
        mint[:12], len(valid), len(early_sigs), window_minutes,
    )

    # Batch-parse with Helius enhanced API
    parsed = await _parse_transactions(early_sigs[:100])

    buyers: set[str] = set()
    for tx in parsed:
        fp = tx.get("feePayer")
        if fp:
            buyers.add(fp)

    return list(buyers)


# ── Scoring ───────────────────────────────────────────────────────────────────

def _score_wallet(
    wins: int,
    avg_multiple: float,
    avg_entry_mcap: float | None,
) -> tuple[float, int]:
    """
    Returns (score, tier).
    win_rate is 1.0 by definition (we only observe winning tokens).
    """
    win_rate = 1.0

    score  = win_rate * 35
    score += (min(avg_multiple, 10.0) / 10.0) * 25
    score += 20 if (avg_entry_mcap and avg_entry_mcap < 200_000) else 0
    score += 10 if wins >= 3 else 0
    score += 10 if wins >= 5 else 0
    score  = round(score, 1)

    tier = (
        1 if score >= 80 else
        2 if score >= 60 else
        3 if score >= 40 else 0
    )
    return score, tier


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single analyst tick.
    Returns (wallets_analyzed, wallets_saved).
    """
    last_log = await get_last_agent_run("wallet_analyst")
    since    = last_log.run_at if last_log else None

    winning_scans = await get_winning_scans(since=since)
    if not winning_scans:
        await log_agent_run(
            "wallet_analyst", tokens_found=0, tokens_saved=0,
            notes="no new winning tokens",
        )
        logger.info("Wallet Analyst: no new winning tokens since last run")
        return 0, 0

    logger.info(
        "Wallet Analyst: analyzing %d winning token(s)%s",
        len(winning_scans),
        f" since {since.strftime('%Y-%m-%d %H:%M')}" if since else "",
    )

    # Accumulate observations: wallet → {wins, multiples, entry_mcs}
    wallet_obs: dict[str, dict] = defaultdict(
        lambda: {"wins": 0, "multiples": [], "entry_mcs": []}
    )

    for scan in winning_scans:
        buyers = await _get_early_buyers(scan.contract_address)
        peak_x = scan.peak_multiplier or 2.0
        for buyer in buyers:
            wallet_obs[buyer]["wins"] += 1
            wallet_obs[buyer]["multiples"].append(peak_x)
            if scan.entry_price:
                wallet_obs[buyer]["entry_mcs"].append(scan.entry_price)
        logger.info(
            "Wallet Analyst: %s → %d early buyer(s) (peak %.1fx)",
            scan.token_name, len(buyers), peak_x,
        )

    # Score and save wallets
    wallets_saved = 0
    for address, obs in wallet_obs.items():
        wins       = obs["wins"]
        multiples  = obs["multiples"]
        entry_mcs  = obs["entry_mcs"]
        avg_mult   = sum(multiples) / len(multiples) if multiples else 1.0
        avg_entry  = sum(entry_mcs) / len(entry_mcs) if entry_mcs else None

        score, tier = _score_wallet(wins, avg_mult, avg_entry)
        if tier == 0:
            continue   # below threshold — skip

        await upsert_wallet(
            address=address,
            score=score,
            tier=tier,
            win_rate=1.0,
            avg_multiple=round(avg_mult, 2),
            wins=wins,
            losses=0,
            total_trades=wins,
            avg_entry_mcap=avg_entry,
        )
        wallets_saved += 1

    await log_agent_run(
        "wallet_analyst",
        tokens_found=len(winning_scans),
        tokens_saved=wallets_saved,
        notes=f"{len(wallet_obs)} unique wallets found",
    )

    logger.info(
        "Wallet Analyst: done — winning_tokens=%d unique_wallets=%d saved=%d",
        len(winning_scans), len(wallet_obs), wallets_saved,
    )
    return len(wallet_obs), wallets_saved


# ── Background loop ───────────────────────────────────────────────────────────

async def wallet_analyst_loop() -> None:
    """Runs the wallet analyst once per hour."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Wallet Analyst agent started — running every %ds", POLL_INTERVAL)
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Wallet Analyst loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
