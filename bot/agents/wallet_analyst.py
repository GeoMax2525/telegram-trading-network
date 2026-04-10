"""
wallet_analyst.py — Agent 2: Wallet Analyst

Runs every hour as a background task.

Uses Helius RPC + Enhanced API to build real trade histories:
  1. Starts from winning tokens (peak_multiplier >= 2) to find wallets
  2. For each wallet, fetches full transaction history via Helius
  3. Identifies token buy/sell pairs from transfer data
  4. Calculates real win rate, avg multiple, early entry rate
  5. Scores and tiers wallets based on actual performance

Scoring formula (0–100):
  real_win_rate × 35
  + avg_multiple/10 × 25  (capped at 10x)
  + early_entry_rate × 20  (% of entries under $200K MC)
  + consistency × 10  (total_trades >= 10)
  + volume × 10  (wins >= 5)

Tiers: 1 (80+), 2 (60-79), 3 (40-59), 0 (below 40, ignored)
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime

import aiohttp

from bot.config import HELIUS_RPC_URL, HELIUS_API_KEY
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    get_last_agent_run, log_agent_run,
    get_winning_scans, upsert_wallet,
)

logger = logging.getLogger(__name__)

HELIUS_ENHANCED_URL = "https://api.helius.xyz/v0/transactions"
POLL_INTERVAL       = 1800   # 30 minutes (chaos mode: was 1 hour)
STARTUP_DELAY       = 90     # seconds after bot start
MAX_WALLETS_PER_RUN = 30     # cap to avoid rate limits
EARLY_MC_THRESHOLD  = 200_000  # $200K MC = early entry


# ── Helius helpers ────────────────────────────────────────────────────────────

async def _get_signatures(address: str, limit: int = 200) -> list[dict]:
    """
    getSignaturesForAddress via Helius RPC.
    Works for both token mints (to find early buyers) and wallet addresses
    (to build trade history).
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignaturesForAddress",
        "params": [address, {"limit": limit, "commitment": "confirmed"}],
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
        logger.debug("getSignaturesForAddress failed for %s: %s", address[:12], exc)
        return []


async def _parse_transactions(signatures: list[str]) -> list[dict]:
    """
    Helius Enhanced Transactions API — batch parse up to 100 signatures.
    Returns enriched transaction objects with tokenTransfers, type, etc.
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
                    return []
                data = await resp.json(content_type=None)
                return data if isinstance(data, list) else []
    except Exception as exc:
        logger.debug("parse_transactions failed: %s", exc)
        return []


# ── Early buyer detection ────────────────────────────────────────────────────

async def _get_early_buyers(mint: str, window_minutes: int = 10) -> list[str]:
    """
    Returns deduplicated wallet addresses that transacted on this token
    within the first `window_minutes` after launch.
    """
    sigs = await _get_signatures(mint, limit=500)
    if not sigs:
        return []

    valid = [s for s in sigs if s.get("blockTime") and not s.get("err")]
    if not valid:
        return []

    launch_time = min(s["blockTime"] for s in valid)
    cutoff = launch_time + window_minutes * 60

    early_sigs = [s["signature"] for s in valid if s["blockTime"] <= cutoff]
    if not early_sigs:
        return []

    parsed = await _parse_transactions(early_sigs[:100])

    buyers: set[str] = set()
    for tx in parsed:
        fp = tx.get("feePayer")
        if fp:
            buyers.add(fp)

    return list(buyers)


# ── Real trade history for a wallet ──────────────────────────────────────────

async def _analyze_wallet_trades(wallet_address: str) -> dict:
    """
    Fetches a wallet's recent transaction history from Helius and
    builds real trade stats by tracking token buy/sell pairs.

    Returns:
        {
            "wins": int,
            "losses": int,
            "total_trades": int,
            "multiples": [float, ...],
            "entry_mcs": [float, ...],
            "early_entries": int,
        }
    """
    sigs_data = await _get_signatures(wallet_address, limit=200)
    if not sigs_data:
        return {"wins": 0, "losses": 0, "total_trades": 0,
                "multiples": [], "entry_mcs": [], "early_entries": 0}

    valid_sigs = [
        s["signature"] for s in sigs_data
        if s.get("blockTime") and not s.get("err")
    ]
    if not valid_sigs:
        return {"wins": 0, "losses": 0, "total_trades": 0,
                "multiples": [], "entry_mcs": [], "early_entries": 0}

    # Parse transactions in batches of 100
    all_parsed = []
    for i in range(0, min(len(valid_sigs), 200), 100):
        batch = valid_sigs[i:i + 100]
        parsed = await _parse_transactions(batch)
        all_parsed.extend(parsed)

    # Track token flows: mint -> {bought_amount, sold_amount, buy_time}
    token_flows: dict[str, dict] = defaultdict(
        lambda: {"bought_sol": 0.0, "sold_sol": 0.0, "buy_time": None}
    )

    for tx in all_parsed:
        transfers = tx.get("tokenTransfers") or []
        native_transfers = tx.get("nativeTransfers") or []

        # Calculate SOL spent/received in this tx
        sol_out = sum(
            abs(nt.get("amount", 0)) / 1e9
            for nt in native_transfers
            if nt.get("fromUserAccount") == wallet_address
        )
        sol_in = sum(
            abs(nt.get("amount", 0)) / 1e9
            for nt in native_transfers
            if nt.get("toUserAccount") == wallet_address
        )

        for transfer in transfers:
            mint = transfer.get("mint")
            if not mint:
                continue

            # Token received = buy
            if transfer.get("toUserAccount") == wallet_address:
                token_flows[mint]["bought_sol"] += sol_out
                if token_flows[mint]["buy_time"] is None:
                    token_flows[mint]["buy_time"] = tx.get("timestamp")

            # Token sent out = sell
            elif transfer.get("fromUserAccount") == wallet_address:
                token_flows[mint]["sold_sol"] += sol_in

    # Analyze completed trades (tokens that were both bought and sold)
    wins = 0
    losses = 0
    multiples = []
    entry_mcs = []
    early_entries = 0

    for mint, flow in token_flows.items():
        bought = flow["bought_sol"]
        sold = flow["sold_sol"]

        if bought <= 0.001:
            continue  # skip dust / airdrops

        # Only count as a trade if there was meaningful activity
        if sold > 0.001:
            # Completed trade
            multiple = sold / bought if bought > 0 else 0
            multiples.append(round(multiple, 2))

            if multiple >= 1.0:
                wins += 1
            else:
                losses += 1

            # Check entry MC
            pair = await fetch_token_data(mint)
            if pair:
                metrics = parse_token_metrics(pair)
                mc = metrics.get("market_cap", 0) or 0
                if mc > 0:
                    entry_mcs.append(mc)
                    if mc < EARLY_MC_THRESHOLD:
                        early_entries += 1

    total_trades = wins + losses

    return {
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "multiples": multiples,
        "entry_mcs": entry_mcs,
        "early_entries": early_entries,
    }


# ── Scoring ──────────────────────────────────────────────────────────────────

def _score_wallet(
    wins: int,
    losses: int,
    total_trades: int,
    avg_multiple: float,
    early_entry_rate: float,
) -> tuple[float, int]:
    """
    Scores a wallet 0–100 using real trade data.
    Returns (score, tier).
    """
    win_rate = wins / total_trades if total_trades > 0 else 0.0

    score  = win_rate * 35
    score += (min(avg_multiple, 10.0) / 10.0) * 25
    score += early_entry_rate * 20
    score += 10 if total_trades >= 10 else (5 if total_trades >= 5 else 0)
    score += 10 if wins >= 5 else (5 if wins >= 3 else 0)
    score  = round(score, 1)

    tier = (
        1 if score >= 60 else
        2 if score >= 40 else
        3 if score >= 20 else 0
    )
    return score, tier


# ── Main run ─────────────────────────────────────────────────────────────────

async def run_once() -> tuple[int, int]:
    """
    Single analyst tick.
    1. Find early buyers from winning tokens
    2. Analyze each wallet's full trade history via Helius
    3. Score and save
    Returns (wallets_analyzed, wallets_saved).
    """
    last_log = await get_last_agent_run("wallet_analyst")
    since = last_log.run_at if last_log else None

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

    # Step 1: Collect unique wallet addresses from early buyers
    all_wallets: set[str] = set()
    for scan in winning_scans:
        buyers = await _get_early_buyers(scan.contract_address)
        all_wallets.update(buyers)
        logger.info(
            "Wallet Analyst: %s → %d early buyers (peak %.1fx)",
            scan.token_name, len(buyers), scan.peak_multiplier or 0,
        )

    if not all_wallets:
        await log_agent_run(
            "wallet_analyst", tokens_found=len(winning_scans), tokens_saved=0,
            notes="no early buyers found",
        )
        return 0, 0

    # Cap wallets per run to avoid rate limits
    wallet_list = list(all_wallets)[:MAX_WALLETS_PER_RUN]
    logger.info("Wallet Analyst: analyzing %d wallets (capped at %d)",
                len(wallet_list), MAX_WALLETS_PER_RUN)

    # Step 2: Analyze each wallet's real trade history
    wallets_saved = 0
    for address in wallet_list:
        try:
            stats = await _analyze_wallet_trades(address)
        except Exception as exc:
            logger.warning("Wallet Analyst: failed to analyze %s: %s", address[:12], exc)
            continue

        total = stats["total_trades"]
        if total < 1:
            continue  # no completed trades found

        wins = stats["wins"]
        losses = stats["losses"]
        multiples = stats["multiples"]
        entry_mcs = stats["entry_mcs"]
        early_entries = stats["early_entries"]

        avg_mult = sum(multiples) / len(multiples) if multiples else 1.0
        avg_entry = sum(entry_mcs) / len(entry_mcs) if entry_mcs else None
        win_rate = wins / total if total > 0 else 0.0
        early_rate = early_entries / total if total > 0 else 0.0

        score, tier = _score_wallet(wins, losses, total, avg_mult, early_rate)
        if tier == 0:
            continue  # below threshold

        await upsert_wallet(
            address=address,
            score=score,
            tier=tier,
            win_rate=round(win_rate, 4),
            avg_multiple=round(avg_mult, 2),
            wins=wins,
            losses=losses,
            total_trades=total,
            avg_entry_mcap=avg_entry,
        )
        wallets_saved += 1

        logger.info(
            "Wallet Analyst: %s..%s score=%.0f tier=%d wr=%.0f%% avg=%.1fx "
            "trades=%d early=%.0f%%",
            address[:4], address[-4:], score, tier,
            win_rate * 100, avg_mult, total, early_rate * 100,
        )

    await log_agent_run(
        "wallet_analyst",
        tokens_found=len(winning_scans),
        tokens_saved=wallets_saved,
        notes=f"{len(all_wallets)} unique wallets, {wallets_saved} scored via Helius",
    )

    logger.info(
        "Wallet Analyst: done — tokens=%d wallets=%d saved=%d",
        len(winning_scans), len(all_wallets), wallets_saved,
    )
    return len(all_wallets), wallets_saved


# ── Background loop ──────────────────────────────────────────────────────────

async def wallet_analyst_loop() -> None:
    """Runs the wallet analyst once per hour."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("Wallet Analyst agent started — running every %ds (Helius enrichment)", POLL_INTERVAL)
    while True:
        try:
            await run_once()
        except Exception as exc:
            logger.error("Wallet Analyst loop error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)
