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

from bot.config import HELIUS_API_KEY
from bot.helius import (
    get_address_transactions,
    get_address_transactions_all,
    get_signatures_for_address,
    parse_transactions,
    get_multiple_address_transactions,
)
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    get_last_agent_run, log_agent_run,
    get_winning_scans, upsert_wallet, get_params,
    upsert_wallet_token_trade, get_wallet_token_trades,
    upsert_wallet_cluster, get_all_wallet_clusters,
    AsyncSessionLocal, select, Wallet, WalletTokenTrade, Token,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL       = 1800   # 30 minutes (Business plan: 100M credits)
STARTUP_DELAY       = 90     # seconds after bot start
MAX_WALLETS_PER_RUN = 100    # Business plan: 200 req/s, 100M credits
EARLY_MC_THRESHOLD  = 200_000  # $200K MC = early entry


# ── Helius helpers ────────────────────────────────────────────────────────────
# All Helius calls now go through bot.helius shared client with:
#   - Connection pooling (persistent session)
#   - Exponential backoff retry on 429/5xx
#   - Rate-limit aware semaphore (40 concurrent, under 50 req/s cap)
#   - Proper error logging (no more silent failures)

async def _get_signatures(address: str, limit: int = 200) -> list[dict]:
    """getSignaturesForAddress via shared Helius client."""
    return await get_signatures_for_address(address, limit=limit, label=f"wa_sigs:{address[:8]}")


async def _parse_transactions(signatures: list[str]) -> list[dict]:
    """Batch parse signatures via shared Helius client."""
    return await parse_transactions(signatures, label="wa_parse_tx")


# ── Early buyer detection ────────────────────────────────────────────────────

async def _fetch_sigs_with_retry(address: str, limit: int = 200, retries: int = 3) -> list[dict]:
    """Fetch signatures — retry is handled by shared Helius client."""
    sigs = await _get_signatures(address, limit=limit)
    if not sigs:
        logger.info("Early buyers: 0 sigs for %s after retries", address[:12])
    return sigs


async def _get_pair_address(mint: str) -> str | None:
    """Get the DexScreener pair address for a token mint."""
    from bot.scanner import fetch_token_data
    pair = await fetch_token_data(mint)
    if pair:
        return pair.get("pairAddress")
    return None


async def _fetch_dexscreener_traders(pair_address: str, created_at_ms: int | None, window_minutes: int) -> list[str]:
    """
    Fallback: fetch early traders from DexScreener trades endpoint.
    Returns list of wallet addresses (makers) from early trades.
    """
    url = f"https://api.dexscreener.com/latest/dex/trades/solana/{pair_address}"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("DexScreener trades fetch failed: %s", exc)
        return []

    trades = data.get("trades") or (data if isinstance(data, list) else [])
    if not trades:
        return []

    wallets: set[str] = set()
    if created_at_ms:
        cutoff_ms = created_at_ms + window_minutes * 60_000
        for t in trades:
            ts = t.get("blockTimestamp") or t.get("timestamp") or 0
            maker = t.get("maker") or t.get("wallet")
            if maker and ts <= cutoff_ms:
                wallets.add(maker)
    else:
        # No creation time — just take first 20 unique makers
        for t in trades[:50]:
            maker = t.get("maker") or t.get("wallet")
            if maker:
                wallets.add(maker)
            if len(wallets) >= 20:
                break

    return list(wallets)


async def _get_early_buyers(
    mint: str,
    window_minutes: int = 10,
    created_at_ms: int | None = None,
) -> list[str]:
    """
    Returns deduplicated wallet addresses that bought this token
    within the first window_minutes after launch.

    Strategy (Enhanced API first, then fallbacks):
    1. Try Enhanced Transactions API on token mint (richest data)
    2. If 0 results, try getSignaturesForAddress + parse (wider net)
    3. If 0 results, try on the pair/pool address
    4. If still 0, fallback to DexScreener trades endpoint
    """
    buyers: set[str] = set()

    # Determine launch time for the window filter
    launch_time: int | None = None
    if created_at_ms:
        launch_time = created_at_ms // 1000

    # ── Attempt 1: Enhanced Transactions API on token mint ───────────
    # This returns pre-parsed enriched data — much better than raw sigs
    enhanced_txns = await get_address_transactions(
        mint, limit=100, label=f"eb_enhanced:{mint[:8]}",
    )
    if enhanced_txns:
        if launch_time is None:
            timestamps = [t.get("timestamp", 0) for t in enhanced_txns if t.get("timestamp")]
            if timestamps:
                launch_time = min(timestamps)

        if launch_time:
            cutoff = launch_time + window_minutes * 60
            for tx in enhanced_txns:
                tx_ts = tx.get("timestamp", 0)
                if tx_ts and tx_ts <= cutoff:
                    fp = tx.get("feePayer")
                    if fp:
                        buyers.add(fp)
                    for transfer in (tx.get("tokenTransfers") or []):
                        to_addr = transfer.get("toUserAccount")
                        if to_addr and transfer.get("mint") == mint:
                            buyers.add(to_addr)
            logger.info(
                "Early buyers %s: Enhanced API → %d txns, launch=%d, found %d wallets",
                mint[:12], len(enhanced_txns), launch_time, len(buyers),
            )

    # ── Attempt 2: getSignaturesForAddress + parse ───────────────────
    if not buyers:
        all_sigs = await _fetch_sigs_with_retry(mint, limit=500)
        if all_sigs:
            valid = [s for s in all_sigs if s.get("blockTime") and not s.get("err")]
            if valid:
                if launch_time is None:
                    launch_time = min(s["blockTime"] for s in valid)
                cutoff = launch_time + window_minutes * 60
                early_sigs = [s["signature"] for s in valid if s["blockTime"] <= cutoff]
                logger.info(
                    "Early buyers %s: %d total sigs, launch=%d, early=%d",
                    mint[:12], len(valid), launch_time, len(early_sigs),
                )
                if early_sigs:
                    parsed = await _parse_transactions(early_sigs[:100])
                    for tx in parsed:
                        fp = tx.get("feePayer")
                        if fp:
                            buyers.add(fp)
                        for transfer in (tx.get("tokenTransfers") or []):
                            to_addr = transfer.get("toUserAccount")
                            if to_addr and transfer.get("mint") == mint:
                                buyers.add(to_addr)

    # ── Attempt 3: try pair/pool address ─────────────────────────────
    if not buyers:
        pair_addr = await _get_pair_address(mint)
        if pair_addr:
            logger.info("Early buyers: trying pair address %s", pair_addr[:12])
            pair_txns = await get_address_transactions(
                pair_addr, limit=100, label=f"eb_pair:{pair_addr[:8]}",
            )
            if pair_txns and launch_time:
                cutoff = launch_time + window_minutes * 60
                for tx in pair_txns:
                    tx_ts = tx.get("timestamp", 0)
                    if tx_ts and tx_ts <= cutoff:
                        fp = tx.get("feePayer")
                        if fp:
                            buyers.add(fp)
                        for transfer in (tx.get("tokenTransfers") or []):
                            to_addr = transfer.get("toUserAccount")
                            if to_addr and transfer.get("mint") == mint:
                                buyers.add(to_addr)

    # ── Attempt 4: DexScreener trades fallback ───────────────────────
    if not buyers:
        pair_addr = await _get_pair_address(mint)
        if pair_addr:
            logger.info("Early buyers: trying DexScreener trades for %s", pair_addr[:12])
            dex_buyers = await _fetch_dexscreener_traders(pair_addr, created_at_ms, window_minutes)
            buyers.update(dex_buyers)

    logger.info("Early buyers %s: found %d wallets", mint[:12], len(buyers))
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
            "per_token": [
                {
                    "mint": str,
                    "first_buy_at": datetime | None,
                    "last_sell_at": datetime | None,
                    "bought_sol": float,
                    "sold_sol": float,
                    "multiple": float | None,
                    "entry_mcap": float | None,
                }, ...
            ]
        }
    """
    empty = {"wins": 0, "losses": 0, "total_trades": 0,
             "multiples": [], "entry_mcs": [], "early_entries": 0,
             "per_token": []}

    # Enhanced Transactions API for ALL wallets (Business plan: 100M credits)
    all_parsed = await get_address_transactions_all(
        wallet_address, max_txns=500, label=f"wa_trades:{wallet_address[:8]}",
    )
    if not all_parsed:
        logger.info("Wallet trades %s: 0 transactions", wallet_address[:12])
        return empty

    # Track token flows: mint -> {bought_sol, sold_sol, first_buy_ts, last_sell_ts}
    token_flows: dict[str, dict] = defaultdict(
        lambda: {
            "bought_sol": 0.0, "sold_sol": 0.0,
            "first_buy_ts": None, "last_sell_ts": None,
        }
    )

    for tx in all_parsed:
        transfers = tx.get("tokenTransfers") or []
        native_transfers = tx.get("nativeTransfers") or []
        tx_ts = tx.get("timestamp")  # seconds epoch

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

            flow = token_flows[mint]
            # Token received = buy
            if transfer.get("toUserAccount") == wallet_address:
                flow["bought_sol"] += sol_out
                if tx_ts and (flow["first_buy_ts"] is None or tx_ts < flow["first_buy_ts"]):
                    flow["first_buy_ts"] = tx_ts
            # Token sent out = sell
            elif transfer.get("fromUserAccount") == wallet_address:
                flow["sold_sol"] += sol_in
                if tx_ts and (flow["last_sell_ts"] is None or tx_ts > flow["last_sell_ts"]):
                    flow["last_sell_ts"] = tx_ts

    # Analyze completed trades + build per-token records
    wins = 0
    losses = 0
    multiples = []
    entry_mcs = []
    early_entries = 0
    per_token: list[dict] = []

    for mint, flow in token_flows.items():
        bought = flow["bought_sol"]
        sold = flow["sold_sol"]

        if bought <= 0.001:
            continue  # skip dust / airdrops

        first_buy_at = datetime.utcfromtimestamp(flow["first_buy_ts"]) if flow["first_buy_ts"] else None
        last_sell_at = datetime.utcfromtimestamp(flow["last_sell_ts"]) if flow["last_sell_ts"] else None

        # Check entry MC from DexScreener
        entry_mcap = None
        pair = await fetch_token_data(mint)
        if pair:
            metrics = parse_token_metrics(pair)
            mc = metrics.get("market_cap", 0) or 0
            if mc > 0:
                entry_mcap = mc

        record = {
            "mint": mint,
            "first_buy_at": first_buy_at,
            "last_sell_at": last_sell_at,
            "bought_sol": round(bought, 6),
            "sold_sol": round(sold, 6),
            "multiple": None,
            "entry_mcap": entry_mcap,
        }

        # Only count as a completed trade if there was meaningful sell volume
        if sold > 0.001:
            multiple = sold / bought if bought > 0 else 0
            record["multiple"] = round(multiple, 2)
            multiples.append(round(multiple, 2))

            if multiple >= 1.0:
                wins += 1
            else:
                losses += 1

            if entry_mcap:
                entry_mcs.append(entry_mcap)
                if entry_mcap < EARLY_MC_THRESHOLD:
                    early_entries += 1

        per_token.append(record)

    total_trades = wins + losses

    return {
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "multiples": multiples,
        "entry_mcs": entry_mcs,
        "early_entries": early_entries,
        "per_token": per_token,
    }


# ── Scoring ──────────────────────────────────────────────────────────────────

def _score_wallet_raw(
    wins: int,
    losses: int,
    total_trades: int,
    avg_multiple: float,
    early_entry_rate: float,
) -> float:
    """
    Compute the 0-100 base score from trade stats. Pure function — the
    tier assignment uses DB-driven thresholds and lives in _score_wallet.
    """
    win_rate = wins / total_trades if total_trades > 0 else 0.0

    score = win_rate * 40

    if avg_multiple >= 5.0:
        score += 25
    elif avg_multiple >= 3.0:
        score += 18
    elif avg_multiple >= 2.0:
        score += 12
    elif avg_multiple >= 1.0:
        score += 6

    score += early_entry_rate * 15

    if total_trades >= 10:
        score += 10
    elif total_trades >= 5:
        score += 8
    elif total_trades >= 3:
        score += 6
    elif total_trades >= 1:
        score += 3

    if wins >= 5:
        score += 10
    elif wins >= 3:
        score += 6
    elif wins >= 1:
        score += 3

    return round(score, 1)


async def _score_wallet(
    wins: int,
    losses: int,
    total_trades: int,
    avg_multiple: float,
    early_entry_rate: float,
    score_bonus: float = 0.0,
) -> tuple[float, int]:
    """
    Scores a wallet 0–100 and assigns tier 1/2/3/0 using thresholds
    from agent_params. score_bonus is added AFTER the base score
    (classification bonuses from _classify_wallet).

    Returns (final_score, tier).
    """
    base = _score_wallet_raw(wins, losses, total_trades, avg_multiple, early_entry_rate)
    score = round(min(100.0, base + float(score_bonus)), 1)

    win_rate = wins / total_trades if total_trades > 0 else 0.0

    # Thresholds from agent_params (user-specified realistic values)
    p = await get_params(
        "tier1_min_wr", "tier1_min_mult", "tier1_min_trades", "tier1_min_score",
        "tier2_min_wr", "tier2_min_mult", "tier2_min_trades", "tier2_min_score",
        "tier3_min_wr", "tier3_min_trades", "tier3_min_score",
    )

    if (
        score      >= p["tier1_min_score"]
        and win_rate >= p["tier1_min_wr"]
        and avg_multiple >= p["tier1_min_mult"]
        and total_trades >= p["tier1_min_trades"]
    ):
        tier = 1
    elif (
        score      >= p["tier2_min_score"]
        and win_rate >= p["tier2_min_wr"]
        and avg_multiple >= p["tier2_min_mult"]
        and total_trades >= p["tier2_min_trades"]
    ):
        tier = 2
    elif (
        score      >= p["tier3_min_score"]
        and win_rate >= p["tier3_min_wr"]
        and total_trades >= p["tier3_min_trades"]
    ):
        tier = 3
    else:
        tier = 0

    return score, tier


# ── Classification thresholds ────────────────────────────────────────────────

EARLY_INSIDER_MIN_TOKENS      = 3        # 3+ tokens matching
EARLY_INSIDER_MIN_MULTIPLE    = 2.0      # peak >= 2x
EARLY_INSIDER_MAX_ENTRY_MCAP  = 200_000  # entry mcap <= $200K
EARLY_INSIDER_MAX_ENTRY_DELAY = 600      # seconds (10 minutes) after launch

SNIPER_BUY_WINDOW_SEC         = 60       # buy within 60s of launch
SNIPER_MIN_HOLD_SEC           = 1800     # held 30+ min
SNIPER_MIN_TOKENS             = 3

COORDINATED_MIN_SHARED_TOKENS = 3        # share 3+ tokens
COORDINATED_COTIMING_WINDOW_SEC = 300    # buy within 5 min of each other

EARLY_INSIDER_BONUS     = 10
COORDINATED_BONUS       = 15
SNIPER_HOLDER_BONUS     = 10


async def _classify_wallet(wallet_address: str) -> tuple[str, float]:
    """
    Return (wallet_type, score_bonus) based on persisted wallet_token_trades.

    Priority (strongest first, highest bonus wins on ties):
      coordinated_group (+15)  — handled by _detect_clusters, passthrough
      early_insider     (+10)  — 3+ tokens ≥2x peak, entry_mcap ≤ $200K,
                                 entered within 10 min of first_seen_at
      sniper_holder     (+10)  — 3+ tokens bought within 60s of launch
                                 AND held 30+ min
      gmgn_smart        (0)    — source == "gmgn"
      unknown           (0)    — nothing matched

    Cluster assignment happens separately in _detect_clusters after all
    wallets have been analyzed this run. This function does NOT override
    a pre-existing coordinated_group classification.
    """
    trades = await get_wallet_token_trades(wallet_address)

    # Check existing row — if already in a cluster, keep it
    async with AsyncSessionLocal() as session:
        existing = (await session.execute(
            select(Wallet).where(Wallet.address == wallet_address)
        )).scalar_one_or_none()
    if existing and existing.cluster_id:
        return "coordinated_group", COORDINATED_BONUS

    # Early-insider check
    early_insider_hits = 0
    for t in trades:
        if t.multiple is None or t.multiple < EARLY_INSIDER_MIN_MULTIPLE:
            continue
        if t.entry_mcap is None or t.entry_mcap > EARLY_INSIDER_MAX_ENTRY_MCAP:
            continue
        # Entry within 10 min of launch — delay = first_buy - launch
        # Negative delta means wallet bought BEFORE we first saw the token
        # (they're definitely "first in") so treat as 0.
        if t.first_buy_at is None:
            continue
        if t.token_launch_at is None:
            # No launch time from Token table — assume this wallet WAS early
            # since it bought a token that went 2x+. Better to over-classify
            # than miss every insider because of missing metadata.
            delay = 0
        else:
            delay = (t.first_buy_at - t.token_launch_at).total_seconds()
        if delay > EARLY_INSIDER_MAX_ENTRY_DELAY:
            continue
        early_insider_hits += 1

    if early_insider_hits >= EARLY_INSIDER_MIN_TOKENS:
        return "early_insider", EARLY_INSIDER_BONUS

    # Sniper-holder check
    sniper_hits = 0
    for t in trades:
        if t.first_buy_at is None:
            continue
        if t.token_launch_at is None:
            # No launch time — can't verify sniper timing, skip
            continue
        delay = (t.first_buy_at - t.token_launch_at).total_seconds()
        if delay > SNIPER_BUY_WINDOW_SEC:
            continue
        # Hold duration = last_sell_at - first_buy_at, or "still holding"
        if t.last_sell_at is None:
            # Still holding — count as held long enough if older than threshold
            held_sec = (datetime.utcnow() - t.first_buy_at).total_seconds()
        else:
            held_sec = (t.last_sell_at - t.first_buy_at).total_seconds()
        if held_sec >= SNIPER_MIN_HOLD_SEC:
            sniper_hits += 1

    if sniper_hits >= SNIPER_MIN_TOKENS:
        return "sniper_holder", SNIPER_HOLDER_BONUS

    # GMGN fallback
    if existing and existing.source == "gmgn":
        return "gmgn_smart", 0.0

    return "unknown", 0.0


async def _detect_clusters(wallet_addresses: list[str]) -> int:
    """
    Find wallets that co-timed their buys: a pair of wallets is
    coordinated if they share >= COORDINATED_MIN_SHARED_TOKENS tokens
    where their first_buy_at timestamps were within COORDINATED_COTIMING_WINDOW_SEC
    of each other. Assigns the shared cluster_id to all matching wallets
    and upserts a WalletCluster row. Returns the number of clusters
    created/updated this run.
    """
    if len(wallet_addresses) < 2:
        return 0

    # Load all trades for these wallets, grouped by wallet
    wallet_trades: dict[str, list] = {}
    for addr in wallet_addresses:
        wallet_trades[addr] = await get_wallet_token_trades(addr)

    # Build per-token list: token → [(wallet, first_buy_at), ...]
    token_buyers: dict[str, list] = {}
    for addr, trades in wallet_trades.items():
        for t in trades:
            if t.first_buy_at is None:
                continue
            token_buyers.setdefault(t.token_address, []).append((addr, t.first_buy_at))

    # For each token, find pairs of buyers within 5 min
    # Build adjacency: pair (a, b) → count of co-timing tokens
    from collections import defaultdict as _dd
    pair_counts: dict[tuple[str, str], int] = _dd(int)
    for mint, buyers in token_buyers.items():
        if len(buyers) < 2:
            continue
        # Sort by buy time
        buyers.sort(key=lambda x: x[1])
        for i in range(len(buyers)):
            for j in range(i + 1, len(buyers)):
                a_addr, a_time = buyers[i]
                b_addr, b_time = buyers[j]
                if a_addr == b_addr:
                    continue
                delta = (b_time - a_time).total_seconds()
                if delta > COORDINATED_COTIMING_WINDOW_SEC:
                    break  # later buyers will be even further
                pair = tuple(sorted((a_addr, b_addr)))
                pair_counts[pair] += 1

    # Build clusters via union-find on pairs that hit the threshold
    parent: dict[str, str] = {}

    def _find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def _union(a: str, b: str) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[ra] = rb

    qualified_pairs = 0
    for (a, b), count in pair_counts.items():
        if count >= COORDINATED_MIN_SHARED_TOKENS:
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            _union(a, b)
            qualified_pairs += 1

    if not parent:
        return 0

    # Group wallets by their union-find root
    groups: dict[str, list[str]] = {}
    for addr in list(parent.keys()):
        root = _find(addr)
        groups.setdefault(root, []).append(addr)

    # Assign cluster_ids — stable, hashed from the sorted member list
    import hashlib as _hashlib
    clusters_updated = 0
    for root, members in groups.items():
        if len(members) < 2:
            continue
        members_sorted = sorted(set(members))
        cid_raw = ",".join(members_sorted)
        cid = "cluster_" + _hashlib.md5(cid_raw.encode()).hexdigest()[:8]

        # Aggregate stats across the cluster's members
        total_wins = 0
        total_losses = 0
        total_trades_all = 0
        multiples_all = []
        last_active = None
        for addr in members_sorted:
            async with AsyncSessionLocal() as session:
                w = (await session.execute(
                    select(Wallet).where(Wallet.address == addr)
                )).scalar_one_or_none()
            if w is None:
                continue
            total_wins += w.wins
            total_losses += w.losses
            total_trades_all += w.total_trades
            multiples_all.append(w.avg_multiple)
            if w.last_updated_at and (last_active is None or w.last_updated_at > last_active):
                last_active = w.last_updated_at

        cwr = total_wins / total_trades_all if total_trades_all > 0 else 0.0
        cmult = sum(multiples_all) / len(multiples_all) if multiples_all else 1.0

        await upsert_wallet_cluster(
            cluster_id=cid,
            wallet_addresses=members_sorted,
            wins=total_wins,
            losses=total_losses,
            win_rate=round(cwr, 4),
            avg_multiple=round(cmult, 2),
            last_active=last_active,
        )

        # Tag each member wallet with the cluster_id + upgrade type
        for addr in members_sorted:
            async with AsyncSessionLocal() as session:
                w = (await session.execute(
                    select(Wallet).where(Wallet.address == addr)
                )).scalar_one_or_none()
                if w is None:
                    continue
                old_type = w.wallet_type or "unknown"
                old_cid = w.cluster_id
                w.cluster_id = cid
                # Upgrade type to coordinated_group unless already stronger
                if old_type in ("unknown", "gmgn_smart", "sniper_holder", "early_insider"):
                    w.wallet_type = "coordinated_group"
                    if old_type != "coordinated_group":
                        logger.info(
                            "Wallet %s..%s upgraded: %s → coordinated_group [%s]",
                            addr[:4], addr[-4:], old_type, cid,
                        )
                # Re-score with cluster bonus
                try:
                    score, tier = await _score_wallet(
                        wins=w.wins, losses=w.losses, total_trades=w.total_trades,
                        avg_multiple=w.avg_multiple, early_entry_rate=0.0,
                        score_bonus=COORDINATED_BONUS,
                    )
                    w.score = score
                    w.tier = tier
                except Exception:
                    pass
                w.last_updated_at = datetime.utcnow()
                await session.commit()

        clusters_updated += 1

    return clusters_updated


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

    # Step 1: Collect unique wallet addresses from early buyers (concurrent)
    all_wallets: set[str] = set()

    async def _scan_buyers(scan):
        buyers = await _get_early_buyers(scan.contract_address)
        logger.info(
            "Wallet Analyst: %s → %d early buyers (peak %.1fx)",
            scan.token_name, len(buyers), scan.peak_multiplier or 0,
        )
        return buyers

    buyer_results = await asyncio.gather(
        *[_scan_buyers(s) for s in winning_scans],
        return_exceptions=True,
    )
    for res in buyer_results:
        if isinstance(res, list):
            all_wallets.update(res)
        elif isinstance(res, Exception):
            logger.warning("Wallet Analyst: early buyer scan failed: %s", res)

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

    # Step 2: Analyze each wallet's real trade history (concurrent batches)
    wallets_saved = 0
    analyze_semaphore = asyncio.Semaphore(25)  # Business plan: 200 req/s

    async def _analyze_and_save(address: str) -> bool:
        """Analyze a single wallet and save if it passes scoring. Returns True if saved."""
        async with analyze_semaphore:
            try:
                stats = await _analyze_wallet_trades(address)
            except Exception as exc:
                logger.warning("Wallet Analyst: failed to analyze %s: %s", address[:12], exc)
                return False

            total = stats["total_trades"]
            if total < 1:
                return False

            wins = stats["wins"]
            losses = stats["losses"]
            multiples = stats["multiples"]
            entry_mcs = stats["entry_mcs"]
            early_entries = stats["early_entries"]

            avg_mult = sum(multiples) / len(multiples) if multiples else 1.0
            avg_entry = sum(entry_mcs) / len(entry_mcs) if entry_mcs else None
            win_rate = wins / total if total > 0 else 0.0
            early_rate = early_entries / total if total > 0 else 0.0

            # Persist per-token records
            for rec in stats.get("per_token", []):
                mint = rec["mint"]
                token_launch_at = None
                try:
                    async with AsyncSessionLocal() as s:
                        tok = (await s.execute(
                            select(Token).where(Token.mint == mint)
                        )).scalar_one_or_none()
                        if tok and tok.first_seen_at:
                            token_launch_at = tok.first_seen_at
                except Exception as exc:
                    logger.warning("Wallet analyst: token lookup failed for %s: %s", mint[:12], exc)
                try:
                    await upsert_wallet_token_trade(
                        wallet_address=address,
                        token_address=mint,
                        first_buy_at=rec["first_buy_at"],
                        last_sell_at=rec["last_sell_at"],
                        total_bought_sol=rec["bought_sol"],
                        total_sold_sol=rec["sold_sol"],
                        multiple=rec["multiple"],
                        entry_mcap=rec["entry_mcap"],
                        token_launch_at=token_launch_at,
                    )
                except Exception as exc:
                    logger.debug("persist wallet_token_trade %s/%s failed: %s",
                                 address[:12], mint[:12], exc)

            wallet_type, score_bonus = await _classify_wallet(address)

            score, tier = await _score_wallet(
                wins, losses, total, avg_mult, early_rate,
                score_bonus=score_bonus,
            )
            min_save = (await get_params("wallet_min_score_to_save"))["wallet_min_score_to_save"]
            if score < min_save:
                return False

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
                wallet_type=wallet_type,
            )

            logger.info(
                "Wallet Analyst: %s..%s score=%.0f tier=%d wr=%.0f%% avg=%.1fx "
                "trades=%d early=%.0f%%",
                address[:4], address[-4:], score, tier,
                win_rate * 100, avg_mult, total, early_rate * 100,
            )
            return True

    results = await asyncio.gather(
        *[_analyze_and_save(addr) for addr in wallet_list],
        return_exceptions=True,
    )
    for res in results:
        if res is True:
            wallets_saved += 1
        elif isinstance(res, Exception):
            logger.warning("Wallet Analyst: wallet analysis exception: %s", res)

    # Step 3: Cluster detection — cross-wallet co-timing. Runs over
    # every wallet in the Wallet table (not just this batch) so we
    # catch clusters that span multiple analysis runs.
    try:
        async with AsyncSessionLocal() as session:
            all_addrs = (await session.execute(
                select(Wallet.address)
            )).scalars().all()
        clusters_updated = await _detect_clusters(list(all_addrs))
    except Exception as exc:
        logger.error("cluster detection error: %s", exc)
        clusters_updated = 0

    await log_agent_run(
        "wallet_analyst",
        tokens_found=len(winning_scans),
        tokens_saved=wallets_saved,
        notes=(
            f"{len(all_wallets)} unique, {wallets_saved} scored, "
            f"{clusters_updated} clusters"
        ),
    )

    logger.info(
        "Wallet Analyst: done — tokens=%d wallets=%d saved=%d clusters=%d",
        len(winning_scans), len(all_wallets), wallets_saved, clusters_updated,
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
