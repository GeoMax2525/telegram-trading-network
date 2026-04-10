"""
mc_repair.py — Background MC repair task

Processes tokens with no market_cap in batches of 500 every 10 minutes.
For each token:
  1. Try DexScreener for current MC
  2. If found: save as market_cap + launch_mc, check for winner
  3. If not found: mark as dead

Runs automatically as background task.
"""

import asyncio
import logging

import aiohttp

from bot import state
from bot.config import HELIUS_RPC_URL
from bot.scanner import fetch_token_data, parse_token_metrics
from bot.agents.wallet_analyst import _get_early_buyers, _score_wallet
from database.models import (
    get_tokens_no_mc, count_tokens_no_mc,
    update_token_market_cap, mark_token_dead,
    upsert_wallet, upsert_pattern, get_tier_wallets,
    log_agent_run,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
POLL_INTERVAL = 600  # 10 minutes
STARTUP_DELAY = 180  # 3 minutes after boot


async def _repair_batch() -> dict:
    """Process one batch of tokens with no MC."""
    tokens = await get_tokens_no_mc(limit=BATCH_SIZE)
    if not tokens:
        return {"processed": 0}

    stats = {"processed": 0, "found": 0, "dead": 0, "winners": 0, "wallets": 0, "errors": 0}

    for tok in tokens:
        stats["processed"] += 1
        try:
            # Try DexScreener
            pair = await fetch_token_data(tok.mint)

            if pair:
                metrics = parse_token_metrics(pair)
                mc = metrics.get("market_cap", 0) or 0

                if mc > 0:
                    await update_token_market_cap(tok.mint, mc)
                    stats["found"] += 1

                    # Check if winner vs launch_mc
                    launch_mc = getattr(tok, "launch_mc", None) or mc
                    if launch_mc > 0:
                        multiple = mc / launch_mc
                        if multiple >= 2.0:
                            stats["winners"] += 1
                            name = tok.name or tok.symbol or tok.mint[:12]
                            created_at_ms = pair.get("pairCreatedAt")

                            # Extract early buyers
                            try:
                                early = await _get_early_buyers(
                                    tok.mint, window_minutes=10, created_at_ms=created_at_ms,
                                )
                                tier_wallets = await get_tier_wallets(max_tier=3)
                                known = {w.address for w in tier_wallets}
                                for wallet in early[:20]:
                                    if wallet in known:
                                        continue
                                    score, tier = _score_wallet(
                                        wins=1, losses=0, total_trades=1,
                                        avg_multiple=multiple, early_entry_rate=1.0,
                                    )
                                    if tier > 0:
                                        await upsert_wallet(
                                            address=wallet, score=score, tier=tier,
                                            win_rate=1.0, avg_multiple=round(multiple, 2),
                                            wins=1, losses=0, total_trades=1,
                                            avg_entry_mcap=launch_mc,
                                        )
                                        stats["wallets"] += 1
                            except Exception:
                                pass

                            # Save pattern
                            liq = metrics.get("liquidity_usd", 0) or 0
                            threshold = "10x" if multiple >= 10 else "5x" if multiple >= 5 else "2x"
                            await upsert_pattern(
                                pattern_type=f"winner_{threshold}",
                                outcome_threshold=threshold,
                                sample_count=stats["winners"],
                                avg_entry_mcap=launch_mc,
                                mcap_range_low=launch_mc * 0.5,
                                mcap_range_high=launch_mc * 2.0,
                                avg_liquidity=liq * 0.5 if liq else 5000,
                                min_liquidity=3000, avg_ai_score=70,
                                avg_rugcheck_score=5,
                                best_hours=None, best_days=None,
                                confidence_score=min(95, 50 + multiple * 5),
                            )
                            logger.info("MC repair WINNER: %s %.1fx mc=$%.0f", name, multiple, mc)
                else:
                    await mark_token_dead(tok.mint)
                    stats["dead"] += 1
            else:
                await mark_token_dead(tok.mint)
                stats["dead"] += 1

            await asyncio.sleep(0.3)  # rate limit

        except Exception as exc:
            stats["errors"] += 1
            logger.debug("MC repair error %s: %s", tok.mint[:12], exc)

    return stats


async def mc_repair_loop() -> None:
    """Background loop: repair MC for tokens with no market cap."""
    await asyncio.sleep(STARTUP_DELAY)

    remaining = await count_tokens_no_mc()
    if remaining == 0:
        logger.info("MC repair: no tokens need repair")
        return

    logger.info("MC repair started — %d tokens need MC data", remaining)
    state.backfill_progress = f"MC repair: {remaining} tokens remaining"

    total_processed = 0

    while True:
        remaining = await count_tokens_no_mc()
        if remaining == 0:
            state.backfill_progress = f"MC repair complete: {total_processed} processed"
            logger.info("MC repair complete: %d total processed", total_processed)
            break

        stats = await _repair_batch()
        total_processed += stats["processed"]

        state.backfill_progress = (
            f"MC repair: {total_processed} done, {remaining} left | "
            f"Found:{stats['found']} Dead:{stats['dead']} "
            f"Winners:{stats['winners']} Wallets:{stats['wallets']}"
        )

        if stats["processed"] > 0:
            logger.info(
                "MC repair batch: +%d processed (%d remaining) found=%d dead=%d winners=%d",
                stats["processed"], remaining, stats["found"], stats["dead"], stats["winners"],
            )

            await log_agent_run(
                "mc_repair",
                tokens_found=stats["found"],
                tokens_saved=stats["winners"],
                notes=f"dead={stats['dead']} wallets={stats['wallets']}",
            )

        await asyncio.sleep(POLL_INTERVAL)
