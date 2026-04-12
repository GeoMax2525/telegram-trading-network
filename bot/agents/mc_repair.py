"""
mc_repair.py — Background MC repair task

Processes tokens with no market_cap in batches every 10 minutes.
For each token:
  1. Try DexScreener for current MC
  2. If DexScreener fails, try GMGN (works for bonding curve tokens)
  3. If both fail: mark as dead
"""

import asyncio
import logging

from bot import state
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    get_tokens_no_mc, count_tokens_no_mc,
    update_token_market_cap, mark_token_dead,
    log_agent_run,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 200
POLL_INTERVAL = 600  # 10 minutes
STARTUP_DELAY = 180  # 3 minutes after boot


async def _get_mc(mint: str) -> float:
    """Try to get MC from DexScreener, then GMGN. Returns 0 if both fail."""
    # Try DexScreener
    try:
        pair = await fetch_token_data(mint)
        if pair:
            metrics = parse_token_metrics(pair)
            mc = metrics.get("market_cap", 0) or 0
            if mc > 0:
                return mc
    except Exception:
        pass

    # Try GMGN
    try:
        from bot.agents.gmgn_agent import gmgn_token_info
        info = await gmgn_token_info(mint)
        if info:
            mc = float(info.get("market_cap") or info.get("usd_market_cap") or 0)
            if mc > 0:
                return mc
            # Calculate from price * supply
            price = float(info.get("price") or 0)
            supply = float(info.get("total_supply") or info.get("circulating_supply") or 1e9)
            if price > 0:
                return price * supply
    except Exception:
        pass

    return 0


async def _repair_batch() -> dict:
    """Process one batch of tokens with no MC."""
    tokens = await get_tokens_no_mc(limit=BATCH_SIZE)
    if not tokens:
        return {"processed": 0}

    stats = {"processed": 0, "found": 0, "dead": 0, "errors": 0}

    for tok in tokens:
        stats["processed"] += 1
        try:
            mc = await _get_mc(tok.mint)

            if mc > 0:
                await update_token_market_cap(tok.mint, mc)
                stats["found"] += 1
            else:
                await mark_token_dead(tok.mint)
                stats["dead"] += 1

            await asyncio.sleep(0.5)  # rate limit

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
            f"Found:{stats['found']} Dead:{stats['dead']}"
        )

        if stats["processed"] > 0:
            logger.info(
                "MC repair batch: +%d (%d left) found=%d dead=%d",
                stats["processed"], remaining, stats["found"], stats["dead"],
            )

            await log_agent_run(
                "mc_repair",
                tokens_found=stats["found"],
                tokens_saved=stats["found"],
                notes=f"dead={stats['dead']}",
            )

        await asyncio.sleep(POLL_INTERVAL)
