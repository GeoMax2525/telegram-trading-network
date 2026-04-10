"""
gmgn_agent.py — GMGN Integration Agent

Two jobs running concurrently:
1. Token discovery: poll GMGN trending/new pairs every 2 minutes
2. Smart wallet import: fetch GMGN smart money wallets every hour
"""

import asyncio
import logging

import aiohttp

from bot.agents.wallet_analyst import _score_wallet
from database.models import (
    token_exists, save_token, upsert_wallet, log_agent_run,
    AsyncSessionLocal, select, Token,
)

logger = logging.getLogger(__name__)

GMGN_TRENDING = "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=swaps&direction=desc&limit=50"
GMGN_NEW_PAIRS = "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/6h?orderby=volume&direction=desc&limit=50"
GMGN_SMART_WALLETS = "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets?period=7d&limit=100"
GMGN_WALLET_STATS = "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet_stats/{address}?period=7d"

TOKEN_POLL = 120      # 2 minutes
WALLET_POLL = 3600    # 1 hour
STARTUP_DELAY = 90

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _fetch_json(url: str) -> dict | list | None:
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status != 200:
                    logger.debug("GMGN HTTP %d: %s", resp.status, url[:60])
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("GMGN fetch failed: %s", exc)
        return None


# ── Token discovery ──────────────────────────────────────────────────────────

async def _poll_gmgn_tokens() -> int:
    """Fetch trending and new tokens from GMGN. Returns count saved."""
    saved = 0

    for url, label in [(GMGN_TRENDING, "trending"), (GMGN_NEW_PAIRS, "new_pairs")]:
        data = await _fetch_json(url)
        if not data:
            continue

        # GMGN returns {"code": 0, "data": {"rank": [...]}}
        tokens = []
        if isinstance(data, dict):
            tokens = (data.get("data") or {}).get("rank") or []
        if not tokens:
            continue

        for i, t in enumerate(tokens):
            mint = t.get("address") or t.get("token_address") or t.get("mint")
            if not mint:
                continue
            if await token_exists(mint):
                # Update trending flag
                try:
                    async with AsyncSessionLocal() as session:
                        result = await session.execute(select(Token).where(Token.mint == mint))
                        tok = result.scalar_one_or_none()
                        if tok:
                            tok.gmgn_trending = (label == "trending")
                            tok.gmgn_rank = i + 1
                            from datetime import datetime
                            tok.last_updated_at = datetime.utcnow()
                            await session.commit()
                except Exception:
                    pass
                continue

            name = t.get("name") or t.get("symbol") or "?"
            symbol = t.get("symbol") or "?"
            mc = float(t.get("market_cap") or t.get("usd_market_cap") or 0) or None
            liq = float(t.get("liquidity") or 0) or None
            price = float(t.get("price") or 0) or None

            await save_token(
                mint=mint, name=name, symbol=symbol,
                price_usd=price, market_cap=mc,
                liquidity_usd=liq, volume_24h=None,
                source="gmgn",
            )

            # Set trending flag
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(select(Token).where(Token.mint == mint))
                    tok = result.scalar_one_or_none()
                    if tok:
                        tok.gmgn_trending = (label == "trending")
                        tok.gmgn_rank = i + 1
                        await session.commit()
            except Exception:
                pass

            saved += 1

    if saved:
        logger.info("GMGN tokens: saved %d new tokens", saved)
    return saved


# ── Smart wallet import ──────────────────────────────────────────────────────

async def _import_gmgn_wallets() -> int:
    """Fetch GMGN smart money wallets and import to Wallets table."""
    data = await _fetch_json(GMGN_SMART_WALLETS)
    if not data:
        return 0

    wallets = []
    if isinstance(data, dict):
        wallets = (data.get("data") or {}).get("wallets") or data.get("data") or []
    if isinstance(data, list):
        wallets = data
    if not wallets:
        return 0

    imported = 0

    for w in wallets:
        address = w.get("address") or w.get("wallet_address")
        if not address:
            continue

        # Try to get stats from GMGN
        wr = float(w.get("win_rate") or w.get("winrate") or 0)
        avg_mult = float(w.get("avg_multiple") or w.get("pnl_avg") or 1.0)
        total_trades = int(w.get("total_trades") or w.get("trade_count") or 0)
        wins = int(w.get("wins") or round(wr * total_trades) if total_trades else 0)
        losses = total_trades - wins
        profit = float(w.get("total_profit") or w.get("realized_profit") or 0)

        # Normalize win_rate to 0-1
        if wr > 1:
            wr = wr / 100.0

        # Score wallet
        early_rate = 0.5  # assume moderate early entry for GMGN wallets
        score, tier = _score_wallet(
            wins=max(wins, 1), losses=losses, total_trades=max(total_trades, 1),
            avg_multiple=max(avg_mult, 1.0), early_entry_rate=early_rate,
        )

        if tier > 0:
            await upsert_wallet(
                address=address, score=score, tier=tier,
                win_rate=round(wr, 4), avg_multiple=round(avg_mult, 2),
                wins=wins, losses=losses, total_trades=total_trades,
                avg_entry_mcap=None, source="gmgn",
            )
            imported += 1

    if imported:
        logger.info("GMGN wallets: imported %d smart wallets", imported)
        await log_agent_run("gmgn_wallets", tokens_found=len(wallets), tokens_saved=imported)

    return imported


# ── Background loops ─────────────────────────────────────────────────────────

async def gmgn_agent_loop() -> None:
    """Two concurrent jobs: token polling + wallet import."""
    await asyncio.sleep(STARTUP_DELAY)
    logger.info("GMGN agent started — tokens every %ds, wallets every %ds", TOKEN_POLL, WALLET_POLL)

    async def _token_loop():
        while True:
            try:
                await _poll_gmgn_tokens()
            except Exception as exc:
                logger.error("GMGN token poll error: %s", exc)
            await asyncio.sleep(TOKEN_POLL)

    async def _wallet_loop():
        while True:
            try:
                await _import_gmgn_wallets()
            except Exception as exc:
                logger.error("GMGN wallet import error: %s", exc)
            await asyncio.sleep(WALLET_POLL)

    await asyncio.gather(_token_loop(), _wallet_loop(), return_exceptions=True)
