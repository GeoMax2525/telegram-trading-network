"""
gmgn_agent.py — GMGN Integration Agent (Official OpenAPI)

Uses https://openapi.gmgn.ai with X-APIKEY header.

Three concurrent jobs:
1. Token discovery: trending + new pairs (every 2 min)
2. Smart wallet import: smart money wallets + stats (every hour)
3. Smart money trade tracking: live trade feed (every 5 min)
"""

import asyncio
import logging
import time
import uuid

import aiohttp

from bot import state as app_state
from bot.config import GMGN_API_KEY
from bot.agents.wallet_analyst import _score_wallet
from database.models import (
    token_exists, save_token, upsert_wallet, log_agent_run,
    AsyncSessionLocal, select, Token,
)

logger = logging.getLogger(__name__)

# Official authenticated API
GMGN_HOST = "https://openapi.gmgn.ai"

TOKEN_POLL = 120      # 2 minutes
WALLET_POLL = 3600    # 1 hour
TRADE_POLL = 300      # 5 minutes
STARTUP_DELAY = 90


# ── API client ───────────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if GMGN_API_KEY:
        h["X-APIKEY"] = GMGN_API_KEY
    return h


def _has_auth() -> bool:
    return bool(GMGN_API_KEY)


PUB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://gmgn.ai/",
    "Origin": "https://gmgn.ai",
}

# Public endpoint mapping: OpenAPI path → public URL template
PUBLIC_URLS = {
    "/v1/market/rank": "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/{interval}",
    "/v1/token/info": "https://gmgn.ai/defi/quotation/v1/tokens/sol/{address}",
    "/v1/token/security": "https://gmgn.ai/defi/quotation/v1/tokens/sol/{address}/security",
    "/v1/user/smartmoney": "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/recent_trades",
    "/v1/user/kol": "https://gmgn.ai/defi/quotation/v1/kol/sol/recent_trades",
    "/v1/market/token_top_traders": "https://gmgn.ai/defi/quotation/v1/tokens/sol/{address}/top_traders",
    "/v1/user/wallet_stats": "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallet_stats/{address}",
}


async def _fetch(path: str, params: dict | None = None) -> dict | None:
    """
    Fetch from GMGN. Uses public endpoints with Referer header (proven working).
    Falls back to OpenAPI if public fails and API key is set.
    """
    params = dict(params or {})

    # ── Primary: Public endpoint with Referer ────────────────────────
    pub_template = PUBLIC_URLS.get(path)
    if pub_template:
        pub_params = {k: v for k, v in params.items() if k != "chain"}
        pub_url = pub_template
        if "{interval}" in pub_url:
            pub_url = pub_url.format(interval=pub_params.pop("interval", "1h"))
        if "{address}" in pub_url:
            pub_url = pub_url.format(address=pub_params.pop("address", ""))

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as session:
                async with session.get(pub_url, headers=PUB_HEADERS, params=pub_params or None) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict) and data.get("code") in (None, 0):
                            return data
                    elif resp.status == 403:
                        logger.debug("GMGN public 403 (Cloudflare): %s", pub_url[:50])
                    else:
                        logger.debug("GMGN public %d: %s", resp.status, pub_url[:50])
        except Exception as exc:
            logger.debug("GMGN public error: %s", exc)

    # ── Fallback: Authenticated OpenAPI ──────────────────────────────
    if _has_auth():
        params["timestamp"] = str(int(time.time()))
        params["client_id"] = str(uuid.uuid4())
        url = f"{GMGN_HOST}{path}"
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as session:
                async with session.get(url, headers=_headers(), params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict) and data.get("code") in (None, 0):
                            return data
                    logger.debug("GMGN openapi %d: %s", resp.status, path)
        except Exception as exc:
            logger.debug("GMGN openapi error: %s", exc)

    return None


# ── Public API functions ─────────────────────────────────────────────────────

async def gmgn_token_info(mint: str) -> dict | None:
    """GET /v1/token/info — full token data."""
    data = await _fetch("/v1/token/info", {"chain": "sol", "address": mint})
    if data and isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_token_security(mint: str) -> dict | None:
    """GET /v1/token/security — contract security."""
    data = await _fetch("/v1/token/security", {"chain": "sol", "address": mint})
    if data and isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_top_traders(mint: str) -> list:
    """GET /v1/market/token_top_traders — top traders for a token."""
    data = await _fetch("/v1/market/token_top_traders", {"chain": "sol", "address": mint})
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("traders") or []
    return []


async def gmgn_trending(interval: str = "1h", limit: int = 50) -> list:
    """GET /v1/market/rank — trending tokens."""
    data = await _fetch("/v1/market/rank", {
        "chain": "sol", "interval": interval,
        "orderby": "swaps", "direction": "desc", "limit": limit,
    })
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("rank") or []
    return []


async def gmgn_smart_money_trades(limit: int = 50) -> list:
    """GET /v1/user/smartmoney — smart money trade feed."""
    data = await _fetch("/v1/user/smartmoney", {"chain": "sol", "limit": limit})
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("list") or []
    return []


async def gmgn_kol_trades(limit: int = 50) -> list:
    """GET /v1/user/kol — KOL trade feed."""
    data = await _fetch("/v1/user/kol", {"chain": "sol", "limit": limit})
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("list") or []
    return []


async def gmgn_wallet_stats(address: str) -> dict | None:
    """GET /v1/user/wallet_stats — wallet performance stats."""
    data = await _fetch("/v1/user/wallet_stats", {"chain": "sol", "address": address})
    if data and isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_kline(mint: str, interval: str = "1m", limit: int = 100) -> list:
    """GET /v1/market/token_kline — OHLCV candle data."""
    data = await _fetch("/v1/market/token_kline", {
        "chain": "sol", "address": mint,
        "interval": interval, "limit": limit,
    })
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("klines") or []
    return []


# ── Token discovery ──────────────────────────────────────────────────────────

async def _poll_gmgn_tokens() -> int:
    """Fetch trending tokens from GMGN and save new ones."""
    saved = 0

    for interval in ("1h", "6h"):
        tokens = await gmgn_trending(interval=interval, limit=50)
        if not tokens:
            continue

        for i, t in enumerate(tokens):
            mint = t.get("address")
            if not mint:
                continue
            if await token_exists(mint):
                # Update trending flag
                try:
                    async with AsyncSessionLocal() as session:
                        result = await session.execute(select(Token).where(Token.mint == mint))
                        tok = result.scalar_one_or_none()
                        if tok:
                            tok.gmgn_trending = True
                            tok.gmgn_rank = i + 1
                            from datetime import datetime
                            tok.last_updated_at = datetime.utcnow()
                            await session.commit()
                except Exception:
                    pass
                continue

            name = t.get("name") or t.get("symbol") or "?"
            symbol = t.get("symbol") or "?"
            mc = float(t.get("market_cap") or 0) or None
            liq = float(t.get("liquidity") or 0) or None
            price = float(t.get("price") or 0) or None

            await save_token(
                mint=mint, name=name, symbol=symbol,
                price_usd=price, market_cap=mc,
                liquidity_usd=liq, volume_24h=float(t.get("volume") or 0) or None,
                source="gmgn",
            )

            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(select(Token).where(Token.mint == mint))
                    tok = result.scalar_one_or_none()
                    if tok:
                        tok.gmgn_trending = True
                        tok.gmgn_rank = i + 1
                        await session.commit()
            except Exception:
                pass

            saved += 1
            app_state.harvester_gmgn_today += 1

    if saved:
        logger.info("GMGN tokens: saved %d new tokens", saved)
    return saved


# ── Smart wallet import ──────────────────────────────────────────────────────

async def _import_gmgn_wallets() -> int:
    """Import smart money wallets from GMGN trade feed."""
    trades = await gmgn_smart_money_trades(limit=100)
    if not trades:
        return 0

    # Extract unique wallets from trades
    wallet_map: dict[str, dict] = {}
    for t in trades:
        maker = t.get("maker")
        if not maker or maker in wallet_map:
            continue
        tags = []
        mi = t.get("maker_info") or {}
        tags = mi.get("tags") or []
        wallet_map[maker] = {
            "address": maker,
            "tags": tags,
            "is_smart": "smart_degen" in tags,
            "is_kol": "renowned" in tags or "kol" in tags,
        }

    imported = 0
    for address, info in wallet_map.items():
        # Fetch wallet stats
        stats = await gmgn_wallet_stats(address)
        if not stats:
            continue

        wr = float(stats.get("win_rate") or 0)
        avg_mult = float(stats.get("avg_profit_percent") or 0) / 100 + 1 if stats.get("avg_profit_percent") else 1.0
        total_trades = int(stats.get("total_trades") or stats.get("buy_count", 0) or 0)
        wins = int(stats.get("win_count") or round(wr * total_trades / 100) if total_trades else 0)
        losses = total_trades - wins

        if wr > 1:
            wr = wr / 100.0

        score, tier = _score_wallet(
            wins=max(wins, 1), losses=losses, total_trades=max(total_trades, 1),
            avg_multiple=max(avg_mult, 1.0), early_entry_rate=0.5,
        )

        if tier > 0:
            await upsert_wallet(
                address=address, score=score, tier=tier,
                win_rate=round(wr, 4), avg_multiple=round(avg_mult, 2),
                wins=wins, losses=losses, total_trades=total_trades,
                avg_entry_mcap=None, source="gmgn",
            )
            imported += 1

        await asyncio.sleep(0.5)  # rate limit between wallet stat fetches

    if imported:
        logger.info("GMGN wallets: imported %d from %d trade makers", imported, len(wallet_map))
        await log_agent_run("gmgn_wallets", tokens_found=len(wallet_map), tokens_saved=imported)

    return imported


# ── Smart money trade tracking ───────────────────────────────────────────────

async def _track_smart_money_trades() -> int:
    """Track recent smart money buys for signal generation."""
    trades = await gmgn_smart_money_trades(limit=50)
    if not trades:
        return 0

    new_signals = 0
    for trade in trades:
        mint = trade.get("base_address")
        side = trade.get("side")
        if not mint or side != "buy":
            continue
        if await token_exists(mint):
            continue

        symbol = (trade.get("base_token") or {}).get("symbol") or "?"
        price = float(trade.get("price_usd") or 0) or None
        amount = float(trade.get("amount_usd") or 0) or 0

        if amount >= 100:
            await save_token(
                mint=mint, name=symbol, symbol=symbol,
                price_usd=price, market_cap=None,
                liquidity_usd=None, volume_24h=None,
                source="gmgn_smart",
            )
            new_signals += 1
            app_state.harvester_gmgn_today += 1

    if new_signals:
        logger.info("GMGN smart money: %d new tokens from trades", new_signals)
    return new_signals


# ── Background loops ─────────────────────────────────────────────────────────

async def gmgn_agent_loop() -> None:
    """Three concurrent jobs."""
    await asyncio.sleep(STARTUP_DELAY)
    auth = "authenticated" if _has_auth() else "NO API KEY"
    logger.info("GMGN agent started (%s) — host=%s", auth, GMGN_HOST)

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

    async def _trade_loop():
        while True:
            try:
                await _track_smart_money_trades()
            except Exception as exc:
                logger.error("GMGN trade tracking error: %s", exc)
            await asyncio.sleep(TRADE_POLL)

    await asyncio.gather(
        _token_loop(), _wallet_loop(), _trade_loop(),
        return_exceptions=True,
    )
