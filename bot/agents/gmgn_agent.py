"""
gmgn_agent.py — GMGN Integration Agent (Authenticated API)

Uses official GMGN API with Ed25519 signed requests.

Three concurrent jobs:
1. Token discovery: trending + new pairs + cooking signals (every 2 min)
2. Smart wallet import: smart money wallets + stats (every hour)
3. Smart money trade tracking: live trade feed (every 5 min)

API endpoints (authenticated):
  /v1/token/info, /v1/token/security — token data
  /v1/market/token_top_holders, /v1/market/token_top_traders — holders/traders
  /v1/user/smartmoney — smart money trade feed
  /v1/user/kol — KOL trade feed
"""

import asyncio
import base64
import hashlib
import json
import logging
import time

import aiohttp

from bot import state as app_state
from bot.config import GMGN_API_KEY, GMGN_PRIVATE_KEY
from bot.agents.wallet_analyst import _score_wallet
from database.models import (
    token_exists, save_token, upsert_wallet, log_agent_run,
    AsyncSessionLocal, select, Token,
)

logger = logging.getLogger(__name__)

GMGN_BASE = "https://gmgn.ai/api"

TOKEN_POLL = 120      # 2 minutes
WALLET_POLL = 3600    # 1 hour
TRADE_POLL = 300      # 5 minutes
STARTUP_DELAY = 90

# Fallback public endpoints (no auth required)
GMGN_TRENDING_PUBLIC = "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h?orderby=swaps&direction=desc&limit=50"
GMGN_NEW_PUBLIC = "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/6h?orderby=volume&direction=desc&limit=50"
GMGN_SMART_PUBLIC = "https://gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets?period=7d&limit=100"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://gmgn.ai/",
    "Origin": "https://gmgn.ai",
}


# ── Auth helper ──────────────────────────────────────────────────────────────

def _sign_request(method: str, path: str, body: str = "") -> dict:
    """
    Signs a GMGN API request with Ed25519.
    Returns headers dict with Authorization.
    """
    if not GMGN_API_KEY or not GMGN_PRIVATE_KEY:
        return HEADERS

    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        timestamp = str(int(time.time()))
        message = f"{method}\n{path}\n{timestamp}\n{body}"

        # Load private key
        pem = GMGN_PRIVATE_KEY
        if not pem.startswith("-----"):
            pem = f"-----BEGIN PRIVATE KEY-----\n{pem}\n-----END PRIVATE KEY-----"
        key = load_pem_private_key(pem.encode(), password=None)
        signature = base64.b64encode(key.sign(message.encode())).decode()

        return {
            **HEADERS,
            "X-GMGN-ApiKey": GMGN_API_KEY,
            "X-GMGN-Timestamp": timestamp,
            "X-GMGN-Signature": signature,
        }
    except ImportError:
        logger.debug("GMGN: cryptography not installed, using public endpoints")
        return HEADERS
    except Exception as exc:
        logger.debug("GMGN: signing failed: %s, using public endpoints", exc)
        return HEADERS


def _has_auth() -> bool:
    return bool(GMGN_API_KEY and GMGN_PRIVATE_KEY)


# ── Fetch helpers ────────────────────────────────────────────────────────────

async def _fetch(url: str, signed: bool = False) -> dict | list | None:
    """Fetch GMGN endpoint. Uses auth if available, always sends Referer."""
    try:
        if signed and _has_auth():
            from urllib.parse import urlparse
            parsed = urlparse(url)
            headers = _sign_request("GET", parsed.path + ("?" + parsed.query if parsed.query else ""))
        else:
            headers = dict(HEADERS)

        # Always include Referer (required by GMGN Cloudflare)
        headers["Referer"] = "https://gmgn.ai/"
        headers["Origin"] = "https://gmgn.ai"

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 429:
                    logger.warning("GMGN: rate limited (429) on %s", url[:60])
                    return None
                if resp.status == 403:
                    # Cloudflare block — log for debugging
                    body = await resp.text()
                    is_cf = "Cloudflare" in body[:500]
                    logger.warning("GMGN: 403 %s — %s", url[:60],
                                   "Cloudflare block" if is_cf else "auth rejected")
                    return None
                if resp.status != 200:
                    logger.info("GMGN HTTP %d: %s", resp.status, url[:60])
                    return None
                data = await resp.json(content_type=None)
                code = data.get("code") if isinstance(data, dict) else None
                if code is not None and code != 0:
                    logger.info("GMGN API error code=%s: %s", code, url[:60])
                return data
    except Exception as exc:
        logger.debug("GMGN fetch failed: %s", exc)
        return None


async def gmgn_token_info(mint: str) -> dict | None:
    """GET /v1/token/info — full token data."""
    url = f"https://gmgn.ai/defi/quotation/v1/tokens/sol/{mint}"
    data = await _fetch(url, signed=True)
    if data and isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_token_security(mint: str) -> dict | None:
    """Token security check."""
    url = f"https://gmgn.ai/defi/quotation/v1/tokens/sol/{mint}/security"
    data = await _fetch(url, signed=True)
    if data and isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_top_traders(mint: str) -> list:
    """Top traders for a token — useful for insider detection."""
    url = f"https://gmgn.ai/defi/quotation/v1/tokens/sol/{mint}/top_traders"
    data = await _fetch(url, signed=True)
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("traders") or []
    return []


async def gmgn_smart_money_trades(limit: int = 50) -> list:
    """Recent smart money trades."""
    url = f"https://gmgn.ai/defi/quotation/v1/smartmoney/sol/recent_trades?limit={limit}"
    data = await _fetch(url, signed=True)
    if data and isinstance(data, dict):
        return (data.get("data") or {}).get("list") or (data.get("data") or {}).get("trades") or []
    return []


# ── Token discovery ──────────────────────────────────────────────────────────

async def _poll_gmgn_tokens() -> int:
    """Fetch trending and new tokens from GMGN. Returns count saved."""
    saved = 0

    for url, label in [(GMGN_TRENDING_PUBLIC, "trending"), (GMGN_NEW_PUBLIC, "new_pairs")]:
        data = await _fetch(url)
        if not data:
            continue

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
            app_state.harvester_gmgn_today += 1

    if saved:
        logger.info("GMGN tokens: saved %d new tokens", saved)
    return saved


# ── Smart wallet import ──────────────────────────────────────────────────────

async def _import_gmgn_wallets() -> int:
    """Fetch GMGN smart money wallets and import to Wallets table."""
    data = await _fetch(GMGN_SMART_PUBLIC)
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

        wr = float(w.get("win_rate") or w.get("winrate") or 0)
        avg_mult = float(w.get("avg_multiple") or w.get("pnl_avg") or 1.0)
        total_trades = int(w.get("total_trades") or w.get("trade_count") or 0)
        wins = int(w.get("wins") or round(wr * total_trades) if total_trades else 0)
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

    if imported:
        logger.info("GMGN wallets: imported %d smart wallets", imported)
        await log_agent_run("gmgn_wallets", tokens_found=len(wallets), tokens_saved=imported)

    return imported


# ── Smart money trade tracking ───────────────────────────────────────────────

async def _track_smart_money_trades() -> int:
    """Track recent smart money trades for signal generation."""
    trades = await gmgn_smart_money_trades(limit=50)
    if not trades:
        return 0

    new_signals = 0
    for trade in trades:
        mint = trade.get("base_address") or trade.get("token_address")
        side = trade.get("side")
        maker = trade.get("maker")

        if not mint or side != "buy":
            continue

        # Check if this is a new token we should track
        if not await token_exists(mint):
            symbol = (trade.get("base_token") or {}).get("symbol") or "?"
            price = float(trade.get("price_usd") or 0) or None
            amount = float(trade.get("amount_usd") or 0) or 0

            if amount >= 100:  # Minimum $100 trade to be worth tracking
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
    """Three concurrent jobs: token polling + wallet import + trade tracking."""
    await asyncio.sleep(STARTUP_DELAY)
    auth_status = "authenticated" if _has_auth() else "public endpoints only"
    logger.info("GMGN agent started (%s) — tokens:%ds wallets:%ds trades:%ds",
                auth_status, TOKEN_POLL, WALLET_POLL, TRADE_POLL)

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
