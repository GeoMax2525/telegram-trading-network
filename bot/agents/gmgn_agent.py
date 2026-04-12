"""
gmgn_agent.py — GMGN Integration Agent (via gmgn-cli subprocess)

Uses gmgn-cli Node.js tool which handles auth + Cloudflare properly.
Python aiohttp gets Cloudflare-blocked from Railway; gmgn-cli does not.

Three concurrent jobs:
1. Token discovery: trending tokens (every 2 min)
2. Smart wallet import: smart money wallets from trades (every hour)
3. Smart money trade tracking: live trade feed (every 5 min)
"""

import asyncio
import json
import logging

from bot import state as app_state
from bot.agents.wallet_analyst import _score_wallet
from database.models import (
    token_exists, save_token, upsert_wallet, log_agent_run,
    AsyncSessionLocal, select, Token,
)

logger = logging.getLogger(__name__)

TOKEN_POLL = 120      # 2 minutes
WALLET_POLL = 3600    # 1 hour
TRADE_POLL = 300      # 5 minutes
STARTUP_DELAY = 90


# ── gmgn-cli subprocess wrapper ─────────────────────────────────────────────

async def _run_cli(*args: str, timeout: int = 30) -> dict | list | None:
    """Run gmgn-cli command and return parsed JSON output."""
    cmd = ["npx", "gmgn-cli", *args, "--raw"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        output = stdout.decode().strip()

        if not output:
            if stderr:
                err = stderr.decode().strip()
                if "rate limit" in err.lower() or "429" in err:
                    logger.warning("GMGN CLI rate limited: %s", err[:100])
                elif "401" in err or "invalid" in err.lower():
                    logger.warning("GMGN CLI auth error: %s", err[:100])
                else:
                    logger.debug("GMGN CLI stderr: %s", err[:100])
            return None

        return json.loads(output)
    except asyncio.TimeoutError:
        logger.debug("GMGN CLI timeout: %s", " ".join(args[:3]))
        return None
    except json.JSONDecodeError:
        logger.debug("GMGN CLI non-JSON output: %s", output[:100] if output else "empty")
        return None
    except FileNotFoundError:
        logger.warning("GMGN CLI: npx not found — install Node.js")
        return None
    except Exception as exc:
        logger.debug("GMGN CLI error: %s", exc)
        return None


# ── Public API functions ─────────────────────────────────────────────────────

async def gmgn_trending(interval: str = "1h", limit: int = 50) -> list:
    data = await _run_cli("market", "trending", "--chain", "sol",
                          "--interval", interval, "--limit", str(limit))
    if isinstance(data, dict):
        return (data.get("data") or {}).get("rank") or []
    return []


async def gmgn_smart_money_trades(limit: int = 50) -> list:
    data = await _run_cli("track", "smartmoney", "--chain", "sol", "--limit", str(limit))
    if isinstance(data, dict):
        return data.get("list") or (data.get("data") or {}).get("list") or []
    return []


async def gmgn_token_info(mint: str) -> dict | None:
    data = await _run_cli("token", "info", "--chain", "sol", "--address", mint)
    if isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_token_security(mint: str) -> dict | None:
    data = await _run_cli("token", "security", "--chain", "sol", "--address", mint)
    if isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_top_traders(mint: str) -> list:
    data = await _run_cli("token", "traders", "--chain", "sol", "--address", mint)
    if isinstance(data, dict):
        return (data.get("data") or {}).get("traders") or []
    return []


async def gmgn_wallet_stats(address: str) -> dict | None:
    data = await _run_cli("portfolio", "stats", "--chain", "sol", "--address", address)
    if isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_kline(mint: str, resolution: str = "1m") -> list:
    data = await _run_cli("market", "kline", "--chain", "sol",
                          "--address", mint, "--resolution", resolution)
    if isinstance(data, dict):
        return (data.get("data") or {}).get("klines") or []
    return []


# ── Token discovery ──────────────────────────────────────────────────────────

async def _poll_gmgn_tokens() -> int:
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
    """
    Import smart money wallets:
    1. Get recent smart money BUY trades → extract unique makers
    2. Fetch portfolio stats for each maker
    3. Score and save to Wallets table
    """
    # Step 1: Get smart money buy trades
    trades = await gmgn_smart_money_trades(limit=100)
    if not trades:
        logger.info("GMGN wallets: no smart money trades returned")
        return 0

    # Extract unique wallet addresses from buy trades
    wallet_addrs = set()
    for t in trades:
        maker = t.get("maker")
        side = t.get("side")
        if maker and side == "buy":
            wallet_addrs.add(maker)

    logger.info("GMGN wallets: %d unique buy makers from %d trades", len(wallet_addrs), len(trades))

    # Step 2: Fetch stats for each wallet
    imported = 0
    for address in list(wallet_addrs)[:20]:  # cap to avoid rate limits
        stats = await gmgn_wallet_stats(address)
        if not stats:
            continue

        # Parse GMGN portfolio stats format:
        # buy=774, sell=771, pnl_stat.winrate=0.43, pnl_stat.token_num=576
        pnl = stats.get("pnl_stat") or {}
        wr = float(pnl.get("winrate") or stats.get("win_rate") or 0)
        total_trades = int(stats.get("buy") or pnl.get("token_num") or 0)
        if total_trades < 3:
            continue  # not enough data

        # Count wins from PnL buckets
        wins_2x = int(pnl.get("pnl_2x_5x_num") or 0)
        wins_5x = int(pnl.get("pnl_gt_5x_num") or 0)
        wins_0x = int(pnl.get("pnl_0x_2x_num") or 0)
        wins = wins_0x + wins_2x + wins_5x
        losses = total_trades - wins

        # Avg multiple estimate from win distribution
        if wins > 0:
            avg_mult = (wins_0x * 1.5 + wins_2x * 3.5 + wins_5x * 7.0) / wins
        else:
            avg_mult = 1.0

        # Tags
        tags = (stats.get("common") or {}).get("tags") or []

        logger.info(
            "GMGN wallet %s..%s: trades=%d wr=%.0f%% avg=%.1fx tags=%s",
            address[:4], address[-4:], total_trades, wr * 100, avg_mult, tags[:3],
        )

        # Step 3: Score
        score, tier = _score_wallet(
            wins=max(wins, 1), losses=losses, total_trades=total_trades,
            avg_multiple=avg_mult, early_entry_rate=0.5,
        )

        if tier > 0:
            await upsert_wallet(
                address=address, score=score, tier=tier,
                win_rate=round(wr, 4), avg_multiple=round(avg_mult, 2),
                wins=wins, losses=losses, total_trades=total_trades,
                avg_entry_mcap=None, source="gmgn",
            )
            imported += 1

        await asyncio.sleep(1.5)  # rate limit between portfolio stats calls

    if imported:
        logger.info("GMGN wallets: imported %d from %d makers", imported, len(wallet_addrs))
    await log_agent_run("gmgn_wallets", tokens_found=len(wallet_addrs), tokens_saved=imported)

    return imported


# ── Smart money trade tracking ───────────────────────────────────────────────

async def _track_smart_money_trades() -> int:
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
        logger.info("GMGN smart money: %d new tokens", new_signals)
    return new_signals


# ── Background loops ─────────────────────────────────────────────────────────

async def gmgn_agent_loop() -> None:
    await asyncio.sleep(STARTUP_DELAY)

    # Quick test — check if gmgn-cli / npx works on this host
    test = await gmgn_trending(interval="1h", limit=1)
    if not test:
        logger.warning("GMGN agent: gmgn-cli not available (needs Node.js). Disabled on this host.")
        logger.info("GMGN agent: use /testgmgn in Claude Code session for GMGN data.")
        return

    logger.info("GMGN agent started — tokens:%ds wallets:%ds trades:%ds",
                TOKEN_POLL, WALLET_POLL, TRADE_POLL)

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
