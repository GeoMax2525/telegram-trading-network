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

import aiohttp

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
    """Run gmgn-cli command and return parsed JSON output.

    Stderr and non-JSON output are logged at INFO on failure so that
    CLI flag / schema mismatches surface in Railway logs immediately
    instead of silently returning None. Rate-limit and auth errors
    stay at WARNING to avoid flooding.
    """
    cmd = ["npx", "gmgn-cli", *args, "--raw"]
    cmd_label = " ".join(args[:4])
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        output = stdout.decode().strip()

        if not output:
            err = stderr.decode().strip() if stderr else ""
            err_lower = err.lower()
            if "rate limit" in err_lower or "429" in err:
                logger.warning("GMGN CLI rate limited (%s): %s", cmd_label, err[:150])
            elif "401" in err or "403" in err or "unauthorized" in err_lower:
                logger.warning("GMGN CLI auth error (%s): %s", cmd_label, err[:150])
            else:
                # Surface at INFO so flag/arg mismatches don't hide forever
                logger.info("GMGN CLI empty stdout (%s) — stderr: %s",
                            cmd_label, err[:200] if err else "<empty>")
            return None

        return json.loads(output)
    except asyncio.TimeoutError:
        logger.info("GMGN CLI timeout (%s)", cmd_label)
        return None
    except json.JSONDecodeError:
        logger.info("GMGN CLI non-JSON output (%s): %s",
                    cmd_label, output[:200] if output else "empty")
        return None
    except FileNotFoundError:
        logger.warning("GMGN CLI: npx not found — install Node.js")
        return None
    except Exception as exc:
        logger.info("GMGN CLI error (%s): %s", cmd_label, exc)
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


async def gmgn_kol_trades(limit: int = 100) -> list:
    """Method B fallback — gmgn-cli track kol --raw."""
    data = await _run_cli("track", "kol", "--chain", "sol", "--limit", str(limit))
    if isinstance(data, dict):
        return data.get("list") or (data.get("data") or {}).get("list") or []
    return []


async def _http_get_json(url: str, timeout: int = 15) -> dict | None:
    """HTTP fallback for methods C and D. May be Cloudflare-blocked from Railway."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://gmgn.ai/",
    }
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    logger.debug("GMGN HTTP %d for %s", resp.status, url)
                    return None
                return await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("GMGN HTTP error (%s): %s", url, exc)
        return None


async def gmgn_http_smartmoney_wallets(limit: int = 100) -> list:
    """Method C fallback — direct openapi smartmoney wallet list."""
    url = f"https://openapi.gmgn.ai/defi/quotation/v1/smartmoney/sol/wallets?period=7d&limit={limit}"
    data = await _http_get_json(url)
    if not isinstance(data, dict):
        return []
    d = data.get("data") or {}
    # Shape variants observed across versions
    return d.get("wallets") or d.get("list") or d.get("rank") or []


async def gmgn_http_rank_wallets(limit: int = 100) -> list:
    """Method D fallback — direct openapi rank wallet list."""
    url = f"https://openapi.gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?limit={limit}"
    data = await _http_get_json(url)
    if not isinstance(data, dict):
        return []
    d = data.get("data") or {}
    return d.get("rank") or d.get("wallets") or d.get("list") or []


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
    # NB: the CLI flag is --wallet, not --address (confirmed against
    # .agents/skills/gmgn-portfolio/SKILL.md). Passing --address causes
    # the CLI to reject the call silently and return empty stdout, which
    # _run_cli turns into None, which in turn was the silent-failure
    # pathway for every GMGN wallet import since 2026-04-12.
    data = await _run_cli("portfolio", "stats", "--chain", "sol", "--wallet", address)
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

def _extract_buy_makers(trades: list) -> set[str]:
    """Pull unique maker addresses from buy-side trade rows."""
    out: set[str] = set()
    for t in trades or []:
        if not isinstance(t, dict):
            continue
        maker = t.get("maker") or t.get("wallet_address") or t.get("address")
        side = t.get("side")
        if maker and (side is None or side == "buy"):
            out.add(maker)
    return out


def _extract_wallet_addrs(wallets: list) -> set[str]:
    """Pull wallet addresses from a wallet-ranking list (no side filter)."""
    out: set[str] = set()
    for w in wallets or []:
        if not isinstance(w, dict):
            continue
        addr = w.get("wallet_address") or w.get("address") or w.get("maker")
        if addr:
            out.add(addr)
    return out


async def _fetch_smart_wallets_fallback() -> tuple[set[str], str]:
    """
    Try every known path for discovering smart-money wallets. Returns
    (addresses, method_label). First method that yields a non-empty set wins.
    """
    methods = [
        ("A: cli track smartmoney",
         lambda: gmgn_smart_money_trades(limit=100),
         _extract_buy_makers),
        ("B: cli track kol",
         lambda: gmgn_kol_trades(limit=100),
         _extract_buy_makers),
        ("C: http smartmoney/sol/wallets",
         lambda: gmgn_http_smartmoney_wallets(limit=100),
         _extract_wallet_addrs),
        ("D: http rank/sol/wallets/7d",
         lambda: gmgn_http_rank_wallets(limit=100),
         _extract_wallet_addrs),
    ]
    for label, fetch, extract in methods:
        try:
            raw = await fetch()
        except Exception as exc:
            logger.debug("GMGN wallets fallback %s raised: %s", label, exc)
            continue
        addrs = extract(raw)
        logger.info("GMGN wallets fallback %s: %d rows, %d unique addresses",
                    label, len(raw) if raw else 0, len(addrs))
        if addrs:
            return addrs, label
    return set(), "none"


async def _import_gmgn_wallets() -> int:
    """
    Import smart money wallets. Tries 4 fallback methods for address discovery,
    then fetches portfolio stats, scores, and saves each surviving wallet.
    """
    wallet_addrs, method = await _fetch_smart_wallets_fallback()
    if not wallet_addrs:
        logger.info("GMGN wallets: all 4 fallback methods returned empty — skipping cycle")
        return 0

    logger.info("GMGN wallets: using method [%s] — %d candidate addresses",
                method, len(wallet_addrs))

    # Skip reason counters for end-of-cycle summary
    skip_stats_none   = 0
    skip_few_trades   = 0
    skip_tier_zero    = 0

    imported = 0
    for address in list(wallet_addrs)[:20]:  # cap to avoid rate limits
        short = f"{address[:4]}..{address[-4:]}"
        stats = await gmgn_wallet_stats(address)
        if not stats:
            skip_stats_none += 1
            logger.info("GMGN wallet %s: SKIP stats=None (portfolio lookup returned nothing)", short)
            await asyncio.sleep(1.5)
            continue

        # Parse GMGN portfolio stats format:
        # buy=774, sell=771, pnl_stat.winrate=0.43, pnl_stat.token_num=576
        pnl = stats.get("pnl_stat") or {}
        wr = float(pnl.get("winrate") or stats.get("win_rate") or 0)
        total_trades = int(stats.get("buy") or pnl.get("token_num") or 0)
        if total_trades < 3:
            skip_few_trades += 1
            logger.info("GMGN wallet %s: SKIP total_trades=%d < 3", short, total_trades)
            await asyncio.sleep(1.5)
            continue

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

        tags = (stats.get("common") or {}).get("tags") or []

        # Score the wallet but DO NOT gate on tier — per user spec, every
        # GMGN wallet is saved as gmgn_smart regardless of score. Agent 2
        # may later upgrade it to early_insider / coordinated_group / etc.
        score, tier = await _score_wallet(
            wins=max(wins, 1), losses=losses, total_trades=total_trades,
            avg_multiple=avg_mult, early_entry_rate=0.5,
        )

        logger.info(
            "GMGN wallet %s: trades=%d wr=%.0f%% avg=%.2fx score=%.1f tier=%d tags=%s",
            short, total_trades, wr * 100, avg_mult, score, tier, tags[:3],
        )

        await upsert_wallet(
            address=address, score=score, tier=tier,
            win_rate=round(wr, 4), avg_multiple=round(avg_mult, 2),
            wins=wins, losses=losses, total_trades=total_trades,
            avg_entry_mcap=None, source="gmgn",
            wallet_type="gmgn_smart",
        )
        imported += 1

        await asyncio.sleep(1.5)  # rate limit between portfolio stats calls

    logger.info(
        "GMGN wallets: imported=%d skipped(stats_none=%d, few_trades=%d, tier_zero=%d) "
        "from %d addresses via [%s]",
        imported, skip_stats_none, skip_few_trades, skip_tier_zero,
        len(wallet_addrs), method,
    )
    await log_agent_run(
        "gmgn_wallets",
        tokens_found=len(wallet_addrs),
        tokens_saved=imported,
        notes=(
            f"method={method} "
            f"skip(stats={skip_stats_none}, few={skip_few_trades}, tier0={skip_tier_zero})"
        ),
    )

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

        symbol = (trade.get("base_token") or {}).get("symbol") or "?"
        price = float(trade.get("price_usd") or 0) or None
        amount = float(trade.get("amount_usd") or 0) or 0

        if amount < 100:
            continue

        # Upsert Token.gmgn_smart_money = True so scanner candidates can match
        # the smart_money_gmgn pattern_type at learning time, whether or not
        # this is a brand-new token or one we've already seen.
        if await token_exists(mint):
            try:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(select(Token).where(Token.mint == mint))
                    tok = result.scalar_one_or_none()
                    if tok:
                        tok.gmgn_smart_money = True
                        from datetime import datetime
                        tok.last_updated_at = datetime.utcnow()
                        await session.commit()
            except Exception:
                pass
            continue

        await save_token(
            mint=mint, name=symbol, symbol=symbol,
            price_usd=price, market_cap=None,
            liquidity_usd=None, volume_24h=None,
            source="gmgn_smart",
        )
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(select(Token).where(Token.mint == mint))
                tok = result.scalar_one_or_none()
                if tok:
                    tok.gmgn_smart_money = True
                    await session.commit()
        except Exception:
            pass

        new_signals += 1
        app_state.harvester_gmgn_today += 1

    if new_signals:
        logger.info("GMGN smart money: %d new tokens flagged", new_signals)
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
