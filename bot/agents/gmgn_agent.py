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

TOKEN_POLL = 300      # 5 minutes (was 2 — too fast, hitting rate limits)
WALLET_POLL = 3600    # 1 hour
TRADE_POLL = 600      # 10 minutes (was 5 — rate limit protection)
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
    # HTTP fallback
    data = await _http_get_json(
        f"https://openapi.gmgn.ai/defi/quotation/v1/tokens/sol/{mint}", timeout=10,
    )
    if isinstance(data, dict):
        return data.get("data") or data
    return None


async def gmgn_token_security(mint: str) -> dict | None:
    data = await _run_cli("token", "security", "--chain", "sol", "--address", mint)
    if isinstance(data, dict):
        return data.get("data") or data
    # HTTP fallback
    data = await _http_get_json(
        f"https://openapi.gmgn.ai/defi/quotation/v1/tokens/sol/{mint}/security", timeout=10,
    )
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


async def gmgn_token_holders(mint: str) -> list:
    """Get top token holders — shows concentration and whale activity."""
    data = await _run_cli("token", "holders", "--chain", "sol", "--address", mint)
    if isinstance(data, dict):
        return (data.get("data") or {}).get("holders") or data.get("holders") or []
    return []


async def gmgn_market_signals(limit: int = 50) -> list:
    """Get market signals: price spikes, smart money buys, large buys."""
    data = await _run_cli("market", "signal", "--chain", "sol", "--limit", str(limit))
    if isinstance(data, dict):
        signals = data.get("data") or data.get("signals") or []
        if isinstance(signals, dict):
            # Flatten signal groups into one list
            all_sigs = []
            for group in signals.values():
                if isinstance(group, list):
                    all_sigs.extend(group)
            return all_sigs
        return signals if isinstance(signals, list) else []
    return []


async def gmgn_trenches(status: str = "completed", limit: int = 50) -> list:
    """Get trenches tokens — new creation, near completion, completed."""
    data = await _run_cli("market", "trenches", "--chain", "sol",
                          "--status", status, "--limit", str(limit))
    if isinstance(data, dict):
        return (data.get("data") or {}).get("tokens") or data.get("list") or []
    return []


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
            # Hard mint-suffix gate — skip non-launchpad tokens entirely.
            from bot.scanner import mint_suffix_ok as _suf_ok
            if not _suf_ok(mint):
                logger.debug(
                    "GMGN: skipped %s — mint suffix filter failed",
                    mint[:12],
                )
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
    for address in list(wallet_addrs)[:50]:  # Business plan: higher cap
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
        # Strict wallet filter: 60%+ WR, 30+ trades minimum
        # Research shows only wallets meeting these thresholds are worth copying
        if total_trades < 30:
            skip_few_trades += 1
            logger.info("GMGN wallet %s: SKIP total_trades=%d < 30", short, total_trades)
            await asyncio.sleep(1.5)
            continue
        if wr < 0.55:
            skip_few_trades += 1
            logger.info("GMGN wallet %s: SKIP win_rate=%.0f%% < 55%%", short, wr * 100)
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
    from bot.scanner import mint_suffix_ok as _suf_ok
    for trade in trades:
        mint = trade.get("base_address")
        side = trade.get("side")
        if not mint or side != "buy":
            continue
        # Hard mint-suffix gate.
        if not _suf_ok(mint):
            logger.debug(
                "GMGN smart: skipped %s — mint suffix filter failed",
                mint[:12],
            )
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


# ── Top coin early buyer extraction ─────────────────────────────────────────
# The missing link: find what's winning → who bought it early → follow them.
# This is how the insider wallet database actually gets populated with
# proven early buyers instead of just GMGN's generic smart money list.

TOP_COIN_POLL = 1200  # every 20 minutes (rate limit protection)

async def _extract_top_coin_buyers() -> int:
    """
    Pull top trending/gaining coins from GMGN, get their early buyers,
    save those wallets with high scores. This feeds the insider signal.

    Flow: trending coins → top_traders per coin → upsert_wallet as early_insider
    """
    # Get top gaining tokens across multiple timeframes
    all_mints: list[tuple[str, str, float]] = []  # (mint, name, mc)

    for interval in ("1h", "6h"):
        tokens = await gmgn_trending(interval=interval, limit=20)
        for t in (tokens or []):
            mint = t.get("address")
            name = t.get("name") or t.get("symbol") or "?"
            mc = float(t.get("market_cap") or 0)
            if mint and mc > 10_000:
                all_mints.append((mint, name, mc))

    if not all_mints:
        return 0

    # Deduplicate
    seen: set[str] = set()
    unique_mints = []
    for m, n, mc in all_mints:
        if m not in seen:
            seen.add(m)
            unique_mints.append((m, n, mc))

    logger.info("GMGN top coin buyers: analyzing %d trending tokens for early buyers",
                len(unique_mints))

    wallets_found = 0
    wallets_saved = 0

    # Check top 10 trending coins for their early buyers
    for mint, name, mc in unique_mints[:10]:
        try:
            traders = await gmgn_top_traders(mint)
            if not traders:
                await asyncio.sleep(1)
                continue

            for trader in traders[:10]:  # top 10 traders per coin
                addr = trader.get("address") or trader.get("wallet_address")
                if not addr:
                    continue

                wallets_found += 1

                # Check if this is actually a profitable early buyer
                profit = float(trader.get("profit") or trader.get("realized_profit") or 0)
                buy_amount = float(trader.get("buy_amount_cur") or trader.get("cost") or 0)

                if profit <= 0 and buy_amount <= 0:
                    continue  # skip losers and dust

                # Calculate rough multiple
                if buy_amount > 0 and profit > 0:
                    multiple = 1.0 + (profit / buy_amount)
                else:
                    multiple = 2.0  # assume winner if profit > 0

                # Score higher for bigger winners
                if multiple >= 5.0:
                    score = 80.0
                    tier = 2
                elif multiple >= 2.0:
                    score = 65.0
                    tier = 2
                else:
                    score = 50.0
                    tier = 3

                await upsert_wallet(
                    address=addr,
                    score=score,
                    tier=tier,
                    win_rate=0.60 if multiple >= 2.0 else 0.40,
                    avg_multiple=round(min(multiple, 20.0), 2),
                    wins=1,
                    losses=0,
                    total_trades=1,
                    avg_entry_mcap=mc * 0.1 if mc else None,  # estimate early entry MC
                    source="gmgn",
                    wallet_type="early_insider" if multiple >= 3.0 else "gmgn_smart",
                )
                wallets_saved += 1

            await asyncio.sleep(1.5)  # rate limit

        except Exception as exc:
            logger.warning("GMGN top buyers: failed for %s: %s", name[:20], exc)
            await asyncio.sleep(1)

    if wallets_saved:
        logger.info(
            "GMGN top coin buyers: found %d wallets, saved %d (from %d coins)",
            wallets_found, wallets_saved, len(unique_mints[:10]),
        )
        await log_agent_run(
            "gmgn_top_buyers",
            tokens_found=len(unique_mints[:10]),
            tokens_saved=wallets_saved,
            notes=f"extracted early buyers from top trending coins",
        )

    return wallets_saved


# ── Background loops ─────────────────────────────────────────────────────────

async def gmgn_agent_loop() -> None:
    await asyncio.sleep(STARTUP_DELAY)

    # Quick test — check if gmgn-cli / npx works on this host
    test = await gmgn_trending(interval="1h", limit=1)
    if not test:
        logger.warning("GMGN agent: gmgn-cli not available (needs Node.js). Disabled on this host.")
        logger.info("GMGN agent: use /testgmgn in Claude Code session for GMGN data.")
        return

    logger.info("GMGN agent started — tokens:%ds wallets:%ds trades:%ds top_buyers:%ds",
                TOKEN_POLL, WALLET_POLL, TRADE_POLL, TOP_COIN_POLL)

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

    async def _top_buyer_loop():
        while True:
            try:
                await _extract_top_coin_buyers()
            except Exception as exc:
                logger.error("GMGN top buyer extraction error: %s", exc)
            await asyncio.sleep(TOP_COIN_POLL)

    async def _kol_loop():
        """Track KOL (Key Opinion Leader) trades — influencer wallets."""
        while True:
            try:
                kol_trades = await gmgn_kol_trades(limit=50)
                if kol_trades:
                    kol_wallets = 0
                    for trade in kol_trades:
                        addr = trade.get("maker") or trade.get("wallet_address")
                        side = trade.get("side")
                        if not addr or (side and side != "buy"):
                            continue
                        # KOL wallets are high-signal — save as tier 2
                        await upsert_wallet(
                            address=addr, score=70.0, tier=2,
                            win_rate=0.55, avg_multiple=2.5,
                            wins=1, losses=0, total_trades=1,
                            source="gmgn", wallet_type="early_insider",
                        )
                        kol_wallets += 1
                    if kol_wallets:
                        logger.info("GMGN KOL: imported %d KOL wallets", kol_wallets)
            except Exception as exc:
                logger.error("GMGN KOL tracking error: %s", exc)
            await asyncio.sleep(TRADE_POLL)

    async def _signal_loop():
        """Track market signals — price spikes, smart money buys, large buys.
        Injects high-signal tokens directly into scanner pipeline."""
        while True:
            try:
                signals = await gmgn_market_signals(limit=30)
                if signals:
                    from bot import state as _st
                    injected = 0
                    from bot.scanner import mint_suffix_ok as _suf
                    for sig in signals:
                        mint = sig.get("address") or sig.get("base_address")
                        if not mint or not _suf(mint):
                            continue
                        name = sig.get("name") or sig.get("symbol") or "?"
                        mc = float(sig.get("market_cap") or sig.get("usd_market_cap") or 0)
                        signal_type = sig.get("signal_type") or sig.get("type") or "unknown"

                        _st.pending_candidates.append({
                            "mint": mint,
                            "name": name,
                            "symbol": sig.get("symbol") or "?",
                            "mcap": mc,
                            "liquidity": float(sig.get("liquidity") or 0),
                            "source": "gmgn_signal",
                            "gmgn_signal_type": signal_type,
                        })
                        injected += 1

                    if injected:
                        logger.info("GMGN signals: injected %d tokens into scanner (%s)",
                                    injected, ", ".join(set(
                                        s.get("signal_type") or "?" for s in signals[:5]
                                    )))
            except Exception as exc:
                logger.error("GMGN signal tracking error: %s", exc)
            await asyncio.sleep(TRADE_POLL)

    await asyncio.gather(
        _token_loop(), _wallet_loop(), _trade_loop(),
        _top_buyer_loop(), _kol_loop(), _signal_loop(),
        return_exceptions=True,
    )
