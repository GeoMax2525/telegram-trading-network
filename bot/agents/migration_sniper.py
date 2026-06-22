"""
migration_sniper.py — Migration Dip Buyer (its own alert + buy source).

CapitalOS's one real strategy edge: when a pump.fun token graduates to Raydium,
it usually dips first, then runs. Buy that post-migration dip with defined rules.

Flow:
  1. Subscribe to PumpPortal `subscribeMigration` — fires the instant a token
     graduates pump.fun → Raydium.
  2. Record the token's migration market cap as the baseline.
  3. Watch it (poll DexScreener). When it dips >= migration_dip_pct from the
     migration MC AND passes the gates (liquidity floor), fire a MIGRATION DIP
     alert and open a paper trade with its OWN rules: size, TP +80%, SL -35%.
  4. The position then rides the normal paper_monitor exits like any trade,
     tagged pattern_type="migration_dip" so it's tracked separately in
     /sourcestats and the reports.

Gated OFF by default (migration_sniper_enabled=0) so it never interferes until
deliberately enabled. Every threshold is tunable via /setparam.
"""

import asyncio
import json
import logging
from html import escape as _esc

import aiohttp

from bot import state

logger = logging.getLogger(__name__)

WS_URL = "wss://pumpportal.fun/api/data"
STARTUP_DELAY = 55
RECONNECT_DELAY = 5
ENABLE_RECHECK_SEC = 60
POLL_INTERVAL = 15          # how often we re-check watched tokens for the dip
MAX_WATCH = 100

# mint -> {"migration_mc": float, "name": str, "symbol": str,
#          "first_seen": float, "low_mc": float, "bought": bool}
_watch: dict[str, dict] = {}


async def _enabled() -> bool:
    try:
        from database.models import get_param
        v = await get_param("migration_sniper_enabled")
        return bool(v and v >= 0.5)
    except Exception:
        return False


def _now() -> float:
    return asyncio.get_event_loop().time()


def _handle_migration(data: dict) -> None:
    """Record a freshly-migrated token to watch for its dip."""
    mint = data.get("mint")
    if not mint or mint in _watch:
        return
    if len(_watch) >= MAX_WATCH:
        # Evict the oldest
        oldest = min(_watch, key=lambda m: _watch[m]["first_seen"])
        _watch.pop(oldest, None)
    _watch[mint] = {
        "migration_mc": None,   # filled on first price poll
        "name": data.get("name") or data.get("symbol"),
        "symbol": data.get("symbol"),
        "first_seen": _now(),
        "low_mc": None,
        "bought": False,
    }
    logger.info("migration_sniper: watching migrated token %s", mint[:12])


async def _poll_loop() -> None:
    """Poll watched tokens for the post-migration dip; buy + alert on trigger."""
    from database.models import get_params, open_paper_trade, count_open_paper_trades, \
        has_open_paper_trade, compute_paper_balance
    from bot.scanner import fetch_token_data, parse_token_metrics

    while True:
        await asyncio.sleep(POLL_INTERVAL)
        if not await _enabled():
            _watch.clear()
            continue
        if not _watch:
            continue

        cfg = await get_params(
            "migration_dip_pct", "migration_min_liq_usd", "migration_size_sol",
            "migration_tp_x", "migration_sl_pct", "migration_watch_min",
            "max_open_paper_trades",
        )
        dip_pct = float(cfg.get("migration_dip_pct") or 20.0)
        min_liq = float(cfg.get("migration_min_liq_usd") or 15000.0)
        size = float(cfg.get("migration_size_sol") or 0.25)
        tp_x = float(cfg.get("migration_tp_x") or 1.8)
        sl_pct = float(cfg.get("migration_sl_pct") or 35.0)
        watch_min = float(cfg.get("migration_watch_min") or 30.0)
        max_open = int(float(cfg.get("max_open_paper_trades") or 5))
        now = _now()

        for mint in list(_watch.keys()):
            w = _watch[mint]
            try:
                # Evict tokens we've watched past the window without a dip-buy.
                if (now - w["first_seen"]) > watch_min * 60.0:
                    _watch.pop(mint, None)
                    continue
                if w["bought"]:
                    continue

                pair = await fetch_token_data(mint, allow_any_dex=True)
                if pair is None:
                    continue
                m = parse_token_metrics(pair)
                mc = float(m.get("market_cap") or 0)
                liq = float(m.get("liquidity_usd") or 0)
                if mc <= 0:
                    continue

                # Baseline = the first MC we see post-migration.
                if w["migration_mc"] is None:
                    w["migration_mc"] = mc
                    w["low_mc"] = mc
                    if not w.get("name"):
                        w["name"] = m.get("name") or m.get("symbol") or mint[:6]
                    continue
                w["low_mc"] = min(w["low_mc"] or mc, mc)

                drop = (w["migration_mc"] - mc) / w["migration_mc"] * 100.0
                if drop < dip_pct:
                    continue  # not dipped enough yet

                # Gates
                if liq < min_liq:
                    logger.info("migration_sniper: %s dipped %.0f%% but liq $%.0f < $%.0f",
                                mint[:12], drop, liq, min_liq)
                    continue
                if await has_open_paper_trade(mint):
                    w["bought"] = True
                    continue
                if await count_open_paper_trades() >= max_open:
                    continue

                # Buy the dip
                name = w.get("name") or m.get("name") or mint[:6]
                try:
                    bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                    if bal < size + 0.02:
                        continue
                    await open_paper_trade(
                        token_address=mint, token_name=name,
                        entry_mc=mc, entry_price=mc, paper_sol=size,
                        confidence=70.0, pattern_type="migration_dip",
                        tp_x=tp_x, sl_pct=sl_pct,
                        trade_reasoning=(f"Migration dip buy — graduated then dipped "
                                         f"{drop:.0f}% from ${w['migration_mc']/1000:.0f}K"),
                    )
                    w["bought"] = True
                    state.paper_trades_today += 1
                    logger.info("migration_sniper: BUY %s — dipped %.0f%% to $%.0fK | %.2f SOL",
                                name[:16], drop, mc / 1000, size)
                    await _alert(mint, name, w["migration_mc"], mc, drop, liq, size, tp_x, sl_pct)
                except Exception as exc:
                    logger.error("migration_sniper: buy failed %s: %s", mint[:12], exc)
            except Exception as exc:
                logger.debug("migration_sniper: poll error %s: %s", mint[:12], exc)


async def _alert(mint, name, mig_mc, mc, drop, liq, size, tp_x, sl_pct) -> None:
    """Post the MIGRATION DIP entry card to the caller group + community."""
    try:
        from bot.config import CALLER_GROUP_ID, SCAN_TOPIC_ID
        from bot.community_feed import post_to_community
        bot_ref = getattr(state, "bot", None)
        if bot_ref is None:
            return
        text = (
            f"🎓 <b>MIGRATION DIP BUY</b> — {_esc(str(name)[:22])}\n\n"
            f"Graduated → dipped <b>{drop:.0f}%</b>\n"
            f"🪙 Migration MC: ${mig_mc/1000:.0f}K → now <b>${mc/1000:.0f}K</b>\n"
            f"💧 Liquidity: ${liq/1000:.0f}K | Size: {size:.2f} SOL\n\n"
            f"⚡ <b>Strategy:</b> Post-graduation dip\n"
            f"TP <b>{tp_x:.1f}x</b> (+{(tp_x-1)*100:.0f}%) | SL <b>{sl_pct:.0f}%</b>"
        )
        try:
            await bot_ref.send_message(CALLER_GROUP_ID, text,
                                       message_thread_id=SCAN_TOPIC_ID, parse_mode="HTML")
        except Exception:
            pass
        try:
            await post_to_community(bot_ref, text, parse_mode="HTML")
        except Exception:
            pass
    except Exception as exc:
        logger.debug("migration_sniper: alert failed: %s", exc)


async def migration_sniper_loop() -> None:
    """Stream PumpPortal migration events. Auto-reconnects. Gated off by
    migration_sniper_enabled (default 0)."""
    await asyncio.sleep(STARTUP_DELAY)
    asyncio.create_task(_poll_loop())
    logger.info("migration_sniper: started (gated by migration_sniper_enabled)")

    while True:
        if not await _enabled():
            await asyncio.sleep(ENABLE_RECHECK_SEC)
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                    await ws.send_json({"method": "subscribeMigration"})
                    logger.info("migration_sniper: connected — subscribed to migrations")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if not await _enabled():
                                logger.info("migration_sniper: disabled — closing stream")
                                break
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(data, dict) and data.get("mint"):
                                _handle_migration(data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            logger.warning("migration_sniper: connection closed")
                            break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("migration_sniper: connection error: %s — reconnecting in %ds",
                           exc, RECONNECT_DELAY)
        except Exception as exc:
            logger.error("migration_sniper: loop error: %s", exc)
        await asyncio.sleep(RECONNECT_DELAY)
