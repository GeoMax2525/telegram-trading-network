"""
algo_engine.py — runs new pump.fun tokens through each enabled custom algo's
filter set (X-FILES, ZANZIBAR, …). A match in AUTO mode opens a trade
(independent of the scanner, alongside 4am); MANUAL mode alerts the operator.

Data source: pump.fun's coins endpoint gives MC, age, description, socials, NSFW.
Growth % is computed from a tracked first-seen MC baseline. VIEWERS is Photon-only
(no clean API) — that single filter is skipped until a feed is wired; everything
else runs today.

Own PumpPortal subscribeNewToken stream → watchlist → poll → match → trade/alert.
Fully independent: gated by algo_engine_enabled and per-algo mode.
"""

import asyncio
import json
import logging
import time as _time

import aiohttp

from bot import state

logger = logging.getLogger(__name__)

WS_URL = "wss://pumpportal.fun/api/data"
COIN_URL = "https://frontend-api-v3.pump.fun/coins/{ca}"
_HEADERS = {"accept": "application/json", "user-agent": "Mozilla/5.0"}
STARTUP_DELAY = 60
RECONNECT_DELAY = 5
POLL_INTERVAL = 12
MAX_WATCH = 300
ENABLE_RECHECK = 60

# mint -> {"first_mc": float, "first_seen": ts, "name": str, "matched": set}
_watch: dict[str, dict] = {}


async def _enabled() -> bool:
    try:
        from database.models import get_param
        v = await get_param("algo_engine_enabled")
        return bool(v and v >= 0.5)
    except Exception:
        return False


async def fetch_pumpfun_coin(mint: str) -> dict | None:
    """Pull a token's social/meta data from pump.fun. Returns a normalized dict
    or None. Covers every algo filter except viewers (Photon-only)."""
    url = COIN_URL.format(ca=mint)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_HEADERS,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                d = await r.json()
    except Exception:
        return None
    created = d.get("created_timestamp") or 0
    age_min = ((_time.time() * 1000 - created) / 1000 / 60) if created else 0
    return {
        "mint": mint,
        "name": d.get("name") or d.get("symbol") or mint[:6],
        "mc": float(d.get("usd_market_cap") or 0),
        "age_min": age_min,
        "desc_len": len(d.get("description") or ""),
        "twitter": bool(d.get("twitter")),
        "telegram": bool(d.get("telegram")),
        "website": bool(d.get("website")),
        "nsfw": bool(d.get("nsfw")),
        "reply_count": int(d.get("reply_count") or 0),
    }


def _matches(algo, data: dict, growth_pct: float | None) -> bool:
    """True if the token passes EVERY threshold this algo defines. Filters with
    no data (viewers, or growth before a baseline) are skipped, not failed."""
    mc = data["mc"]
    if algo.min_mc and mc < algo.min_mc:
        return False
    if algo.max_mc and mc > algo.max_mc:
        return False
    if algo.min_age_min and data["age_min"] < algo.min_age_min:
        return False
    if algo.max_age_min and data["age_min"] > algo.max_age_min:
        return False
    if algo.require_twitter and not data["twitter"]:
        return False
    if algo.require_telegram and not data["telegram"]:
        return False
    if algo.min_desc_len and data["desc_len"] < algo.min_desc_len:
        return False
    if algo.nsfw_filter and data["nsfw"]:
        return False
    if algo.min_growth_pct and growth_pct is not None and growth_pct < algo.min_growth_pct:
        return False
    # min_viewers: Photon-only — skipped until a viewer feed is wired.
    return True


# pump.fun discovery endpoints (frontend-api-v3) — every useful token source.
# We already hit /coins/{ca} successfully from Railway, so these same-domain
# list endpoints should work with the same headers. Defensive: any that 401/403
# (auth/Cloudflare) just log + skip without breaking the others.
_API = "https://frontend-api-v3.pump.fun"
_DISCOVERY = [
    ("/coins/latest", {}),                                                   # freshest launches
    ("/coins/currently-live", {"limit": 50, "includeNsfw": "true"}),          # actively trading
    ("/coins", {"sort": "about_to_graduate", "order": "DESC",                 # near graduation
                "limit": 50, "includeNsfw": "true"}),
    ("/coins", {"sort": "last_trade_timestamp", "order": "DESC",              # most active
                "limit": 50, "includeNsfw": "true"}),
    ("/coins", {"sort": "reply_count", "order": "DESC",                       # most engaged
                "limit": 50, "includeNsfw": "true"}),
    ("/coins/king-of-the-hill", {}),                                          # KOTH
]


async def _fetch_coin_list(path: str, params: dict) -> list[dict]:
    """Hit a pump.fun discovery endpoint; return a list of coin dicts (handles
    both list and single-object responses). Never raises."""
    url = _API + path
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params, headers=_HEADERS,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.debug("algo_engine: %s → HTTP %s", path, r.status)
                    return []
                d = await r.json(content_type=None)
    except Exception as exc:
        logger.debug("algo_engine: discovery %s failed: %s", path, exc)
        return []
    if isinstance(d, dict):
        # Some endpoints wrap the list; KOTH returns a single coin.
        if d.get("mint"):
            return [d]
        for k in ("coins", "data", "results"):
            if isinstance(d.get(k), list):
                return d[k]
        return []
    return d if isinstance(d, list) else []


async def _discovery_loop() -> None:
    """Poll pump.fun's discovery endpoints and feed the algo watchlist — so the
    algos see newest + trending + about-to-graduate + most-active tokens, not
    just the block-0 firehose."""
    from database.models import get_params
    while True:
        await asyncio.sleep(20)
        if not await _enabled():
            continue
        interval = float((await get_params("algo_discovery_interval_sec"))
                         .get("algo_discovery_interval_sec") or 30.0)
        added = 0
        for path, params in _DISCOVERY:
            for coin in await _fetch_coin_list(path, params):
                mint = coin.get("mint")
                if not mint or mint in _watch:
                    continue
                if coin.get("complete"):   # already graduated — algos want pre-grad
                    continue
                _handle_new_token(coin)
                added += 1
            await asyncio.sleep(1)   # be gentle on Cloudflare
        if added:
            logger.info("algo_engine: discovery added %d tokens from pump.fun", added)
        await asyncio.sleep(max(interval - len(_DISCOVERY), 5))


def _handle_new_token(d: dict) -> None:
    mint = d.get("mint")
    if not mint or mint in _watch:
        return
    from bot.scanner import mint_suffix_ok
    if not mint_suffix_ok(mint):
        return
    if len(_watch) >= MAX_WATCH:
        oldest = min(_watch, key=lambda m: _watch[m]["first_seen"])
        _watch.pop(oldest, None)
    _watch[mint] = {"first_mc": None, "first_seen": _time.time(),
                    "name": d.get("name") or d.get("symbol"), "matched": set()}


async def _poll_loop() -> None:
    from database.models import get_all_algos, get_params, open_paper_trade, \
        has_open_paper_trade, count_open_paper_trades, compute_paper_balance
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        if not await _enabled():
            _watch.clear()
            continue
        algos = [a for a in await get_all_algos() if a.mode in ("manual", "auto")]
        if not algos:
            continue
        cfg = await get_params("algo_size_sol", "algo_watch_min",
                               "algo_tp_x", "algo_sl_pct", "max_open_paper_trades")
        size = float(cfg.get("algo_size_sol") or 0.2)
        watch_min = float(cfg.get("algo_watch_min") or 30.0)
        tp_x = float(cfg.get("algo_tp_x") or 5.0)
        sl_pct = float(cfg.get("algo_sl_pct") or 25.0)
        max_open = int(float(cfg.get("max_open_paper_trades") or 5))
        now = _time.time()

        for mint in list(_watch.keys()):
            w = _watch[mint]
            try:
                if (now - w["first_seen"]) > watch_min * 60:
                    _watch.pop(mint, None)
                    continue
                data = await fetch_pumpfun_coin(mint)
                if data is None or data["mc"] <= 0:
                    continue
                if w["first_mc"] is None:
                    w["first_mc"] = data["mc"]
                growth = ((data["mc"] - w["first_mc"]) / w["first_mc"] * 100
                          if w["first_mc"] else None)

                for algo in algos:
                    if algo.name in w["matched"]:
                        continue
                    if not _matches(algo, data, growth):
                        continue
                    w["matched"].add(algo.name)
                    if algo.mode == "manual":
                        await _alert(algo, data, growth)
                    elif algo.mode == "auto":
                        if await has_open_paper_trade(mint):
                            continue
                        if await count_open_paper_trades() >= max_open:
                            continue
                        bal = await compute_paper_balance(state.PAPER_STARTING_BALANCE)
                        if bal < size + 0.02:
                            continue
                        try:
                            await open_paper_trade(
                                token_address=mint, token_name=data["name"],
                                entry_mc=data["mc"], entry_price=data["mc"],
                                paper_sol=size, confidence=70.0,
                                pattern_type=f"algo:{algo.name}", tp_x=tp_x, sl_pct=sl_pct,
                                trade_reasoning=f"Algo {algo.name} match — "
                                                f"MC ${data['mc']/1000:.0f}K, growth {growth or 0:.0f}%",
                                channel_name=algo.name,
                            )
                            state.paper_trades_today += 1
                            logger.info("algo_engine: %s AUTO-BUY %s — MC $%.0fK",
                                        algo.name, data["name"][:16], data["mc"]/1000)
                            await _alert(algo, data, growth, bought=True)
                        except Exception as exc:
                            logger.error("algo_engine: buy failed %s: %s", mint[:12], exc)
            except Exception as exc:
                logger.debug("algo_engine: poll %s: %s", mint[:12], exc)


async def _alert(algo, data, growth, bought=False) -> None:
    try:
        from bot.config import CALLER_GROUP_ID, SCAN_TOPIC_ID
        from bot.community_feed import post_to_community
        from html import escape as _esc
        bot_ref = getattr(state, "bot", None)
        if bot_ref is None:
            return
        head = (f"🧪 <b>{_esc(algo.name)} — AUTO BUY</b>" if bought
                else f"🧪 <b>{_esc(algo.name)} — SIGNAL</b> (manual)")
        soc = " ".join(s for s, ok in [("🐦", data["twitter"]), ("✈️", data["telegram"]),
                                       ("🌐", data["website"])] if ok)
        text = (f"{head}\n\n"
                f"🪙 {_esc(str(data['name'])[:22])}\n"
                f"MC ${data['mc']/1000:.0f}K · age {data['age_min']:.0f}m · "
                f"growth {growth or 0:.0f}%\n"
                f"{data['desc_len']} char desc · {data['reply_count']} replies {soc}\n"
                f"<code>{_esc(data['mint'])}</code>")
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
        logger.debug("algo_engine: alert failed: %s", exc)


async def algo_engine_loop() -> None:
    """Stream new pump.fun tokens and run them through the enabled algos.
    Independent of the scanner. Gated by algo_engine_enabled."""
    await asyncio.sleep(STARTUP_DELAY)
    asyncio.create_task(_poll_loop())
    asyncio.create_task(_discovery_loop())   # pump.fun endpoint discovery
    logger.info("algo_engine started (gated by algo_engine_enabled)")
    while True:
        if not await _enabled():
            await asyncio.sleep(ENABLE_RECHECK)
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, heartbeat=30) as ws:
                    await ws.send_json({"method": "subscribeNewToken"})
                    logger.info("algo_engine: connected — streaming new tokens")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if not await _enabled():
                                break
                            try:
                                d = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(d, dict) and d.get("mint"):
                                _handle_new_token(d)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("algo_engine: conn error %s — reconnect in %ds", exc, RECONNECT_DELAY)
        except Exception as exc:
            logger.error("algo_engine: loop error: %s", exc)
        await asyncio.sleep(RECONNECT_DELAY)
