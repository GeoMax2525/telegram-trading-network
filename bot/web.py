"""
bot/web.py — FastAPI dashboard backend.

Serves the static cyberpunk dashboard at `/` and a consolidated JSON payload
at `/api/dashboard`. Runs in the same asyncio event loop as the aiogram bot
(spawned from main.py) so it shares the same DB pool and runtime state.

Read-only. No auth gating in v1 — all data shown is the same operator-visible
data the /hub command already exposes in Telegram. Telegram WebApp initData
is read and stashed in request state for future per-user gating.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from bot import state
from database.models import (
    AgentLog,
    AgentParam,
    AsyncSessionLocal,
    ClaudeEntryDecision,
    PaperTrade,
    Wallet,
    compute_paper_balance,
    count_open_paper_trades,
    get_all_params,
    get_hub_stats,
    get_recent_paper_trades,
    get_top_wallets,
)
from sqlalchemy import desc, func, select

logger = logging.getLogger(__name__)

# ── App + paths ───────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"

app = FastAPI(title="Revolt Trading Hub", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Static dashboard
if DASHBOARD_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")

START_TS = time.time()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(str(DASHBOARD_DIR / "index.html"))


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "uptime_s": int(time.time() - START_TS)}


# ── SOL price (Live Market Pulse) ─────────────────────────────────────────────

_SOL_CACHE: dict = {"price": 0.0, "change_24h": 0.0, "fetched_at": 0.0}
_SOL_HISTORY: deque[tuple[float, float]] = deque(maxlen=60)  # (ts, price)


async def _refresh_sol_price() -> None:
    """Pull SOL price from CoinGecko (free, ~30 req/min)."""
    now = time.time()
    if now - _SOL_CACHE["fetched_at"] < 25:
        return  # honor cache, stay well under rate limit
    url = ("https://api.coingecko.com/api/v3/simple/price"
           "?ids=solana&vs_currencies=usd&include_24hr_change=true")
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as sess:
            async with sess.get(url) as r:
                if r.status != 200:
                    return
                data = await r.json()
                sol = data.get("solana") or {}
                price = float(sol.get("usd") or 0.0)
                change = float(sol.get("usd_24h_change") or 0.0)
                if price > 0:
                    _SOL_CACHE.update({"price": price, "change_24h": change, "fetched_at": now})
                    _SOL_HISTORY.append((now, price))
    except Exception as exc:
        logger.debug("SOL price refresh failed: %s", exc)


def _sol_volatility() -> float:
    """Stdev of last 20 price ticks as a 0..1 'volatility intensity'."""
    if len(_SOL_HISTORY) < 4:
        return 0.0
    pts = [p for _, p in list(_SOL_HISTORY)[-20:]]
    mean = sum(pts) / len(pts)
    if mean <= 0:
        return 0.0
    var = sum((p - mean) ** 2 for p in pts) / len(pts)
    stdev_pct = (math.sqrt(var) / mean) * 100  # % of mean
    # Map: 0% → 0.0,  0.5% → ~1.0
    return max(0.0, min(1.0, stdev_pct / 0.5))


@app.get("/api/sol")
async def api_sol() -> JSONResponse:
    await _refresh_sol_price()
    hist = list(_SOL_HISTORY)
    base_ts = hist[0][0] if hist else 0
    points = [{"t": round(t - base_ts, 1), "p": round(p, 4)} for t, p in hist]
    return JSONResponse({
        "price":      round(_SOL_CACHE["price"], 4),
        "change_24h": round(_SOL_CACHE["change_24h"], 2),
        "volatility": round(_sol_volatility(), 3),
        "history":    points,
        "fetched_at": _SOL_CACHE["fetched_at"],
    })


# ── Helpers ───────────────────────────────────────────────────────────────────

# Map dashboard agent slots → (display name, AgentLog row name, meta builder)
AGENT_SLOTS = [
    ("SCN", "SCANNER",    "scanner_agent"),
    ("HRV", "HARVESTER",  "harvester"),
    ("WLT", "WALLETS",    "wallet_analyst"),
    ("GMG", "GMGN",       "gmgn_agent"),
    ("PTN", "PATTERNS",   "pattern_engine"),
    ("CNF", "CONFIDENCE", "confidence_engine"),
    ("LRN", "LEARNING",   "learning_loop"),
    ("CHT", "CHARTS",     "chart_detector"),
]


async def _last_agent_runs() -> dict[str, datetime]:
    """One round-trip — newest run per agent_name."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(AgentLog.agent_name, func.max(AgentLog.run_at))
            .group_by(AgentLog.agent_name)
        )).all()
    return {name: ts for name, ts in rows if name}


def _agent_state(last_run: datetime | None) -> str:
    if last_run is None:
        return "WARN"
    age = (datetime.utcnow() - last_run).total_seconds()
    if age < 180:
        return "ONLINE"
    if age < 900:
        return "WARN"
    return "ERR"


def _fmt_mc(mc: float | None) -> str:
    if not mc:
        return "—"
    if mc >= 1_000_000:
        return f"{mc/1_000_000:.2f}M"
    if mc >= 1_000:
        return f"{mc/1_000:.0f}K"
    return f"{mc:.0f}"


def _trade_x(t: PaperTrade) -> str:
    if t.peak_multiple:
        return f"{t.peak_multiple:.2f}x"
    if t.entry_mc and t.peak_mc:
        return f"{t.peak_mc/t.entry_mc:.2f}x"
    return "—"


def _trade_pnl_pct(t: PaperTrade) -> tuple[float, str]:
    """Returns (signed_pct_float, display_str). Positive = win."""
    if t.paper_pnl_sol and t.paper_sol_spent:
        pct = (t.paper_pnl_sol / t.paper_sol_spent) * 100
        sign = "+" if pct >= 0 else ""
        return pct, f"{sign}{pct:.0f}%"
    return 0.0, "—"


async def _claude_today() -> dict:
    """Today's Claude spend + recent decision rows."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        spend = (await session.execute(
            select(func.coalesce(func.sum(ClaudeEntryDecision.cost_usd), 0.0))
            .where(ClaudeEntryDecision.decided_at >= today)
        )).scalar() or 0.0

        decisions_today = (await session.execute(
            select(func.count(ClaudeEntryDecision.id))
            .where(ClaudeEntryDecision.decided_at >= today)
        )).scalar() or 0

        recent = list((await session.execute(
            select(ClaudeEntryDecision)
            .order_by(desc(ClaudeEntryDecision.id))
            .limit(8)
        )).scalars().all())
    return {"spend_today": float(spend), "count_today": int(decisions_today), "recent": recent}


# ── Main payload ──────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
async def api_dashboard(request: Request) -> JSONResponse:
    # Bot params + balances
    params = await get_all_params()
    starting = float(params.get("paper_starting_balance", 20.0))
    balance = await compute_paper_balance(starting)
    open_count = await count_open_paper_trades()

    # Aggregates
    hub = await get_hub_stats()
    recent = await get_recent_paper_trades(limit=12)
    top_wallets = await get_top_wallets(limit=7)
    last_runs = await _last_agent_runs()
    claude = await _claude_today()

    # Header block
    header = {
        "brand":      "REVOLT",
        "operator":   "GEO",
        "trade_mode": state.trade_mode.upper(),
        "decision":   "AI · CLAUDE" if int(params.get("claude_strategist_enabled", 0)) else "RULES",
        "balance":    round(balance, 4),
        "balance_unit": "SOL",
    }

    # Performance
    closed_today_pnl = float(hub.get("today_pnl") or 0.0)
    alltime_pnl = round(balance - starting, 4)
    trades_today = int(hub.get("trades_today") or 0)
    win_rate = int(hub.get("win_rate") or 0)
    expectancy = round((closed_today_pnl / trades_today), 4) if trades_today else 0.0

    performance = {
        "today_pnl":    round(closed_today_pnl, 4),
        "alltime_pnl":  alltime_pnl,
        "win_rate":     win_rate,
        "open_pos":     open_count,
        "max_open":     int(params.get("paper_max_open_trades", 8)),
        "trades_24h":   trades_today,
        "expectancy":   expectancy,
    }

    # Paper trading
    paper = {
        "balance":     round(balance, 4),
        "starting":    round(starting, 4),
        "balance_pct": round(min(100.0, (balance / starting) * 100), 1) if starting else 0.0,
        "probe_size":  float(params.get("paper_probe_size", 0.2)),
        "max_probe":   float(params.get("paper_max_probe_size", 0.5)),
        "drawdown_pct": round(max(0.0, (1 - balance / starting) * 100), 1) if starting else 0.0,
    }

    # Agents (8 slots)
    agents = []
    for key, name, log_name in AGENT_SLOTS:
        last = last_runs.get(log_name)
        st = _agent_state(last)
        meta_bits = []
        if log_name == "scanner_agent":
            meta_bits.append(f"{int(state.scanner_candidates_today or 0)} cand · 24h")
        elif log_name == "harvester":
            meta_bits.append(f"{int(hub.get('token_count') or 0):,} tokens")
        elif log_name == "wallet_analyst":
            meta_bits.append(f"{int(hub.get('wallet_total') or 0)} tier·active")
        elif log_name == "pattern_engine":
            meta_bits.append(f"{int(hub.get('pattern_total') or 0)} fingerprints")
        elif log_name == "confidence_engine":
            meta_bits.append(f"th · {int(params.get('conf_paper_threshold', 45))}")
        elif log_name == "learning_loop":
            meta_bits.append(f"loop · 60s")
        elif log_name == "chart_detector":
            meta_bits.append(f">{int(params.get('chart_min_mc', 500))}k mc")
        elif log_name == "gmgn_agent":
            meta_bits.append("feed · live")
        # Locked overrides should show "LOCKED"
        if log_name == "scanner_agent" and int(params.get("scanner_enabled", 1)) == 0:
            st = "WARN"
            meta_bits.append("DISABLED")
        if log_name == "harvester" and int(params.get("tg_scraper_enabled", 1)) == 0:
            pass  # tg_scraper, not harvester
        agents.append({
            "key": key, "name": name, "state": st,
            "meta": " · ".join(meta_bits) if meta_bits else "—",
            "last_run": last.isoformat() if last else None,
        })

    # Recent trades — 12 newest, HQ-only
    trades = []
    for t in recent:
        is_open = t.status == "open"
        pnl_pct, pnl_str = _trade_pnl_pct(t)
        if is_open:
            win = None
            reason = "open"
        else:
            win = (t.paper_pnl_sol or 0) > 0
            reason = (t.close_reason or "closed").lower()
        trades.append({
            "id":     t.id,
            "win":    win,
            "open":   is_open,
            "token":  (t.token_name or t.token_address[:6])[:14],
            "mc":     _fmt_mc(t.entry_mc),
            "x":      _trade_x(t),
            "pnl":    pnl_str,
            "pnl_pct": round(pnl_pct, 1),
            "reason": reason,
            "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        })

    # Top wallets
    wallets = []
    for i, w in enumerate(top_wallets, start=1):
        addr = w.address or ""
        short = f"{addr[:4]}..{addr[-4:]}" if len(addr) > 10 else addr
        wallets.append({
            "rank":  i,
            "addr":  short,
            "score": round(float(w.score or 0.0), 1),
            "wl":    f"{int(w.wins or 0)} / {int(w.losses or 0)}",
        })

    # Neural core gauges — rough heuristics from real data
    open_trades_for_calc = await _avg_open_trade_pnl_pct()
    confidence = max(0, min(100, int(params.get("conf_paper_threshold", 45) + 40)))
    # Risk: scale by drawdown + cold streak
    cold = 30 if state.session_cold_streak else 0
    risk = max(0, min(100, int(paper["drawdown_pct"] * 3 + cold)))
    # Conviction: scale by hot streak + recent win rate
    hot = 25 if state.session_hot_streak else 0
    conviction = max(0, min(100, int(win_rate * 0.7 + hot)))
    # Liquidity: derived from regime
    regime_bias = {"HOT": 85, "NEUTRAL": 60, "COLD": 35}.get(state.market_regime, 60)

    neural = {
        "confidence": confidence,
        "risk":       risk,
        "conviction": conviction,
        "liquidity":  regime_bias,
    }

    # Decision stream — interleave recent Claude entries + recent trades
    stream = []
    for d in claude["recent"]:
        verb = "GO" if d.go else "NO"
        size = f" · {d.size_sol:.2f} SOL" if d.size_sol else ""
        tk = (d.token_name or (d.token_address or "?")[:6])[:10]
        stream.append({
            "ts":   d.decided_at.isoformat() if d.decided_at else None,
            "kind": "claude",
            "ok":   bool(d.go),
            "text": f"> CLD · {verb} · {tk}{size}",
        })
    for t in recent[:6]:
        if t.status == "closed":
            tk = (t.token_name or t.token_address[:6])[:10]
            sign = "+" if (t.paper_pnl_sol or 0) >= 0 else ""
            stream.append({
                "ts":   t.closed_at.isoformat() if t.closed_at else None,
                "kind": "close",
                "ok":   (t.paper_pnl_sol or 0) >= 0,
                "text": f"> POS · {tk} · {t.close_reason or '—'} · {sign}{t.paper_pnl_sol or 0:.3f}",
            })
    stream.sort(key=lambda s: s.get("ts") or "", reverse=True)
    stream = stream[:12]

    # System panel
    uptime_s = int(time.time() - START_TS)
    system = {
        "claude_spend":  round(claude["spend_today"], 2),
        "claude_budget": float(params.get("claude_daily_budget_usd", 5.0)),
        "claude_calls":  claude["count_today"],
        "regime":        state.market_regime or "NEUTRAL",
        "uptime_s":      uptime_s,
        "uptime_label":  _fmt_uptime(uptime_s),
        "scanner_on":    bool(int(params.get("scanner_enabled", 1))),
        "tg_scraper_on": bool(int(params.get("tg_scraper_enabled", 1))),
        "trade_mode":    state.trade_mode,
    }

    # Telegram WebApp initData (for future per-user gating)
    init_data = request.headers.get("x-telegram-initdata") or ""
    auth = {"telegram_initdata_present": bool(init_data)}

    return JSONResponse({
        "ts":          datetime.utcnow().isoformat(),
        "header":      header,
        "agents":      agents,
        "performance": performance,
        "paper":       paper,
        "wallets":     wallets,
        "trades":      trades,
        "neural":      neural,
        "stream":      stream,
        "system":      system,
        "auth":        auth,
    })


async def _avg_open_trade_pnl_pct() -> float:
    """Avg paper PnL % across currently-open admin trades."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(PaperTrade.paper_pnl_sol, PaperTrade.paper_sol_spent)
            .where(PaperTrade.subscriber_id.is_(None), PaperTrade.status == "open")
        )).all()
    if not rows:
        return 0.0
    pcts = [(p / s) * 100 for p, s in rows if p is not None and s]
    return round(sum(pcts) / len(pcts), 2) if pcts else 0.0


def _fmt_uptime(s: int) -> str:
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m, _   = divmod(rem, 60)
    return f"{d:02d}d · {h:02d}h · {m:02d}m"


# ── Server bootstrap ──────────────────────────────────────────────────────────

async def run_server(port: int | None = None) -> None:
    """Start uvicorn in the current event loop."""
    import uvicorn
    port = int(port or os.getenv("PORT", "8080"))
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)
    logger.info("Dashboard web server: listening on 0.0.0.0:%d", port)
    await server.serve()
