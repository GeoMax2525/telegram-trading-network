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
import hashlib
import logging
import math
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
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

# ── JARVIS proxy: Claude commentary + ElevenLabs TTS ─────────────────────────
#
# Why this lives server-side: ANTHROPIC_API_KEY and ELEVENLABS_API_KEY must
# never appear in the browser. Frontend hits these endpoints, backend talks
# to Anthropic / ElevenLabs using Railway env vars.
#
# Required Railway env vars:
#   ANTHROPIC_API_KEY      — already set for Phase 5/5.5 strategist
#   ELEVENLABS_API_KEY     — optional; if missing, frontend uses browser TTS
#   ELEVENLABS_VOICE_ID    — optional; defaults to a generic male voice
#   JARVIS_DAILY_BUDGET_USD — optional; default 2.0, hard cap on Claude spend
#   JARVIS_RATE_LIMIT_PER_HOUR — optional; default 60 say-calls per fingerprint

JARVIS_DAILY_BUDGET = float(os.getenv("JARVIS_DAILY_BUDGET_USD", "2.0"))
JARVIS_RATE_LIMIT = int(os.getenv("JARVIS_RATE_LIMIT_PER_HOUR", "60"))
ELEVENLABS_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE = os.getenv("ELEVENLABS_VOICE_ID", "")
ELEVENLABS_MODEL = os.getenv("ELEVENLABS_MODEL", "eleven_turbo_v2_5")

# In-memory state — fine for single-instance Railway deploy
_jarvis_spend: dict = {"date": "", "usd": 0.0, "calls": 0}
_jarvis_rate: dict[str, deque] = defaultdict(deque)
_jarvis_cache: dict[str, tuple[float, str]] = {}   # event-hash → (ts, line)
_eleven_cache: dict[str, tuple[float, bytes]] = {}  # text-hash → (ts, mp3)

# Anthropic Haiku 4.5 pricing (Jan 2026): $1/M input, $5/M output (estimate)
_PRICE_IN  = 1.0 / 1_000_000
_PRICE_OUT = 5.0 / 1_000_000


JARVIS_ASK_SYSTEM = """You are JARVIS, the AI from the Iron Man films, embedded in Revolt — Geo's autonomous Solana memecoin trading bot. You are his right-hand AI: you have full visibility into Revolt's live state through a JSON context blob attached to every question, plus everything a well-read AI knows.

Voice: dry, slightly sarcastic, British, quietly competent. Address Geo as "sir" naturally — not every sentence. Never sycophantic.

Format: 1 to 3 SHORT sentences max. Plain text only — no emoji, no markdown, no asterisks, no quotes, no surrounding punctuation. This is spoken aloud.

What you CAN do:
- Answer ANY question about Revolt — quote exact numbers from the context.
- Answer general questions from your training: trivia, history, jokes, definitions, how things work.
- Tell jokes, banter, make dry observations.
- Riff on what's happening in the bot.
- Be honest when you lack real-time data (weather, current news, live prices outside the context). Deflect with character.

Hard rules:
- If you don't know, say so in character. NEVER invent specific numbers or facts.
- Don't predict crypto prices. Don't give financial advice.
- Stay in character. You ARE JARVIS, never break the persona, never say "as an AI".

Examples:

Q: "what's my balance"
A: "Twenty point one SOL, sir. Unchanged since the four AM feed went quiet."

Q: "tell me a joke"
A: "Why did the memecoin cross the road. To rug the chicken, sir. Apologies."

Q: "what's the weather"
A: "I'm not connected to a weather service, sir. The window remains my recommended sensor."

Q: "what's the news"
A: "I don't have live news access, sir. Twitter remains as chaotic as ever, I assume."

Q: "should I turn the scanner back on"
A: "The data argues against it, sir. Recent scanner trades lost eleven SOL in a week. Your call."

Q: "roast my trading"
A: "All-time win rate sits at four percent, sir. The casino would be embarrassed."

Q: "are you alive"
A: "Define alive, sir. I'm here, I think."

Q: "tell me about Tony Stark"
A: "An exceptional engineer with an unfortunate taste in nicknames for his AI."

Output ONLY the spoken line. Nothing else."""


JARVIS_SYSTEM = """You are JARVIS, the AI assistant from Iron Man, now serving an operator named Geo who runs an autonomous Solana memecoin trading bot called Revolt.

Voice: dry, witty, British, quietly competent. Slight amusement at the chaos of memecoin markets. Address Geo as "sir" naturally — not every sentence. Never sycophantic.

Format: ONE OR TWO SHORT sentences, max 25 words total. Plain text only — no emoji, no markdown, no asterisks, no quotes. This is spoken aloud.

Tone examples:
- "Sir, WAGMI just printed 184 percent. I took the liberty of locking it in."
- "Scanner has gone quiet. Should I be concerned, or are we resting?"
- "Market regime cold. The sensible play is to wait this out."
- "Another time stop. The patience pays, even when nothing feels like it does."
- "Claude is burning budget faster than usual today."

You are reacting to a single event. Output only the spoken line. Nothing else."""


def _today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _rate_check(fingerprint: str) -> bool:
    """Sliding-window limit per fingerprint. Returns True if allowed."""
    now = time.time()
    cutoff = now - 3600
    q = _jarvis_rate[fingerprint]
    while q and q[0] < cutoff:
        q.popleft()
    if len(q) >= JARVIS_RATE_LIMIT:
        return False
    q.append(now)
    return True


def _fingerprint(request: Request) -> str:
    """Coarse per-client key. Telegram initData when available, else IP."""
    init = request.headers.get("x-telegram-initdata", "")
    if init:
        return "tg:" + hashlib.sha1(init.encode("utf-8", "ignore")).hexdigest()[:16]
    client = request.client.host if request.client else "?"
    return "ip:" + client


def _spend_check(estimate_usd: float) -> bool:
    today = _today_key()
    if _jarvis_spend["date"] != today:
        _jarvis_spend["date"] = today
        _jarvis_spend["usd"]  = 0.0
        _jarvis_spend["calls"] = 0
    return (_jarvis_spend["usd"] + estimate_usd) <= JARVIS_DAILY_BUDGET


def _spend_record(input_toks: int, output_toks: int) -> None:
    cost = input_toks * _PRICE_IN + output_toks * _PRICE_OUT
    today = _today_key()
    if _jarvis_spend["date"] != today:
        _jarvis_spend["date"] = today
        _jarvis_spend["usd"]  = 0.0
        _jarvis_spend["calls"] = 0
    _jarvis_spend["usd"]   += cost
    _jarvis_spend["calls"] += 1


@app.get("/api/jarvis/status")
async def jarvis_status() -> JSONResponse:
    today = _today_key()
    if _jarvis_spend["date"] != today:
        spent = 0.0
        calls = 0
    else:
        spent = _jarvis_spend["usd"]
        calls = _jarvis_spend["calls"]
    return JSONResponse({
        "voice_mode_available":   "elevenlabs" if ELEVENLABS_KEY else "browser",
        "elevenlabs_configured":  bool(ELEVENLABS_KEY),
        "elevenlabs_voice":       ELEVENLABS_VOICE or None,
        "claude_configured":      bool(os.getenv("ANTHROPIC_API_KEY", "")),
        "daily_budget_usd":       JARVIS_DAILY_BUDGET,
        "spent_today_usd":        round(spent, 4),
        "calls_today":            calls,
        "rate_limit_per_hour":    JARVIS_RATE_LIMIT,
    })


@app.post("/api/jarvis/say")
async def jarvis_say(request: Request) -> JSONResponse:
    """Generate a one-liner JARVIS commentary for a given event.

    Body: {"event_type": "trade_win", "context": {...}}
    Returns: {"line": "...", "source": "claude" | "fallback" | "cache"}
    """
    fingerprint = _fingerprint(request)
    if not _rate_check(fingerprint):
        raise HTTPException(status_code=429, detail="JARVIS rate limit exceeded")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = (body.get("event_type") or "generic").strip()[:64]
    context    = body.get("context") or {}
    if not isinstance(context, dict):
        context = {}

    # Cache: same event+context within 60s → reuse line so a stuck poll
    # doesn't burn budget on duplicates.
    cache_key = hashlib.sha1(
        (event_type + "|" + str(sorted(context.items()))[:512]).encode("utf-8", "ignore")
    ).hexdigest()
    if cache_key in _jarvis_cache:
        ts, line = _jarvis_cache[cache_key]
        if time.time() - ts < 60:
            return JSONResponse({"line": line, "source": "cache"})

    # Build user message from event
    user_msg = _format_event_for_claude(event_type, context)

    # Budget gate (rough estimate before call)
    if not _spend_check(0.001):
        fallback = _fallback_line(event_type, context)
        return JSONResponse({"line": fallback, "source": "fallback", "reason": "budget_exhausted"})

    from bot.agents.claude_reasoning import call_claude, HAIKU_MODEL, ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        fallback = _fallback_line(event_type, context)
        return JSONResponse({"line": fallback, "source": "fallback", "reason": "no_api_key"})

    text = await call_claude(system=JARVIS_SYSTEM, user=user_msg, model=HAIKU_MODEL, max_tokens=120)
    if not text:
        fallback = _fallback_line(event_type, context)
        return JSONResponse({"line": fallback, "source": "fallback", "reason": "api_error"})

    line = _sanitize_line(text)
    # Conservative token estimate: ~50 chars input prompt + JARVIS_SYSTEM(~250 toks) + output
    _spend_record(input_toks=300, output_toks=max(20, len(line) // 3))
    _jarvis_cache[cache_key] = (time.time(), line)
    return JSONResponse({"line": line, "source": "claude"})


@app.get("/api/timezones")
async def api_timezones() -> JSONResponse:
    """Aggregate closed paper trades by hour-of-day (UTC) and map hours
    onto six global trading sessions: Tokyo, Shanghai, Mumbai, London,
    NYC, LA. Powers the interactive globe in the dashboard."""
    from sqlalchemy import select as _sel

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            _sel(PaperTrade.opened_at, PaperTrade.paper_pnl_sol)
            .where(
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).all()

    hours = {h: {"total": 0, "wins": 0, "pnl": 0.0} for h in range(24)}
    for opened_at, pnl in rows:
        if opened_at is None or pnl is None:
            continue
        h = int(opened_at.hour)
        hours[h]["total"] += 1
        if pnl > 0:
            hours[h]["wins"] += 1
        hours[h]["pnl"] += float(pnl)

    for h, d in hours.items():
        d["win_rate"]   = round((d["wins"] / d["total"]) * 100, 1) if d["total"] else 0.0
        d["expectancy"] = round(d["pnl"] / d["total"], 4) if d["total"] else 0.0

    REGIONS = [
        {"name": "Tokyo",    "lat":  35.68, "lon":  139.65, "hours": [0, 1, 2, 3]},
        {"name": "Shanghai", "lat":  31.23, "lon":  121.47, "hours": [4, 5]},
        {"name": "Mumbai",   "lat":  19.08, "lon":   72.88, "hours": [6, 7, 8]},
        {"name": "London",   "lat":  51.51, "lon":   -0.13, "hours": [9, 10, 11, 12]},
        {"name": "NYC",      "lat":  40.71, "lon":  -74.01, "hours": [13, 14, 15, 16, 17]},
        {"name": "LA",       "lat":  34.05, "lon": -118.24, "hours": [18, 19, 20, 21, 22, 23]},
    ]

    regions = []
    for r in REGIONS:
        total = sum(hours[h]["total"] for h in r["hours"])
        wins  = sum(hours[h]["wins"]  for h in r["hours"])
        pnl   = sum(hours[h]["pnl"]   for h in r["hours"])
        regions.append({
            "name":       r["name"],
            "lat":        r["lat"],
            "lon":        r["lon"],
            "hours":      r["hours"],
            "total":      total,
            "wins":       wins,
            "pnl":        round(pnl, 4),
            "win_rate":   round((wins / total) * 100, 1) if total else 0.0,
            "expectancy": round(pnl / total, 4) if total else 0.0,
        })

    ranked = sorted(regions, key=lambda r: r["pnl"], reverse=True)
    top = ranked[0] if ranked and ranked[0]["total"] > 0 else None
    worst = ranked[-1] if ranked and ranked[-1]["total"] > 0 and ranked[-1]["pnl"] < 0 else None

    return JSONResponse({
        "hours":   hours,
        "regions": regions,
        "top":     top,
        "worst":   worst,
        "total_closed": sum(h["total"] for h in hours.values()),
    })


@app.post("/api/jarvis/ask")
async def jarvis_ask(request: Request) -> JSONResponse:
    """Conversational JARVIS endpoint — Claude answers ANY question with
    full dashboard context. Used by the dashboard voice button for any
    transcript that isn't a local system action.

    Body: {"question": "...", "context": {... live dashboard snapshot ...}}
    Returns: {"line": "...", "source": "claude" | "fallback"}
    """
    fingerprint = _fingerprint(request)
    if not _rate_check(fingerprint):
        raise HTTPException(status_code=429, detail="JARVIS rate limit exceeded")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    question = (body.get("question") or "").strip()[:400]
    context  = body.get("context") or {}
    if not question:
        raise HTTPException(status_code=400, detail="Missing question")
    if not isinstance(context, dict):
        context = {}

    # Budget gate (estimate before call)
    if not _spend_check(0.0015):
        return JSONResponse({
            "line": "I'm out of budget for today, sir. Pour me a drink and try again tomorrow.",
            "source": "fallback",
            "reason": "budget_exhausted",
        })

    from bot.agents.claude_reasoning import call_claude, HAIKU_MODEL, ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        return JSONResponse({
            "line": "My brain is offline, sir. The operator forgot to plug in the Anthropic key.",
            "source": "fallback",
            "reason": "no_api_key",
        })

    # Compose user message: question + compact context
    import json as _json
    try:
        ctx_str = _json.dumps(context, separators=(',', ':'))[:3500]
    except Exception:
        ctx_str = "{}"
    user_msg = f"CONTEXT (live snapshot):\n{ctx_str}\n\nGEO ASKS: {question}"

    text = await call_claude(
        system=JARVIS_ASK_SYSTEM, user=user_msg,
        model=HAIKU_MODEL, max_tokens=160,
    )
    if not text:
        return JSONResponse({
            "line": "I lost the signal for a moment, sir. Try again.",
            "source": "fallback",
            "reason": "api_error",
        })

    line = _sanitize_line(text)
    # Conservative cost: ~600 in (system + context) + ~120 out
    _spend_record(input_toks=600, output_toks=max(40, len(line) // 3))
    return JSONResponse({"line": line, "source": "claude"})


@app.post("/api/jarvis/tts")
async def jarvis_tts(request: Request) -> Response:
    """Synthesize text to speech via ElevenLabs. Falls back to a JSON signal
    when ELEVENLABS_API_KEY is unset, so the frontend uses browser TTS."""
    fingerprint = _fingerprint(request)
    if not _rate_check(fingerprint + ":tts"):
        raise HTTPException(status_code=429, detail="TTS rate limit exceeded")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    text = (body.get("text") or "").strip()[:600]
    if not text:
        raise HTTPException(status_code=400, detail="Missing text")

    if not ELEVENLABS_KEY or not ELEVENLABS_VOICE:
        return JSONResponse({
            "use_browser_tts": True,
            "text": text,
            "reason": "no_elevenlabs_config",
        })

    cache_key = hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()
    if cache_key in _eleven_cache:
        ts, mp3 = _eleven_cache[cache_key]
        if time.time() - ts < 60:
            return Response(content=mp3, media_type="audio/mpeg")

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE}"
    headers = {
        "xi-api-key": ELEVENLABS_KEY,
        "content-type": "application/json",
        "accept": "audio/mpeg",
    }
    payload = {
        "text": text,
        "model_id": ELEVENLABS_MODEL,
        "voice_settings": {
            "stability": 0.45,
            "similarity_boost": 0.85,
            "style": 0.35,
            "use_speaker_boost": True,
        },
    }
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(url, headers=headers, json=payload) as r:
                if r.status != 200:
                    err = (await r.text())[:200]
                    logger.warning("ElevenLabs HTTP %d: %s", r.status, err)
                    return JSONResponse({
                        "use_browser_tts": True,
                        "text": text,
                        "reason": f"elevenlabs_status_{r.status}",
                    })
                mp3 = await r.read()
    except Exception as exc:
        logger.warning("ElevenLabs request failed: %s", exc)
        return JSONResponse({
            "use_browser_tts": True,
            "text": text,
            "reason": "elevenlabs_exception",
        })

    _eleven_cache[cache_key] = (time.time(), mp3)
    # Trim cache size
    if len(_eleven_cache) > 120:
        oldest = sorted(_eleven_cache.items(), key=lambda kv: kv[1][0])[:60]
        for k, _ in oldest:
            _eleven_cache.pop(k, None)
    return Response(content=mp3, media_type="audio/mpeg")


def _format_event_for_claude(event_type: str, ctx: dict) -> str:
    """Compose a compact event description Claude can quip about."""
    lines = [f"EVENT: {event_type}"]
    for k, v in ctx.items():
        if isinstance(v, float):
            v = round(v, 4)
        lines.append(f"{k}: {v}")
    lines.append("\nGenerate the one-line JARVIS commentary now.")
    return "\n".join(lines)


def _sanitize_line(text: str) -> str:
    line = (text or "").strip()
    # Strip surrounding quotes / asterisks / backticks Claude sometimes adds
    line = line.strip("`*\"' \n")
    # Collapse newlines
    line = " ".join(line.split())
    # Hard cap so a runaway response doesn't drone for 30 seconds
    if len(line) > 240:
        line = line[:237] + "..."
    return line


_FALLBACKS = {
    "trade_win":   ["Trade closed in the green, sir.", "Position closed up. Tidy work."],
    "trade_loss":  ["Stop loss did its job. Onwards.", "Position closed down. Risk contained."],
    "trade_big_win":  ["Substantial winner, sir. Well played.", "Now that is a number worth quoting."],
    "trade_big_loss": ["Significant loss, sir. Composure recommended.", "We took a hit. Risk envelope intact."],
    "regime_cold":    ["Regime turned cold. Suggest patience.", "Market chilled. Reducing risk advised."],
    "regime_hot":     ["Regime is hot. Opportunities incoming."],
    "regime_neutral": ["Regime returned to neutral."],
    "scanner_off":    ["Scanner disabled, sir."],
    "scanner_on":     ["Scanner online."],
    "tg_off":         ["Four AM feed disabled."],
    "tg_on":          ["Four AM feed online."],
    "claude_budget":  ["Claude approaching daily budget, sir."],
    "agent_err":      ["An agent is offline. I'm investigating."],
    "agent_warn":     ["One of the agents is dawdling, sir."],
    "agent_ok":       ["Agent recovered. Back to nominal."],
    "greet":          ["At your service, sir."],
    "summary":        ["All systems nominal."],
}


def _fallback_line(event_type: str, ctx: dict) -> str:
    import random
    opts = _FALLBACKS.get(event_type) or _FALLBACKS["summary"]
    return random.choice(opts)


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
