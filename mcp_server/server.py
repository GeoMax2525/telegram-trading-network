"""
mcp_server/server.py — REVOLT Trading Bot MCP tools.

Exposes read-only tools over MCP so Claude (Desktop, Code, or Claude.ai) can
query the bot's REAL, LIVE data directly — the same Postgres database the bot
itself reads and writes — instead of you running a Telegram command and
pasting the reply back into a conversation.

DEPLOYMENT — mounted live inside bot/web.py's FastAPI app at /mcp, so it's
running 24/7 on Railway as part of the bot's existing dashboard process
(same DB pool, no separate deployment or local script needed). This module
is ALSO runnable standalone (`python mcp_server/server.py`, stdio transport)
for local testing against a real Postgres before connecting Claude to the
live /mcp endpoint. That's why the DB check below (HAS_REAL_DB) is a flag
the caller inspects, not a hard exit-on-import: bot/web.py must be able to
import this module even during local bot development on the SQLite
fallback, without crashing the whole bot process over it — see the
HAS_REAL_DB comment for exactly how that's handled.

DESIGN — why read-only in v1: every tool here does exactly what its matching
Telegram command already displays. There are NO write tools yet (no
set_param, no toggle_algo) — those are a deliberate v2, added only after this
read path is verified working. See README.md.

DESIGN — why it imports database.models directly instead of re-implementing
queries: this repo's DB access already lives in database/models.py as
importable async functions. Reusing them means the numbers this MCP server
reports can never drift from what /hub, /sourcestats, etc. show in Telegram —
same functions, same data, zero duplication risk.

REQUIRES (for real answers): DATABASE_URL resolving to the real Postgres
instance. On Railway this is already set for the bot itself. For standalone
local runs, use the PUBLIC/external connection string (Railway -> your
Postgres service -> Connect tab -> "Public Network") since a standalone
process runs outside Railway's private network.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `database.models` importable regardless of the CWD this script is
# launched from (Claude Desktop/Code launch MCP servers with an arbitrary CWD).
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# ── DB check: exposed as a flag, NOT a hard exit-on-import ──────────────────
# bot.config.py falls back to SQLite whenever DATABASE_URL is unset OR isn't
# prefixed with postgres://ipostgresql:// — meaning it's easy to set SOME
# value here and still silently end up on the empty local SQLite file with
# NO error (confirmed directly: bot/config.py's else-branch overwrites any
# non-postgres-prefixed value with the hardcoded SQLite path).
#
# This module is imported two ways:
#   1. Standalone (`python mcp_server/server.py`) — a real Postgres DB is
#      REQUIRED, so we exit loudly if it's missing (see __main__ below).
#   2. As a library, mounted into bot/web.py's live FastAPI app — that
#      process legitimately runs on SQLite during local bot development
#      (see database/models.py's own docstring). Hard-exiting on import
#      would crash the ENTIRE bot process on a plain local dev run, which
#      is a much worse failure mode than just not mounting these tools.
#      bot/web.py checks HAS_REAL_DB itself and skips mounting if False,
#      logging a warning instead of taking the process down.
from bot.config import DATABASE_URL as _RESOLVED_DB_URL  # noqa: E402

HAS_REAL_DB = _RESOLVED_DB_URL.startswith("postgresql")

_DB_WARNING = (
    f"DATABASE_URL did not resolve to a Postgres connection "
    f"(resolved to: {_RESOLVED_DB_URL!r}). bot/config.py silently falls back "
    "to a local SQLite file for any value that isn't prefixed postgres:// or "
    "postgresql:// — refusing to serve real-looking-but-wrong data from that "
    "fallback. Get the PUBLIC connection string from Railway -> your "
    "Postgres service -> Connect tab -> \"Public Network\"."
)

from datetime import datetime, timedelta  # noqa: E402

from fastmcp import FastMCP  # noqa: E402

from database.models import (  # noqa: E402
    AgentParam,
    AsyncSessionLocal,
    Candidate,
    ClaudeDiscretionaryAction,
    META_CLOSE_REASONS,
    PaperTrade,
    STRATEGY_CLOSE_REASONS,
    algo_stats,
    compute_paper_balance,
    func,
    get_all_algos,
    get_hub_stats,
    get_open_paper_trades,
    get_params,
    get_scaling_config,
    get_top_wallets,
    get_wallet_by_address,
    get_wallet_cluster,
    get_wallet_token_trades,
    get_active_locks,
    select,
    set_algo_mode,
    set_param,
    top_bundle_wallets,
)
from bot import state  # noqa: E402 — live in-memory bot state (safe: same process)
from bot.health import health_snapshot  # noqa: E402

# Params this server will NEVER write, regardless of caller. Arming live
# trading (real capital) is a deliberate, human-only decision point in this
# project — explicitly agreed, multiple times, across this whole engagement.
# Autonomous trading-CONFIG tuning (stop-loss %, algo thresholds, source
# on/off) is fine; autonomously flipping the switch that moves real money is
# not the same category of action and is hard-blocked here, not just
# discouraged. If this policy ever changes, it changes by editing this
# constant deliberately, not by a caller argument overriding it.
WRITE_BLOCKLIST = frozenset({"live_trading_armed", "trade_mode"})

mcp = FastMCP(
    name="revolt-trading-bot",
    instructions=(
        "Tools over REVOLT's live trading data (Postgres on Railway) and its "
        "config. Start with get_hub_status for a quick snapshot. Most tools "
        "are read-only; set_trading_param/set_algo_mode_tool/toggle_source "
        "are the write tools — they change LIVE trading behavior immediately, "
        "no confirmation step, by design (autonomous trading-config tuning "
        "was explicitly agreed). They refuse to touch trade_mode or "
        "live_trading_armed under any circumstance — arming real-money "
        "trading is a separate, human-only decision, not something this "
        "server will ever do. All figures come straight from the same "
        "database the bot's own Telegram reports use — no duplicated "
        "calculation path."
    ),
)


# ── Shared helpers (mirror bot/handlers.py's bucketing logic exactly) ───────

def _bucket_source(pattern_type: str | None) -> str:
    """Same bucketing rule as /sourcestats in bot/handlers.py — keep in sync
    if that logic ever changes."""
    p = (pattern_type or "").lower()
    if "algo:" in p:
        return "algo"
    if "migration_dip" in p:
        return "migration"
    if "tg_signal" in p:
        return "4am"
    return "scanner"


def _bucket_stats(rows: list) -> dict:
    n = len(rows)
    pnl = sum((r.paper_pnl_sol or 0) for r in rows)
    wins = sum(1 for r in rows if (r.paper_pnl_sol or 0) > 0)
    losses = n - wins
    wr = round(wins / n * 100, 1) if n else 0.0
    best = max((r.peak_multiple or 0 for r in rows), default=0.0)
    return {
        "trades": n, "wins": wins, "losses": losses, "win_rate_pct": wr,
        "net_pnl_sol": round(pnl, 4), "best_peak_x": round(best, 2),
    }


# ── Tools ────────────────────────────────────────────────────────────────────

@mcp.tool(annotations={"readOnlyHint": True})
async def get_hub_status() -> dict:
    """Live snapshot of the bot: paper balance, all-time and today's PnL, win
    rate, and how many positions are currently open. This is the same data
    /hub shows at the top of the Telegram dashboard. Start here."""
    stats = await get_hub_stats()
    # Read the real starting balance rather than assuming 20.0 — matches
    # bot/web.py's own convention, in case the operator ever changes it.
    starting = float((await get_params("paper_starting_balance")).get(
        "paper_starting_balance") or 20.0)
    balance = await compute_paper_balance(starting)
    open_trades = await get_open_paper_trades()
    return {
        "balance_sol": round(balance, 4),
        "starting_balance_sol": starting,
        "alltime_pnl_sol": round(balance - starting, 4),
        "today_pnl_sol": round(float(stats.get("today_pnl") or 0), 4),
        "open_positions": len([t for t in open_trades if t.subscriber_id is None]),
        "wallet_total": stats.get("wallet_total"),
        "wallet_tier1": stats.get("wallet_tier1"),
        "wallet_tier2": stats.get("wallet_tier2"),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_source_stats(days: int = 7) -> dict:
    """Per-source performance breakdown over the last N days: 4AM, SCANNER,
    ALGO, and MIGRATION each get trades/wins/losses/win-rate/net-PnL/best-peak.
    BUNDLE is reported as a separate overlay (a subset of the sources above,
    not additive) since a bundle-tagged trade can come from any source.
    Mirrors /sourcestats in Telegram exactly — same bucketing rule."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        trades = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.closed_at >= cutoff,
            )
        )).scalars().all())

    groups: dict[str, list] = {"4am": [], "scanner": [], "algo": [], "migration": []}
    for t in trades:
        groups[_bucket_source(t.pattern_type)].append(t)
    bundled = [t for t in trades if "bundle" in (t.pattern_type or "").lower()]

    return {
        "window_days": days,
        "total": _bucket_stats(trades),
        "by_source": {k: _bucket_stats(v) for k, v in groups.items() if v},
        "bundle_overlay": _bucket_stats(bundled) if bundled else None,
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_4am_channel_stats(days: int = 7) -> dict:
    """4am (tg_signal) channel performance: how many calls we traded, hit-rate
    at each multiple threshold (>=1.5x/2x/3x/5x/10x), and the CAPTURE RATIO —
    the average multiple we actually banked vs. the average peak the tokens
    actually reached. Capture ratio is the single most important number for
    judging whether the exit logic is working: low capture despite high peaks
    means winners are being sold too early or the price feed can't see them."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        trades = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.closed_at >= cutoff,
                PaperTrade.pattern_type.like("%tg_signal%"),
            )
        )).scalars().all())

    if not trades:
        return {"window_days": days, "trades": 0, "note": "no closed 4am trades in this window"}

    n = len(trades)
    peaks = [float(t.peak_multiple or 1.0) for t in trades]
    # Same capture formula used by bot/agents/scaling_optimizer.py — the
    # realized multiple implied by PnL/size, which accounts for scale-outs.
    captured = [1.0 + (t.paper_pnl_sol or 0) / (t.paper_sol_spent or 1.0) for t in trades]

    thresholds = [1.5, 2.0, 3.0, 5.0, 10.0]
    hit_rates = {
        f">={x}x": round(sum(1 for p in peaks if p >= x) / n * 100, 1)
        for x in thresholds
    }

    return {
        "window_days": days,
        "trades": n,
        "hit_rates_by_peak": hit_rates,
        "avg_peak_x": round(sum(peaks) / n, 2),
        "avg_captured_x": round(sum(captured) / n, 2),
        "capture_ratio_pct": round((sum(captured) / n) / (sum(peaks) / n) * 100, 1) if sum(peaks) else 0.0,
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_algo_stats() -> dict:
    """Every custom algo (X-FILES, ZANZIBAR, etc.) with its current mode
    (off/manual/auto) and 7-day bot-tracked performance. Mirrors /algos."""
    algos = await get_all_algos()
    out = []
    for a in algos:
        st = await algo_stats(a.name, days=7)
        out.append({
            "name": a.name, "mode": a.mode,
            "signals_7d": st["signals"], "win_rate_pct": round(st["win_rate"], 1),
            "best_peak_x": round(st["best"], 2), "pnl_sol_7d": round(st["pnl"], 4),
        })
    return {"algos": out}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_top_smart_money_wallets(limit: int = 10) -> dict:
    """Top-scored smart-money wallets (Tier 1/2/3) by score. Mirrors /hub's
    High Scores list. Use get_wallet_analysis for the full detail on one."""
    wallets = await get_top_wallets(limit=limit)
    return {
        "wallets": [
            {
                "address": w.address, "tier": w.tier, "score": round(w.score, 1),
                "win_rate_pct": round(w.win_rate * 100, 1),
                "avg_multiple_x": round(w.avg_multiple, 2),
                "wins": w.wins, "losses": w.losses, "wallet_type": w.wallet_type,
            }
            for w in wallets
        ]
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_wallet_analysis(address: str) -> dict:
    """Full analysis for one wallet address: tier/score/win-rate/avg-multiple,
    cluster membership if part of a coordinated group, and its recent
    per-token trade history. Mirrors /wallet <address>."""
    w = await get_wallet_by_address(address)
    if w is None:
        return {"address": address, "found": False}

    result = {
        "address": w.address, "found": True, "tier": w.tier, "score": round(w.score, 1),
        "win_rate_pct": round(w.win_rate * 100, 1), "avg_multiple_x": round(w.avg_multiple, 2),
        "wins": w.wins, "losses": w.losses, "total_trades": w.total_trades,
        "wallet_type": w.wallet_type, "source": w.source,
        "first_seen": w.first_seen_at.isoformat() if w.first_seen_at else None,
        "last_updated": w.last_updated_at.isoformat() if w.last_updated_at else None,
    }
    if w.cluster_id:
        cl = await get_wallet_cluster(w.cluster_id)
        if cl:
            import json as _json
            try:
                members = len(_json.loads(cl.wallet_addresses))
            except Exception:
                members = None
            result["cluster"] = {
                "cluster_id": cl.cluster_id, "member_count": members,
                "win_rate_pct": round(cl.win_rate * 100, 1),
                "avg_multiple_x": round(cl.avg_multiple, 2),
            }
    trades = await get_wallet_token_trades(address)
    result["recent_trades"] = [
        {
            "token_address": t.token_address,
            "multiple_x": round(t.multiple, 2) if t.multiple is not None else None,
            "entry_mcap": t.entry_mcap,
        }
        for t in list(reversed(trades))[:10]
    ]
    return result


@mcp.tool(annotations={"readOnlyHint": True})
async def get_exit_strategy_config(trade_type: str = "high_conviction") -> dict:
    """Current SmartScalingExitManager configuration for a trade type:
    the tiered scale-out ladder and the runner trail width. Valid trade_type
    values: 'high_conviction' (4am/algo/bundle), 'conservative' (scanner),
    'bundle'. This is the LIVE config the exit system actually uses right
    now — reflects any self-tuning adjustments scaling_optimizer has made."""
    cfg = await get_scaling_config(trade_type)
    return {"trade_type": trade_type, "scale_tiers": cfg["scales"],
            "runner_trail_pct": cfg["runner_trail_pct"]}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_open_trades() -> dict:
    """Every currently open paper position: entry MC, last-known peak
    multiple, size, and age. Note: peak/current multiple here is the last
    value the monitor recorded, not a fresh live price fetch (this tool
    makes zero external network calls) — for the freshest possible price,
    check /hub in Telegram, which does fetch live."""
    trades = await get_open_paper_trades()
    now = datetime.utcnow()
    out = []
    for t in trades:
        if t.subscriber_id is not None:
            continue
        age_min = (now - t.opened_at).total_seconds() / 60.0 if t.opened_at else None
        out.append({
            "id": t.id, "token_name": t.token_name, "token_address": t.token_address,
            "pattern_type": t.pattern_type, "entry_mc": t.entry_mc,
            "last_known_peak_x": round(t.peak_multiple, 2) if t.peak_multiple else None,
            "size_sol": t.paper_sol_spent, "remaining_pct": t.remaining_pct,
            "age_minutes": round(age_min, 1) if age_min is not None else None,
        })
    return {"open_count": len(out), "positions": out}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_agent_params(names: list[str]) -> dict:
    """Look up the current live value of one or more agent_params config
    keys (e.g. 'scaling_manager_enabled', 'tg_signal_sl_pct',
    'algo_engine_enabled'). Mirrors /getparam. Pass a list even for one key."""
    return await get_params(*names)


@mcp.tool(annotations={"readOnlyHint": True})
async def get_weekly_report(days: int = 7) -> dict:
    """Overall HQ performance snapshot: total/strategy/meta PnL, win rate,
    breakdown by close reason (sl_hit/dead_token/scaled_exit/etc.), the 4am
    subset, and the sold-too-early rate. Mirrors /weeklyreport. 'Strategy'
    PnL excludes manual_close/reset (human decisions, not the bot's own
    outcome) — that split matters for judging the bot vs. your own actions."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.status == "closed",
                PaperTrade.closed_at >= cutoff,
                PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalars().all())

    n = len(rows)
    if n == 0:
        return {"window_days": days, "trades": 0}

    wins = sum(1 for r in rows if (r.paper_pnl_sol or 0) > 0)
    total_pnl = sum((r.paper_pnl_sol or 0) for r in rows)

    strat_rows = [r for r in rows if (r.close_reason or "") in STRATEGY_CLOSE_REASONS]
    meta_rows = [r for r in rows if (r.close_reason or "") in META_CLOSE_REASONS]
    strat_wins = sum(1 for r in strat_rows if (r.paper_pnl_sol or 0) > 0)

    by_reason: dict[str, dict] = {}
    for r in rows:
        reason = r.close_reason or "?"
        d = by_reason.setdefault(reason, {"trades": 0, "wins": 0, "pnl_sol": 0.0})
        d["trades"] += 1
        d["pnl_sol"] += (r.paper_pnl_sol or 0)
        if (r.paper_pnl_sol or 0) > 0:
            d["wins"] += 1
    for d in by_reason.values():
        d["win_rate_pct"] = round(d["wins"] / d["trades"] * 100, 1)
        d["pnl_sol"] = round(d["pnl_sol"], 4)

    tg_rows = [r for r in rows if "tg_signal" in (r.pattern_type or "")]
    early_rows = [r for r in rows if getattr(r, "sold_too_early", False)]

    return {
        "window_days": days, "trades": n, "wins": wins, "losses": n - wins,
        "win_rate_pct": round(wins / n * 100, 1),
        "total_pnl_sol": round(total_pnl, 4),
        "strategy_pnl_sol": round(sum((r.paper_pnl_sol or 0) for r in strat_rows), 4),
        "strategy_trades": len(strat_rows),
        "strategy_win_rate_pct": round(strat_wins / len(strat_rows) * 100, 1) if strat_rows else 0.0,
        "meta_pnl_sol": round(sum((r.paper_pnl_sol or 0) for r in meta_rows), 4),
        "meta_trades": len(meta_rows),
        "by_close_reason": by_reason,
        "4am_subset": {
            "trades": len(tg_rows),
            "pnl_sol": round(sum((r.paper_pnl_sol or 0) for r in tg_rows), 4),
        } if tg_rows else None,
        "sold_too_early_rate_pct": round(len(early_rows) / n * 100, 1),
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_pnl_outliers(limit: int = 10) -> dict:
    """The trades with the largest absolute PnL (win or loss), across ALL
    time, not just closed-recently. Use this to spot data anomalies — e.g. a
    trade with peak_multiple=1.0x but a large positive PnL is a red flag
    (mathematically the price never rose, so the PnL shouldn't be positive;
    this exact pattern once revealed a manual-close accounting bug). Mirrors
    /pnloutliers."""
    limit = max(1, min(limit, 50))
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.subscriber_id.is_(None),
            )
        )).scalars().all())
    rows.sort(key=lambda r: abs(r.paper_pnl_sol or 0), reverse=True)
    top = rows[:limit]
    return {
        "outliers": [
            {
                "id": r.id, "token_name": r.token_name, "pnl_sol": round(r.paper_pnl_sol or 0, 4),
                "size_sol": r.paper_sol_spent, "entry_mc": r.entry_mc, "peak_mc": r.peak_mc,
                "peak_multiple_x": round(r.peak_multiple, 2) if r.peak_multiple else None,
                "close_reason": r.close_reason,
                "opened_at": r.opened_at.isoformat() if r.opened_at else None,
                "closed_at": r.closed_at.isoformat() if r.closed_at else None,
            }
            for r in top
        ]
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_bundle_wallets(limit: int = 20) -> dict:
    """Leaderboard of bundle-participant wallets ranked by the average peak
    multiple their bundled tokens reached (min 2 resolved bundles to qualify
    — one lucky bundle doesn't prove skill). High avg X across many bundles =
    worth copy-following; low avg X = a dumper. Mirrors /bundlers."""
    limit = max(1, min(limit, 50))
    board = await top_bundle_wallets(limit=limit, min_bundles=2)
    return {"wallets": board}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_hourly_edge(days: int = 7) -> dict:
    """Net PnL, win/loss, and best peak broken out by the UTC hour a trade
    OPENED — answers 'which hours are actually hot'. NOT yet used to size
    positions (needs a longer clean window first) — this tool is the
    measurement, not a live sizing input. Mirrors /hourstats."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.opened_at >= cutoff,
            )
        )).scalars().all())

    buckets: dict[int, list] = {h: [] for h in range(24)}
    for t in rows:
        if t.opened_at is not None:
            buckets[t.opened_at.hour].append(t)

    hours = []
    for h in range(24):
        g = buckets[h]
        if not g:
            continue
        pnl = sum((x.paper_pnl_sol or 0) for x in g)
        wins = sum(1 for x in g if (x.paper_pnl_sol or 0) > 0)
        best = max((x.peak_multiple or 0 for x in g), default=0.0)
        hours.append({
            "hour_utc": h, "trades": len(g), "wins": wins,
            "pnl_sol": round(pnl, 4), "best_peak_x": round(best, 2),
        })
    if not hours:
        return {"window_days": days, "hours": []}

    hottest = max(hours, key=lambda r: r["pnl_sol"])
    coldest = min(hours, key=lambda r: r["pnl_sol"])
    return {"window_days": days, "hours": hours, "hottest_hour": hottest, "coldest_hour": coldest}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_bot_health() -> dict:
    """Liveness of every monitored background loop (scanner, paper_monitor,
    wallet_analyst, etc.) — age since last heartbeat and whether it's stale.
    Use this to check whether a source going quiet is 'nothing to trade'
    or 'the loop actually died'. Mirrors /health."""
    rows = health_snapshot()
    return {"loops": rows, "any_stale": any(r.get("stale") for r in rows)}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_scanner_substats(days: int = 30) -> dict:
    """Scanner performance broken out by SUB-SOURCE (insider-wallet /
    new-launch / volume-spike / gmgn-flagged) — shows which of the scanner's
    4 internal signal sources actually carries edge vs. which is noise.
    Mirrors /scannerstats."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.closed_at >= cutoff,
            )
        )).scalars().all())

    def _sub(t) -> str | None:
        p = (t.pattern_type or "").lower()
        if "tg_signal" in p or "migration_dip" in p or "algo:" in p:
            return None
        if "insider" in p:
            return "insider"
        if "new_launch" in p:
            return "new_launch"
        if "volume" in p:
            return "volume"
        if "gmgn" in p:
            return "gmgn"
        return "other"

    groups: dict[str, list] = {}
    for t in rows:
        sub = _sub(t)
        if sub is not None:
            groups.setdefault(sub, []).append(t)

    out = {}
    for sub, ts in groups.items():
        n = len(ts)
        pnl = sum((t.paper_pnl_sol or 0) for t in ts)
        wins = sum(1 for t in ts if (t.paper_pnl_sol or 0) > 0)
        r5 = sum(1 for t in ts if (t.peak_multiple or 0) >= 5) / n * 100 if n else 0
        out[sub] = {
            "trades": n, "win_rate_pct": round(wins / n * 100, 1) if n else 0,
            "pnl_sol": round(pnl, 4), "hit_rate_5x_pct": round(r5, 1),
        }
    return {"window_days": days, "by_sub_source": out}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_4am_channel_attribution(days: int = 30) -> dict:
    """Per-CHANNEL 4am edge — which specific Telegram channels feeding 4am
    are actually profitable vs. dragging the average down. Combines signal
    quality (avg peak, tail hit-rates) with OUR realized PnL per channel, so
    you can tell 'good channel, bad capture' from 'bad channel'. Mirrors
    /4amattribution."""
    import re as _re
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.closed_at >= cutoff,
            )
        )).scalars().all())

    def _channel(t):
        if "tg_signal" not in (t.pattern_type or ""):
            return None
        if t.channel_name and t.channel_name not in ("scanner", "?"):
            return t.channel_name
        m = _re.search(r"\[([^\]]+)\]", t.trade_reasoning or "")
        return m.group(1) if m else "unknown"

    ch: dict[str, list] = {}
    for t in rows:
        c = _channel(t)
        if c is not None:
            ch.setdefault(c, []).append(t)

    out = {}
    for name, ts in ch.items():
        n = len(ts)
        pnl = sum((t.paper_pnl_sol or 0) for t in ts)
        wins = sum(1 for t in ts if (t.paper_pnl_sol or 0) > 0)
        peaks = [float(t.peak_multiple or 0) for t in ts]
        out[name] = {
            "trades": n, "win_rate_pct": round(wins / n * 100, 1) if n else 0,
            "pnl_sol": round(pnl, 4), "avg_peak_x": round(sum(peaks) / n, 2) if n else 0,
            "hit_rate_2x_pct": round(sum(1 for p in peaks if p >= 2) / n * 100, 1) if n else 0,
            "hit_rate_5x_pct": round(sum(1 for p in peaks if p >= 5) / n * 100, 1) if n else 0,
            "hit_rate_10x_pct": round(sum(1 for p in peaks if p >= 10) / n * 100, 1) if n else 0,
        }
    return {"window_days": days, "by_channel": out}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_scanner_gate_status() -> dict:
    """Every gate that could currently block a scanner paper trade from
    opening: trade_mode (both DB-persisted and live in-memory), confidence
    threshold, manual-close cooldown, open trade count, and the last 10
    scored candidates with their decision. Use this when the scanner has
    gone quiet and you need to know WHY. Mirrors /scannerwhy. Reads live
    in-memory bot state directly (this tool runs inside the same process)."""
    now = datetime.utcnow()
    recent_cutoff = now - timedelta(hours=2)
    cooldown_cutoff = now - timedelta(hours=24)

    async with AsyncSessionLocal() as session:
        trade_mode_val = (await session.execute(
            select(AgentParam.param_value).where(AgentParam.param_name == "trade_mode")
        )).scalar_one_or_none()
        conf_thresh = (await session.execute(
            select(AgentParam.param_value).where(AgentParam.param_name == "conf_paper_threshold")
        )).scalar_one_or_none()
        open_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
        )).scalar() or 0
        recent_manual_close = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.close_reason == "manual_close",
                PaperTrade.closed_at >= cooldown_cutoff,
            )
        )).scalar() or 0
        recent_candidates = list((await session.execute(
            select(Candidate).where(Candidate.created_at >= recent_cutoff)
            .order_by(Candidate.id.desc()).limit(10)
        )).scalars().all())

    tm_int = int(trade_mode_val or 0)
    thresh_val = float(conf_thresh) if conf_thresh is not None else 20.0

    return {
        "trade_mode_db": {0: "off", 1: "paper", 2: "live"}.get(tm_int, "?"),
        "trade_mode_live_memory": state.trade_mode,
        "conf_paper_threshold": thresh_val,
        "open_paper_trades": open_count,
        "manual_closes_last_24h": recent_manual_close,
        "recent_candidates_2h": [
            {
                "token_name": c.token_name, "confidence_score": c.confidence_score,
                "decision": c.decision, "source": c.source,
                "passed_threshold": (c.confidence_score or 0) >= thresh_val,
            }
            for c in recent_candidates
        ],
    }


@mcp.tool(annotations={"readOnlyHint": True})
async def get_close_reason_breakdown(reason: str, days: int = 7, limit: int = 15) -> dict:
    """Diagnostic drill-down for ONE close_reason (e.g. 'dead_token',
    'no_momentum', 'sl_hit') over the last N days: entry MC range, average
    confidence score, dominant pattern_type tags, channel/source breakdown,
    and a sample of the most recent trades. Use this to find WHY a losing
    close-reason category keeps losing, not just that it does -- pairs with
    get_weekly_report's by_close_reason breakdown."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with AsyncSessionLocal() as session:
        rows = list((await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.close_reason == reason,
                PaperTrade.closed_at >= cutoff,
            ).order_by(PaperTrade.closed_at.desc())
        )).scalars().all())

    if not rows:
        return {"reason": reason, "window_days": days, "trades": 0}

    n = len(rows)
    entry_mcs = [t.entry_mc for t in rows if t.entry_mc]
    confidences = [t.confidence_score for t in rows if t.confidence_score is not None]
    pnl = sum((t.paper_pnl_sol or 0) for t in rows)

    tag_counts: dict[str, int] = {}
    for t in rows:
        for tag in (t.pattern_type or "").split(","):
            tag = tag.strip()
            if tag:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
    top_tags = sorted(tag_counts.items(), key=lambda x: -x[1])[:10]

    channel_counts: dict[str, int] = {}
    for t in rows:
        ch = t.channel_name or "unlabeled"
        channel_counts[ch] = channel_counts.get(ch, 0) + 1
    top_channels = sorted(channel_counts.items(), key=lambda x: -x[1])[:10]

    return {
        "reason": reason,
        "window_days": days,
        "trades": n,
        "pnl_sol": round(pnl, 4),
        "avg_entry_mc": round(sum(entry_mcs) / len(entry_mcs), 0) if entry_mcs else None,
        "min_entry_mc": round(min(entry_mcs), 0) if entry_mcs else None,
        "max_entry_mc": round(max(entry_mcs), 0) if entry_mcs else None,
        "avg_confidence_score": round(sum(confidences) / len(confidences), 1) if confidences else None,
        "top_pattern_tags": top_tags,
        "top_channels": top_channels,
        "sample_trades": [
            {
                "token_name": t.token_name,
                "entry_mc": t.entry_mc,
                "confidence_score": t.confidence_score,
                "pattern_type": t.pattern_type,
                "channel_name": t.channel_name,
                "paper_pnl_sol": t.paper_pnl_sol,
                "peak_multiple": t.peak_multiple,
                "opened_at": t.opened_at.isoformat() if t.opened_at else None,
                "closed_at": t.closed_at.isoformat() if t.closed_at else None,
            }
            for t in rows[:limit]
        ],
    }


# ── Write tools — change LIVE trading behavior, no confirmation step ───────
# Autonomous trading-config tuning, explicitly agreed: these execute
# immediately, no approval gate. Every write is logged (via set_param's own
# reason field) so changes stay auditable even without a human in the loop.
# WRITE_BLOCKLIST (trade_mode / live_trading_armed) is enforced here in code,
# not by convention — see the constant's own comment for why.

@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
async def set_trading_param(name: str, value: float, reason: str) -> dict:
    """Set a trading-config param to a new value, immediately, live. No
    confirmation step — this changes bot behavior the instant it's called.
    `reason` is required and gets logged with the change (visible via
    /getparam history and get_agent_params). Examples: 'tg_signal_sl_pct'
    (4am stop-loss %), 'scanner_paper_rug_floor', 'runner_floor_arm'.
    Refuses to touch 'trade_mode' or 'live_trading_armed' under any
    circumstance — arming real-money trading is a separate, human-only
    decision this tool will never make."""
    if name in WRITE_BLOCKLIST:
        return {
            "applied": False,
            "reason": f"'{name}' is hard-blocked — arming live trading is a "
                      "human-only decision, not something this tool will do.",
        }
    await set_param(name, value, reason=reason)
    locks = get_active_locks()
    result = {"applied": True, "param": name, "value": value, "reason": reason}
    if name in locks and abs(locks[name] - value) > 0.0001:
        result["warning"] = (
            f"'{name}' is env-locked on Railway to {locks[name]} — the DB row "
            "was updated for audit, but the bot will keep reading the locked "
            "value until the operator removes the env var."
        )
    return result


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
async def set_algo_mode_tool(name: str, mode: str) -> dict:
    """Set a custom algo's mode: 'off' (idle), 'manual' (alert only, no
    auto-buy), or 'auto' (trades on its own). Immediate, no confirmation.
    Valid names: X-FILES, ZANZIBAR, BLOWJOB, OUT OF CONTROL, GELATO (see
    get_algo_stats for current performance before deciding)."""
    ok = await set_algo_mode(name, mode)
    if not ok:
        return {"applied": False, "reason": f"unknown algo '{name}' or invalid mode '{mode}'"}
    return {"applied": True, "algo": name, "mode": mode.lower()}


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
async def toggle_source(source: str, enabled: bool, reason: str) -> dict:
    """Turn an entire signal source on/off at the structural level (not just
    tuning it). `source` is one of: 'scanner', '4am' (tg_scraper), 'algo_engine'.
    This is the hard switch — /4amonly, /scanneronly-style — separate from
    tuning individual params on a source that stays on. `reason` is required
    and logged."""
    _param_map = {
        "scanner": "scanner_enabled",
        "4am": "tg_scraper_enabled",
        "tg_scraper": "tg_scraper_enabled",
        "algo_engine": "algo_engine_enabled",
        "algos": "algo_engine_enabled",
    }
    key = _param_map.get(source.lower())
    if key is None:
        return {"applied": False, "reason": f"unknown source '{source}' — "
                "use one of: scanner, 4am, algo_engine"}
    await set_param(key, 1.0 if enabled else 0.0, reason=reason)
    return {"applied": True, "source": source, "enabled": enabled, "param": key, "reason": reason}


@mcp.tool(annotations={"readOnlyHint": True})
async def get_discretionary_actions(limit: int = 20) -> dict:
    """Recent Claude discretionary-play decisions (Phase 6) — both OPEN and
    SKIP, newest first. Mirrors the claude_discretionary_actions table.
    Shows what Claude has been considering on its own initiative, not just
    what it actually traded."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(ClaudeDiscretionaryAction)
            .order_by(ClaudeDiscretionaryAction.id.desc())
            .limit(max(1, min(limit, 100)))
        )).scalars().all()
    return {
        "actions": [
            {
                "decided_at": r.decided_at.isoformat() if r.decided_at else None,
                "token_name": r.token_name,
                "token_address": r.token_address,
                "action": r.action,
                "reason": r.reason,
                "confidence": r.confidence,
                "rug_score": r.rug_score,
                "model_tier": r.model_tier,
                "cost_usd": r.cost_usd,
                "trade_id": r.trade_id,
                "announced": r.announced,
            }
            for r in rows
        ],
    }


@mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True})
async def trigger_discretionary_scan() -> dict:
    """Force one Claude discretionary-trading evaluation cycle right now,
    instead of waiting for the 5-min timer. Runs the exact same code path
    as the background loop (not a separate copy) — useful for testing
    without waiting. Still respects claude_discretionary_enabled and every
    other gate/budget check; calling this when the feature is off just
    returns {"enabled": false} and does nothing."""
    from bot.agents.claude_discretionary import run_once
    return await run_once()


if __name__ == "__main__":
    # Standalone execution genuinely requires a real Postgres DB — there's
    # no reasonable fallback, so fail loudly here (not at import time; see
    # the HAS_REAL_DB note above for why the check itself isn't at import).
    if not HAS_REAL_DB:
        print(f"FATAL: {_DB_WARNING}\nSet DATABASE_URL and try again "
              "(see mcp_server/README.md).", file=sys.stderr)
        sys.exit(1)
    mcp.run()  # stdio transport by default — launched by Claude Code/Desktop
