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
    AsyncSessionLocal,
    PaperTrade,
    algo_stats,
    compute_paper_balance,
    get_all_algos,
    get_hub_stats,
    get_open_paper_trades,
    get_params,
    get_scaling_config,
    get_top_wallets,
    get_wallet_by_address,
    get_wallet_cluster,
    get_wallet_token_trades,
    select,
)

mcp = FastMCP(
    name="revolt-trading-bot",
    instructions=(
        "Read-only tools over REVOLT's live trading data (Postgres on Railway). "
        "Start with get_hub_status for a quick snapshot, then get_source_stats "
        "or get_4am_channel_stats for a deeper read. All figures come straight "
        "from the same database the bot's own Telegram reports use — there is "
        "no separate/duplicated calculation path. There are no write tools; "
        "this cannot change any bot behavior."
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


if __name__ == "__main__":
    # Standalone execution genuinely requires a real Postgres DB — there's
    # no reasonable fallback, so fail loudly here (not at import time; see
    # the HAS_REAL_DB note above for why the check itself isn't at import).
    if not HAS_REAL_DB:
        print(f"FATAL: {_DB_WARNING}\nSet DATABASE_URL and try again "
              "(see mcp_server/README.md).", file=sys.stderr)
        sys.exit(1)
    mcp.run()  # stdio transport by default — launched by Claude Code/Desktop
