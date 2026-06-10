"""
claude_cold.py — Phase 5 cold path: daily strategy review by Claude.

Runs once per day at 9 AM UTC. Reads last 24h of closed trades, asks
Sonnet to identify patterns, then writes a config-diff suggestion to
agent_params as JSON. Operator reviews via /strategy_review and applies
with /apply_review.

Phase 6 (later) auto-applies safe suggestions. For now, every change
requires human approval — keeps strategy drift under operator control.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta

from sqlalchemy import select

from bot.agents.claude_reasoning import (
    SONNET_MODEL, call_claude, claude_available, parse_json_response,
)
from database.models import (
    AsyncSessionLocal, ClaudePositionAction, ClaudeReview, PaperTrade,
    get_all_params, set_param,
)

logger = logging.getLogger(__name__)

DAILY_RUN_HOUR_UTC = 9
SYSTEM_PROMPT = """You are a senior trading strategist reviewing 24 hours of paper-trading data for "Revolt Agent Hub" — a Solana memecoin sniper bot. The bot trades 4am Telegram signals on pump.fun launches.

Your job: read the closed-trade data and propose specific parameter changes that would improve expectancy/trade. Be precise about which parameter and the new value. Don't propose generic advice like "tighten risk" — propose "stop_loss_pct from 18 to 15".

Available parameters and current values are provided. ONLY propose changes to parameters in the provided list.

Output STRICT JSON only:
{
  "summary": "2-3 sentence read of the day's performance",
  "recommendations": [
    {
      "param": "<exact param name from current list>",
      "current": <number>,
      "proposed": <number>,
      "reason": "<one sentence, specific>",
      "confidence": "low|medium|high",
      "auto_safe": true|false
    }
  ],
  "no_change_explanation": "<set if recommendations is empty>"
}

auto_safe = true means: the proposed value is within ±20% of current AND the parameter is a soft tuner (not a risk floor). Phase 6 will auto-apply auto_safe changes; the rest stay manual.

No markdown fences. No prose outside the JSON."""


def _build_trade_summary(closed_24h: list, all_params: dict) -> str:
    """Compress 24h of closed trades into a digest Claude can reason over."""
    n = len(closed_24h)
    if n == 0:
        return "No closed trades in last 24h."

    wins = sum(1 for t in closed_24h if (t.paper_pnl_sol or 0) > 0)
    losses = sum(1 for t in closed_24h if (t.paper_pnl_sol or 0) < 0)
    total_pnl = sum(t.paper_pnl_sol or 0 for t in closed_24h)
    wr = (wins / n * 100) if n else 0
    avg_win = (
        sum(t.paper_pnl_sol for t in closed_24h if (t.paper_pnl_sol or 0) > 0)
        / max(1, wins)
    ) if wins else 0
    avg_loss = (
        sum(t.paper_pnl_sol for t in closed_24h if (t.paper_pnl_sol or 0) < 0)
        / max(1, losses)
    ) if losses else 0

    # By close reason
    by_reason = {}
    for t in closed_24h:
        r = t.close_reason or "?"
        by_reason.setdefault(r, []).append(t)
    reason_lines = []
    for r, ts in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
        pnl = sum(t.paper_pnl_sol or 0 for t in ts)
        w = sum(1 for t in ts if (t.paper_pnl_sol or 0) > 0)
        reason_lines.append(
            f"  {r}: n={len(ts)}, wins={w}, pnl={pnl:+.4f} SOL"
        )

    # Top winners / losers
    sorted_pnl = sorted(closed_24h, key=lambda t: t.paper_pnl_sol or 0)
    top_losses = sorted_pnl[:3]
    top_wins = sorted_pnl[-3:][::-1]

    win_lines = [
        f"  {(t.token_name or '?')[:20]} {t.peak_multiple or 0:.1f}x peak +{t.paper_pnl_sol:.4f}"
        for t in top_wins
    ]
    loss_lines = [
        f"  {(t.token_name or '?')[:20]} ({t.close_reason or '?'}) {t.paper_pnl_sol:+.4f}"
        for t in top_losses
    ]

    # Current params
    relevant = [
        "tg_signal_tp_x", "tg_signal_sl_pct", "tg_signal_trail_trigger",
        "tg_signal_trail_pct", "tg_signal_cooldown_hours",
        "paper_probe_size", "max_open_paper_trades",
        "time_stop_minutes", "time_stop_threshold", "hard_timeout_hours",
        "regime_hot_vol_ratio", "regime_cold_vol_ratio",
        "regime_hot_sol_24h", "regime_cold_sol_24h",
        "regime_hot_probe_mult", "regime_cold_probe_mult",
    ]
    param_lines = []
    for k in relevant:
        v = all_params.get(k)
        if v is not None:
            param_lines.append(f"  {k} = {v}")

    return (
        f"PERFORMANCE (last 24h)\n"
        f"Closed: {n} trades | Wins: {wins} | Losses: {losses} | WR: {wr:.0f}%\n"
        f"Total PnL: {total_pnl:+.4f} SOL\n"
        f"Avg win: {avg_win:+.4f} | Avg loss: {avg_loss:+.4f}\n"
        f"Expectancy/trade: {total_pnl/n:.4f} SOL\n"
        f"\n"
        f"BY CLOSE REASON\n" + "\n".join(reason_lines) + "\n"
        f"\n"
        f"TOP 3 WINS\n" + "\n".join(win_lines) + "\n"
        f"\n"
        f"TOP 3 LOSSES\n" + "\n".join(loss_lines) + "\n"
        f"\n"
        f"CURRENT PARAMS (these are what you can recommend changing)\n"
        + "\n".join(param_lines)
    )


REPORT_SYSTEM_PROMPT = """You are the senior trading strategist for "Revolt Agent Hub" — a Solana memecoin paper-trading bot. It trades 4am Telegram channel signals on pump.fun launches. A Claude "active trader" layer manages open positions (exits, partials, trail widening, ladder disable).

You are given the full trade history digest, the active trader's recent action log, and current parameters. Write an honest operator report:

1. VERDICT — one blunt paragraph: is this system making money, and is the edge real or luck?
2. WHAT'S WORKING — be specific, cite numbers from the digest.
3. WHAT'S LEAKING — where is PnL being lost? Early exits? Bad entries? Dead positions?
4. THE ACTIVE TRADER — judge the Claude action log: are the exits/holds good calls?
5. CHANGES — max 3 concrete recommendations, most impactful first. Parameter values or architecture, not vague advice.
6. LIVE-READY? — would you put real SOL on this today? What evidence is still missing?

Plain text only, no markdown formatting or tables. Keep it under 450 words. Be direct — the operator wants truth, not encouragement."""


def _build_full_digest(trades: list, actions: list, all_params: dict, days: int) -> str:
    n = len(trades)
    wins = [t for t in trades if (t.paper_pnl_sol or 0) > 0]
    losses = [t for t in trades if (t.paper_pnl_sol or 0) < 0]
    total_pnl = sum(t.paper_pnl_sol or 0 for t in trades)
    wr = len(wins) / n * 100 if n else 0
    avg_win = sum(t.paper_pnl_sol for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t.paper_pnl_sol for t in losses) / len(losses) if losses else 0

    def realized_mult(t) -> float:
        spent = t.paper_sol_spent or 0.2
        return 1 + (t.paper_pnl_sol or 0) / spent if spent else 1

    peaks = [t.peak_multiple or 1 for t in trades]
    avg_peak = sum(peaks) / n if n else 0
    avg_realized = sum(realized_mult(t) for t in trades) / n if n else 0
    early_count = sum(1 for t in trades if t.sold_too_early)

    by_reason: dict[str, list] = {}
    for t in trades:
        by_reason.setdefault(t.close_reason or "?", []).append(t)
    reason_lines = [
        f"  {r}: n={len(ts)}, wins={sum(1 for t in ts if (t.paper_pnl_sol or 0) > 0)}, "
        f"pnl={sum(t.paper_pnl_sol or 0 for t in ts):+.3f} SOL"
        for r, ts in sorted(by_reason.items(), key=lambda kv: -len(kv[1]))
    ]

    # Worst capture gaps: big peak, little realized
    gaps = sorted(
        (t for t in trades if (t.peak_multiple or 0) >= 3),
        key=lambda t: (t.peak_multiple or 0) - realized_mult(t),
        reverse=True,
    )[:8]
    gap_lines = [
        f"  {(t.token_name or '?')[:18]}: peaked {t.peak_multiple:.1f}x, "
        f"realized {realized_mult(t):.1f}x ({t.close_reason or '?'})"
        for t in gaps
    ] or ["  (none ≥3x peak)"]

    best = sorted(trades, key=lambda t: t.paper_pnl_sol or 0, reverse=True)[:5]
    worst = sorted(trades, key=lambda t: t.paper_pnl_sol or 0)[:5]
    best_lines = [
        f"  {(t.token_name or '?')[:18]}: {t.paper_pnl_sol:+.3f} SOL, "
        f"peak {t.peak_multiple or 0:.1f}x ({t.close_reason or '?'})"
        for t in best
    ]
    worst_lines = [
        f"  {(t.token_name or '?')[:18]}: {t.paper_pnl_sol:+.3f} SOL ({t.close_reason or '?'})"
        for t in worst
    ]

    # Claude's own per-close postmortems (persisted from close commentary)
    with_pm = [t for t in trades if t.close_commentary]
    pm_recent = sorted(with_pm, key=lambda t: t.closed_at or datetime.min, reverse=True)[:15]
    pm_lines = [
        f"  {(t.token_name or '?')[:16]} ({t.paper_pnl_sol:+.3f}): "
        f"{t.close_commentary[:150]}"
        for t in pm_recent
    ] or ["  (none persisted yet — only trades closed after this feature shipped have one)"]

    act_counts: dict[str, int] = {}
    for a in actions:
        act_counts[a.action] = act_counts.get(a.action, 0) + 1
    action_lines = [
        f"  {k}: {v}" for k, v in sorted(act_counts.items(), key=lambda kv: -kv[1])
    ] or ["  (no actions logged)"]
    recent_action_lines = [
        f"  {a.action} {(a.token_name or '?')[:14]} @{a.current_mult or 0:.2f}x — "
        f"{(a.reason or '')[:90]}"
        for a in actions[:12]
    ]

    relevant = [
        "tg_signal_tp_x", "tg_signal_sl_pct", "tg_signal_trail_trigger",
        "tg_signal_trail_pct", "paper_probe_size", "max_open_paper_trades",
        "time_stop_minutes", "hard_timeout_hours", "conf_paper_threshold",
        "claude_active_enabled", "claude_strategist_enabled",
    ]
    param_lines = [
        f"  {k} = {all_params.get(k)}" for k in relevant if all_params.get(k) is not None
    ]

    window = f"last {days}d" if days > 0 else "all time"
    return (
        f"WINDOW: {window}\n\n"
        f"OVERALL\n"
        f"Closed: {n} | WR: {wr:.0f}% | Total PnL: {total_pnl:+.3f} SOL | "
        f"Expectancy: {total_pnl / n if n else 0:+.4f} SOL/trade\n"
        f"Avg win: {avg_win:+.4f} | Avg loss: {avg_loss:+.4f}\n"
        f"Avg peak while open: {avg_peak:.2f}x | Avg realized: {avg_realized:.2f}x | "
        f"Sold-too-early flags: {early_count}\n\n"
        f"BY CLOSE REASON\n" + "\n".join(reason_lines) + "\n\n"
        f"WORST CAPTURE GAPS (peak vs realized)\n" + "\n".join(gap_lines) + "\n\n"
        f"TOP 5 WINS\n" + "\n".join(best_lines) + "\n\n"
        f"TOP 5 LOSSES\n" + "\n".join(worst_lines) + "\n\n"
        f"YOUR OWN CLOSE POSTMORTEMS (most recent {len(pm_recent)} of {len(with_pm)})\n"
        + "\n".join(pm_lines) + "\n\n"
        f"ACTIVE TRADER ACTION COUNTS (last {len(actions)} logged)\n"
        + "\n".join(action_lines) + "\n\n"
        f"MOST RECENT ACTIONS\n" + "\n".join(recent_action_lines) + "\n\n"
        f"CURRENT PARAMS\n" + "\n".join(param_lines)
    )


async def run_full_report(days: int = 7) -> str | None:
    """On-demand operator report: full trade history + action log → Claude
    freeform analysis. Returns plain text, or None if no key/data/failure."""
    if not claude_available():
        return None

    async with AsyncSessionLocal() as session:
        q = select(PaperTrade).where(
            PaperTrade.subscriber_id.is_(None),
            PaperTrade.status == "closed",
            PaperTrade.close_reason != "reset",
        )
        if days > 0:
            q = q.where(PaperTrade.closed_at >= datetime.utcnow() - timedelta(days=days))
        trades = list((await session.execute(q)).scalars().all())
        actions = list((await session.execute(
            select(ClaudePositionAction)
            .order_by(ClaudePositionAction.id.desc())
            .limit(150)
        )).scalars().all())

    if not trades:
        return "No closed trades in that window — nothing to analyze."

    all_params = await get_all_params()
    digest = _build_full_digest(trades, actions, all_params, days)

    text = await call_claude(
        system=REPORT_SYSTEM_PROMPT,
        user=digest,
        model=SONNET_MODEL,
        max_tokens=1500,
        timeout_sec=120.0,
    )
    if not text:
        logger.warning("claude_cold: full report call returned no text")
        return None
    return text.strip()


async def run_daily_review() -> dict | None:
    """Pull last 24h trades, ask Claude, persist review. Returns None
    if no key or Claude failed."""
    if not claude_available():
        return None

    cutoff = datetime.utcnow() - timedelta(hours=24)
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(PaperTrade).where(
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.status == "closed",
                PaperTrade.closed_at >= cutoff,
                PaperTrade.close_reason != "reset",
            )
        )).scalars().all()
    closed = list(rows)

    all_params = await get_all_params()
    user_msg = _build_trade_summary(closed, all_params)

    text = await call_claude(
        system=SYSTEM_PROMPT,
        user=user_msg,
        model=SONNET_MODEL,
        max_tokens=2048,
        timeout_sec=120.0,
    )
    if not text:
        logger.warning("claude_cold: Claude call returned no text")
        return None

    parsed = parse_json_response(text)
    if not parsed:
        logger.warning("claude_cold: Claude returned unparseable JSON")
        return None

    # Persist review JSON to claude_reviews so operator can /strategy_review
    parsed["generated_at"] = datetime.utcnow().isoformat()
    parsed["trade_count"] = len(closed)
    try:
        async with AsyncSessionLocal() as session:
            session.add(ClaudeReview(
                trade_count=len(closed),
                review_json=json.dumps(parsed),
                applied=False,
            ))
            await session.commit()
    except Exception as exc:
        logger.warning("claude_cold: failed to persist review: %s", exc)

    logger.info(
        "claude_cold: review generated — %d closed trades, %d recommendations",
        len(closed), len(parsed.get("recommendations") or []),
    )
    return parsed


async def claude_cold_loop() -> None:
    """Run daily review at 9 AM UTC. Sleeps until next 9 AM after each run."""
    logger.info("claude_cold: starting (daily strategy review at 9 AM UTC)")

    while True:
        try:
            now = datetime.utcnow()
            target = now.replace(hour=DAILY_RUN_HOUR_UTC, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            sleep_sec = (target - now).total_seconds()
            logger.info("claude_cold: next review at %s UTC (in %.1fh)",
                        target.isoformat(), sleep_sec / 3600)
            await asyncio.sleep(sleep_sec)

            if not claude_available():
                logger.info("claude_cold: ANTHROPIC_API_KEY not set — skipping")
                continue

            await run_daily_review()

        except asyncio.CancelledError:
            logger.info("claude_cold: cancelled")
            raise
        except Exception as exc:
            logger.error("claude_cold: error %s, sleeping 1h", exc)
            await asyncio.sleep(3600)


async def get_latest_review() -> dict | None:
    """Operator-facing: returns the most recent review JSON, or None."""
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(ClaudeReview)
            .order_by(ClaudeReview.generated_at.desc())
            .limit(1)
        )).scalar_one_or_none()
    if not row or not row.review_json:
        return None
    try:
        return json.loads(row.review_json)
    except Exception:
        return None


async def _mark_latest_applied() -> None:
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(ClaudeReview)
            .order_by(ClaudeReview.generated_at.desc())
            .limit(1)
        )).scalar_one_or_none()
        if row:
            row.applied = True
            row.applied_at = datetime.utcnow()
            await session.commit()


async def apply_review(safe_only: bool = True) -> tuple[int, list[str]]:
    """Apply the latest review's recommendations. With safe_only=True,
    only auto_safe=True changes apply. Returns (applied_count, log_lines)."""
    review = await get_latest_review()
    if not review:
        return 0, ["No review available — run /strategy_review first or wait for daily run"]

    recs = review.get("recommendations") or []
    if not recs:
        return 0, ["No recommendations in latest review"]

    applied = 0
    log = []
    for r in recs:
        if safe_only and not r.get("auto_safe"):
            log.append(f"SKIP (not auto_safe): {r['param']} → {r['proposed']} ({r.get('reason','?')})")
            continue
        param = r.get("param")
        proposed = r.get("proposed")
        if not param or proposed is None:
            continue
        try:
            await set_param(
                param, float(proposed),
                reason=f"Claude review {review.get('generated_at','?')}: {r.get('reason','?')}",
            )
            log.append(f"APPLIED: {param} = {proposed}  ({r.get('reason','?')})")
            applied += 1
        except Exception as exc:
            log.append(f"FAILED {param}: {exc}")

    if applied > 0:
        await _mark_latest_applied()
    return applied, log
