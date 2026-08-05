"""
claude_discretionary.py — Phase 6: Claude picks its own paper trades.

Separate from every other entry source (scanner, 4am, algo_engine,
claude_strategist). Those all react to an incoming signal or a scanner
tick. This loop is proactive: every 5 minutes it looks through candidates
that already passed every existing safety screen (rug score, liquidity
floor, holder concentration, mcap band — enforced upstream by
confidence_engine.py / scanner_agent.py) but fell short of the rule
engine's auto-trade confidence bar, and asks whether ANY of them is worth
taking on Claude's own discretionary judgment.

Two-stage funnel to keep this cheap:
  Stage 1 (Haiku, batched): "which of these, if any, deserve a deeper look"
  Stage 2 (Sonnet, per survivor): full context, real OPEN/SKIP decision

Deliberately reuses existing safety machinery rather than inventing new
one-offs:
  - open_paper_trade(pattern_type="claude_discretionary", ...) — its own
    independent gate (claude_discretionary_enabled), not routed through
    scanner_enabled/tg_scraper_enabled.
  - has_open_paper_trade / has_recent_close / count_open_paper_trades /
    compute_paper_balance — the same triple-gate every other source uses.
  - live_trading_armed stays whatever it already is — open_paper_trade's
    live-mirror call is untouched here, same no-op-unless-armed behavior
    as every other source.

Closing needs no new logic: claude_warm_loop already manages every open
paper trade regardless of source. This module only watches for its OWN
positions transitioning to closed, to relay the close via Ecconos.
"""

import asyncio
import json
import logging
import time as _time
from datetime import datetime, timedelta

from sqlalchemy import select

from bot.agents.claude_reasoning import (
    HAIKU_MODEL, SONNET_MODEL, call_claude, claude_available,
    parse_json_response,
)
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    AsyncSessionLocal, Candidate, ClaudeDiscretionaryAction, PaperTrade,
    compute_paper_balance, count_open_paper_trades, get_params,
    has_open_paper_trade, has_recent_close, open_paper_trade,
    PaperTradeBlocked,
)

logger = logging.getLogger(__name__)

POLL_TICK_SEC = 300  # 5 min — candidate discovery doesn't need claude_warm's freshness

# Conservative cost estimates (Haiku ~$1/M in + $5/M out; Sonnet ~$3/M in + $15/M out)
HAIKU_COST_PER_CALL_USD = 0.004   # batched triage call, slightly larger than a single check
SONNET_COST_PER_CALL_USD = 0.02   # full-context single-candidate decision

# Safety bounds — Claude can never set values outside these regardless of
# what it returns. Same discipline as claude_warm._clamp / claude_strategist.BOUNDS.
BOUNDS = {
    "tp_x":     (2.0, 15.0),
    "sl_pct":   (15.0, 40.0),
    "size_mult": (0.5, 1.5),  # multiplies claude_discretionary_size_sol
}

# ── Module state (in-process, acceptable-on-restart — same pattern as claude_warm) ──
_daily_spend: dict = {"date": "", "usd": 0.0}
_trades_today: dict = {"date": "", "n": 0}
_announced_open: set[int] = set()    # trade_id already got its open announcement
_announced_close: set[int] = set()   # trade_id already got its close announcement
_watching_trade_ids: set[int] = set()  # our own open positions, for the close-watcher


TRIAGE_SYSTEM_PROMPT = """You are Claude's fast filter for discretionary memecoin paper trades on "Revolt Agent Hub". You'll see a batch of candidate tokens that already passed every safety screen (rug score, liquidity floor, holder concentration, mcap band) but weren't confident enough for the rule engine to auto-trade.

Your only job: pick which candidates (0, 1, or a few) are worth a full, expensive deep-dive. Be selective — most batches should return few or zero picks. You are not deciding whether to trade, only whether it's worth spending more to look closer.

Output STRICT JSON only:
{"picks": ["<token_address>", ...], "reason": "<one short sentence>"}

An empty picks list is a completely normal, expected response."""

DECIDE_SYSTEM_PROMPT = """You are Claude, deciding whether to open a discretionary paper position on "Revolt Agent Hub" — a Solana memecoin paper-trading bot. This candidate already passed every safety screen (rug score, liquidity, holder concentration, mcap band); your judgment is the only remaining filter.

This is PAPER capital, not real money — but you're picking this trade entirely on your own judgment, not a signal or rule, and you'll explain your reasoning publicly to the community afterward. Be honest and specific, not hype.

Output STRICT JSON only:
{
  "action": "OPEN"|"SKIP",
  "tp_x": <2.0-15.0>,
  "sl_pct": <15.0-40.0>,
  "size_mult": <0.5-1.5, multiplies the base position size>,
  "reason": "<2-3 specific sentences — what you actually saw, will be posted publicly>",
  "confidence": "low|medium|high"
}

Only ever pick "OPEN" at confidence "high" — if you're not genuinely confident, SKIP. There's no penalty for skipping; there's a real cost (public, on your own name) to a bad public call."""


def _today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _spend_remaining(budget: float) -> float:
    today = _today_key()
    if _daily_spend["date"] != today:
        _daily_spend.update(date=today, usd=0.0)
    return max(0.0, budget - _daily_spend["usd"])


def _spend_record(usd: float) -> None:
    today = _today_key()
    if _daily_spend["date"] != today:
        _daily_spend.update(date=today, usd=0.0)
    _daily_spend["usd"] += usd


def _trades_today_count() -> int:
    today = _today_key()
    if _trades_today["date"] != today:
        _trades_today.update(date=today, n=0)
    return _trades_today["n"]


def _trades_today_record() -> None:
    today = _today_key()
    if _trades_today["date"] != today:
        _trades_today.update(date=today, n=0)
    _trades_today["n"] += 1


def _clamp(v, lo, hi):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return (lo + hi) / 2.0
    return max(lo, min(hi, v))


async def _fetch_monitor_candidates(window_min: float, min_rug_score: float, limit: int = 12) -> list["Candidate"]:
    cutoff = datetime.utcnow() - timedelta(minutes=window_min)
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(Candidate)
            .where(
                Candidate.decision == "monitor",
                Candidate.created_at >= cutoff,
                Candidate.rug_score.is_not(None),
                Candidate.rug_score >= min_rug_score,
                Candidate.executed.is_(False),
            )
            .order_by(Candidate.created_at.desc())
            .limit(limit)
        )).scalars().all()
    return list(rows)


async def _dedup_ok(mint: str) -> bool:
    if await has_open_paper_trade(mint):
        return False
    if await has_recent_close(mint, within_hours=2.0):
        return False
    return True


async def _log_decision(
    *, candidate: "Candidate | None", token_name: str, token_address: str,
    action: str, params: dict, reason: str, confidence: str,
    model_tier: str, latency_ms: int, cost_usd: float, trade_id: int | None = None,
) -> int | None:
    try:
        async with AsyncSessionLocal() as session:
            row = ClaudeDiscretionaryAction(
                candidate_id=candidate.id if candidate else None,
                token_name=token_name[:64] if token_name else None,
                token_address=token_address[:64] if token_address else None,
                action=action[:16],
                params_json=json.dumps(params, separators=(",", ":")),
                reason=(reason or "")[:512],
                confidence=(confidence or "")[:16],
                rug_score=float(candidate.rug_score) if candidate and candidate.rug_score is not None else None,
                model_tier=model_tier[:16],
                latency_ms=latency_ms,
                cost_usd=cost_usd,
                trade_id=trade_id,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row.id
    except Exception as exc:
        logger.warning("claude_discretionary: failed to log decision: %s", exc)
        return None


async def _triage(candidates: list["Candidate"]) -> list["Candidate"]:
    """Stage 1 — cheap batched Haiku filter. Returns the subset worth a
    deeper (Sonnet) look. Empty on any failure — fails closed, not open."""
    if not candidates:
        return []
    payload = [
        {
            "token_address": c.token_address,
            "token_name": (c.token_name or "?")[:32],
            "confidence_score": round(float(c.confidence_score or 0), 1),
            "rug_score": round(float(c.rug_score or 0), 1),
            "chart_pattern": c.chart_pattern,
            "source": c.source,
        }
        for c in candidates
    ]
    user_msg = "CANDIDATES\n\n" + json.dumps(payload, separators=(",", ":"))
    t0 = _time.time()
    text = await call_claude(
        system=TRIAGE_SYSTEM_PROMPT, user=user_msg,
        model=HAIKU_MODEL, max_tokens=300, timeout_sec=15.0,
    )
    latency_ms = int((_time.time() - t0) * 1000)
    _spend_record(HAIKU_COST_PER_CALL_USD)
    if not text:
        return []
    parsed = parse_json_response(text)
    if not parsed:
        logger.debug("claude_discretionary: triage unparseable (%dms)", latency_ms)
        return []
    picks = set(parsed.get("picks") or [])
    if not picks:
        return []
    by_addr = {c.token_address: c for c in candidates}
    return [by_addr[a] for a in picks if a in by_addr]


async def _decide(candidate: "Candidate") -> dict | None:
    """Stage 2 — full-context Sonnet decision for one candidate."""
    mint = candidate.token_address
    try:
        pair = await fetch_token_data(mint, allow_any_dex=True)
    except Exception:
        pair = None
    if pair is None:
        return None
    metrics = parse_token_metrics(pair)
    current_mc = float(metrics.get("market_cap") or 0)
    if current_mc <= 0:
        return None

    gmgn_sig: dict = {}
    try:
        from bot.agents.gmgn_agent import gmgn_top_traders, gmgn_token_holders
        traders, holders = await asyncio.gather(
            gmgn_top_traders(mint), gmgn_token_holders(mint), return_exceptions=True,
        )
        if isinstance(traders, list):
            gmgn_sig["top_traders_count"] = len(traders)
        if isinstance(holders, list):
            top10 = holders[:10]
            total_pct = sum(float(h.get("percent") or h.get("percentage") or 0) for h in top10 if isinstance(h, dict))
            gmgn_sig["top10_concentration_pct"] = round(total_pct, 1)
    except Exception as exc:
        logger.debug("claude_discretionary: gmgn signals (%s): %s", mint[:12], exc)

    ctx = {
        "token": {
            "name": (candidate.token_name or mint[:8])[:32],
            "mint": mint,
            "market_cap_usd": int(current_mc),
            "liquidity_usd": float((pair.get("liquidity") or {}).get("usd") or 0),
            "volume_h24_usd": float((pair.get("volume") or {}).get("h24") or 0),
            "price_change_h1_pct": float((pair.get("priceChange") or {}).get("h1") or 0),
            "txns_m5_buys": int((pair.get("txns") or {}).get("m5", {}).get("buys") or 0),
            "txns_m5_sells": int((pair.get("txns") or {}).get("m5", {}).get("sells") or 0),
        },
        "scores": {
            "confidence_score": round(float(candidate.confidence_score or 0), 1),
            "rug_score": round(float(candidate.rug_score or 0), 1),
            "chart_score": round(float(candidate.chart_score or 0), 1) if candidate.chart_score is not None else None,
            "chart_pattern": candidate.chart_pattern,
        },
        "gmgn": gmgn_sig,
    }
    user_msg = "CANDIDATE\n\n" + json.dumps(ctx, separators=(",", ":"), default=str)

    t0 = _time.time()
    text = await call_claude(
        system=DECIDE_SYSTEM_PROMPT, user=user_msg,
        model=SONNET_MODEL, max_tokens=500, timeout_sec=30.0,
    )
    latency_ms = int((_time.time() - t0) * 1000)
    _spend_record(SONNET_COST_PER_CALL_USD)
    if not text:
        return None
    parsed = parse_json_response(text)
    if not parsed:
        logger.debug("claude_discretionary: decide unparseable for %s (%dms)", mint[:12], latency_ms)
        return None

    return {
        "action": str(parsed.get("action", "SKIP")).upper(),
        "tp_x": _clamp(parsed.get("tp_x"), *BOUNDS["tp_x"]),
        "sl_pct": _clamp(parsed.get("sl_pct"), *BOUNDS["sl_pct"]),
        "size_mult": _clamp(parsed.get("size_mult", 1.0), *BOUNDS["size_mult"]),
        "reason": str(parsed.get("reason", ""))[:500],
        "confidence": str(parsed.get("confidence", "low")).lower(),
        "latency_ms": latency_ms,
        "current_mc": current_mc,
        "entry_price": metrics.get("price"),
    }


async def _open_play(candidate: "Candidate", decision: dict, base_size_sol: float) -> None:
    mint = candidate.token_address
    name = candidate.token_name or mint[:8]
    size_sol = round(base_size_sol * decision["size_mult"], 4)

    try:
        pt = await open_paper_trade(
            token_address=mint,
            token_name=name,
            entry_mc=decision["current_mc"],
            entry_price=decision.get("entry_price"),
            paper_sol=size_sol,
            confidence=float(candidate.confidence_score or 0),
            pattern_type="claude_discretionary",
            tp_x=decision["tp_x"],
            sl_pct=decision["sl_pct"],
            trade_reasoning=decision["reason"],
            channel_name="claude_discretionary",
        )
    except PaperTradeBlocked as exc:
        logger.info("claude_discretionary: OPEN blocked by gate for %s: %s", name[:20], exc)
        await _log_decision(
            candidate=candidate, token_name=name, token_address=mint,
            action="SKIP", params=decision, reason=f"gate blocked: {exc}",
            confidence=decision["confidence"], model_tier="sonnet_decision",
            latency_ms=decision["latency_ms"], cost_usd=SONNET_COST_PER_CALL_USD,
        )
        return
    except Exception as exc:
        logger.warning("claude_discretionary: open_paper_trade failed for %s: %s", name[:20], exc)
        return

    _trades_today_record()
    _watching_trade_ids.add(pt.id)
    decision_id = await _log_decision(
        candidate=candidate, token_name=name, token_address=mint,
        action="OPEN", params=decision, reason=decision["reason"],
        confidence=decision["confidence"], model_tier="sonnet_decision",
        latency_ms=decision["latency_ms"], cost_usd=SONNET_COST_PER_CALL_USD,
        trade_id=pt.id,
    )
    logger.info(
        "claude_discretionary: OPEN %s id=%s size=%.3f tp=%.1fx sl=%.0f%% — %s",
        name[:20], pt.id, size_sol, decision["tp_x"], decision["sl_pct"],
        decision["reason"][:80],
    )

    cfg = await get_params("claude_announce_enabled")
    if float(cfg.get("claude_announce_enabled", 1.0) or 1.0) >= 0.5:
        try:
            from bot.ecconos.announce import post_as_ecconos
            text = (
                f"Opened a discretionary paper play: {name}\n"
                f"Size: {size_sol:.3f} SOL | TP {decision['tp_x']:.1f}x | SL {decision['sl_pct']:.0f}%\n\n"
                f"{decision['reason']}"
            )
            await post_as_ecconos(text)
            if decision_id:
                async with AsyncSessionLocal() as session:
                    row = await session.get(ClaudeDiscretionaryAction, decision_id)
                    if row:
                        row.announced = True
                        await session.commit()
        except Exception as exc:
            logger.debug("claude_discretionary: open announcement failed: %s", exc)


async def _skip(candidate: "Candidate", decision: dict) -> None:
    logger.info(
        "claude_discretionary: SKIP %s — %s",
        (candidate.token_name or candidate.token_address[:8])[:20],
        decision["reason"][:80],
    )
    await _log_decision(
        candidate=candidate, token_name=candidate.token_name or "", token_address=candidate.token_address,
        action="SKIP", params=decision, reason=decision["reason"],
        confidence=decision["confidence"], model_tier="sonnet_decision",
        latency_ms=decision["latency_ms"], cost_usd=SONNET_COST_PER_CALL_USD,
    )


async def _check_closes() -> None:
    """Relay a close announcement for any of OUR positions that closed
    since we last checked. No new close logic — claude_warm_loop (or SL/
    TP/trail) already closed it; we just notice and announce."""
    if not _watching_trade_ids:
        return
    cfg = await get_params("claude_announce_enabled")
    if float(cfg.get("claude_announce_enabled", 1.0) or 1.0) < 0.5:
        return
    done = set()
    for trade_id in list(_watching_trade_ids):
        if trade_id in _announced_close:
            done.add(trade_id)
            continue
        try:
            async with AsyncSessionLocal() as session:
                pt = await session.get(PaperTrade, trade_id)
        except Exception:
            continue
        if not pt or pt.status != "closed":
            continue
        pnl = float(pt.paper_pnl_sol or 0)
        name = pt.token_name or pt.token_address[:8]
        commentary = pt.close_commentary
        try:
            from bot.ecconos.announce import post_as_ecconos
            if commentary:
                text = f"Closed {name}: {pnl:+.4f} SOL ({pt.close_reason or '?'})\n\n{commentary}"
            else:
                text = f"Closed {name}: {pnl:+.4f} SOL ({pt.close_reason or '?'})"
            await post_as_ecconos(text)
        except Exception as exc:
            logger.debug("claude_discretionary: close announcement failed: %s", exc)
        _announced_close.add(trade_id)
        done.add(trade_id)
    _watching_trade_ids.difference_update(done & _announced_close)


async def run_once() -> dict:
    """One evaluation tick: check for closes, and (if enabled) look for a
    new discretionary play. Returns a small summary dict — used by both
    the background loop and the on-demand MCP trigger_discretionary_scan
    tool, so manual testing exercises the exact same code path as
    production, not a parallel copy of it."""
    summary = {"enabled": False, "candidates_seen": 0, "triaged": 0, "opened": 0, "skipped": 0}

    if not claude_available():
        return summary

    cfg = await get_params(
        "claude_discretionary_enabled",
        "claude_discretionary_interval_sec",
        "claude_discretionary_candidate_window_min",
        "claude_discretionary_min_rug_score",
        "claude_discretionary_daily_budget_usd",
        "claude_discretionary_max_trades_per_day",
        "claude_discretionary_max_open",
        "claude_discretionary_size_sol",
    )
    enabled = float(cfg.get("claude_discretionary_enabled") or 0.0) >= 0.5
    summary["enabled"] = enabled
    await _check_closes()
    if not enabled:
        return summary

    daily_budget = float(cfg.get("claude_discretionary_daily_budget_usd") or 2.0)
    window_min = float(cfg.get("claude_discretionary_candidate_window_min") or 20.0)
    min_rug = float(cfg.get("claude_discretionary_min_rug_score") or 55.0)
    max_trades_day = int(cfg.get("claude_discretionary_max_trades_per_day") or 3)
    max_open = int(cfg.get("claude_discretionary_max_open") or 3)
    base_size = float(cfg.get("claude_discretionary_size_sol") or 0.15)

    if _spend_remaining(daily_budget) < HAIKU_COST_PER_CALL_USD:
        logger.info("claude_discretionary: daily budget exhausted, sleeping")
        return summary
    if _trades_today_count() >= max_trades_day:
        return summary

    n_open = await count_open_paper_trades()
    if n_open >= max_open:
        return summary
    bal = await compute_paper_balance()
    if bal < base_size * BOUNDS["size_mult"][0] + 0.02:
        return summary

    candidates = await _fetch_monitor_candidates(window_min, min_rug)
    fresh = []
    for c in candidates:
        if await _dedup_ok(c.token_address):
            fresh.append(c)
    summary["candidates_seen"] = len(fresh)
    if not fresh:
        return summary

    picks = await _triage(fresh)
    summary["triaged"] = len(picks)
    if not picks:
        return summary

    logger.info(
        "claude_discretionary: triage picked %d/%d candidate(s) for deep review",
        len(picks), len(fresh),
    )

    for candidate in picks:
        if _spend_remaining(daily_budget) < SONNET_COST_PER_CALL_USD:
            break
        if _trades_today_count() >= max_trades_day:
            break
        decision = await _decide(candidate)
        if decision is None:
            continue
        if decision["action"] == "OPEN" and decision["confidence"] == "high":
            await _open_play(candidate, decision, base_size)
            summary["opened"] += 1
        else:
            await _skip(candidate, decision)
            summary["skipped"] += 1

    return summary


async def claude_discretionary_loop() -> None:
    """Background loop — calls run_once() every POLL_TICK_SEC (5 min) by
    default. No-ops entirely unless claude_discretionary_enabled=1 and
    ANTHROPIC_API_KEY is set (checked inside run_once)."""
    logger.info(
        "claude_discretionary: starting (will no-op if claude_discretionary_enabled=0 or no API key)"
    )
    while True:
        try:
            await run_once()
            interval = POLL_TICK_SEC
            try:
                cfg = await get_params("claude_discretionary_interval_sec")
                interval = float(cfg.get("claude_discretionary_interval_sec") or POLL_TICK_SEC)
            except Exception:
                pass
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("claude_discretionary: cancelled")
            raise
        except Exception as exc:
            logger.error("claude_discretionary: loop error: %s", exc)
            await asyncio.sleep(60)
