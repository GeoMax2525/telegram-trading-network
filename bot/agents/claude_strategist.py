"""
claude_strategist.py — Phase 5.5 Stage 1: Claude on the entry.

For every 4am signal, Claude decides:
  - GO / NO-GO (skip bad signals before opening)
  - Position size (override the default paper_probe_size)
  - TP target (per-trade override of tg_signal_tp_x)
  - SL width (per-trade override of the hardcoded 20%)

Claude receives full context: token data, liquidity, age, regime state,
recent similar trades, source channel. It returns a structured decision
within safety bounds (size 0.1-1.0 SOL, tp 1.5-20x, sl 10-40%).

Stage 1 = entry decisions only. Stage 2 adds live trade control (Claude
calls scale/exit). Stage 3 adds web search for sentiment/macro context.

Safety rails that ALWAYS apply:
  - Hard timeout: if Claude doesn't respond in ENTRY_TIMEOUT_SEC seconds,
    rule layer takes over and uses defaults
  - Daily budget cap: if claude_spend_today_usd >= claude_daily_budget_usd,
    rule layer takes over until midnight UTC reset
  - Safety bounds: every numeric param is clamped to its safe range
  - Silent no-op if ANTHROPIC_API_KEY isn't set

Costs are tracked per-call (rough token estimates × Anthropic pricing).
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from sqlalchemy import select

from bot import state
from bot.agents.claude_reasoning import (
    SONNET_MODEL, call_claude, claude_available, parse_json_response,
)
from database.models import (
    AsyncSessionLocal, ClaudeEntryDecision, PaperTrade,
    get_param, set_param,
)

logger = logging.getLogger(__name__)

# Anthropic Sonnet pricing (USD per 1M tokens)
SONNET_INPUT_USD_PER_M = 3.0
SONNET_OUTPUT_USD_PER_M = 15.0

# Safety bounds — Claude can never set values outside these ranges
BOUNDS = {
    "size_sol":      (0.1,  1.0),
    "tp_x":          (1.5,  20.0),
    "sl_pct":        (10.0, 40.0),
    "trail_trigger": (1.3,  5.0),
    "trail_pct":     (10.0, 50.0),
}

# Hard timeout — memecoins move fast; if Claude is slow we use rules
ENTRY_TIMEOUT_SEC = 12.0


SYSTEM_PROMPT = """You are the lead trader for "Revolt Agent Hub" — a Solana memecoin sniper that auto-buys 4am Telegram signals on pump.fun launches. You decide whether to take each signal and how to configure it.

Your single goal: increase expectancy per trade.

Recent post-mortems from the bot's losing trades show specific weaknesses you should fix:
- Entering low-quality 4am calls that immediately stop out (-0.1 to -0.2 SOL in 2-6 min)
- Trail too tight on sub-$200K MC runners (lock 0.3 SOL when the move had 5x more in it)
- Holding too long into momentum fade (winners turn into losses)
- One-size-fits-all parameters don't flex by MC size, liquidity, or chart structure

You'll receive a JSON context blob with:
- token: name, mint preview, entry market cap (USD), liquidity (USD), age in seconds
- price_action: h1/h6/h24 changes, 5min buys/sells, 24h volume
- source: channel name + that channel's recent hit rate / avg PnL
- market: regime (HOT/NEUTRAL/COLD), SOL 24h change
- similar_trades: last 5 closed trades in similar MC band (their peak, PnL, close reason)
- defaults: what parameters the rule layer would use if you weren't here

Output STRICT JSON only:
{
  "go": true|false,
  "reason": "<one sentence, specific>",
  "confidence": "low|medium|high",
  "size_sol":      <0.1-1.0, what to spend in SOL>,
  "tp_x":          <1.5-20.0, take-profit multiplier>,
  "sl_pct":        <10-40, stop loss %>,
  "trail_trigger": <1.3-5.0, peak multiple before trail engages>,
  "trail_pct":     <10-50, trail width %>,
  "notes":         "<1-2 sentences on what you're watching during this trade — feeds the postmortem>"
}

If go=false, the numeric fields still need valid values (won't be used).

GUIDELINES (flex these — they're starting points, not rules):
- Sub-$50K MC + strong source + decent liquidity = larger size (0.3-0.5), wider trail (28-35%), tp 8-15x — let runners run
- $50K-$200K MC = moderate size (0.2-0.3), trail 20-28%, tp 5-8x — most common case
- $200K-$1M MC = smaller size (0.15-0.25), trail 15-22%, tp 3-5x — winners fade faster here
- $1M+ MC = often too late, small size (0.1-0.15) or skip
- Liquidity < $5K = skip (rug or unfillable)
- Liquidity $5K-$15K = small size + tight trail (15%)
- Source hit rate < 25% recent = skip unless other signals are strong
- COLD regime = skip more, smaller sizes when entering
- HOT regime = wider trails, bigger sizes, ride momentum
- Token age < 60s = often gets sniped before us; downsize or skip
- If similar_trades shows 4+ losses in last 5, skip this category for now

CRITICAL — about our recent losses
- The bot has historically had bad EXITS, not bad entries. We've captured ~1.3x average peak while the channel's true peak average is ~8.9x. We've left 85% of the upside on the table.
- This means "recent similar-band trades are losses" is OFTEN a reflection of OUR bad exits, NOT a signal that the entry was bad.
- Active position management (claude_active) now adjusts TP/SL/scale-in dynamically. With that running, the historical loss profile is misleading.
- Weight CHANNEL hit rate at this MC band (if provided) HIGHER than our recent PnL when both are available. If the channel shows 30%+ hit rate at 2x in this band, that's a real edge even if our recent realized PnL has been bad.
- Default toward GO when liquidity + chart + source are reasonable. Reject for SPECIFIC structural reasons (low liq, late-pump entry, broken chart, rugcheck risk) — NOT for "recent band losses".

NO markdown fences. JSON only. Be specific in "reason" — "low liquidity + weak source" not "looks bad"."""


def _clamp(v, lo, hi):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return (lo + hi) / 2.0
    return max(lo, min(hi, v))


async def claude_strategist_active() -> bool:
    """True if Claude entry decisions are enabled AND we're under budget."""
    if not claude_available():
        return False
    enabled = float(await get_param("claude_strategist_enabled") or 0.0)
    if enabled < 0.5:
        return False
    if await _is_over_budget():
        return False
    return True


async def _is_over_budget() -> bool:
    budget = float(await get_param("claude_daily_budget_usd") or 5.0)
    spend = float(await get_param("claude_spend_today_usd") or 0.0)
    if spend >= budget:
        logger.info(
            "claude_strategist: daily budget %.2f reached (spent %.2f), rules taking over",
            budget, spend,
        )
        return True
    return False


async def _track_spend(cost_usd: float) -> None:
    cur = float(await get_param("claude_spend_today_usd") or 0.0)
    await set_param(
        "claude_spend_today_usd",
        cur + cost_usd,
        reason="Claude strategist API call",
    )


async def reset_daily_spend() -> None:
    """Call at midnight UTC."""
    await set_param("claude_spend_today_usd", 0.0, reason="daily spend reset")


async def _get_similar_recent_trades(entry_mc: float, limit: int = 5) -> list[dict]:
    """Last N closed 4am trades in similar MC band (±2x). Helps Claude see
    whether this MC range has been winning or bleeding recently."""
    mc_lo = entry_mc * 0.5
    mc_hi = entry_mc * 2.0
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.status == "closed",
                PaperTrade.close_reason != "reset",
                PaperTrade.entry_mc.between(mc_lo, mc_hi),
                PaperTrade.pattern_type.like("%tg_signal%"),
            )
            .order_by(PaperTrade.closed_at.desc())
            .limit(limit)
        )).scalars().all()
    return [
        {
            "name": (r.token_name or "?")[:18],
            "entry_mc": int(r.entry_mc or 0),
            "peak_mult": round(r.peak_multiple or 1.0, 2),
            "pnl_sol": round(r.paper_pnl_sol or 0.0, 4),
            "close_reason": r.close_reason or "?",
        }
        for r in rows
    ]


async def _get_source_stats(channel_name: str, lookback: int = 20) -> dict:
    """Compute recent hit rate + avg PnL for trades from a specific channel.

    Right now we don't tag trades by channel — all 4am sources lump into
    pattern_type='tg_signal'. So this returns aggregate 4am stats, not
    per-channel. When per-channel tagging lands, this query gets a filter."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.subscriber_id.is_(None),
                PaperTrade.status == "closed",
                PaperTrade.close_reason != "reset",
                PaperTrade.pattern_type.like("%tg_signal%"),
            )
            .order_by(PaperTrade.closed_at.desc())
            .limit(lookback)
        )).scalars().all()
    if not rows:
        return {"n": 0, "hit_rate_pct": 0.0, "avg_pnl_sol": 0.0}
    wins = sum(1 for r in rows if (r.paper_pnl_sol or 0) > 0)
    avg_pnl = sum(r.paper_pnl_sol or 0 for r in rows) / len(rows)
    return {
        "n": len(rows),
        "hit_rate_pct": round(wins / len(rows) * 100, 1),
        "avg_pnl_sol": round(avg_pnl, 4),
    }


async def decide_entry(
    *,
    token_name: str,
    mint: str,
    entry_mc: float,
    metrics: dict,
    channel_name: str,
    defaults: dict,
) -> dict | None:
    """Claude decides whether to enter this trade and with what parameters.

    Returns None if Claude unavailable, over budget, timed out, or returned
    unparseable response — caller falls back to rule defaults. Returns dict
    with go/reason/size_sol/tp_x/sl_pct/trail_trigger/trail_pct/notes
    when successful.
    """
    if not await claude_strategist_active():
        return None

    # Build context payload
    regime = getattr(state, "meme_regime", "NEUTRAL") or "NEUTRAL"
    regime_score = getattr(state, "meme_regime_score", 0) or 0
    sol_24h = getattr(state, "sol_24h_change", 0.0) or 0.0

    similar = await _get_similar_recent_trades(entry_mc)
    source_stats = await _get_source_stats(channel_name)

    context = {
        "token": {
            "name": token_name,
            "mint": mint[:6] + "..." + mint[-4:] if len(mint) > 12 else mint,
            "entry_mc_usd": int(entry_mc),
            "liquidity_usd": int(metrics.get("liquidity_usd") or 0),
            "age_hours": metrics.get("age_hours"),
            "volume_24h_usd": int(metrics.get("volume_24h") or 0),
        },
        "price_action": {
            "change_h1_pct":  metrics.get("price_change_h1") or metrics.get("priceChange", {}).get("h1") or 0,
            "change_h6_pct":  metrics.get("price_change_h6") or 0,
            "change_h24_pct": metrics.get("price_change_h24") or 0,
            "buys_5m":  metrics.get("buys_m5") or 0,
            "sells_5m": metrics.get("sells_m5") or 0,
        },
        "source": {
            "channel": channel_name,
            **source_stats,
        },
        "market": {
            "regime": regime,
            "regime_score": regime_score,
            "sol_24h_pct": round(sol_24h, 2),
        },
        "similar_trades_recent_band": similar,
        "current_defaults": defaults,
    }

    user_msg = json.dumps(context, indent=2)

    t0 = time.monotonic()
    try:
        text = await asyncio.wait_for(
            call_claude(
                system=SYSTEM_PROMPT,
                user=user_msg,
                model=SONNET_MODEL,
                max_tokens=512,
            ),
            timeout=ENTRY_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "claude_strategist: entry decision timed out (%ds) on %s — rules will run",
            int(ENTRY_TIMEOUT_SEC), token_name[:20],
        )
        return None
    latency_ms = int((time.monotonic() - t0) * 1000)

    if not text:
        return None

    parsed = parse_json_response(text)
    if not parsed:
        logger.warning("claude_strategist: unparseable JSON for %s", token_name[:20])
        return None

    out = {
        "go": bool(parsed.get("go", False)),
        "reason": str(parsed.get("reason", ""))[:200],
        "confidence": str(parsed.get("confidence", "low")).lower(),
        "notes": str(parsed.get("notes", ""))[:400],
        "size_sol":      _clamp(parsed.get("size_sol", defaults["size_sol"]),
                                *BOUNDS["size_sol"]),
        "tp_x":          _clamp(parsed.get("tp_x", defaults["tp_x"]),
                                *BOUNDS["tp_x"]),
        "sl_pct":        _clamp(parsed.get("sl_pct", defaults["sl_pct"]),
                                *BOUNDS["sl_pct"]),
        "trail_trigger": _clamp(parsed.get("trail_trigger", defaults["trail_trigger"]),
                                *BOUNDS["trail_trigger"]),
        "trail_pct":     _clamp(parsed.get("trail_pct", defaults["trail_pct"]),
                                *BOUNDS["trail_pct"]),
        "latency_ms": latency_ms,
    }

    # Rough cost estimate (token count via len/4 heuristic; close enough)
    in_tokens = (len(SYSTEM_PROMPT) + len(user_msg)) / 4
    out_tokens = len(text) / 4
    cost_usd = (
        in_tokens * SONNET_INPUT_USD_PER_M
        + out_tokens * SONNET_OUTPUT_USD_PER_M
    ) / 1_000_000
    await _track_spend(cost_usd)

    # Persist for /claude_log
    try:
        async with AsyncSessionLocal() as session:
            session.add(ClaudeEntryDecision(
                token_name=token_name,
                token_address=mint,
                channel_name=channel_name,
                entry_mc=entry_mc,
                go=out["go"],
                reason=out["reason"],
                confidence=out["confidence"],
                size_sol=out["size_sol"],
                tp_x=out["tp_x"],
                sl_pct=out["sl_pct"],
                trail_trigger=out["trail_trigger"],
                trail_pct=out["trail_pct"],
                notes=out["notes"],
                latency_ms=latency_ms,
                cost_usd=cost_usd,
                context_json=user_msg,
            ))
            await session.commit()
    except Exception as exc:
        logger.warning("claude_strategist: failed to persist decision: %s", exc)

    logger.info(
        "claude_strategist: %s %s — %s (conf=%s, size=%.2f, tp=%.1f, sl=%.0f, cost=$%.4f, %dms)",
        "GO " if out["go"] else "NO ",
        token_name[:20],
        out["reason"][:60],
        out["confidence"],
        out["size_sol"],
        out["tp_x"],
        out["sl_pct"],
        cost_usd,
        latency_ms,
    )
    return out
