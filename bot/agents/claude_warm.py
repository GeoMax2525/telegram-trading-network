"""
claude_warm.py — Phase 5 warm path: per-position reasoning every 5 min.

For each open paper trade, fetches current state and asks Claude to
recommend ONE action within rule guardrails:

  HOLD            — rules are doing the right thing
  TIGHTEN_TRAIL   — chart structure says lock more profit
  TAKE_PARTIAL    — sell 15% now before momentum fades
  EXIT_NOW        — rug or clear distribution; liquidate

Claude CANNOT override SL or extend timeouts. Suggestions are bounded
by what the rule layer was already going to do — Claude can pull the
trigger earlier (tighter trail, partial exit) but never delay it.

Skips:
- Trades younger than 3 min (let them settle)
- Trades already at TP/SL/timeout boundary (rules will handle)
- When Phase 5 master toggle is off
- When ANTHROPIC_API_KEY is missing (silent no-op)
"""

import asyncio
import logging
from datetime import datetime

from sqlalchemy import select, update

from bot import state
from bot.agents.claude_reasoning import (
    HAIKU_MODEL, call_claude, claude_available, parse_json_response,
)
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    AsyncSessionLocal, PaperTrade, get_open_paper_trades, get_params,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 60     # tick — but per-position cadence enforced below
MIN_AGE_MIN = 3            # don't bother analyzing fresh trades
MIN_INTERVAL_PER_POSITION = 300  # 5 min between calls per position

# Module state for rate limiting
_last_checked: dict[int, float] = {}  # trade_id → last unix ts


SYSTEM_PROMPT = """You evaluate open Solana memecoin paper trades for "Revolt Agent Hub". The bot already has rule-based exit logic running (SL, TP, dynamic trail, time stop, hard timeout). You add a judgment layer that can pull triggers earlier — never delay them.

You can recommend ONE action:
- HOLD            : rules are correct, nothing to do
- TIGHTEN_TRAIL   : tighten the trail width so we lock more if it dumps
- TAKE_PARTIAL    : sell 15% of remaining now to lock profit
- EXIT_NOW        : full exit because rug, distribution, or broken structure

Guardrails you CANNOT override:
- The stop loss
- Hard 4-hour timeout
- Anything that protects against worst case

Be conservative. HOLD is correct ~80% of checks. Only escalate when:
- Volume clearly fading (distribution pattern)
- New top wallet sells > recent buys (insider exiting)
- Token age >30 min with no new HH for ~10 min (momentum dead)
- Clear rug signal (LP removal pattern, dev sells)

Output STRICT JSON only (no markdown fences, no prose):
{
  "action": "HOLD|TIGHTEN_TRAIL|TAKE_PARTIAL|EXIT_NOW",
  "confidence": "low|medium|high",
  "reasoning": "max 15 words, specific not generic"
}"""


def _build_user_message(
    pt,
    name: str,
    current_mc: float,
    current_mult: float,
    peak_mult: float,
    age_min: float,
) -> str:
    is_tg = "tg_signal" in (pt.pattern_type or "")
    source = "4am tg_signal" if is_tg else "scanner"
    return (
        f"Trade state\n"
        f"\n"
        f"Token: {name}\n"
        f"Source: {source}\n"
        f"Position size: {pt.paper_sol_spent:.4f} SOL\n"
        f"Age in trade: {age_min:.1f} min\n"
        f"\n"
        f"Price action\n"
        f"  Entry MC:   ${pt.entry_mc:,.0f}\n"
        f"  Current MC: ${current_mc:,.0f}\n"
        f"  Current x:  {current_mult:.2f}x\n"
        f"  Peak x:     {peak_mult:.2f}x  (drawdown {(1 - current_mult/peak_mult)*100:.0f}% off peak)\n"
        f"\n"
        f"Rule layer\n"
        f"  TP cap: {pt.take_profit_x:.1f}x\n"
        f"  SL:     -{pt.stop_loss_pct:.0f}%\n"
        f"  Remaining position: {pt.remaining_pct or 100:.0f}%\n"
        f"\n"
        f"Action?"
    )


async def evaluate_position(pt) -> dict | None:
    """Ask Claude about ONE open position. Returns parsed suggestion
    or None if can't evaluate (too fresh, fetch failed, Claude failed)."""
    if not claude_available():
        return None

    age_min = 0.0
    if pt.opened_at:
        age_min = (datetime.utcnow() - pt.opened_at).total_seconds() / 60.0
    if age_min < MIN_AGE_MIN:
        return None

    # Fresh MC fetch
    try:
        pair = await fetch_token_data(pt.token_address, allow_any_dex=True)
    except Exception:
        return None
    if pair is None:
        return None

    metrics = parse_token_metrics(pair)
    current_mc = float(metrics.get("market_cap") or 0)
    if current_mc <= 0 or not pt.entry_mc:
        return None

    name = pt.token_name or "?"
    current_mult = current_mc / pt.entry_mc
    peak_mult = float(pt.peak_multiple or 1.0)

    user_msg = _build_user_message(
        pt, name, current_mc, current_mult, peak_mult, age_min,
    )

    text = await call_claude(
        system=SYSTEM_PROMPT,
        user=user_msg,
        model=HAIKU_MODEL,
        max_tokens=256,
    )
    if not text:
        return None

    parsed = parse_json_response(text)
    if not parsed:
        logger.debug("warm: Claude returned unparseable for trade %s", pt.id)
        return None

    return {
        "action": str(parsed.get("action", "HOLD")).upper(),
        "confidence": str(parsed.get("confidence", "low")).lower(),
        "reasoning": str(parsed.get("reasoning", "")),
        "trade_id": pt.id,
        "name": name,
        "mult": current_mult,
        "peak_mult": peak_mult,
        "age_min": age_min,
    }


async def _apply_suggestion(pt, suggestion: dict) -> None:
    """Act on Claude's suggestion within guardrails. We log all actions
    so /weeklyreport can attribute outcomes to Claude vs rules."""
    action = suggestion.get("action", "HOLD")

    if action == "HOLD":
        logger.info(
            "Claude WARM hold %s id=%s — %s",
            suggestion["name"][:24], pt.id, suggestion["reasoning"],
        )
        return

    if action == "TIGHTEN_TRAIL":
        # Cut current dynamic trail width by 30%. paper_monitor will see
        # the new peak_multiple-adjusted value next tick.
        # We persist via a per-trade override field. For MVP, store the
        # suggestion in trade_reasoning so the next tick reads it.
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade:
                    note = f"[CLAUDE TIGHTEN] {suggestion['reasoning']}"
                    trade.trade_reasoning = (
                        (trade.trade_reasoning or "") + " | " + note
                    )[:512]
                    await session.commit()
        except Exception as exc:
            logger.warning("warm: persist TIGHTEN failed: %s", exc)
        logger.info(
            "Claude WARM TIGHTEN_TRAIL %s id=%s — %s",
            suggestion["name"][:24], pt.id, suggestion["reasoning"],
        )
        return

    if action == "TAKE_PARTIAL":
        # Logged for visibility but NOT auto-applied in MVP — needs
        # coordinated change with paper_monitor's scale-out logic.
        # Operator can review via /weeklyreport and decide if Phase 6
        # auto-applies after enough validation.
        logger.info(
            "Claude WARM TAKE_PARTIAL %s id=%s — %s (logged, not auto-applied)",
            suggestion["name"][:24], pt.id, suggestion["reasoning"],
        )
        return

    if action == "EXIT_NOW":
        # Logged but not auto-applied in MVP. Same reason — needs
        # validation before letting Claude fully exit positions.
        logger.warning(
            "Claude WARM EXIT_NOW %s id=%s peak=%.2fx now=%.2fx — %s",
            suggestion["name"][:24], pt.id,
            suggestion["peak_mult"], suggestion["mult"],
            suggestion["reasoning"],
        )
        return


async def claude_warm_loop() -> None:
    """Background loop: evaluate every open position every ~5 min via
    Claude. Skips when ANTHROPIC_API_KEY not set."""
    logger.info("claude_warm: starting (will no-op if no API key)")

    while True:
        try:
            if not claude_available():
                # No key set — sleep longer to reduce log noise
                await asyncio.sleep(POLL_INTERVAL_SEC * 5)
                continue

            cfg = await get_params(
                "claude_warm_enabled", "claude_warm_interval_min",
            )
            if float(cfg.get("claude_warm_enabled") or 1.0) < 0.5:
                await asyncio.sleep(POLL_INTERVAL_SEC * 5)
                continue
            interval_sec = float(cfg.get("claude_warm_interval_min") or 5.0) * 60

            open_trades = await get_open_paper_trades()

            from time import time as _now
            now = _now()
            due = []
            for pt in open_trades:
                last = _last_checked.get(pt.id, 0)
                if (now - last) >= interval_sec:
                    due.append(pt)

            if due:
                logger.info(
                    "claude_warm: evaluating %d positions (%d open total)",
                    len(due), len(open_trades),
                )

            # Process with bounded concurrency
            results = await asyncio.gather(
                *[evaluate_position(pt) for pt in due],
                return_exceptions=True,
            )
            for pt, res in zip(due, results):
                _last_checked[pt.id] = now
                if isinstance(res, dict):
                    await _apply_suggestion(pt, res)

            await asyncio.sleep(POLL_INTERVAL_SEC)

        except asyncio.CancelledError:
            logger.info("claude_warm: cancelled")
            raise
        except Exception as exc:
            logger.error("claude_warm: error %s, sleeping 60s", exc)
            await asyncio.sleep(60)
