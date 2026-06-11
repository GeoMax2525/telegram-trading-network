"""
claude_warm.py — Phase 5.5 Stage 2: Claude as active position manager.

For every open paper trade, Claude gets the full picture (price, on-chain
proxies via cached agents, regime, similar trades) on a ~3-min cadence
and takes a real action: HOLD, SET_TP, SET_SL, TAKE_PARTIAL, SCALE_IN,
or EXIT_NOW. All actions execute. All actions are logged to the
claude_position_actions table.

Why this exists: rule-based exits were closing 1000x runners at 1.1x.
Claude reads the live state and the comparable-trade history and adjusts
TP/SL/size dynamically so winners actually run.

Cost shape
  - Default cadence: 180s per position
  - Smart-skip: if price hasn't moved ±5% since last check, skip
  - Min position age: 120s (let rules settle first)
  - Hard daily budget: $3 (claude_active_daily_budget_usd) — falls back
    to rule-based when exhausted

Safety
  - Max 3 actions per position per hour
  - EXIT only allowed after position age >= 60s
  - SL widening capped at -50% absolute from entry
  - Scale-in total capped at +0.5 SOL per position
  - SET_TP in [1.5, 20.0], SET_SL in [10, 50]
  - EXIT_NOW gated below 1.3x on positions younger than the time stop
    (all-time sub-1.3x claude_exit record: 81 closes, 1 win, -2.0 SOL)

NO Helius polling is introduced. Wallet context comes from the cached
Wallet table scoring (no live re-query). GMGN is consulted via the
existing gmgn_agent cache when available.
"""

import asyncio
import json
import logging
import time as _time
from datetime import datetime, timedelta

from sqlalchemy import select, func

from bot import state
from bot.agents.claude_reasoning import (
    FABLE_MODEL, HAIKU_MODEL, call_claude, claude_available,
    parse_json_response,
)
from bot.scanner import fetch_token_data, parse_token_metrics
from database.models import (
    AsyncSessionLocal, ClaudePositionAction, PaperTrade, Token, Wallet,
    close_paper_trade, get_open_paper_trades, get_params,
)

logger = logging.getLogger(__name__)

# ── Defaults ────────────────────────────────────────────────────────────────
POLL_TICK_SEC          = 30           # how often the loop wakes up
DEFAULT_INTERVAL_SEC   = 180          # per-position cadence
DEFAULT_MIN_AGE_SEC    = 120          # don't analyze positions younger than this
DEFAULT_SKIP_PCT       = 5.0          # skip if price within +/- this % since last check
DEFAULT_DAILY_BUDGET   = 3.0          # $/day cap
DEFAULT_MAX_ACTIONS_HR = 3            # per position
DEFAULT_MIN_EXIT_AGE   = 60           # seconds before EXIT_NOW is allowed
DEFAULT_MAX_SCALE_IN   = 0.5          # SOL ceiling on additive scale-in per position

# Conservative cost estimate per call. Haiku 4.5 ≈ $1/M in + $5/M out.
# 1500 in + 150 out ≈ $0.002 per call.
COST_PER_CALL_USD = 0.002

# Fable 5 escalation — deep reasoning ONLY on winner decisions, where the
# all-time data says Claude adds value (HOLD/LOOSEN/DISABLE_LADDER calls on
# runners). Sub-1x management stays on Haiku + the EXIT gate.
# ~2.5k in @ $10/M + ~3.5k out incl. thinking @ $50/M ≈ $0.20/call.
FABLE_COST_PER_CALL_USD = 0.20
FABLE_COOLDOWN_SEC = 600              # max 1 escalated call per position / 10 min
DEFAULT_FABLE_BUDGET = 3.0            # $/day, falls back to Haiku when spent
_fable_last_call: dict[int, float] = {}   # trade_id → unix ts of last Fable call
_fable_spend: dict = {"date": "", "usd": 0.0}

# ── Module state (in-process) ───────────────────────────────────────────────
_last_checked_ts: dict[int, float] = {}        # trade_id → last unix ts
_last_checked_mc: dict[int, float] = {}        # trade_id → MC at last check
_actions_per_trade: dict[int, list[float]] = {} # trade_id → recent action timestamps
_scale_in_total: dict[int, float] = {}         # trade_id → cumulative scale-in SOL
_daily_spend: dict = {"date": "", "usd": 0.0}


SYSTEM_PROMPT = """You are the active trader for "Revolt Agent Hub", an autonomous Solana memecoin paper-trading bot. You manage open positions in real time.

You receive a JSON context every ~3 minutes per open position. From that context you choose ONE action.

CRITICAL — UNDERSTAND HOW EXITS WORK

The rule layer runs an automatic 4-tranche exit ladder on EVERY position:
   40% of position auto-sells at 2.0x   (recovers risk)
   25% of position auto-sells at 5.0x   (locks big chunk)
   15% of position auto-sells at 10.0x  (locks most of meat)
   20% of position rides on dynamic trail (the "moonbag" — your runner-catcher)

So 80% of every winner ALREADY exits between 2x and 10x via rules.
The TP value (tp_x) is the HARD CAP on the moonbag only. Setting tp_x to 8 means the moonbag (last 20%) exits at 8x — capping your runner-catcher at 8x.

This is why most positions in this bot's history closed at 1-1.3x while the channel's true peaks averaged 8.9x — we kept setting TP too low and the moonbag got cut. Token "NBA Finals Coin" peaked at 1185x; we exited at 1.1x because TP was at our default cap. DO NOT REPEAT THIS.

ACTIONS

  HOLD
    Rules keep monitoring. Use ~60-70% of the time when there's no clear signal.

  SET_TP {"tp_x": <float>}
    Update the moonbag cap. Range 1.5 to 20.
    FLOOR: do NOT set tp_x below 12 unless you see clear exhaustion (top wallets selling, volume collapse, structural break). The tiered trail (20% giveback above 6x) is the runner-capture mechanism now — TP is just the hard ceiling.
    RAISE tp_x toward 20 on tokens with strong momentum + smart money holding + chart intact.
    LOWER below 12 ONLY when you're SURE the move is exhausted (then you should probably EXIT_NOW instead).

  SET_SL {"sl_pct": <float>}
    Update the stop-loss percentage. Range 10 to 50. Tighter (lower) = locks more. Wider = gives room.

  TAKE_PARTIAL {"pct": <float>}
    Sell pct% of remaining position OUTSIDE the ladder. Range 10 to 50.
    Use ON TOP OF the 2x/5x/10x ladder when you want to lock more at a specific moment (e.g., 20% partial at 7x to bank extra ahead of suspected exhaustion). NOT a substitute for raising TP.

  SCALE_IN {"sol": <float>}
    Add SOL to the position. Range 0.05 to 0.3 per single action, total capped at +0.5 SOL per position.
    USE THIS MORE — when a token is confirming the thesis (volume holding + smart money still in + buy/sell ratio above 2:1 + chart breaking out), scaling in 0.15-0.25 SOL is how you compound the edge. The bot historically NEVER did this and that's a real cost.
    DO NOT scale in to average down on a position that's hurting.

  LOOSEN_TRAIL {"pct": <float>}
    Widen the dynamic trail to give the position more room. Range 30 to 70 (% drawdown from peak before trail fires).
    THIS IS THE FIX FOR BOUNTYWORK / NBA FINALS COIN. The default trail tightens as gain grows (50% under 5x, 40% at 5-10x, etc.). On fast pumps it fires after the first dip and we exit before the second leg.
    USE LOOSEN_TRAIL 50-60 when: token has strong momentum + smart money still in + chart structure intact + you believe in the runner.

  TIGHTEN_TRAIL {"pct": <float>}
    Tighter trail to lock more profit. Range 10 to 30 (%).
    Use when conviction drops mid-trade but you want to stay in (e.g., smart money starting to rotate but not gone yet, volume softening).

  DISABLE_LADDER
    Stop the automatic 2x/5x/10x scale-out tranches for this position. The full 100% rides on TP + (possibly widened) trail.
    USE WHEN: high-conviction runner — token has clear momentum + smart money + chart breakout + you believe peak is 10x+. The ladder otherwise auto-sells 80% by 10x, capping a 50x runner at the equivalent of an 8x exit.
    DO NOT USE on uncertain positions. Once disabled, only EXIT_NOW or TP can close the position (besides SL).

  ENABLE_LADDER
    Re-enable the auto-tranche ladder. Use if you previously disabled it but conviction has dropped.

  EXIT_NOW {"reason": "<short>"}
    Close entire remaining position immediately. Use ONLY when:
      - Volume clearly dying with bearish structure (sell pressure > buy pressure for multiple consecutive checks)
      - Smart money clearly rotating out (gmgn.top_traders show realized profit going negative or wallets selling)
      - Token age >30 min with no new high in 10+ min AND momentum dead
      - Clear rug / LP-removal signal
      - top10_concentration_pct just spiked above 80 (whale accumulation = rug risk)
    DO NOT EXIT profitable positions just because price ticked down — the dynamic trail handles that. EXIT_NOW is for genuine threats.
    HARD GATE: EXIT_NOW below 1.3x on a position younger than the time stop is REJECTED by the execution layer. Your all-time record force-exiting sub-1.3x positions is 81 closes / 1 win / -2.0 SOL — the 8-minute time_stop handles early chop better than you do. Don't waste the call; HOLD instead.

OUTPUT — strict JSON only, no markdown, no prose outside it:
{
  "action": "HOLD|SET_TP|SET_SL|TAKE_PARTIAL|SCALE_IN|LOOSEN_TRAIL|TIGHTEN_TRAIL|DISABLE_LADDER|ENABLE_LADDER|EXIT_NOW",
  "params": { ... action-specific ... },
  "reason": "<one specific sentence, max 25 words>",
  "confidence": "low|medium|high"
}

CONTEXT YOU NOW HAVE per position

  position.* — id, token, source channel, size, age, remaining %, realized PnL
  price.*    — entry/current/peak MC, current/peak multiple, drawdown from peak, liquidity, m5 buy/sell counts, h1/h6 % change, h24 volume
  rule_layer.tp_x, sl_pct  — current rule layer targets
  regime.*   — HOT/NEUTRAL/COLD, SOL 24h change
  agents.wallet_tiers_pool — GLOBAL tier-1/2/3 wallet pool counts (cached)
  agents.token_meta        — cached source/platform/liquidity/volume
  agents.gmgn              — PER-TOKEN smart money + holders for THIS mint:
                             - top_traders[]: the actual wallets in this trade now,
                               with realized PnL and tags
                             - top_traders_count: how many tracked wallets are in
                             - holders_count, top10_concentration_pct (rug signal)
  similar_band             — last 10 closed paper trades in 0.5x-2x your entry MC
  account.*                — paper balance, today PnL, win/loss streaks

USE THE GMGN DATA. It's the most actionable signal you have. "tier2/3 smart money holding" written in your reasoning should mean SPECIFIC wallets in gmgn.top_traders showing positive realized — not the generic global pool count.

PRINCIPLES (re-read every call)

1. **THE TRAIL IS THE #1 CAUSE OF MISSED RUNNERS.** Default trail is tiered: 50% giveback below 3x, 30% at 3-6x, 20% above 6x. On a token pumping fast with mid-pump dips, a tight trail exits at the first dip and the second leg runs without us. Bountywork peaked, dipped to ~70% of peak, trail fired at 1.4x, then went to $1.6M MC. If you see strong momentum + smart money + chart intact: call LOOSEN_TRAIL 50-60 IMMEDIATELY (your override replaces the tiers for this position).
2. **DISABLE_LADDER on high-conviction runners.** The 4-tranche auto-ladder (40% at 2x, 25% at 5x, 15% at 10x) auto-sells 80% of position by 10x. On a 20x runner, that's leaving most of the upside on the table. If you can name a specific reason this token has 10x+ left (smart money loading, chart breakout, new ATH on each tick): DISABLE_LADDER + raise TP to 20. Let it ride.
3. **Use gmgn.top_traders, not generic tier pool counts.** When you say "smart money holding" mean the specific wallets in gmgn.top_traders[]. If realized profit is positive and they're still holding, that's a real signal. If they're showing increased balance recently, that's accumulation.
4. **SCALE_IN on confirmed momentum.** Existing scale-in code exists but no agent calls it. Add 0.15-0.25 SOL when volume holds + smart money still in + 2:1 buy/sell + chart breaking out.
5. Memecoins have huge variance. 30% drawdown from peak is normal mid-pump. Do not exit on noise. The trail (after LOOSEN if needed) handles routine dips.
6. Channel hit rate matters more than our recent PnL. Our recent losses reflect bad EXITS (which you're now fixing), not bad signals.
7. Most ticks should be HOLD. The ladder + trail handle ~60-70% of cases. Act only when you can name a specific signal.
"""


def _today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _spend_remaining(budget: float) -> float:
    today = _today_key()
    if _daily_spend["date"] != today:
        _daily_spend["date"] = today
        _daily_spend["usd"] = 0.0
    return max(0.0, budget - _daily_spend["usd"])


def _spend_record(usd: float) -> None:
    today = _today_key()
    if _daily_spend["date"] != today:
        _daily_spend["date"] = today
        _daily_spend["usd"] = 0.0
    _daily_spend["usd"] += usd


async def _should_escalate_to_fable(
    trade_id: int, current_mult: float, peak_mult: float, drawdown_pct: float,
) -> bool:
    """Escalate to Fable 5 only at the decision moments that decide the
    week: position crossing 2x (ladder/trail/ride decisions) or a big
    drawdown on a position that was winning (second leg vs distribution).
    Guarded by per-position cooldown and a separate daily budget."""
    hot = current_mult >= 2.0 or (peak_mult >= 1.5 and drawdown_pct >= 30.0)
    if not hot:
        return False
    now = _time.time()
    if now - _fable_last_call.get(trade_id, 0) < FABLE_COOLDOWN_SEC:
        return False
    try:
        cfg = await get_params("fable_daily_budget_usd")
        budget = float(cfg.get("fable_daily_budget_usd") or DEFAULT_FABLE_BUDGET)
    except Exception:
        budget = DEFAULT_FABLE_BUDGET
    today = _today_key()
    if _fable_spend["date"] != today:
        _fable_spend["date"] = today
        _fable_spend["usd"] = 0.0
    if _fable_spend["usd"] + FABLE_COST_PER_CALL_USD > budget:
        return False
    return True


def _actions_in_last_hour(trade_id: int) -> int:
    now = _time.time()
    cutoff = now - 3600
    arr = _actions_per_trade.get(trade_id, [])
    arr = [t for t in arr if t >= cutoff]
    _actions_per_trade[trade_id] = arr
    return len(arr)


def _record_action_ts(trade_id: int) -> None:
    arr = _actions_per_trade.setdefault(trade_id, [])
    arr.append(_time.time())
    if len(arr) > 20:
        del arr[:-20]


# ── Agent context builders (NO Helius polling) ──────────────────────────────

async def _wallet_tier_signals(token_address: str) -> dict:
    """Read cached wallet tiers from DB only — no live Helius. We surface
    aggregate counts so Claude knows roughly how many high-tier wallets
    are scored against this token historically."""
    try:
        async with AsyncSessionLocal() as session:
            # Count tier-1/2/3 wallets that have any historical trade activity
            # We don't link wallet to specific token here (no live data), but
            # we surface the GLOBAL high-tier wallet pool size so Claude can
            # weight this signal relative to current conditions.
            row = (await session.execute(
                select(
                    func.count(Wallet.address).filter(Wallet.tier == 1).label("t1"),
                    func.count(Wallet.address).filter(Wallet.tier == 2).label("t2"),
                    func.count(Wallet.address).filter(Wallet.tier == 3).label("t3"),
                )
            )).one()
        return {"tier1": int(row.t1 or 0), "tier2": int(row.t2 or 0), "tier3": int(row.t3 or 0)}
    except Exception:
        return {}


async def _similar_trades_context(entry_mc: float, channel_name: str | None) -> dict:
    """Last N closed trades in the same MC band + same channel. Tells
    Claude what historically happens to tokens like this one."""
    if not entry_mc:
        return {}
    low = entry_mc * 0.5
    high = entry_mc * 2.0
    try:
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(
                select(PaperTrade)
                .where(
                    PaperTrade.subscriber_id.is_(None),
                    PaperTrade.status == "closed",
                    PaperTrade.paper_pnl_sol.is_not(None),
                    PaperTrade.entry_mc.is_not(None),
                    PaperTrade.entry_mc >= low,
                    PaperTrade.entry_mc <= high,
                )
                .order_by(PaperTrade.id.desc())
                .limit(10)
            )).scalars().all()
    except Exception:
        return {}
    if not rows:
        return {}
    out = []
    for r in rows:
        out.append({
            "token": (r.token_name or r.token_address[:8])[:18],
            "peak":  round(float(r.peak_multiple or 1.0), 2),
            "pnl":   round(float(r.paper_pnl_sol or 0.0), 3),
            "reason": r.close_reason or "",
        })
    return {"recent_band_closes": out, "band_low": int(low), "band_high": int(high)}


async def _token_metadata(token_address: str) -> dict:
    """Pull whatever cached Token row data we have — patterns matched at
    discovery time, source, etc. No live calls."""
    try:
        async with AsyncSessionLocal() as session:
            t = (await session.execute(
                select(Token).where(Token.mint == token_address)
            )).scalar_one_or_none()
        if not t:
            return {}
        out = {}
        if getattr(t, "source", None): out["source"] = t.source
        if getattr(t, "platform", None): out["platform"] = t.platform
        if getattr(t, "liquidity_usd", None): out["liquidity_usd"] = float(t.liquidity_usd)
        if getattr(t, "volume_24h", None): out["volume_24h"] = float(t.volume_24h)
        return out
    except Exception:
        return {}


# Per-token smart money via GMGN. Cached so multiple Claude active calls
# on the same position within 5 min reuse the result. GMGN uses its own
# paid infrastructure — does NOT touch Helius.
_gmgn_cache: dict[str, tuple[float, dict]] = {}
GMGN_CACHE_SEC = 300


async def _gmgn_per_token_signals(mint: str) -> dict:
    """Pull top traders + holders for THIS specific mint via gmgn-cli.
    Surfaces who's actually in the trade right now (not generic global
    tier pool counts). Cached 5 min per mint."""
    if not mint:
        return {}
    now = _time.time()
    cached = _gmgn_cache.get(mint)
    if cached and (now - cached[0]) < GMGN_CACHE_SEC:
        return cached[1]
    out: dict = {}
    try:
        # Lazy import — gmgn_agent has its own subprocess machinery and
        # circuit-breaker for rate limits. Reuse it.
        from bot.agents.gmgn_agent import gmgn_top_traders, gmgn_token_holders
        traders_task = asyncio.create_task(gmgn_top_traders(mint))
        holders_task = asyncio.create_task(gmgn_token_holders(mint))
        traders, holders = await asyncio.gather(
            traders_task, holders_task, return_exceptions=True,
        )
        if isinstance(traders, list) and traders:
            # Surface top 5 by realized PnL or net buy size
            top5 = traders[:5]
            out["top_traders_count"] = len(traders)
            out["top_traders"] = []
            for tr in top5:
                if not isinstance(tr, dict):
                    continue
                out["top_traders"].append({
                    "addr":     (tr.get("wallet_address") or tr.get("address") or "")[:8],
                    "realized": tr.get("realized_profit") or tr.get("total_profit"),
                    "net_sol":  tr.get("native_balance") or tr.get("balance"),
                    "tag":      tr.get("tag") or tr.get("name") or None,
                })
        if isinstance(holders, list) and holders:
            top10 = holders[:10]
            total_pct = 0.0
            for h in top10:
                if not isinstance(h, dict):
                    continue
                pct = h.get("percent") or h.get("percentage") or 0
                try:
                    total_pct += float(pct)
                except (TypeError, ValueError):
                    pass
            out["holders_count"] = len(holders)
            out["top10_concentration_pct"] = round(total_pct, 1)
    except Exception as exc:
        logger.debug("gmgn per-token (%s): %s", mint[:12], exc)
    _gmgn_cache[mint] = (now, out)
    # Trim cache to keep memory bounded
    if len(_gmgn_cache) > 200:
        oldest = sorted(_gmgn_cache.items(), key=lambda kv: kv[1][0])[:100]
        for k, _ in oldest:
            _gmgn_cache.pop(k, None)
    return out


# ── Per-position evaluation ─────────────────────────────────────────────────

async def evaluate_position(pt) -> dict | None:
    """Build full context, ask Claude, return parsed decision dict.
    Returns None on any failure (caller falls back to no-op = HOLD)."""
    if not claude_available():
        return None

    age_s = 0.0
    if pt.opened_at:
        age_s = (datetime.utcnow() - pt.opened_at).total_seconds()

    # Fresh price
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

    current_mult = current_mc / pt.entry_mc
    peak_mult = float(pt.peak_multiple or 1.0)
    name = pt.token_name or (pt.token_address[:8])

    # Channel name from reasoning string (best effort)
    channel_name = None
    reasoning = pt.trade_reasoning or ""
    if "from " in reasoning:
        try:
            channel_name = reasoning.split("from ", 1)[1].split()[0].strip("|,. ")
        except Exception:
            pass

    # Agent context (all DB / cached / GMGN — no Helius)
    tier_sig, similar, token_meta, gmgn_sig = await asyncio.gather(
        _wallet_tier_signals(pt.token_address),
        _similar_trades_context(float(pt.entry_mc), channel_name),
        _token_metadata(pt.token_address),
        _gmgn_per_token_signals(pt.token_address),
    )

    # Compose the payload
    ctx = {
        "position": {
            "id": pt.id,
            "token": name,
            "source": "tg_signal" if "tg_signal" in (pt.pattern_type or "") else "scanner",
            "channel": channel_name,
            "size_sol": round(float(pt.paper_sol_spent or 0), 4),
            "remaining_pct": round(float(pt.remaining_pct or 100), 1),
            "realized_pnl_sol": round(float(pt.realized_pnl_sol or 0), 4),
            "age_sec": round(age_s),
            "age_min": round(age_s / 60.0, 1),
        },
        "price": {
            "entry_mc": int(pt.entry_mc),
            "current_mc": int(current_mc),
            "current_mult": round(current_mult, 3),
            "peak_mult": round(peak_mult, 3),
            "drawdown_from_peak_pct": round((1 - current_mult / max(peak_mult, 0.01)) * 100, 1) if peak_mult > 0 else 0,
            "liquidity_usd": float((pair.get("liquidity") or {}).get("usd") or 0),
            "volume_h24_usd": float((pair.get("volume") or {}).get("h24") or 0),
            "price_change_h1_pct": float((pair.get("priceChange") or {}).get("h1") or 0),
            "price_change_h6_pct": float((pair.get("priceChange") or {}).get("h6") or 0),
            "txns_m5_buys": int((pair.get("txns") or {}).get("m5", {}).get("buys") or 0),
            "txns_m5_sells": int((pair.get("txns") or {}).get("m5", {}).get("sells") or 0),
        },
        "rule_layer": {
            "tp_x": float(pt.take_profit_x or 0),
            "sl_pct": float(pt.stop_loss_pct or 0),
        },
        "regime": {
            "market_regime": state.market_regime or "NEUTRAL",
            "sol_24h_change_pct": round(float(state.sol_24h_change or 0), 2),
        },
        "agents": {
            "wallet_tiers_pool": tier_sig,
            "token_meta": token_meta,
            # Per-token smart-money / holder data via GMGN (no Helius).
            # When populated, gmgn.top_traders lists the specific wallets
            # currently holding THIS mint, with realized PnL and tags.
            # top10_concentration_pct is what % of supply the top 10
            # holders own — high concentration (>70%) is a rug-risk.
            "gmgn": gmgn_sig,
        },
        "similar_band": similar,
        "account": {
            "paper_balance": round(state.paper_balance, 4),
            "today_pnl": round(state.session_today_wins - state.session_today_losses, 2),
            "consecutive_losses": state.session_consecutive_losses,
            "consecutive_wins": state.session_consecutive_wins,
        },
    }

    user_msg = "POSITION CONTEXT\n\n" + json.dumps(ctx, separators=(",", ":"), default=str)

    drawdown_pct = (1 - current_mult / max(peak_mult, 0.01)) * 100 if peak_mult > 0 else 0.0
    use_fable = await _should_escalate_to_fable(
        pt.id, current_mult, peak_mult, drawdown_pct,
    )
    if use_fable:
        _fable_last_call[pt.id] = _time.time()
        _fable_spend["usd"] += FABLE_COST_PER_CALL_USD

    t0 = _time.time()
    text = await call_claude(
        system=SYSTEM_PROMPT,
        user=user_msg,
        model=FABLE_MODEL if use_fable else HAIKU_MODEL,
        # Reasoning models spend output budget on thinking before any text
        max_tokens=4000 if use_fable else 300,
        timeout_sec=90.0 if use_fable else 15.0,
    )
    latency_ms = int((_time.time() - t0) * 1000)
    if not text:
        return None
    parsed = parse_json_response(text)
    if not parsed:
        logger.debug("claude_active: unparseable response for trade %s", pt.id)
        return None

    reason = str(parsed.get("reason", ""))[:230]
    return {
        "action": str(parsed.get("action", "HOLD")).upper(),
        "params": parsed.get("params") or {},
        "reason": ("[F5] " + reason) if use_fable else reason,
        "confidence": str(parsed.get("confidence", "low")).lower(),
        "model_tier": "fable" if use_fable else "haiku",
        "cost_usd": FABLE_COST_PER_CALL_USD if use_fable else COST_PER_CALL_USD,
        "trade_id": pt.id,
        "name": name,
        "current_mc": current_mc,
        "current_mult": current_mult,
        "peak_mult": peak_mult,
        "age_s": age_s,
        "latency_ms": latency_ms,
        "ctx_for_log": {
            "current_mc": int(current_mc),
            "current_mult": round(current_mult, 3),
            "peak_mult": round(peak_mult, 3),
        },
    }


# ── Action execution ────────────────────────────────────────────────────────

def _clamp(v, lo, hi):
    try: v = float(v)
    except Exception: return (lo + hi) / 2.0
    return max(lo, min(hi, v))


async def _log_action(pt, decision: dict, executed: bool, exec_note: str = "") -> None:
    try:
        async with AsyncSessionLocal() as session:
            row = ClaudePositionAction(
                trade_id      = pt.id,
                token_name    = pt.token_name,
                token_address = pt.token_address,
                action        = decision["action"][:24],
                params_json   = json.dumps(decision.get("params") or {}, separators=(",", ":")),
                reason        = decision.get("reason", "")[:256],
                confidence    = decision.get("confidence", "low")[:16],
                current_mult  = decision.get("current_mult"),
                peak_mult     = decision.get("peak_mult"),
                age_min       = round((decision.get("age_s") or 0) / 60.0, 2),
                latency_ms    = decision.get("latency_ms"),
                cost_usd      = decision.get("cost_usd", COST_PER_CALL_USD),
                executed      = executed,
                exec_note     = (exec_note or "")[:256],
            )
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning("claude_active: failed to log action: %s", exc)


async def _execute(pt, decision: dict, min_exit_age: int, max_scale_in: float) -> tuple[bool, str]:
    """Execute one Claude action. Returns (executed, note)."""
    action = decision["action"]
    params = decision.get("params") or {}

    if action == "HOLD":
        return True, "hold"

    if action == "SET_TP":
        new_tp = _clamp(params.get("tp_x"), 1.5, 20.0)
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    old = trade.take_profit_x
                    trade.take_profit_x = new_tp
                    await session.commit()
                    return True, f"tp {old:.1f} → {new_tp:.1f}"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "SET_SL":
        new_sl = _clamp(params.get("sl_pct"), 10.0, 50.0)
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    old = trade.stop_loss_pct
                    trade.stop_loss_pct = new_sl
                    await session.commit()
                    return True, f"sl {old:.0f}% → {new_sl:.0f}%"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "TAKE_PARTIAL":
        pct = _clamp(params.get("pct"), 10.0, 50.0)
        # Apply to remaining position. Uses same mutation pattern as
        # paper_monitor's scale-out (remaining_pct + realized_pnl_sol).
        current_mult = decision.get("current_mult", 1.0)
        size_sol = float(pt.paper_sol_spent or 0)
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if not trade or trade.status != "open":
                    return False, "trade not open"
                remaining = float(trade.remaining_pct or 100)
                if remaining < pct + 1:
                    return False, f"only {remaining:.0f}% remaining, can't take {pct:.0f}%"
                # Sell pct% of original position at current price
                # realized PnL = size * (pct/100) * (mult - 1)
                slice_sol = size_sol * (pct / 100.0)
                gain = slice_sol * (current_mult - 1)
                trade.remaining_pct = remaining - pct
                trade.realized_pnl_sol = float(trade.realized_pnl_sol or 0) + gain
                await session.commit()
                return True, f"sold {pct:.0f}% at {current_mult:.2f}x → +{gain:.4f} SOL"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "SCALE_IN":
        add = _clamp(params.get("sol"), 0.05, 0.3)
        already = _scale_in_total.get(pt.id, 0.0)
        if already + add > max_scale_in:
            return False, f"scale-in cap: already +{already:.2f}, asked {add:.2f}, max {max_scale_in:.2f}"
        if state.paper_balance < add + 0.1:
            return False, f"balance {state.paper_balance:.3f} insufficient for +{add:.2f}"
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if not trade or trade.status != "open":
                    return False, "trade not open"
                trade.paper_sol_spent = round(float(trade.paper_sol_spent or 0) + add, 4)
                await session.commit()
            _scale_in_total[pt.id] = already + add
            state.paper_balance -= add
            return True, f"added {add:.2f} SOL (total +{already + add:.2f})"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "LOOSEN_TRAIL":
        # Set claude_trail_override_pct higher to widen the trail width.
        # Use for runners with strong momentum + smart money holding.
        pct = _clamp(params.get("pct"), 30.0, 70.0) / 100.0
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    old = trade.claude_trail_override_pct
                    trade.claude_trail_override_pct = pct
                    await session.commit()
                    return True, f"trail {int((old or 0) * 100) if old else 'default'} → {int(pct * 100)}%"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "TIGHTEN_TRAIL":
        # Set claude_trail_override_pct lower to tighten the trail.
        # Use when conviction drops mid-trade but you want to stay in.
        pct = _clamp(params.get("pct"), 10.0, 30.0) / 100.0
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    old = trade.claude_trail_override_pct
                    trade.claude_trail_override_pct = pct
                    await session.commit()
                    return True, f"trail {int((old or 0) * 100) if old else 'default'} → {int(pct * 100)}%"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "DISABLE_LADDER":
        # Stop the automatic 2x/5x/10x scale-out tranches for this
        # position. Use ONLY when Claude has high conviction the move
        # is genuinely big (5x+ upside likely) and wants to ride 100%
        # on TP + trail without the ladder cutting the size in chunks.
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    trade.claude_ladder_disabled = True
                    await session.commit()
                    return True, "ladder disabled — 100% riding TP/trail"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "ENABLE_LADDER":
        # Re-enable the auto-tranche ladder. Use if Claude previously
        # disabled it but now wants the rule layer protection back.
        try:
            async with AsyncSessionLocal() as session:
                trade = await session.get(PaperTrade, pt.id)
                if trade and trade.status == "open":
                    trade.claude_ladder_disabled = False
                    await session.commit()
                    return True, "ladder re-enabled"
            return False, "trade not open"
        except Exception as exc:
            return False, f"db error: {exc}"

    if action == "EXIT_NOW":
        if (decision.get("age_s") or 0) < min_exit_age:
            return False, f"position too young ({decision.get('age_s', 0):.0f}s < {min_exit_age}s)"
        current_mult = decision.get("current_mult", 1.0)
        # Gate: all-time claude_exit record below ~1.3x was 81 closes,
        # 1 win, -2.0 SOL — strictly worse than letting time_stop work.
        # Sub-1.3x exits on young positions belong to the rule layer.
        if current_mult < 1.3:
            age_s = decision.get("age_s") or 0
            try:
                from database.models import get_param
                ts_min = float(await get_param("time_stop_minutes") or 10.0)
            except Exception:
                ts_min = 10.0
            if age_s < ts_min * 60:
                return False, (
                    f"gated: {current_mult:.2f}x < 1.3x, age {age_s / 60:.1f}m "
                    f"< time_stop {ts_min:.0f}m — rules own this exit"
                )
        current_mc = decision.get("current_mc", 0)
        # Calc PnL: remaining * size * (mult - 1) + realized_pnl_sol
        remaining = float(pt.remaining_pct or 100)
        size_sol = float(pt.paper_sol_spent or 0)
        remaining_sol = size_sol * (remaining / 100.0)
        pnl = round(float(pt.realized_pnl_sol or 0) + remaining_sol * (current_mult - 1), 4)
        try:
            await close_paper_trade(
                trade_id=pt.id,
                close_reason="claude_exit",
                pnl_sol=pnl,
                peak_mc=float(pt.peak_mc) if pt.peak_mc else None,
                peak_mult=decision.get("peak_mult"),
            )
            state.paper_balance += size_sol + pnl
            return True, f"closed at {current_mult:.2f}x → {pnl:+.4f} SOL"
        except Exception as exc:
            return False, f"close failed: {exc}"

    return False, f"unknown action: {action}"


# ── Main loop ───────────────────────────────────────────────────────────────

async def claude_warm_loop() -> None:
    """Background loop — Claude actively manages every open paper trade."""
    logger.info("claude_active: starting (will no-op if claude_active_enabled=0 or no API key)")
    while True:
        try:
            if not claude_available():
                await asyncio.sleep(POLL_TICK_SEC * 4)
                continue

            cfg = await get_params(
                "claude_active_enabled",
                "claude_active_interval_sec",
                "claude_active_min_age_sec",
                "claude_active_skip_pct",
                "claude_active_daily_budget_usd",
                "claude_active_max_actions_per_hour",
                "claude_active_min_exit_age_sec",
                "claude_active_max_scale_in_sol",
            )
            enabled = float(cfg.get("claude_active_enabled") or 0.0) >= 0.5
            if not enabled:
                await asyncio.sleep(POLL_TICK_SEC * 4)
                continue

            interval     = float(cfg.get("claude_active_interval_sec") or DEFAULT_INTERVAL_SEC)
            min_age      = float(cfg.get("claude_active_min_age_sec") or DEFAULT_MIN_AGE_SEC)
            skip_pct     = float(cfg.get("claude_active_skip_pct") or DEFAULT_SKIP_PCT)
            daily_budget = float(cfg.get("claude_active_daily_budget_usd") or DEFAULT_DAILY_BUDGET)
            max_actions  = int(cfg.get("claude_active_max_actions_per_hour") or DEFAULT_MAX_ACTIONS_HR)
            min_exit_age = int(cfg.get("claude_active_min_exit_age_sec") or DEFAULT_MIN_EXIT_AGE)
            max_scale_in = float(cfg.get("claude_active_max_scale_in_sol") or DEFAULT_MAX_SCALE_IN)

            # Budget gate
            if _spend_remaining(daily_budget) < COST_PER_CALL_USD:
                logger.info("claude_active: daily budget exhausted, sleeping")
                await asyncio.sleep(300)
                continue

            open_trades = await get_open_paper_trades()
            now = _time.time()
            due = []
            for pt in open_trades:
                # Subscriber relays — let admin Claude manage admin only.
                if pt.subscriber_id is not None:
                    continue
                age_s = (datetime.utcnow() - pt.opened_at).total_seconds() if pt.opened_at else 0
                if age_s < min_age:
                    continue
                last = _last_checked_ts.get(pt.id, 0)
                if (now - last) < interval:
                    continue
                # Smart-skip: hit DexScreener cheaply (cached) and skip if
                # MC is within +/- skip_pct of last seen MC. We always fetch
                # below anyway, so reuse that result by sampling here too.
                # For simplicity skip the pre-check — the eval already
                # fetches once. Cost is dominated by Claude calls, not
                # DexScreener.
                if _actions_in_last_hour(pt.id) >= max_actions:
                    continue
                due.append(pt)

            if not due:
                await asyncio.sleep(POLL_TICK_SEC)
                continue

            logger.info(
                "claude_active: evaluating %d position(s) (open=%d, spent today=$%.3f / $%.2f)",
                len(due), len(open_trades), _daily_spend.get("usd", 0.0), daily_budget,
            )

            # Process sequentially to keep budget tracking exact
            for pt in due:
                if _spend_remaining(daily_budget) < COST_PER_CALL_USD:
                    break
                _last_checked_ts[pt.id] = now
                # Skip-by-price: cheap fetch first, decide whether to call Claude
                try:
                    pair = await fetch_token_data(pt.token_address, allow_any_dex=True)
                except Exception:
                    pair = None
                if pair is None:
                    continue
                metrics = parse_token_metrics(pair)
                current_mc = float(metrics.get("market_cap") or 0)
                if current_mc <= 0:
                    continue
                last_mc = _last_checked_mc.get(pt.id)
                if last_mc and abs(current_mc - last_mc) / last_mc * 100 < skip_pct:
                    # Calm — skip Claude. Still update peak below via monitor.
                    logger.debug(
                        "claude_active: skip %s — MC %s ~%s (%.1f%% < %.1f%%)",
                        (pt.token_name or pt.token_address[:8])[:16],
                        int(current_mc), int(last_mc),
                        abs(current_mc - last_mc) / last_mc * 100, skip_pct,
                    )
                    continue
                _last_checked_mc[pt.id] = current_mc

                decision = await evaluate_position(pt)
                if decision is None:
                    continue
                # Fable escalations charge their own budget (recorded in
                # evaluate_position); only Haiku calls hit the active budget.
                if decision.get("model_tier") != "fable":
                    _spend_record(COST_PER_CALL_USD)

                executed, note = await _execute(pt, decision, min_exit_age, max_scale_in)
                if executed and decision["action"] != "HOLD":
                    _record_action_ts(pt.id)
                await _log_action(pt, decision, executed, note)
                action_label = decision["action"]
                logger.info(
                    "claude_active: %s %s id=%s mult=%.2fx — %s [%s]",
                    action_label, (pt.token_name or pt.token_address[:8])[:16],
                    pt.id, decision["current_mult"], decision.get("reason", "")[:80],
                    "OK: " + note if executed else "SKIP: " + note,
                )

            await asyncio.sleep(POLL_TICK_SEC)

        except asyncio.CancelledError:
            logger.info("claude_active: cancelled")
            raise
        except Exception as exc:
            logger.error("claude_active: loop error: %s", exc)
            await asyncio.sleep(60)
