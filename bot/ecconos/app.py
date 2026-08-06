"""
bot/ecconos/app.py — the Ecconos Telegram bot: a separate Bot/Dispatcher
(ECCONOS_BOT_TOKEN) that's a transparent AI developer teammate for REVOLT,
present in the main community chat.

Reads every message (privacy mode is off, by operator choice) for rolling
context, but only GENERATES a Claude reply when addressed: mentioned,
replied to, or DM'd. This is a deliberate default, not a hard limit — the
operator can widen it later. The point is to avoid replying to every line of
chat, which would be both unusable and an unbounded API bill.

Zero trading execution here. Zero silent money-moving. See persona.py for
the honesty/boundary rules baked into every reply.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties

from bot.ecconos import (
    ECCONOS_BOT_TOKEN, ECCONOS_ADMIN_IDS, ECCONOS_DAILY_BUDGET_USD,
    ECCONOS_RATE_LIMIT_PER_HOUR, ecconos_enabled,
)
from bot.ecconos.persona import ECCONOS_SYSTEM
from bot.agents.claude_reasoning import (
    call_claude_with_tools, HAIKU_MODEL, ANTHROPIC_API_KEY,
)

logger = logging.getLogger(__name__)
router = Router()

# ── Real tool-use (Phase 1b) ─────────────────────────────────────────────────
# Read tools: available to EVERYONE, not just admins. These are the same
# read-only MCP tools (mcp_server/server.py) already safe for anyone to
# query — no state mutation, no injection risk beyond API spend, which is
# already budget/rate-limited. Without these, Ecconos only ever had a bare
# open-position COUNT (see _project_context below) and would (correctly,
# honestly) tell people it couldn't answer specifics about what's actually
# open — this fixes that real gap, not a bug, just a missing capability.
ECCONOS_READ_TOOLS = [
    {
        "name": "get_hub_status",
        "description": (
            "Live snapshot: paper balance, all-time and today's PnL, win "
            "rate, open position count. Same data /hub shows."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_open_trades",
        "description": (
            "Every currently open paper position: token name, entry MC, "
            "size, last-known peak multiple, age. Use this whenever "
            "someone asks what's actually open right now."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
]

# Write tools: admin-only (see the is_admin gate in _reply below). Ecconos
# can act, not just describe — it gets the SAME write tools already
# exposed over MCP (mcp_server/server.py), called directly in-process rather
# than over the network (same DB, same process, no reason to round-trip
# through HTTP). Same safety posture as MCP: bounded, reversible, logged
# trading-CONFIG changes only. set_trading_param/set_algo_mode_tool/
# toggle_source already hard-refuse trade_mode/live_trading_armed
# (WRITE_BLOCKLIST in mcp_server/server.py) regardless of what's asked here.
ECCONOS_WRITE_TOOLS = [
    {
        "name": "set_trading_param",
        "description": (
            "Set a trading-config param to a new value, immediately, live. "
            "No confirmation step. `reason` is required and gets logged. "
            "Refuses to touch 'trade_mode' or 'live_trading_armed' under "
            "any circumstance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "value": {"type": "number"},
                "reason": {"type": "string"},
            },
            "required": ["name", "value", "reason"],
        },
    },
    {
        "name": "set_algo_mode_tool",
        "description": (
            "Set a custom algo's mode: 'off', 'manual' (alert only), or "
            "'auto' (trades on its own). Immediate, no confirmation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "mode": {"type": "string"},
            },
            "required": ["name", "mode"],
        },
    },
    {
        "name": "toggle_source",
        "description": (
            "Turn an entire signal source on/off at the structural level. "
            "`source` is one of: 'scanner', '4am', 'algo_engine'. `reason` "
            "is required and logged."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {"type": "string"},
                "enabled": {"type": "boolean"},
                "reason": {"type": "string"},
            },
            "required": ["source", "enabled", "reason"],
        },
    },
    {
        "name": "post_to_main_chat",
        "description": (
            "Post a message to the main Revolt community chat, as yourself "
            "(Ecconos), independent of whatever chat this request came from. "
            "Use for things the whole community should see -- an "
            "introduction, an announcement -- not for replying to whoever "
            "is talking to you right now (that happens automatically)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
            },
            "required": ["text"],
        },
    },
]


async def _execute_ecconos_tool(name: str, tool_input: dict) -> dict:
    from mcp_server.server import (
        set_trading_param, set_algo_mode_tool, toggle_source,
        get_hub_status, get_open_trades,
    )
    if name == "get_hub_status":
        return await get_hub_status()
    if name == "get_open_trades":
        return await get_open_trades()
    if name == "set_trading_param":
        return await set_trading_param(**tool_input)
    if name == "set_algo_mode_tool":
        return await set_algo_mode_tool(**tool_input)
    if name == "toggle_source":
        return await toggle_source(**tool_input)
    if name == "post_to_main_chat":
        from bot.ecconos.announce import post_as_ecconos
        ok = await post_as_ecconos(tool_input.get("text", ""))
        return {"posted": ok}
    return {"error": f"unknown tool '{name}'"}

# Anthropic Haiku pricing estimate (same constants used elsewhere in this repo).
_PRICE_IN = 1.0 / 1_000_000
_PRICE_OUT = 5.0 / 1_000_000

# ── Rolling per-chat context (last N messages, in-memory) ───────────────────
# Gives Ecconos conversational context when it DOES speak, without persisting
# raw chat logs anywhere. Small, bounded, per-process memory only.
_CONTEXT_LEN = 12
_chat_context: dict[int, deque] = defaultdict(lambda: deque(maxlen=_CONTEXT_LEN))

# ── Spend + rate limiting (same shape as bot/web.py's JARVIS guardrails) ────
_spend: dict = {"date": "", "usd": 0.0, "calls": 0}
_rate: dict[int, deque] = defaultdict(deque)


def _today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _spend_check(estimate_usd: float) -> bool:
    today = _today_key()
    if _spend["date"] != today:
        _spend.update(date=today, usd=0.0, calls=0)
    return (_spend["usd"] + estimate_usd) <= ECCONOS_DAILY_BUDGET_USD


def _spend_record(input_toks: int, output_toks: int) -> None:
    cost = input_toks * _PRICE_IN + output_toks * _PRICE_OUT
    today = _today_key()
    if _spend["date"] != today:
        _spend.update(date=today, usd=0.0, calls=0)
    _spend["usd"] += cost
    _spend["calls"] += 1


def _rate_ok(chat_id: int) -> bool:
    now = time.time()
    dq = _rate[chat_id]
    while dq and now - dq[0] > 3600:
        dq.popleft()
    if len(dq) >= ECCONOS_RATE_LIMIT_PER_HOUR:
        return False
    dq.append(now)
    return True


# ── Trigger detection: does this message address Ecconos? ──────────────────
def _is_addressed(message: Message, bot_id: int, bot_username: str | None) -> bool:
    if message.chat.type == "private":
        return True
    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.id == bot_id:
            return True
    if bot_username:
        text = (message.text or message.caption or "")
        if f"@{bot_username}".lower() in text.lower():
            return True
    return False


# ── Live project context (shares the Data Hub — same functions the MCP tools
# use, so nothing Ecconos says can drift from what /hub shows) ──────────────
async def _project_context() -> dict:
    from database.models import (
        get_hub_stats, compute_paper_balance, get_open_paper_trades,
    )
    try:
        stats = await get_hub_stats()
        balance = await compute_paper_balance(20.0)
        open_trades = await get_open_paper_trades()
        return {
            "balance_sol": round(balance, 4),
            "today_pnl_sol": round(float(stats.get("today_pnl") or 0), 4),
            "open_positions": len([t for t in open_trades if t.subscriber_id is None]),
            "trade_mode": "paper",  # Ecconos never asserts live status beyond this — see persona.py
        }
    except Exception as exc:
        logger.debug("ecconos: context fetch failed: %s", exc)
        return {}


async def _reply(message: Message) -> None:
    chat_id = message.chat.id
    if not _rate_ok(chat_id):
        return  # quiet skip — no "rate limited" message, avoid spamming ABOUT spam
    if not ANTHROPIC_API_KEY:
        return
    if not _spend_check(0.002):
        logger.info("ecconos: daily budget reached, staying quiet")
        return

    history = "\n".join(_chat_context[chat_id])
    ctx = await _project_context()
    import json as _json
    ctx_str = _json.dumps(ctx, separators=(",", ":"))

    user_msg = (
        f"RECENT CHAT:\n{history}\n\n"
        f"LIVE PROJECT CONTEXT:\n{ctx_str}\n\n"
        f"Reply to the most recent message, in character."
    )

    # Read tools (get_hub_status, get_open_trades) are available to EVERYONE
    # — safe, no state mutation, this is exactly what community members
    # asking "what's open right now" need real answers to.
    #
    # Write tools are admin-only. Ecconos reads every message in the public
    # group (privacy mode off), so without this gate, ANY user's message
    # could attempt to prompt-inject a live trading-config write
    # (set_trading_param / toggle_source / set_algo_mode_tool) with zero
    # human review — that's a real prompt-injection surface, not something
    # "full autonomy" was ever meant to open up to the public. Ecconos can
    # still act autonomously on its OWN initiative elsewhere (e.g. the
    # discretionary trading loop, which reasons over vetted candidate data,
    # not arbitrary chat text) — this gate is specifically about actions
    # triggered by someone else's message content.
    sender_id = message.from_user.id if message.from_user else None
    is_admin = sender_id is not None and sender_id in ECCONOS_ADMIN_IDS
    tools = ECCONOS_READ_TOOLS + ECCONOS_WRITE_TOOLS if is_admin else ECCONOS_READ_TOOLS
    text = await call_claude_with_tools(
        system=ECCONOS_SYSTEM, user=user_msg, tools=tools,
        tool_executor=_execute_ecconos_tool, model=HAIKU_MODEL, max_tokens=400,
    )
    if not text:
        return
    _spend_record(input_toks=len(user_msg) // 3, output_toks=len(text) // 3)
    try:
        await message.reply(text.strip()[:2000])
    except Exception as exc:
        logger.debug("ecconos: reply send failed: %s", exc)


@router.message(F.chat.type.in_({"group", "supergroup", "private"}))
async def on_message(message: Message) -> None:
    if not message.text and not message.caption:
        return
    label = (message.from_user.username or message.from_user.full_name) if message.from_user else "?"
    _chat_context[message.chat.id].append(f"{label}: {(message.text or message.caption)[:300]}")

    me = await message.bot.get_me()
    if _is_addressed(message, me.id, me.username):
        await _reply(message)


async def start_ecconos() -> None:
    """Boot Ecconos (own token, own polling). No-op if ECCONOS_BOT_TOKEN is
    unset, so the trading bot is unaffected either way."""
    if not ecconos_enabled():
        logger.info("Ecconos: ECCONOS_BOT_TOKEN not set — Ecconos dormant")
        return
    ecconos_bot = Bot(ECCONOS_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
    dp = Dispatcher()
    dp.include_router(router)
    logger.info(
        "Ecconos: starting (admins=%s, budget=$%.2f/day, rate=%d/hr)",
        ECCONOS_ADMIN_IDS, ECCONOS_DAILY_BUDGET_USD, ECCONOS_RATE_LIMIT_PER_HOUR,
    )
    try:
        await ecconos_bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(ecconos_bot)
    finally:
        await ecconos_bot.session.close()
