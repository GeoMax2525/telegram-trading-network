"""
claude_reasoning.py — Phase 5 Claude API integration for the trading bot.

Architecture — three speed lanes:

  HOT path  (sub-second, rule-based): paper_monitor TP/SL/trail/timeout.
    Claude NEVER touches this. Rules are deterministic and fast.

  WARM path (5 min per open position, Haiku model): claude_warm.py reads
    open positions and suggests HOLD / TIGHTEN_TRAIL / TAKE_PARTIAL /
    EXIT_NOW within rule guardrails. Cannot override SL.

  COLD path (daily at 9am UTC, Sonnet model): claude_cold.py reads the
    last 24h of closed trades and proposes parameter changes. Operator
    reviews via /strategy_review and applies with /apply_review.

  CLOSE commentary (per trade close, Haiku): on every close, fire-and-
    forget Claude call generates a "why this happened" paragraph that
    gets sent as a follow-up to the close card.

Cost target: ~$50-80/mo at 50 trades/day. Haiku for warm/close keeps
volume cheap; Sonnet only for daily reasoning.

If ANTHROPIC_API_KEY is not set, every claude_available() check returns
False and all Phase 5 components silently no-op. Bot works without it.
"""

import json
import logging
import os

import aiohttp

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

# Model selection — Haiku for high-frequency, Sonnet for reasoning
HAIKU_MODEL = os.getenv("ANTHROPIC_HAIKU_MODEL", "claude-haiku-4-5")
SONNET_MODEL = os.getenv("ANTHROPIC_SONNET_MODEL", "claude-sonnet-4-6")

TIMEOUT_SEC = 15.0

# Last failure detail from call_claude — lets operator commands show the
# real error instead of "check Railway logs".
LAST_ERROR: str | None = None


def claude_available() -> bool:
    """Master check — Phase 5 components no-op when this returns False."""
    return bool(ANTHROPIC_API_KEY)


async def call_claude(
    *,
    system: str,
    user: str,
    model: str = HAIKU_MODEL,
    max_tokens: int = 512,
    tools: list | None = None,
    timeout_sec: float = TIMEOUT_SEC,
) -> str | None:
    """Generic Claude API caller. Returns response text or None on failure
    (no key, network error, non-200, parse error). Callers handle None
    gracefully (fall back to rule-based behavior).

    Optional `tools` argument passes a tool list (e.g. server-side
    web_search). Anthropic handles tool calls internally; the response
    still surfaces final text blocks which this helper concatenates."""
    global LAST_ERROR
    if not ANTHROPIC_API_KEY:
        LAST_ERROR = "ANTHROPIC_API_KEY not set"
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    if tools:
        body["tools"] = tools

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(ANTHROPIC_URL, headers=headers, json=body) as resp:
                if resp.status != 200:
                    err = await resp.text()
                    logger.warning("Claude HTTP %d: %s", resp.status, err[:300])
                    LAST_ERROR = f"HTTP {resp.status} (model={model}): {err[:200]}"
                    return None
                data = await resp.json()
    except Exception as exc:
        logger.warning("Claude API failed: %s", exc)
        LAST_ERROR = f"{type(exc).__name__}: {exc} (model={model}, timeout={timeout_sec}s)"
        return None

    blocks = data.get("content", []) or []
    text = "".join(
        b.get("text", "") for b in blocks
        if isinstance(b, dict) and b.get("type") == "text"
    )
    if text:
        LAST_ERROR = None
        return text
    LAST_ERROR = f"200 OK but no text blocks (model={model}, stop={data.get('stop_reason')})"
    return None


def parse_json_response(text: str) -> dict | None:
    """Strip markdown fences and parse JSON. Returns None on parse failure."""
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if "```" in text:
            text = text.rsplit("```", 1)[0]
        text = text.strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.debug("Claude returned non-JSON: %s  body=%s", exc, text[:200])
        return None
