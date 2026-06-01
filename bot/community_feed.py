"""
community_feed.py — mirrors HQ messages to a public Telegram channel.

The community channel is broadcast-only; the bot must be admin there with
"Post Messages" permission. Silent no-op if COMMUNITY_CHANNEL_ID is 0.

Philosophy: community sees the EXACT same text the operator sees in HQ.
No bespoke formatting — what HQ gets, community gets. The only events
we don't mirror are operator-only ones (admin command output, debug logs).

Failures are swallowed and logged — never blocks the primary HQ send.
"""

import logging

from bot.config import COMMUNITY_CHANNEL_ID

logger = logging.getLogger(__name__)


def community_enabled() -> bool:
    return COMMUNITY_CHANNEL_ID != 0


async def post_to_community(bot, text: str, *, parse_mode: str | None = None) -> None:
    """Best-effort post to the community channel. Never raises."""
    if not community_enabled() or bot is None or not text:
        return
    try:
        await bot.send_message(
            COMMUNITY_CHANNEL_ID, text, parse_mode=parse_mode,
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logger.warning(
            "community_feed: post failed (channel=%d): %s",
            COMMUNITY_CHANNEL_ID, exc,
        )
