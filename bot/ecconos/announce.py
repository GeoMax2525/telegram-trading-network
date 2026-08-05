"""
bot/ecconos/announce.py — one-off, send-only Ecconos messaging.

Separate from bot/ecconos/app.py's Dispatcher-bound Bot on purpose: that
one owns a long-polling getUpdates loop; this one only ever calls
send_message. Telegram's Bot API treats those as independent, unrelated
calls — only concurrent getUpdates calls on the same token collide (409
Conflict). A second Bot instance that never polls is safe to run
alongside the first in the same process.

Used by bot/agents/claude_discretionary.py to post open/close
explanations without importing (or depending on) the chat-handler module.
"""

import logging

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties

from bot.config import MAIN_GROUP_ID
from bot.ecconos import ECCONOS_BOT_TOKEN, ecconos_enabled

logger = logging.getLogger(__name__)

_bot: Bot | None = None


def _get_bot() -> Bot | None:
    global _bot
    if not ecconos_enabled():
        return None
    if _bot is None:
        _bot = Bot(ECCONOS_BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
    return _bot


async def post_as_ecconos(text: str, chat_id: int | None = None) -> bool:
    """Send one message as Ecconos. Never raises — a failed announcement
    must never break the trading loop that called it. Returns True if the
    send succeeded."""
    bot = _get_bot()
    if bot is None:
        return False
    target = chat_id if chat_id is not None else MAIN_GROUP_ID
    if not target:
        logger.debug("ecconos.announce: no target chat_id (MAIN_GROUP_ID unset)")
        return False
    try:
        await bot.send_message(chat_id=target, text=text[:4000])
        return True
    except Exception as exc:
        logger.warning("ecconos.announce: send failed: %s", exc)
        return False
