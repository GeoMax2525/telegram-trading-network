"""
tg_listener.py — ECCO ingestion via a USER account (Telethon/MTProto).

The bot API cannot read messages posted by OTHER bots (trade cards, caller
bots). A user account can see EVERYTHING — human posts, bot posts, channels,
and CAs hidden in inline buttons. This listener joins the same alpha groups
(as a user) and feeds every CA sighting into the same Data Hub as the bot lane.

GATED: dormant unless ECCO_TG_SESSION (a Telethon StringSession for an account
that's IN the target groups) + TG_API_ID + TG_API_HASH are set. The bot still
posts the alerts; this lane only records sightings so nothing is missed.
"""

import os
import asyncio
import logging

from bot.echo import core

logger = logging.getLogger(__name__)

# MUST be a SEPARATE account from the 4am scraper (TG_SESSION_STRING) — running
# two Telethon clients on one session invalidates the auth key. Use a dedicated
# account that's joined the alpha groups.
ECCO_TG_SESSION = os.getenv("ECCO_TG_SESSION", "").strip()
TG_API_ID = os.getenv("TG_API_ID", "").strip()
TG_API_HASH = os.getenv("TG_API_HASH", "").strip()


def tg_listener_enabled() -> bool:
    return bool(ECCO_TG_SESSION and TG_API_ID and TG_API_HASH)


def _cas_from_event(event) -> list[str]:
    """CAs from the message text, hidden link entities, AND inline button URLs
    (trade-card bots stash the CA in a 'Buy'/'Chart' button)."""
    parts = [event.raw_text or ""]
    for ent in (getattr(event.message, "entities", None) or []):
        url = getattr(ent, "url", None)
        if url:
            parts.append(url)
    try:
        for row in (event.message.buttons or []):
            for b in row:
                u = getattr(b, "url", None)
                if u:
                    parts.append(u)
    except Exception:
        pass
    return core.extract_cas(" ".join(parts))


async def ecco_tg_listener_loop() -> None:
    if not tg_listener_enabled():
        logger.info("ECCO TG listener: not configured (ECCO_TG_SESSION/TG_API_ID/TG_API_HASH) — off")
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
    except Exception as exc:
        logger.warning("ECCO TG listener: telethon unavailable: %s", exc)
        return

    await asyncio.sleep(60)  # let the rest of the app boot first
    client = TelegramClient(StringSession(ECCO_TG_SESSION), int(TG_API_ID), TG_API_HASH)

    @client.on(events.NewMessage)
    async def _handler(event):
        try:
            if not (event.is_group or event.is_channel):
                return
            cas = _cas_from_event(event)
            if not cas:
                return
            chat = await event.get_chat()
            chat_id = event.chat_id
            chat_title = getattr(chat, "title", None)
            sender = None
            try:
                sender = await event.get_sender()
            except Exception:
                pass
            is_bot = bool(getattr(sender, "bot", False))
            user_id = None if is_bot else event.sender_id
            username = None
            if sender and not is_bot:
                username = getattr(sender, "username", None) or getattr(sender, "first_name", None)
            link = core.message_link(chat_id, getattr(chat, "username", None), event.message.id)
            await core.ensure_group(chat_id, chat_title)
            for ca in cas:
                try:
                    await core.record_sighting(
                        ca=ca, chat_id=chat_id, chat_title=chat_title,
                        user_id=user_id, username=username,
                        message_id=event.message.id, msg_link=link,
                    )
                    # Snapshot this caller's entry MC now (~2s), off the hot path.
                    from bot.echo.signals import capture_entry_mc
                    asyncio.create_task(capture_entry_mc(ca, chat_id, user_id))
                except Exception as exc:
                    logger.debug("ECCO TG listener: record %s: %s", ca[:8], exc)
            logger.info("ECCO TG listener: %d CA(s) in %s (bot_sender=%s)",
                        len(cas), chat_title or chat_id, is_bot)
        except Exception as exc:
            logger.debug("ECCO TG listener: handler error: %s", exc)

    while True:
        try:
            await client.start()
            me = await client.get_me()
            logger.info("ECCO TG listener: connected as %s — listening to all chats",
                        getattr(me, "username", me.id))
            await client.run_until_disconnected()
        except Exception as exc:
            logger.warning("ECCO TG listener: error %s — reconnecting in 30s", exc)
            await asyncio.sleep(30)
