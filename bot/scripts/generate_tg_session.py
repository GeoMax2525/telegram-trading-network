"""
generate_tg_session.py — One-time script to generate a Telethon session string.

Run locally:
    python bot/scripts/generate_tg_session.py

It will ask for your phone number and Telegram verification code,
then print the session string. Copy that into Railway env vars as
TG_SESSION_STRING.

Requires: pip install telethon

You also need TG_API_ID and TG_API_HASH from https://my.telegram.org
"""

import asyncio
import os
import sys

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("Telethon not installed. Run: pip install telethon")
    sys.exit(1)


async def main():
    api_id = os.getenv("TG_API_ID") or input("Enter TG_API_ID: ").strip()
    api_hash = os.getenv("TG_API_HASH") or input("Enter TG_API_HASH: ").strip()

    if not api_id or not api_hash:
        print("ERROR: TG_API_ID and TG_API_HASH are required.")
        print("Get them from https://my.telegram.org")
        sys.exit(1)

    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.start()

    session_string = client.session.save()
    print("\n" + "=" * 60)
    print("SESSION STRING (copy this into Railway env vars):")
    print("=" * 60)
    print(session_string)
    print("=" * 60)
    print("\nSet in Railway as: TG_SESSION_STRING")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
