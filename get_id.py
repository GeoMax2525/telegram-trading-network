import asyncio
from aiogram import Bot
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    bot = Bot(token=os.getenv("BOT_TOKEN"))
    updates = await bot.get_updates()
    for u in updates:
        if u.message and u.message.chat:
            chat = u.message.chat
            print(f"Chat name: {chat.title or chat.first_name} | ID: {chat.id}")
    await bot.session.close()

asyncio.run(main())
