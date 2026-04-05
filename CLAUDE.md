# telegram-trading-network — Project Summary

## Bot
- **Username:** @LowKeyAlphaAi_bot
- **Bot ID:** 8760436484

## Group IDs
- **CALLER_GROUP_ID:** `-1003852140576` — Callers HQ (where /scan and auto-scan are allowed)
- **MAIN_GROUP_ID:** `-5222687721` — Main group (where Trade Cards are broadcast via Share button)

> Note: CALLER_GROUP_ID changed from `-5223364048` to `-1003852140576` when forum topics were
> enabled, promoting the group to a supergroup with a new Telegram-assigned ID.

## Project Path
`C:\Users\gchel\telegram-trading-network`

## Stack
- Python 3.x + aiogram 3
- SQLite via aiosqlite (database/models.py)
- DexScreener API for token data (bot/scanner.py)
- Virtual environment: `venv/`

## Key Files
| File | Purpose |
|------|---------|
| `main.py` | Entry point — init DB, register router, start polling |
| `bot/handlers.py` | All message/command/callback handlers |
| `bot/config.py` | Loads .env; defines group IDs and scoring weights |
| `bot/scanner.py` | Fetches and scores token data from DexScreener |
| `bot/keyboards.py` | Inline keyboard builder (Share / Flag buttons) |
| `database/models.py` | SQLite schema, log_scan(), get_leaderboard() |

## Running the Bot
```bash
cd C:\Users\gchel\telegram-trading-network && source venv/Scripts/activate && python main.py
```

## Features
- `/scan <address>` — manual token scan (CALLER_GROUP_ID or private chat only)
- **Auto-scan** — paste a bare Solana contract address (32–44 base58 chars, nothing else)
  in CALLER_GROUP_ID and the bot auto-scans it without needing the /scan prefix
- `/leaderboard` — top scanners by scan count
- **Share** button — broadcasts Trade Card to MAIN_GROUP_ID
- **Flag** button — logs token as risky

## aiogram Notes
- Handlers must NOT declare `bot: Bot` as a parameter — use `message.bot` instead.
  Declaring `bot: Bot` causes injection conflicts in aiogram 3 and silently breaks handlers.
- `drop_pending_updates=True` is set in main.py — messages sent while the bot is offline
  are discarded on next startup.
