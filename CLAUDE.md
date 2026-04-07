# LowKey Alpha - Telegram AI Trading Network

## Bot Info
- Bot: @LowKeyAlphaAi_bot
- Token: 8760436484:AAFK0Z9krt0XjRkzRDtUYav0XP1GRowy2F0
- Callers HQ ID: -1003852140576
- Main Group ID: -5222687721

## Hosting
- Railway (auto-deploys on git push)
- PostgreSQL database (persistent)
- Helius RPC: beta.helius-rpc.com
- GitHub: GeoMax2525/telegram-trading-network

## Files
- main.py - starts bot, registers routers
- bot/config.py - settings
- bot/handlers.py - scan, pnl, leaderboard commands
- bot/scanner.py - DexScreener API
- bot/keyboards.py - Trade Card buttons
- bot/keybot.py - /keybot settings menu
- bot/trading.py - Jupiter Ultra swap execution
- bot/wallet.py - Solana wallet
- database/models.py - PostgreSQL models

## Commands
- Auto-scan: paste contract address in Callers HQ
- /pnl <contract> - check PNL
- /sl - signal leaders leaderboard
- /lb - top calls leaderboard
- /keybot - trading settings

## Trade Card Buttons
- Chart, Share Signal, Flag as Risky
- 🔑 Key Buy - one click buy via Jupiter Ultra
- 🔫 Full Clip - instant sell 100%

## KeyBot Settings
- Buy Amount (SOL)
- Take Profit (X multiplier)
- Stop Loss (%)
- Wallet Address
- Open Positions (in progress)

## Next To Build
- Open Positions tracker with auto TP/SL
- Multi-sig withdrawals
- Token launch + profit sharing
- AI auto-trader
