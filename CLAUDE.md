# REVOLT - Telegram AI Trading Network

## SECURITY: never put real secrets in this file or any tracked file
This repo is public. Real values (bot tokens, API keys, DATABASE_URL, RAILWAY_TOKEN) live
ONLY in Railway env vars (production) or local `.env` (git-ignored, confirmed via
`git check-ignore`). Reference them by name here, never by value. (A live bot token
was found hardcoded in this file on 2026-08-09 and rotated + scrubbed — see git history
if investigating; the token itself is invalid now.)

## Bot Info
- Main trading bot: @RevoltTradingBot (token: BOT_TOKEN env var)
- Ecconos (CEO/ops persona, same DB, separate Telegram identity): @EcconosBot (token: ECCONOS_BOT_TOKEN env var)
- Echo (cross-group signal intelligence, dormant unless configured): @EccoOracleBot (token: ECCO_BOT_TOKEN/ECHO_BOT_TOKEN env var)
- Callers HQ ID: CALLER_GROUP_ID env var
- Main Group ID: MAIN_GROUP_ID env var
- Admin Telegram IDs: ADMIN_IDS env var

## Hosting
- Railway (auto-deploys on git push to main) — project `athletic-essence`, service `telegram-trading-network`
- PostgreSQL database (persistent, DATABASE_URL env var)
- Solana RPC: Helius (HELIUS_RPC_URL), with FALLBACK_RPC_URL (e.g. Alchemy) used only when Helius exhausts retries
- GitHub: GeoMax2525/telegram-trading-network (public repo)
- CI: .github/workflows/tests.yml runs pytest on every push to main

## Architecture (current, as of 2026-08-09)
- main.py — boots every background loop (scanner, paper_monitor, wallet_analyst,
  learning_loop, harvester, regime_tracker, tg_scraper, gmgn_agent, laserstream,
  claude_cold/discretionary/warm/engineer, Ecconos, Echo)
- bot/config.py — settings, all secrets from env vars only
- bot/handlers.py — main bot commands (scan, pnl, hub, admin commands)
- bot/scanner.py, bot/agents/scanner_agent.py — DexScreener-based token discovery + scoring
- bot/agents/tg_scraper.py — 4am fast-path signal ingestion (deliberately unfiltered entry,
  risk managed by probe size not entry gates — see Rule #1 comment in that file)
- bot/agents/confidence_engine.py — Agent 5; paper trades gate on PRACTICAL filters
  (rug floor + buy pressure), not the weighted confidence score (Rule #2 — see file)
- bot/agents/wallet_analyst.py, bot/agents/gmgn_agent.py — smart-money wallet scoring,
  two independent sources (Helius Enhanced API + GMGN, GMGN works even if Helius is paused)
- bot/agents/learning_loop.py — Agent 6, self-tunes weights/TP/SL/thresholds every 60s from
  real outcomes
- bot/smart_scaling_exit.py — SmartScalingExitManager, tiered scale-out + ratchet + runner trail
- bot/trading.py, bot/wallet.py — Jupiter Ultra swap execution, Solana wallet (plain RPC only,
  works with any provider via HELIUS_RPC_URL/FALLBACK_RPC_URL)
- bot/live_mirror.py, bot/live_guard.py — live-money 1:1 mirror of paper, gated off by
  live_trading_armed (currently 0 — paper only)
- bot/ecconos/ — Ecconos: CEO/ops Telegram persona + self-shipping code pipeline
  (bot/agents/claude_engineer*.py) with a 6-gate test sequence and hard file blocklist
- bot/echo/ — Echo: separate cross-group intelligence bot, shares the same DB
- mcp_server/server.py — MCP tools (read + write) mounted at /mcp, NO AUTH (v1 tradeoff,
  documented in bot/web.py) — treat the URL itself as sensitive
- database/models.py — all tables, agent_params (DB-tunable config), OpsLog (shared
  engineering-status feed Ecconos reads from)

## Trade Card Buttons
- Chart, Share Signal, Flag as Risky
- 🔑 Key Buy - one click buy via Jupiter Ultra
- 🔫 Full Clip - instant sell 100%

## Commands
- Auto-scan: paste contract address in Callers HQ
- /pnl <contract>, /hub, /keybot, /pausehelius, /resumehelius — see bot/handlers.py for the full admin command list
