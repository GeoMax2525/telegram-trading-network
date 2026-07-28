# REVOLT MCP Tools

Lets Claude (Desktop, Code, or Claude.ai) query the bot's real, live Postgres
data directly — the same data `/hub`, `/sourcestats`, `/4amreport`, etc. show
in Telegram — without you running a command and pasting the reply back.

**Read-only. v1 has no write tools** — nothing here can change the bot's
behavior, params, or trades. See "What's not here yet" below.

## What it is

- **Live on Railway, 24/7, as part of the bot itself.** These tools are
  mounted at `/mcp` inside `bot/web.py`'s existing FastAPI dashboard app —
  the same process that already serves the dashboard on Railway. There's
  nothing to install or run to use this day-to-day; it's already running
  whenever the bot is.
- Every number comes straight from the same query functions the bot uses
  internally (`database/models.py`) — no separate/duplicated calculation
  path, so it can never drift from what Telegram shows.
- If DATABASE_URL somehow doesn't resolve to a real Postgres connection
  (shouldn't happen on Railway — it does there already, for the bot itself),
  `bot/web.py` skips mounting `/mcp` and logs a warning instead of crashing
  the whole bot. This is checked at boot; see `HAS_REAL_DB` in `server.py`.

## Connecting Claude to the live endpoint

You need the bot's public Railway URL (Railway dashboard → the service
running `bot/web.py` → **Settings → Networking** → the public domain it's
assigned, something like `https://<something>.up.railway.app`). The MCP
endpoint is that domain + `/mcp`.

**Claude Code:**
```
claude mcp add --transport http revolt-trading https://<your-railway-domain>/mcp
```

**Claude Desktop / Claude.ai:** Settings → Connectors → Add custom connector
→ paste the same URL. (Not `claude_desktop_config.json` — remote servers
configured there are ignored.)

No auth is configured in v1 — this matches `bot/web.py`'s existing dashboard
API (`/api/dashboard`), which has also had no auth gating since it shipped.
Same data, same trust level, nothing new introduced.

## Standalone / local testing (secondary — not needed day-to-day)

The same tools can also run as a local stdio script, useful for testing
against a real Postgres before trusting the live `/mcp` endpoint, or for
debugging:

```
DATABASE_URL="<Railway Postgres PUBLIC connection string>" ./venv/Scripts/python mcp_server/server.py
```

Use the **Public Network** connection string here (Railway → your Postgres
service → Connect tab), not the internal one — a script on your own machine
runs outside Railway's private network. **Treat that URL like a password** —
never commit it, never paste it into chat.

If `DATABASE_URL` is missing or isn't a real Postgres URL, standalone
execution refuses to start and prints a clear error, instead of silently
querying an empty local database. This is a deliberate, hardened check — an
earlier version of `bot/config.py` was found to silently fall back to a
local SQLite file for any non-Postgres value, which would have made this
tool's answers quietly wrong instead of visibly broken.

## Setup (only needed for standalone/local use above — the live /mcp path needs nothing)

```
./venv/Scripts/pip install -r mcp_server/requirements.txt
```
(adds `fastmcp` on top of what's already in the repo's main `requirements.txt`)

## Tools (all read-only)

| Tool | Mirrors |
|---|---|
| `get_hub_status` | `/hub` top summary — balance, PnL, WR, open count |
| `get_source_stats(days)` | `/sourcestats` — per-source (4AM/SCAN/ALGO) + bundle overlay |
| `get_4am_channel_stats(days)` | Condensed `/4amreport` — hit-rates + capture ratio |
| `get_algo_stats` | `/algos` — per-algo mode + 7d performance |
| `get_top_smart_money_wallets(limit)` | `/hub` High Scores |
| `get_wallet_analysis(address)` | `/wallet <address>` — full detail |
| `get_exit_strategy_config(trade_type)` | Live SmartScalingExitManager ladder for a trade type |
| `get_open_trades` | Currently open positions (last-known price, no live fetch) |
| `get_agent_params(names)` | `/getparam` — current value of one or more config keys |
| `get_weekly_report(days)` | `/weeklyreport` — total/strategy/meta PnL, by close reason, 4am subset |
| `get_pnl_outliers(limit)` | `/pnloutliers` — largest abs-PnL trades (finds data anomalies) |
| `get_bundle_wallets(limit)` | `/bundlers` — bundle-participant wallet leaderboard |
| `get_hourly_edge(days)` | `/hourstats` — net PnL by UTC hour opened |
| `get_bot_health` | `/health` — background loop heartbeats + staleness |
| `get_scanner_substats(days)` | `/scannerstats` — scanner performance by internal sub-source |
| `get_4am_channel_attribution(days)` | `/4amattribution` — per-channel 4am edge |
| `get_scanner_gate_status` | `/scannerwhy` — every gate that could block a scanner trade, incl. live in-memory `trade_mode` |

**Note on `get_scanner_gate_status`:** this is the one tool that reads live
in-memory bot state (`bot.state.trade_mode`) directly, not just the
database — possible only because these tools run inside the same process as
the bot now.

## Known reporting gap this tooling surfaced

`get_weekly_report`'s `strategy_pnl_sol` (and the Telegram `/weeklyreport`
it mirrors) currently **excludes `scaled_exit`** trades — SmartScalingExitManager's
own close reason isn't in `STRATEGY_CLOSE_REASONS` or `META_CLOSE_REASONS`
(`database/models.py`). Those trades still count in the overall total, just
not in the strategy-specific breakdown. Not fixed here — flagging it since
it understates the newest exit mechanism's real performance in that one report.

## What's not here yet (deliberately)

No write tools — no `set_param`, no `toggle_algo`, no closing a trade. That's
a real, separate decision to make once this read path has been used and
trusted for a while. Adding write tools means adding real risk (a bad tool
call could change live trading behavior) and would need explicit
confirmation prompts + probably its own review, not something to bolt on
casually alongside the read tools.
