# REVOLT MCP Server

Lets Claude (Desktop, Code, or Claude.ai) query the bot's real, live Postgres
database directly — the same data `/hub`, `/sourcestats`, `/4amreport`, etc.
show in Telegram — without you running a command and pasting the reply back.

**Read-only. v1 has no write tools** — nothing here can change the bot's
behavior, params, or trades. See "What's not here yet" below.

## What it is (and isn't)

- A **separate, local process** you run on your own machine — not mounted
  into the bot's own Railway process (`bot/web.py`). If this script has a
  bug, crashes, or hangs, the live bot is completely unaffected.
- Connects to the **same Railway Postgres** the bot uses, via `DATABASE_URL`.
  Every number it reports comes from the exact same query functions the bot
  itself uses (`database/models.py`) — there is no separate/duplicated
  calculation path, so it can't drift from what Telegram shows.

## Setup

1. **Use the main repo's venv** (this server imports `database.models`
   directly, so it needs the same SQLAlchemy/asyncpg/etc. dependencies
   already in the repo root's `requirements.txt`). From the repo root:
   ```
   ./venv/Scripts/pip install -r mcp_server/requirements.txt
   ```
   (adds `fastmcp` on top of what's already installed)

2. **Get the real Postgres connection string.** Railway dashboard → your
   Postgres service → **Connect** tab → the **Public Network** URL (not the
   internal one — this script runs on your machine, outside Railway's
   private network, so it needs the externally-reachable connection string).

3. **⚠️ Treat that URL like a password.** It contains real DB credentials
   for production data. Never commit it, never paste it into chat, never put
   it directly in a config file you might check into git.

4. **Set `DATABASE_URL`** to that string when running this server (see
   "Connecting to Claude" below for exactly where it goes).

### Verify it locally before connecting Claude

```
DATABASE_URL="<your Railway public Postgres URL>" ./venv/Scripts/python mcp_server/server.py
```

If `DATABASE_URL` is missing, or set to something that isn't a real
`postgres://`/`postgresql://` URL, the server **refuses to start** and
prints a clear error instead of silently querying an empty local database.
This is a deliberate, hardened check — an earlier version of this bot's own
config (`bot/config.py`) was found to silently fall back to a local SQLite
file for any non-Postgres value, which would have made this tool's answers
quietly wrong instead of visibly broken.

## Connecting to Claude

**Claude Code:**
```
claude mcp add --transport stdio revolt-trading -- <path-to-venv-python> <path-to-mcp_server/server.py>
```
Set `DATABASE_URL` in the environment Claude Code's config uses for this
server (check `claude mcp` docs for the current env-var syntax — this
changes between versions).

**Claude Desktop:** Settings → Developer → Edit Config, add an entry under
`mcpServers` pointing at the same python executable + script path, with
`DATABASE_URL` in its `env` block.

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

## What's not here yet (deliberately)

No write tools — no `set_param`, no `toggle_algo`, no closing a trade. That's
a real, separate decision to make once this read path has been used and
trusted for a while. Adding write tools means adding real risk (a bad tool
call could change live trading behavior) and would need explicit
confirmation prompts + probably its own review, not something to bolt on
casually alongside the read tools.
