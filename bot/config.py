"""
config.py — Loads environment variables and defines all scoring weights,
group IDs, and constants used throughout the bot.
"""

import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# ── Telegram credentials ──────────────────────────────────────────────────────
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

# Group where /scan is allowed (callers / alpha group)
CALLER_GROUP_ID: int = int(os.getenv("CALLER_GROUP_ID", "0"))

# Group where Trade Cards are broadcast
MAIN_GROUP_ID: int = int(os.getenv("MAIN_GROUP_ID", "0"))

# Thread ID of the Scans topic inside CALLER_GROUP_ID.
# Used to route TP/SL notifications to the correct topic thread.
# Set SCAN_TOPIC_ID in Railway env vars (see instructions for finding it).
SCAN_TOPIC_ID: int | None = int(os.getenv("SCAN_TOPIC_ID", "0")) or None

# Comma-separated Telegram user IDs with admin privileges
ADMIN_IDS: list[int] = [
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().lstrip("-").isdigit()
]

# ── AI Scoring weights (must sum to 100) ──────────────────────────────────────
SCORE_WEIGHTS = {
    "liquidity":            20,   # liquidity health (liq/MC ratio)
    "volume":               20,   # volume velocity (turnover + pace)
    "momentum":             20,   # price momentum (sweet spot detection)
    "holder_distribution":  15,   # holder activity + buy/sell balance
    "contract_safety":      15,   # honeypot / wash trading detection
    "market_strength":      10,   # MC range + volume-momentum alignment
}

# ── Verdict thresholds (quality score — timing is applied separately) ─────────
# Final verdict uses both quality + timing in calculate_ai_score()
VERDICT_THRESHOLDS = {
    "STRONG BUY":  80,   # quality >= 80 (timing check in calculate_ai_score)
    "GOOD ENTRY":  70,   # quality >= 70
    "PROMISING":   55,   # quality >= 55
    "RISKY":       35,   # quality >= 35
    "AVOID":        0,   # quality < 35
}

# ── DexScreener ───────────────────────────────────────────────────────────────
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"

# ── Solana RPC ────────────────────────────────────────────────────────────────
# Set HELIUS_RPC_URL in Railway env vars for a dedicated key, e.g.:
#   https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
# ── GMGN API ─────────────────────────────────────────────────────────────────
GMGN_API_KEY: str = os.getenv("GMGN_API_KEY", "")
GMGN_PRIVATE_KEY: str = os.getenv("GMGN_PRIVATE_KEY", "")

HELIUS_RPC_URL: str = os.getenv(
    "HELIUS_RPC_URL",
    "https://mainnet.helius-rpc.com/?api-key=demo",
)

# HELIUS_API_KEY: prefer standalone env var, fall back to extracting from RPC URL.
from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
_parsed_rpc = _urlparse(HELIUS_RPC_URL)
_key_from_url = _parse_qs(_parsed_rpc.query).get("api-key", [""])[0]
HELIUS_API_KEY: str = os.getenv("HELIUS_API_KEY", "") or _key_from_url or "demo"

# Business plan endpoints
HELIUS_PARSE_URL: str = os.getenv(
    "HELIUS_PARSE_URL",
    "https://api.helius.xyz/v0",
)
HELIUS_LASERSTREAM_URL: str = os.getenv("HELIUS_LASERSTREAM_URL", "")

# ── Database ──────────────────────────────────────────────────────────────────
# Railway injects DATABASE_URL as postgres:// or postgresql://.
# asyncpg (SQLAlchemy async driver) requires the postgresql+asyncpg:// scheme.
# Falls back to SQLite when DATABASE_URL is not set (local dev).
_raw_db_url: str = os.getenv("DATABASE_URL", "")
if _raw_db_url.startswith("postgres://"):
    DATABASE_URL: str = _raw_db_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif _raw_db_url.startswith("postgresql://"):
    DATABASE_URL = _raw_db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    DATABASE_URL = "sqlite+aiosqlite:///./trading_network.db"
