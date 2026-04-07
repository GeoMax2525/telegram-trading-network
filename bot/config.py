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

# Comma-separated Telegram user IDs with admin privileges
ADMIN_IDS: list[int] = [
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().lstrip("-").isdigit()
]

# ── AI Scoring weights (must sum to 100) ──────────────────────────────────────
SCORE_WEIGHTS = {
    "liquidity":            20,   # USD liquidity in the pool
    "volume":               20,   # 24h trading volume
    "momentum":             20,   # price change % over 24h
    "holder_distribution":  15,   # estimated holder spread
    "contract_safety":      15,   # basic on-chain safety signals
    "deployer_reputation":  10,   # deployer wallet history heuristic
}

# ── Verdict thresholds (AI score out of 100) ──────────────────────────────────
VERDICT_THRESHOLDS = {
    "STRONG BUY":  80,   # score >= 80
    "PROMISING":   55,   # score >= 55
    "RISKY":       35,   # score >= 35
    "AVOID":        0,   # score < 35
}

# ── DexScreener ───────────────────────────────────────────────────────────────
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"

# ── Solana RPC ────────────────────────────────────────────────────────────────
# Set HELIUS_RPC_URL in Railway env vars for a dedicated key, e.g.:
#   https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
HELIUS_RPC_URL: str = os.getenv(
    "HELIUS_RPC_URL",
    "https://mainnet.helius-rpc.com/?api-key=demo",
)

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
