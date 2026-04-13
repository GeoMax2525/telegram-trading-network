"""
database/models.py — Async database using SQLAlchemy.

Uses PostgreSQL (asyncpg) when DATABASE_URL is set (Railway production),
falls back to SQLite (aiosqlite) when DATABASE_URL is not set (local dev).

Tables:
  scans            — logs every /scan command with token info, AI score, and caller details.
  callers          — approved caller Telegram user IDs (managed via /addcaller).
  keybot_settings  — per-user KeyBot trading presets.
  tokens           — every Solana token harvested by Agent 1.
  agent_logs       — one row per agent run (tokens_found, tokens_saved, run_at).
  candidates       — every candidate scored by Agent 5 (Confidence Engine).
  agent_weights    — learned confidence weights from Agent 6 (Learning Loop).
  ai_trade_params  — learned TP/SL/position size per pattern type (Agent 6).
  agent_params     — all dynamic agent parameters (key-value, adjusted by Agent 6).
  param_changes    — log of every parameter change with reason.
"""

import logging
from datetime import datetime, timedelta

from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, BigInteger,
    select, func, text
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from bot.config import DATABASE_URL

logger = logging.getLogger(__name__)

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


# Close reasons that represent a real strategy outcome — the bot's exit
# decided on its own. Used to split "strategy PnL" from "meta PnL" in
# reporting and to exclude user-driven closes from Agent 6 learning.
STRATEGY_CLOSE_REASONS = frozenset({
    "tp_hit",     # take-profit auto-close
    "sl_hit",     # stop-loss auto-close
    "trail_hit",  # trailing-stop auto-close
    "dead_token", # dead-position auto-close
    "expired",    # 7-day expiration auto-close
})

# Reasons that came from a human action (hub button, /resetbalance, etc.)
# These must not be counted as strategy wins — Agent 6 would learn
# "close at 14x" which the strategy can't actually do without a human.
META_CLOSE_REASONS = frozenset({
    "manual_close",
    "reset",
})


# Sentinel for "field not provided" in partial-update helpers (None means
# "explicitly set to null" so we need a distinct value).
class _Unset:
    pass


_UNSET = _Unset()

# Log DB type immediately at import so Railway shows it on every boot
_is_postgres = DATABASE_URL.startswith("postgresql")
_db_type     = "PostgreSQL" if _is_postgres else "SQLite"
if _is_postgres:
    logger.info("DB: Using PostgreSQL — Positions, KeyBotSettings, Scans all in PostgreSQL")
else:
    logger.warning(
        "DB: WARNING — using SQLite fallback. "
        "Set DATABASE_URL env var to use PostgreSQL. "
        "All data will be LOST on Railway redeploy!"
    )


class Base(DeclarativeBase):
    pass


# ── Tokens table (Agent 1 — Harvester) ───────────────────────────────────────

class Token(Base):
    __tablename__ = "tokens"

    mint            = Column(String(64),  primary_key=True)
    name            = Column(String(128), nullable=True)
    symbol          = Column(String(32),  nullable=True)
    price_usd       = Column(Float,       nullable=True)
    market_cap      = Column(Float,       nullable=True)   # current/last known MC
    launch_mc       = Column(Float,       nullable=True)   # MC when first discovered
    liquidity_usd   = Column(Float,       nullable=True)
    volume_24h      = Column(Float,       nullable=True)
    rugcheck_score  = Column(Integer,     nullable=True)   # 0-1000, higher = safer
    rugcheck_risks  = Column(String(512), nullable=True)   # JSON list of risk names
    source          = Column(String(16),  nullable=True)   # pumpfun / dexscreener
    bonding_curve   = Column(Float,       nullable=True)   # 0-100 bonding curve progress
    social_links    = Column(String(256), nullable=True)   # JSON: {"twitter": bool, "telegram": bool, "website": bool}
    graduated       = Column(Boolean,     nullable=True)   # graduated to Raydium
    reply_count     = Column(Integer,     nullable=True)   # pump.fun social activity
    gmgn_trending   = Column(Boolean,     nullable=True)   # trending on GMGN
    gmgn_rank       = Column(Integer,     nullable=True)   # GMGN rank position
    gmgn_smart_money = Column(Boolean,    nullable=True)   # flagged by GMGN smart money feed
    first_seen_at   = Column(DateTime,    default=datetime.utcnow, nullable=False)
    last_updated_at = Column(DateTime,    default=datetime.utcnow, nullable=False)


# ── Wallets table (Agent 2 — Wallet Analyst) ─────────────────────────────────

class Wallet(Base):
    __tablename__ = "wallets"

    address         = Column(String(64), primary_key=True)
    score           = Column(Float,      nullable=False, default=0.0)
    tier            = Column(Integer,    nullable=False, default=0)   # 1/2/3, 0=ignored
    win_rate        = Column(Float,      nullable=False, default=0.0)
    avg_multiple    = Column(Float,      nullable=False, default=1.0)
    wins            = Column(Integer,    nullable=False, default=0)
    losses          = Column(Integer,    nullable=False, default=0)
    total_trades    = Column(Integer,    nullable=False, default=0)
    avg_entry_mcap  = Column(Float,      nullable=True)
    source          = Column(String(16), nullable=True)   # helius / gmgn / manual
    # Agent 2 classification
    wallet_type     = Column(String(32), nullable=False, default="unknown",
                             server_default="unknown")
    # — unknown / early_insider / coordinated_group / sniper_holder / gmgn_smart
    cluster_id      = Column(String(32), nullable=True)   # shared with other wallets in same cluster
    first_seen_at   = Column(DateTime,   default=datetime.utcnow, nullable=False)
    last_updated_at = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── WalletTokenTrades — per-wallet-per-token trade record ───────────────────
# One row per (wallet, token) pair, populated by wallet_analyst from Helius
# transaction history. Enables timestamp-based classification (early entry,
# sniper hold duration, cross-wallet co-timing).

class WalletTokenTrade(Base):
    __tablename__ = "wallet_token_trades"

    id                = Column(Integer,    primary_key=True, autoincrement=True)
    wallet_address    = Column(String(64), nullable=False, index=True)
    token_address     = Column(String(64), nullable=False, index=True)
    first_buy_at      = Column(DateTime,   nullable=True)   # earliest observed buy
    last_sell_at      = Column(DateTime,   nullable=True)   # latest observed sell (NULL = still holding)
    total_bought_sol  = Column(Float,      nullable=False, default=0.0)
    total_sold_sol    = Column(Float,      nullable=False, default=0.0)
    multiple          = Column(Float,      nullable=True)    # sold/bought, populated once closed
    entry_mcap        = Column(Float,      nullable=True)
    token_launch_at   = Column(DateTime,   nullable=True)    # from Token.first_seen_at proxy
    hold_duration_sec = Column(Integer,    nullable=True)    # last_sell_at - first_buy_at
    updated_at        = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── WalletClusters — groups of co-timing wallets (Agent 2) ──────────────────

class WalletCluster(Base):
    __tablename__ = "wallet_clusters"

    cluster_id        = Column(String(32),  primary_key=True)
    wallet_addresses  = Column(String(2048), nullable=False)  # JSON array
    wins              = Column(Integer,     nullable=False, default=0)
    losses            = Column(Integer,     nullable=False, default=0)
    win_rate          = Column(Float,       nullable=False, default=0.0)
    avg_multiple      = Column(Float,       nullable=False, default=1.0)
    last_active       = Column(DateTime,    nullable=True)
    created_at        = Column(DateTime,    default=datetime.utcnow, nullable=False)


# ── Patterns table (Agent 3 — Pattern Engine) ────────────────────────────────

class Pattern(Base):
    __tablename__ = "patterns"

    id                      = Column(Integer,     primary_key=True, autoincrement=True)
    pattern_type            = Column(String(32),  nullable=False, unique=True, index=True)
    outcome_threshold       = Column(String(8),   nullable=True)   # "2x" / "5x" / "10x" / "rug"
    sample_count            = Column(Integer,     nullable=False, default=0)
    avg_entry_mcap          = Column(Float,       nullable=True)
    mcap_range_low          = Column(Float,       nullable=True)
    mcap_range_high         = Column(Float,       nullable=True)
    avg_liquidity           = Column(Float,       nullable=True)
    min_liquidity           = Column(Float,       nullable=True)
    avg_ai_score            = Column(Float,       nullable=True)
    avg_rugcheck_score      = Column(Float,       nullable=True)
    max_dev_wallet_pct      = Column(Float,       nullable=True)   # reserved
    max_top10_concentration = Column(Float,       nullable=True)   # reserved
    min_holder_count        = Column(Integer,     nullable=True)   # reserved
    best_hours              = Column(String(64),  nullable=True)   # JSON list e.g. "[14,15,16]"
    best_days               = Column(String(64),  nullable=True)   # JSON list e.g. "[0,1,2]"
    confidence_score        = Column(Float,       nullable=False, default=0.0)
    created_at              = Column(DateTime,    default=datetime.utcnow, nullable=False)
    updated_at              = Column(DateTime,    default=datetime.utcnow, nullable=False)


# ── Agent logs table ──────────────────────────────────────────────────────────

class AgentLog(Base):
    __tablename__ = "agent_logs"

    id           = Column(Integer,     primary_key=True, autoincrement=True)
    agent_name   = Column(String(64),  nullable=False, index=True)
    run_at       = Column(DateTime,    default=datetime.utcnow, nullable=False)
    tokens_found = Column(Integer,     nullable=False, default=0)
    tokens_saved = Column(Integer,     nullable=False, default=0)
    notes        = Column(String(256), nullable=True)


# ── Candidates table (Agent 5 — Confidence Engine) ──────────────────────────

class Candidate(Base):
    __tablename__ = "candidates"

    id                = Column(Integer,     primary_key=True, autoincrement=True)
    token_address     = Column(String(64),  nullable=False, index=True)
    token_name        = Column(String(128), nullable=True)
    confidence_score  = Column(Float,       nullable=False, default=0.0)
    fingerprint_score = Column(Float,       nullable=True)
    insider_score     = Column(Float,       nullable=True)
    chart_score       = Column(Float,       nullable=True)
    rug_score         = Column(Float,       nullable=True)
    caller_score      = Column(Float,       nullable=True)
    market_score      = Column(Float,       nullable=True)
    source            = Column(String(32),  nullable=True)   # new_launch / insider_wallet / volume_spike
    chart_pattern     = Column(String(64),  nullable=True)   # pattern name detected by Agent 7
    decision          = Column(String(16),  nullable=False, default="discard")  # execute_full / execute_half / monitor / discard
    executed          = Column(Boolean,     nullable=False, default=False)
    created_at        = Column(DateTime,    default=datetime.utcnow, nullable=False)


# ── Agent Weights table (Agent 6 — Learning Loop) ───────────────────────────

class AgentWeights(Base):
    __tablename__ = "agent_weights"

    id                   = Column(Integer,     primary_key=True, autoincrement=True)
    fingerprint_weight   = Column(Float,       nullable=False, default=0.25)
    insider_weight       = Column(Float,       nullable=False, default=0.25)
    chart_weight         = Column(Float,       nullable=False, default=0.20)
    rug_weight           = Column(Float,       nullable=False, default=0.15)
    caller_weight        = Column(Float,       nullable=False, default=0.10)
    market_weight        = Column(Float,       nullable=False, default=0.05)
    trades_analyzed      = Column(Integer,     nullable=False, default=0)
    notes                = Column(String(512), nullable=True)
    updated_at           = Column(DateTime,    default=datetime.utcnow, nullable=False)


# ── Agent Params table (key-value, all dynamic parameters) ──────────────────

class AgentParam(Base):
    __tablename__ = "agent_params"

    param_name  = Column(String(64),  primary_key=True)
    param_value = Column(Float,       nullable=False)
    updated_at  = Column(DateTime,    default=datetime.utcnow, nullable=False)


class ParamChange(Base):
    __tablename__ = "param_changes"

    id              = Column(Integer,    primary_key=True, autoincrement=True)
    param_name      = Column(String(64), nullable=False, index=True)
    old_value       = Column(Float,      nullable=False)
    new_value       = Column(Float,      nullable=False)
    reason          = Column(String(256),nullable=True)
    trades_analyzed = Column(Integer,    nullable=True)
    win_rate        = Column(Float,      nullable=True)
    changed_at      = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── Paper Trades table ────────────────────────────────────────────────────────

class PaperTrade(Base):
    __tablename__ = "paper_trades"

    id                = Column(Integer,     primary_key=True, autoincrement=True)
    token_address     = Column(String(64),  nullable=False, index=True)
    token_name        = Column(String(128), nullable=True)
    entry_mc          = Column(Float,       nullable=True)
    entry_price       = Column(Float,       nullable=True)
    paper_sol_spent   = Column(Float,       nullable=False, default=0.5)
    confidence_score  = Column(Float,       nullable=True)
    # Stores a comma-separated list of every matched pattern_type at entry
    # (e.g. "new_launch,mid_mc,high_chart,early_entry,trending_gmgn,..."). The
    # full set of 31+ tags can easily exceed 200 chars, so this is wide.
    pattern_type      = Column(String(512), nullable=True)
    take_profit_x     = Column(Float,       nullable=False, default=3.0)
    stop_loss_pct     = Column(Float,       nullable=False, default=30.0)
    status            = Column(String(16),  default="open",  nullable=False)
    peak_mc           = Column(Float,       nullable=True)
    peak_multiple     = Column(Float,       nullable=True)
    close_reason      = Column(String(32),  nullable=True)   # tp_hit / sl_hit / expired
    paper_pnl_sol     = Column(Float,       nullable=True)
    opened_at         = Column(DateTime,    default=datetime.utcnow, nullable=False)
    closed_at         = Column(DateTime,    nullable=True)
    # Post-close tracking (filled by monitor over 24h after close)
    mc_1h_after       = Column(Float,       nullable=True)
    mc_6h_after       = Column(Float,       nullable=True)
    mc_24h_after      = Column(Float,       nullable=True)
    peak_after_close  = Column(Float,       nullable=True)
    sold_too_early    = Column(Boolean,     nullable=True)  # price went 50%+ higher after close
    sold_too_late     = Column(Boolean,     nullable=True)  # SL hit but recovered within 1h
    # Dead-position detection state (updated every monitor tick while open)
    zero_volume_since = Column(DateTime,    nullable=True)  # first tick volume hit 0/null
    last_move_at      = Column(DateTime,    nullable=True)  # last tick with >=2% price move
    last_move_mc      = Column(Float,       nullable=True)  # MC at last_move_at (reference)

    # Aliases so PaperTrade instances can be consumed by code written against Position
    # (Agent 6 learning loop treats paper trades as first-class learning signal).
    @property
    def pnl_sol(self):
        return self.paper_pnl_sol

    @property
    def amount_sol_spent(self):
        return self.paper_sol_spent

    @property
    def is_paper(self) -> bool:
        return True


# ── AI Trade Params table (Agent 6 — learned TP/SL/size) ────────────────────

class AITradeParams(Base):
    __tablename__ = "ai_trade_params"

    id                    = Column(Integer,    primary_key=True, autoincrement=True)
    # Extended pattern_type set — see bot/agents/trade_profiles.ALL_PATTERN_TYPES
    # Legacy: new_launch / insider_wallet / volume_spike
    # MC buckets: low_mc / mid_mc / high_mc
    # Signal quality: high_chart / high_caller
    pattern_type          = Column(String(32), nullable=False, unique=True, index=True)
    optimal_tp_x          = Column(Float,      nullable=False, default=3.0)
    optimal_sl_pct        = Column(Float,      nullable=False, default=30.0)
    optimal_position_pct  = Column(Float,      nullable=False, default=10.0)   # % of wallet
    trail_sl_trigger_pct  = Column(Float,      nullable=False, default=0.50)   # fraction above entry
    trail_sl_enabled      = Column(Integer,    nullable=False, default=0)      # 0 or 1
    sample_size           = Column(Integer,    nullable=False, default=0)
    win_rate              = Column(Float,      nullable=False, default=0.0)
    avg_multiple          = Column(Float,      nullable=False, default=1.0)
    confidence            = Column(Float,      nullable=False, default=0.0)    # 0-100
    updated_at            = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── Positions table ───────────────────────────────────────────────────────────

class Position(Base):
    __tablename__ = "positions"

    id               = Column(Integer,    primary_key=True, autoincrement=True)
    user_id          = Column(BigInteger, nullable=False, index=True)
    token_address    = Column(String(64), nullable=False, index=True)
    token_name       = Column(String(128),nullable=False)
    entry_price      = Column(Float,      nullable=True)   # token price USD at buy
    entry_mc         = Column(Float,      nullable=True)   # market cap USD at buy
    amount_sol_spent = Column(Float,      nullable=False)
    tokens_received  = Column(String(32), nullable=True)   # raw integer as string
    take_profit_x    = Column(Float,      nullable=False, default=3.0)
    stop_loss_pct    = Column(Float,      nullable=False, default=30.0)
    status           = Column(String(16), default="open",  nullable=False, server_default="open")
    opened_at        = Column(DateTime,   default=datetime.utcnow, nullable=False)
    closed_at        = Column(DateTime,   nullable=True)
    close_reason     = Column(String(32), nullable=True)   # tp_hit | sl_hit | manual
    pnl_sol          = Column(Float,      nullable=True)


# ── KeyBot settings table ─────────────────────────────────────────────────────

class KeyBotSettings(Base):
    __tablename__ = "keybot_settings"

    id             = Column(Integer,    primary_key=True, autoincrement=True)
    admin_id       = Column(BigInteger, nullable=False, unique=True, index=True)
    buy_amount_sol = Column(Float,      nullable=False, default=0.5)
    take_profit_x  = Column(Float,      nullable=False, default=3.0)
    stop_loss_pct  = Column(Float,      nullable=False, default=30.0)
    max_positions         = Column(Integer,  nullable=False, default=5)
    daily_loss_limit_sol  = Column(Float,    nullable=False, default=0.0)
    daily_loss_limit_pct  = Column(Float,    nullable=False, default=0.0)
    daily_loss_today_sol  = Column(Float,    nullable=False, default=0.0)
    cooldown_minutes      = Column(Integer,  nullable=False, default=0)
    last_trade_at         = Column(DateTime, nullable=True)
    wallet_address = Column(String(64), nullable=True)
    created_at     = Column(DateTime,   default=datetime.utcnow, nullable=False)
    updated_at     = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── Callers table ─────────────────────────────────────────────────────────────

class Caller(Base):
    __tablename__ = "callers"

    id          = Column(Integer,    primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, nullable=False, unique=True, index=True)
    added_at    = Column(DateTime,   default=datetime.utcnow, nullable=False)


# ── Scans table ───────────────────────────────────────────────────────────────

class Scan(Base):
    __tablename__ = "scans"

    id               = Column(Integer,    primary_key=True, autoincrement=True)
    contract_address = Column(String(64), nullable=False, index=True)
    token_name       = Column(String(128),nullable=False)
    ai_score         = Column(Float,      nullable=False)
    scanned_by       = Column(String(64), nullable=False)
    group_id         = Column(BigInteger, nullable=False)
    scanned_at       = Column(DateTime,   default=datetime.utcnow, nullable=False)

    # PnL tracking
    entry_price      = Column(Float,      nullable=True)   # market cap at scan time
    current_price    = Column(Float,      nullable=True)   # latest market cap
    peak_market_cap  = Column(Float,      nullable=True)   # highest MC ever seen
    multiplier       = Column(Float,      nullable=True)   # current_price / entry_price
    peak_multiplier  = Column(Float,      nullable=True)   # peak_market_cap / entry_price
    points           = Column(Float,      nullable=True)   # = peak_multiplier (for leaderboard)
    entry_liquidity  = Column(Float,      nullable=True)   # liquidity USD at scan time
    is_win           = Column(Boolean,    nullable=True)   # peak_multiplier >= 2.0
    is_loss          = Column(Boolean,    nullable=True)   # closed without hitting 2x
    status           = Column(String(16), default="open",  nullable=False, server_default="open")
    close_reason     = Column(String(32), nullable=True)   # expired | rug_mc | rug_liquidity
    # status values: open | win | break_even | loss


# ── Initialisation ────────────────────────────────────────────────────────────

_NEW_SCAN_COLS = [
    ("entry_price",      "REAL"),
    ("current_price",    "REAL"),
    ("peak_market_cap",  "REAL"),
    ("multiplier",       "REAL"),
    ("peak_multiplier",  "REAL"),
    ("points",           "REAL"),
    ("is_win",           "INTEGER"),
    ("is_loss",          "INTEGER"),
    ("status",           "TEXT DEFAULT 'open'"),
    ("entry_liquidity",  "REAL"),
    ("close_reason",     "TEXT"),
]

_NEW_KEYBOT_COLS = [
    ("max_positions",        "INTEGER DEFAULT 5"),
    ("daily_loss_limit_sol", "REAL DEFAULT 0"),
    ("daily_loss_limit_pct", "REAL DEFAULT 0"),
    ("daily_loss_today_sol", "REAL DEFAULT 0"),
    ("cooldown_minutes",     "INTEGER DEFAULT 0"),
    ("last_trade_at",        "TIMESTAMP"),
]

_NEW_CANDIDATE_COLS = [
    ("source", "TEXT"),
    ("chart_pattern", "TEXT"),
]

_NEW_PAPER_TRADE_COLS = [
    ("mc_1h_after",       "REAL"),
    ("mc_4h_after",       "REAL"),
    ("mc_6h_after",       "REAL"),
    ("mc_24h_after",      "REAL"),
    ("peak_after_close",  "REAL"),
    ("sold_too_early",    "BOOLEAN"),
    ("sold_too_late",     "BOOLEAN"),
    ("zero_volume_since", "TIMESTAMP"),
    ("last_move_at",      "TIMESTAMP"),
    ("last_move_mc",      "REAL"),
]

_NEW_AI_TRADE_PARAMS_COLS = [
    ("trail_sl_trigger_pct", "REAL"),
    ("trail_sl_enabled",     "INTEGER"),
]

_NEW_TOKEN_COLS = [
    ("source",          "TEXT"),
    ("bonding_curve",   "REAL"),
    ("social_links",    "TEXT"),
    ("graduated",       "BOOLEAN"),
    ("reply_count",     "INTEGER"),
    ("launch_mc",       "REAL"),
    ("gmgn_trending",   "BOOLEAN"),
    ("gmgn_rank",       "INTEGER"),
    ("gmgn_smart_money", "BOOLEAN"),
]

_NEW_WALLET_COLS = [
    ("source",      "TEXT"),
    ("wallet_type", "TEXT"),
    ("cluster_id",  "TEXT"),
]

async def init_db() -> None:
    """Creates all tables if they don't already exist, then adds any missing columns."""
    is_postgres = DATABASE_URL.startswith("postgresql")
    logger.info(
        "init_db: %s — tables: positions, keybot_settings, scans, callers",
        "PostgreSQL" if is_postgres else "SQLite (WARNING: data lost on redeploy!)",
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Migrate existing tables — add new columns if absent.
    # PostgreSQL supports IF NOT EXISTS; SQLite needs try/except.
    async with engine.begin() as conn:
        for col_name, col_def in _NEW_SCAN_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE scans ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE scans ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass  # column already exists

        for col_name, col_def in _NEW_KEYBOT_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE keybot_settings ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE keybot_settings ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass  # column already exists

        for col_name, col_def in _NEW_CANDIDATE_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE candidates ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE candidates ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass

        for col_name, col_def in _NEW_TOKEN_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE tokens ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE tokens ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass

        for col_name, col_def in _NEW_WALLET_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE wallets ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(text(f"ALTER TABLE wallets ADD COLUMN {col_name} {col_def}"))
                except Exception:
                    pass

        for col_name, col_def in _NEW_PAPER_TRADE_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE paper_trades ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass

        # Widen paper_trades.pattern_type: original VARCHAR(64) is too
        # narrow now that pattern_type stores the full comma-separated tag
        # list (can exceed 200 chars). PostgreSQL supports ALTER TYPE
        # in-place; SQLite treats TEXT as unbounded so the DDL is a no-op.
        if is_postgres:
            try:
                await conn.execute(text(
                    "ALTER TABLE paper_trades "
                    "ALTER COLUMN pattern_type TYPE VARCHAR(512)"
                ))
            except Exception as _exc:
                logger.debug("pattern_type widen skipped: %s", _exc)

        for col_name, col_def in _NEW_AI_TRADE_PARAMS_COLS:
            if is_postgres:
                await conn.execute(
                    text(f"ALTER TABLE ai_trade_params ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
                )
            else:
                try:
                    await conn.execute(
                        text(f"ALTER TABLE ai_trade_params ADD COLUMN {col_name} {col_def}")
                    )
                except Exception:
                    pass

    logger.info("Database initialised (%s).", "PostgreSQL" if is_postgres else "SQLite")


# ── Caller helpers ────────────────────────────────────────────────────────────

async def add_caller(telegram_id: int) -> bool:
    async with AsyncSessionLocal() as session:
        existing = await session.execute(
            select(Caller).where(Caller.telegram_id == telegram_id)
        )
        if existing.scalar_one_or_none():
            return False
        session.add(Caller(telegram_id=telegram_id))
        await session.commit()
        logger.info("Added caller: %s", telegram_id)
        return True


async def get_caller_ids() -> set[int]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Caller.telegram_id))
        return {row for row in result.scalars()}


# ── Scan write helpers ────────────────────────────────────────────────────────

async def log_scan(
    contract_address: str,
    token_name: str,
    ai_score: float,
    scanned_by: str,
    group_id: int,
    entry_price: float | None = None,
    entry_liquidity: float | None = None,
) -> "Scan":
    async with AsyncSessionLocal() as session:
        scan = Scan(
            contract_address=contract_address,
            token_name=token_name,
            ai_score=ai_score,
            scanned_by=scanned_by,
            group_id=group_id,
            entry_price=entry_price,
            entry_liquidity=entry_liquidity,
            peak_market_cap=entry_price,   # peak starts at entry
            peak_multiplier=1.0 if entry_price else None,
            points=1.0 if entry_price else None,
            status="open",
        )
        session.add(scan)
        await session.commit()
        await session.refresh(scan)
        logger.info("Logged scan: %s score=%.1f by=%s entry_mc=%s", token_name, ai_score, scanned_by, entry_price)
        return scan


def _resolve_status(peak_mult: float, closed: bool) -> tuple[str, bool, bool]:
    """
    Returns (status, is_win, is_loss) based on peak multiplier.
    WIN        = peak >= 2.0  🟢
    BREAK_EVEN = peak >= 1.0 and < 2.0  🟡  (only assigned on close)
    LOSS       = peak < 1.0 on close    🔴
    While open: status is 'win' once 2x hit, otherwise 'open'.
    """
    if peak_mult >= 2.0:
        return ("win", True, False)
    if closed:
        if peak_mult >= 1.0:
            return ("break_even", False, False)
        return ("loss", False, True)
    return ("open", False, False)


async def update_scan_pnl(
    scan_id: int,
    current_mc: float,
    close: bool = False,
    close_reason: str | None = None,
) -> "Scan | None":
    """Updates current MC, peak MC, multipliers, win/loss for one scan."""
    async with AsyncSessionLocal() as session:
        scan = await session.get(Scan, scan_id)
        if scan is None or not scan.entry_price:
            return None

        current_mult = current_mc / scan.entry_price
        new_peak_mc  = max(scan.peak_market_cap or scan.entry_price, current_mc)
        peak_mult    = new_peak_mc / scan.entry_price

        status, is_win, is_loss = _resolve_status(peak_mult, close)

        scan.current_price   = current_mc
        scan.multiplier      = round(current_mult, 4)
        scan.peak_market_cap = new_peak_mc
        scan.peak_multiplier = round(peak_mult, 4)
        scan.points          = round(peak_mult, 4)
        scan.is_win          = is_win
        scan.is_loss         = is_loss
        scan.status          = status
        if close and close_reason:
            scan.close_reason = close_reason

        await session.commit()
        await session.refresh(scan)
        return scan


async def close_old_scans() -> int:
    """Closes all open scans older than 7 days. Returns number closed."""
    cutoff = datetime.utcnow() - timedelta(days=7)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Scan).where(
                Scan.status == "open",
                Scan.entry_price.is_not(None),
                Scan.scanned_at < cutoff,
            )
        )
        scans = list(result.scalars().all())
        for scan in scans:
            peak_mult = (scan.peak_market_cap / scan.entry_price) if (scan.peak_market_cap and scan.entry_price) else 0.0
            status, is_win, is_loss = _resolve_status(peak_mult, closed=True)
            scan.is_win      = is_win
            scan.is_loss     = is_loss
            scan.status      = status
            scan.close_reason = "expired"
        await session.commit()
    logger.info("Closed %d expired scans", len(scans))
    return len(scans)


# ── Scan read helpers ─────────────────────────────────────────────────────────

async def get_scan_by_address(contract_address: str) -> "Scan | None":
    """Returns the most recent scan for a given contract address, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Scan)
            .where(Scan.contract_address == contract_address)
            .order_by(Scan.scanned_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


async def get_open_scans() -> list["Scan"]:
    """Returns open scans under 7 days old that have an entry_price."""
    cutoff = datetime.utcnow() - timedelta(days=7)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Scan)
            .where(
                Scan.status == "open",
                Scan.entry_price.is_not(None),
                Scan.scanned_at >= cutoff,
            )
            .order_by(Scan.scanned_at.desc())
        )
        return list(result.scalars().all())


async def get_leaderboard(limit: int = 10) -> list[dict]:
    """
    Returns top callers ranked by total peak_multiplier points.
    Each entry: {username, scans, wins, break_evens, losses, win_pct, total_points}
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(
                Scan.scanned_by,
                func.count(Scan.id).label("scans"),
                func.sum(func.cast(Scan.is_win,  Integer)).label("wins"),
                func.sum(func.cast(Scan.is_loss, Integer)).label("losses"),
                func.coalesce(func.sum(Scan.peak_multiplier), 0).label("total_points"),
            )
            .group_by(Scan.scanned_by)
            .order_by(func.coalesce(func.sum(Scan.peak_multiplier), 0).desc())
            .limit(limit)
        )
        rows = []
        for r in result:
            wins   = r.wins   or 0
            losses = r.losses or 0
            decided = wins + losses
            win_pct = round(wins / decided * 100) if decided else 0
            rows.append({
                "username":     r.scanned_by,
                "scans":        r.scans,
                "wins":         wins,
                "losses":       losses,
                "win_pct":      win_pct,
                "total_points": round(r.total_points or 0, 2),
            })
        return rows


async def get_break_evens_count(username: str) -> int:
    """Returns number of break_even scans for a user."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Scan.id))
            .where(Scan.scanned_by == username, Scan.status == "break_even")
        )
        return result.scalar() or 0


async def get_signal_leaders(limit: int = 10) -> list[dict]:
    """Returns callers ranked by total peak_multiplier points (W/L only, no break_even column)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(
                Scan.scanned_by,
                func.count(Scan.id).label("scans"),
                func.sum(func.cast(Scan.is_win,  Integer)).label("wins"),
                func.sum(func.cast(Scan.is_loss, Integer)).label("losses"),
                func.coalesce(func.sum(Scan.peak_multiplier), 0).label("total_points"),
            )
            .group_by(Scan.scanned_by)
            .having(func.count(Scan.id) >= 1)
            .order_by(func.coalesce(func.sum(Scan.peak_multiplier), 0).desc())
            .limit(limit)
        )
        rows = []
        for r in result:
            wins   = r.wins   or 0
            losses = r.losses or 0
            decided = wins + losses
            win_pct = round(wins / decided * 100) if decided else 0
            rows.append({
                "username":     r.scanned_by,
                "scans":        r.scans,
                "wins":         wins,
                "losses":       losses,
                "win_pct":      win_pct,
                "total_points": round(r.total_points or 0, 2),
            })
        return rows


async def get_top_calls(limit: int = 10, since: "datetime | None" = None) -> list[dict]:
    """Returns top scans by peak_multiplier, optionally filtered by time window."""
    async with AsyncSessionLocal() as session:
        query = (
            select(
                Scan.token_name,
                Scan.scanned_by,
                Scan.peak_multiplier,
                Scan.close_reason,
                Scan.status,
            )
            .where(Scan.peak_multiplier.is_not(None))
        )
        if since:
            query = query.where(Scan.scanned_at >= since)
        query = query.order_by(Scan.peak_multiplier.desc()).limit(limit)
        result = await session.execute(query)
        return [
            {
                "token_name":      r.token_name,
                "scanned_by":      r.scanned_by,
                "peak_multiplier": r.peak_multiplier,
                "close_reason":    r.close_reason,
                "status":          r.status,
            }
            for r in result
        ]


async def get_top_calls_stats(since: "datetime | None" = None) -> dict:
    """Returns total call count and average peak_multiplier for the given window."""
    async with AsyncSessionLocal() as session:
        query = select(
            func.count(Scan.id).label("total"),
            func.coalesce(func.avg(Scan.peak_multiplier), 0).label("avg_x"),
        ).where(Scan.peak_multiplier.is_not(None))
        if since:
            query = query.where(Scan.scanned_at >= since)
        result = await session.execute(query)
        row = result.one()
        return {"total": row.total or 0, "avg_x": round(row.avg_x or 0, 2)}


async def get_keybot_settings(admin_id: int) -> "KeyBotSettings | None":
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(KeyBotSettings).where(KeyBotSettings.admin_id == admin_id)
        )
        return result.scalar_one_or_none()


async def upsert_keybot_settings(admin_id: int, **kwargs) -> "KeyBotSettings":
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(KeyBotSettings).where(KeyBotSettings.admin_id == admin_id)
        )
        settings = result.scalar_one_or_none()
        if settings is None:
            settings = KeyBotSettings(
                admin_id=admin_id,
                buy_amount_sol=kwargs.get("buy_amount_sol", 0.5),
                take_profit_x=kwargs.get("take_profit_x",  3.0),
                stop_loss_pct=kwargs.get("stop_loss_pct",  30.0),
                wallet_address=kwargs.get("wallet_address"),
            )
            session.add(settings)
        else:
            for key, val in kwargs.items():
                setattr(settings, key, val)
            settings.updated_at = datetime.utcnow()
        await session.commit()
        await session.refresh(settings)
        return settings


# ── Position helpers ──────────────────────────────────────────────────────────

async def open_position(
    user_id:          int,
    token_address:    str,
    token_name:       str,
    amount_sol_spent: float,
    take_profit_x:    float,
    stop_loss_pct:    float,
    entry_price:      float | None = None,
    entry_mc:         float | None = None,
    tokens_received:  str   | None = None,
) -> "Position":
    async with AsyncSessionLocal() as session:
        pos = Position(
            user_id=user_id,
            token_address=token_address,
            token_name=token_name,
            entry_price=entry_price,
            entry_mc=entry_mc,
            amount_sol_spent=amount_sol_spent,
            tokens_received=tokens_received,
            take_profit_x=take_profit_x,
            stop_loss_pct=stop_loss_pct,
            status="open",
        )
        session.add(pos)
        await session.commit()
        await session.refresh(pos)
        logger.info("Opened position %d: %s %.4f SOL", pos.id, token_name, amount_sol_spent)
        return pos


async def close_position(
    position_id:  int,
    close_reason: str,
    pnl_sol:      float | None = None,
) -> "Position | None":
    async with AsyncSessionLocal() as session:
        pos = await session.get(Position, position_id)
        if pos is None or pos.status != "open":
            return None
        pos.status       = "closed"
        pos.close_reason = close_reason
        pos.pnl_sol      = pnl_sol
        pos.closed_at    = datetime.utcnow()
        await session.commit()
        await session.refresh(pos)
        logger.info("Closed position %d: reason=%s pnl=%s", position_id, close_reason, pnl_sol)
        return pos


async def get_open_positions(user_id: int | None = None) -> list["Position"]:
    """Returns all open positions, optionally filtered by user_id."""
    async with AsyncSessionLocal() as session:
        query = select(Position).where(Position.status == "open")
        if user_id is not None:
            query = query.where(Position.user_id == user_id)
        query = query.order_by(Position.opened_at.desc())
        result = await session.execute(query)
        return list(result.scalars().all())


async def get_position_by_id(position_id: int) -> "Position | None":
    async with AsyncSessionLocal() as session:
        return await session.get(Position, position_id)


async def count_open_positions(user_id: int) -> int:
    """Returns the number of currently open positions for a user."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Position.id)).where(
                Position.user_id == user_id,
                Position.status == "open",
            )
        )
        return result.scalar() or 0


async def add_daily_loss(user_id: int, loss_sol: float) -> None:
    """Adds loss_sol to daily_loss_today_sol for a user (only call with positive values)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(KeyBotSettings).where(KeyBotSettings.admin_id == user_id)
        )
        settings = result.scalar_one_or_none()
        if settings:
            settings.daily_loss_today_sol = (settings.daily_loss_today_sol or 0.0) + loss_sol
            await session.commit()
            logger.info("Daily loss updated for user %d: +%.4f SOL (total today: %.4f SOL)",
                        user_id, loss_sol, settings.daily_loss_today_sol)


async def reset_all_daily_losses() -> int:
    """Resets daily_loss_today_sol to 0 for all users. Returns number of rows updated."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(KeyBotSettings).where(KeyBotSettings.daily_loss_today_sol > 0)
        )
        rows = list(result.scalars().all())
        for r in rows:
            r.daily_loss_today_sol = 0.0
        await session.commit()
    logger.info("Midnight reset: cleared daily losses for %d user(s)", len(rows))
    return len(rows)


async def debug_all_positions() -> None:
    """Dumps every row in the positions table to the log. Call on startup or demand."""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Position).order_by(Position.id))
            rows = result.scalars().all()
        if not rows:
            logger.info("DEBUG positions table: 0 rows")
            return
        logger.info("DEBUG positions table: %d rows total", len(rows))
        for p in rows:
            logger.info(
                "  pos id=%d user_id=%d token=%s name=%r status=%s "
                "sol=%.4f entry_mc=%s opened=%s",
                p.id, p.user_id, p.token_address[:12], p.token_name,
                p.status, p.amount_sol_spent,
                f"{p.entry_mc:.0f}" if p.entry_mc else "None",
                p.opened_at.strftime("%Y-%m-%d %H:%M") if p.opened_at else "None",
            )
    except Exception as exc:
        logger.error("debug_all_positions failed: %s", exc)


async def update_position_entry_mc(position_id: int, entry_mc: float) -> None:
    """Patches entry_mc on a position where it was 0/None at buy time."""
    async with AsyncSessionLocal() as session:
        pos = await session.get(Position, position_id)
        if pos and pos.status == "open":
            pos.entry_mc = entry_mc
            await session.commit()
            logger.info("Patched entry_mc for position %d: %.0f", position_id, entry_mc)


async def get_open_position_by_token(user_id: int, token_address: str) -> "Position | None":
    """Returns the latest open position for a (user, token) pair, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position)
            .where(
                Position.user_id == user_id,
                Position.token_address == token_address,
                Position.status == "open",
            )
            .order_by(Position.opened_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


async def get_any_open_position_by_token(token_address: str) -> "Position | None":
    """Returns the most recent open position for a token across all users, or None.
    Used to find server-wallet entry data when building Trade Cards."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position)
            .where(
                Position.token_address == token_address,
                Position.status == "open",
            )
            .order_by(Position.opened_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


async def get_recent_scans(limit: int = 20) -> list["Scan"]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Scan).order_by(Scan.scanned_at.desc()).limit(limit)
        )
        return list(result.scalars().all())


# ── Token helpers (Agent 1) ───────────────────────────────────────────────────

async def token_exists(mint: str) -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Token.mint).where(Token.mint == mint))
        return result.scalar_one_or_none() is not None


async def save_token(
    mint: str,
    name: str | None,
    symbol: str | None,
    price_usd: float | None,
    market_cap: float | None,
    liquidity_usd: float | None,
    volume_24h: float | None,
    rugcheck_score: int | None = None,
    rugcheck_risks: str | None = None,
    source: str | None = None,
    bonding_curve: float | None = None,
    social_links: str | None = None,
    graduated: bool | None = None,
    reply_count: int | None = None,
) -> "Token":
    async with AsyncSessionLocal() as session:
        tok = Token(
            mint=mint,
            name=name,
            symbol=symbol,
            price_usd=price_usd,
            market_cap=market_cap,
            launch_mc=market_cap,  # snapshot MC at first discovery
            liquidity_usd=liquidity_usd,
            volume_24h=volume_24h,
            rugcheck_score=rugcheck_score,
            rugcheck_risks=rugcheck_risks,
            source=source,
            bonding_curve=bonding_curve,
            social_links=social_links,
            graduated=graduated,
            reply_count=reply_count,
        )
        session.add(tok)
        try:
            await session.commit()
        except Exception:
            await session.rollback()  # duplicate key — already saved by another tick
        return tok


async def get_token_count() -> int:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Token.mint)))
        return result.scalar() or 0


async def get_gmgn_stats() -> dict:
    """Returns GMGN-related counts."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        gmgn_wallets = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.source == "gmgn")
        )).scalar() or 0
        gmgn_t1 = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.source == "gmgn", Wallet.tier == 1)
        )).scalar() or 0
        gmgn_trending = (await session.execute(
            select(func.count(Token.mint)).where(
                Token.gmgn_trending == True, Token.last_updated_at >= today,
            )
        )).scalar() or 0
    return {"wallets": gmgn_wallets, "tier1": gmgn_t1, "trending": gmgn_trending}


async def get_all_tokens(limit: int = 500) -> list["Token"]:
    """Returns all tokens, newest first."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Token).order_by(Token.first_seen_at.desc()).limit(limit)
        )
        return list(result.scalars().all())


async def get_tokens_batch(offset: int, batch_size: int = 200) -> list["Token"]:
    """Returns a batch of tokens for backfill processing."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Token).order_by(Token.first_seen_at.asc())
            .offset(offset).limit(batch_size)
        )
        return list(result.scalars().all())


async def get_tokens_no_mc(
    limit: int = 2000,
    max_age_days: int = 30,
) -> list["Token"]:
    """
    Returns tokens with no market_cap, newest first (so under-7-day tokens
    get processed first), and older than `max_age_days` excluded entirely
    (likely dead — not worth spending API calls on).
    """
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Token).where(
                (Token.market_cap.is_(None)) | (Token.market_cap == 0),
                Token.first_seen_at >= cutoff,
                Token.source != "dead",
            ).order_by(Token.first_seen_at.desc()).limit(limit)
        )
        return list(result.scalars().all())


async def count_tokens_no_mc(max_age_days: int = 30) -> int:
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    async with AsyncSessionLocal() as session:
        return (await session.execute(
            select(func.count(Token.mint)).where(
                (Token.market_cap.is_(None)) | (Token.market_cap == 0),
                Token.first_seen_at >= cutoff,
                Token.source != "dead",
            )
        )).scalar() or 0


async def mark_token_dead(mint: str) -> None:
    """Mark a token as dead (source='dead')."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Token).where(Token.mint == mint))
        tok = result.scalar_one_or_none()
        if tok:
            tok.source = "dead"
            tok.last_updated_at = datetime.utcnow()
            await session.commit()


async def set_token_launch_mc(mint: str, launch_mc: float) -> None:
    """Sets launch_mc without overwriting if already set."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Token).where(Token.mint == mint))
        tok = result.scalar_one_or_none()
        if tok and not tok.launch_mc:
            tok.launch_mc = launch_mc
            await session.commit()


async def update_token_market_cap(mint: str, market_cap: float) -> None:
    """Update a token's current market cap. Also sets launch_mc if not already set."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Token).where(Token.mint == mint))
        tok = result.scalar_one_or_none()
        if tok:
            tok.market_cap = market_cap
            # Set launch_mc if it was never captured
            if not tok.launch_mc and market_cap > 0:
                tok.launch_mc = market_cap
            tok.last_updated_at = datetime.utcnow()
            await session.commit()


async def get_pumpfun_count_today() -> int:
    """Returns number of pump.fun tokens harvested today."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Token.mint)).where(
                Token.source == "pumpfun",
                Token.first_seen_at >= today,
            )
        )
        return result.scalar() or 0


async def get_pumpswap_count_today() -> int:
    """Returns number of PumpSwap tokens harvested today."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Token.mint)).where(
                Token.source == "pumpswap",
                Token.first_seen_at >= today,
            )
        )
        return result.scalar() or 0


async def get_token_by_mint(mint: str) -> "Token | None":
    """Returns a Token row by mint address, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Token).where(Token.mint == mint)
        )
        return result.scalar_one_or_none()


# ── Agent log helpers ─────────────────────────────────────────────────────────

async def log_agent_run(
    agent_name: str,
    tokens_found: int,
    tokens_saved: int,
    notes: str | None = None,
) -> "AgentLog":
    async with AsyncSessionLocal() as session:
        entry = AgentLog(
            agent_name=agent_name,
            tokens_found=tokens_found,
            tokens_saved=tokens_saved,
            notes=notes,
        )
        session.add(entry)
        await session.commit()
        await session.refresh(entry)
        return entry


async def get_last_agent_run(agent_name: str) -> "AgentLog | None":
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AgentLog)
            .where(AgentLog.agent_name == agent_name)
            .order_by(AgentLog.run_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


# ── Wallet helpers (Agent 2) ──────────────────────────────────────────────────

async def upsert_wallet(
    address: str,
    score: float,
    tier: int,
    win_rate: float,
    avg_multiple: float,
    wins: int,
    losses: int,
    total_trades: int,
    avg_entry_mcap: float | None,
    source: str | None = None,
    wallet_type: str | None = None,
    cluster_id: str | None = None,
) -> "Wallet":
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Wallet).where(Wallet.address == address)
        )
        w = result.scalar_one_or_none()
        if w is None:
            w = Wallet(
                address=address, score=score, tier=tier,
                win_rate=win_rate, avg_multiple=avg_multiple,
                wins=wins, losses=losses, total_trades=total_trades,
                avg_entry_mcap=avg_entry_mcap,
                source=source,
                wallet_type=wallet_type or "unknown",
                cluster_id=cluster_id,
            )
            session.add(w)
        else:
            w.score         = score
            w.tier          = tier
            w.win_rate      = win_rate
            w.avg_multiple  = avg_multiple
            w.wins          = wins
            w.losses        = losses
            w.total_trades  = total_trades
            w.avg_entry_mcap = avg_entry_mcap
            if source is not None:
                w.source = source
            # Only overwrite type/cluster when explicitly provided so a
            # second call without classification data doesn't wipe them
            if wallet_type is not None:
                w.wallet_type = wallet_type
            if cluster_id is not None:
                w.cluster_id = cluster_id
            w.last_updated_at = datetime.utcnow()
        await session.commit()
        await session.refresh(w)
        return w


# ── WalletTokenTrades helpers ───────────────────────────────────────────────

async def upsert_wallet_token_trade(
    wallet_address: str,
    token_address: str,
    first_buy_at,
    last_sell_at,
    total_bought_sol: float,
    total_sold_sol: float,
    multiple: float | None,
    entry_mcap: float | None,
    token_launch_at,
) -> None:
    """
    Upsert one per-wallet-per-token trade record. hold_duration_sec is
    computed from (last_sell_at - first_buy_at) when both present.
    """
    hold_duration = None
    if first_buy_at and last_sell_at:
        hold_duration = int((last_sell_at - first_buy_at).total_seconds())

    async with AsyncSessionLocal() as session:
        existing = (await session.execute(
            select(WalletTokenTrade).where(
                WalletTokenTrade.wallet_address == wallet_address,
                WalletTokenTrade.token_address == token_address,
            )
        )).scalar_one_or_none()

        if existing is None:
            session.add(WalletTokenTrade(
                wallet_address=wallet_address,
                token_address=token_address,
                first_buy_at=first_buy_at,
                last_sell_at=last_sell_at,
                total_bought_sol=total_bought_sol,
                total_sold_sol=total_sold_sol,
                multiple=multiple,
                entry_mcap=entry_mcap,
                token_launch_at=token_launch_at,
                hold_duration_sec=hold_duration,
            ))
        else:
            # Take earliest buy + latest sell across updates
            if first_buy_at and (existing.first_buy_at is None or first_buy_at < existing.first_buy_at):
                existing.first_buy_at = first_buy_at
            if last_sell_at and (existing.last_sell_at is None or last_sell_at > existing.last_sell_at):
                existing.last_sell_at = last_sell_at
            existing.total_bought_sol = total_bought_sol
            existing.total_sold_sol = total_sold_sol
            existing.multiple = multiple
            if entry_mcap is not None:
                existing.entry_mcap = entry_mcap
            if token_launch_at is not None and existing.token_launch_at is None:
                existing.token_launch_at = token_launch_at
            # Recompute hold duration
            if existing.first_buy_at and existing.last_sell_at:
                existing.hold_duration_sec = int(
                    (existing.last_sell_at - existing.first_buy_at).total_seconds()
                )
            existing.updated_at = datetime.utcnow()

        await session.commit()


async def get_wallet_token_trades(wallet_address: str) -> list["WalletTokenTrade"]:
    async with AsyncSessionLocal() as session:
        return list((await session.execute(
            select(WalletTokenTrade)
            .where(WalletTokenTrade.wallet_address == wallet_address)
            .order_by(WalletTokenTrade.first_buy_at.asc())
        )).scalars().all())


async def get_token_buyers_with_timing(token_address: str) -> list["WalletTokenTrade"]:
    """Every wallet that bought this token, ordered by first_buy_at asc."""
    async with AsyncSessionLocal() as session:
        return list((await session.execute(
            select(WalletTokenTrade)
            .where(WalletTokenTrade.token_address == token_address)
            .order_by(WalletTokenTrade.first_buy_at.asc())
        )).scalars().all())


# ── WalletClusters helpers ──────────────────────────────────────────────────

async def upsert_wallet_cluster(
    cluster_id: str,
    wallet_addresses: list[str],
    wins: int = 0,
    losses: int = 0,
    win_rate: float = 0.0,
    avg_multiple: float = 1.0,
    last_active=None,
) -> None:
    import json as _json
    async with AsyncSessionLocal() as session:
        existing = (await session.execute(
            select(WalletCluster).where(WalletCluster.cluster_id == cluster_id)
        )).scalar_one_or_none()

        if existing is None:
            session.add(WalletCluster(
                cluster_id=cluster_id,
                wallet_addresses=_json.dumps(sorted(wallet_addresses)),
                wins=wins,
                losses=losses,
                win_rate=win_rate,
                avg_multiple=avg_multiple,
                last_active=last_active,
            ))
        else:
            existing.wallet_addresses = _json.dumps(sorted(wallet_addresses))
            existing.wins = wins
            existing.losses = losses
            existing.win_rate = win_rate
            existing.avg_multiple = avg_multiple
            if last_active is not None:
                existing.last_active = last_active
        await session.commit()


async def get_all_wallet_clusters() -> list["WalletCluster"]:
    async with AsyncSessionLocal() as session:
        return list((await session.execute(
            select(WalletCluster).order_by(WalletCluster.last_active.desc())
        )).scalars().all())


async def get_top_wallets(limit: int = 10) -> list["Wallet"]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Wallet)
            .where(Wallet.tier > 0)
            .order_by(Wallet.score.desc())
            .limit(limit)
        )
        return list(result.scalars().all())


async def get_wallet_counts() -> dict:
    """Returns total wallet count and per-tier counts."""
    async with AsyncSessionLocal() as session:
        total  = (await session.execute(select(func.count(Wallet.address)).where(Wallet.tier > 0))).scalar() or 0
        tier1  = (await session.execute(select(func.count(Wallet.address)).where(Wallet.tier == 1))).scalar() or 0
        tier2  = (await session.execute(select(func.count(Wallet.address)).where(Wallet.tier == 2))).scalar() or 0
        tier3  = (await session.execute(select(func.count(Wallet.address)).where(Wallet.tier == 3))).scalar() or 0
    return {"total": total, "tier1": tier1, "tier2": tier2, "tier3": tier3}


async def get_winning_scans(since: "datetime | None" = None) -> list["Scan"]:
    """Returns scans with peak_multiplier >= 2 and a known entry MC."""
    async with AsyncSessionLocal() as session:
        q = select(Scan).where(
            Scan.peak_multiplier >= 2,
            Scan.entry_price.is_not(None),
        )
        if since is not None:
            q = q.where(Scan.scanned_at > since)
        result = await session.execute(q.order_by(Scan.scanned_at.asc()))
        return list(result.scalars().all())


# ── Pattern helpers (Agent 3) ─────────────────────────────────────────────────

async def upsert_pattern(
    pattern_type: str,
    outcome_threshold: str | None,
    sample_count: int,
    avg_entry_mcap: float | None,
    mcap_range_low: float | None,
    mcap_range_high: float | None,
    avg_liquidity: float | None,
    min_liquidity: float | None,
    avg_ai_score: float | None,
    avg_rugcheck_score: float | None,
    best_hours: str | None,
    best_days: str | None,
    confidence_score: float,
) -> "Pattern":
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pattern).where(Pattern.pattern_type == pattern_type)
        )
        p = result.scalar_one_or_none()
        now = datetime.utcnow()
        if p is None:
            p = Pattern(
                pattern_type=pattern_type,
                outcome_threshold=outcome_threshold,
                sample_count=sample_count,
                avg_entry_mcap=avg_entry_mcap,
                mcap_range_low=mcap_range_low,
                mcap_range_high=mcap_range_high,
                avg_liquidity=avg_liquidity,
                min_liquidity=min_liquidity,
                avg_ai_score=avg_ai_score,
                avg_rugcheck_score=avg_rugcheck_score,
                best_hours=best_hours,
                best_days=best_days,
                confidence_score=confidence_score,
                created_at=now,
                updated_at=now,
            )
            session.add(p)
        else:
            p.outcome_threshold  = outcome_threshold
            p.sample_count       = sample_count
            p.avg_entry_mcap     = avg_entry_mcap
            p.mcap_range_low     = mcap_range_low
            p.mcap_range_high    = mcap_range_high
            p.avg_liquidity      = avg_liquidity
            p.min_liquidity      = min_liquidity
            p.avg_ai_score       = avg_ai_score
            p.avg_rugcheck_score = avg_rugcheck_score
            p.best_hours         = best_hours
            p.best_days          = best_days
            p.confidence_score   = confidence_score
            p.updated_at         = now
        await session.commit()
        await session.refresh(p)
        return p


async def get_active_patterns() -> list["Pattern"]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pattern).order_by(Pattern.confidence_score.desc())
        )
        return list(result.scalars().all())


async def get_pattern_counts() -> dict:
    async with AsyncSessionLocal() as session:
        total   = (await session.execute(select(func.count(Pattern.id)))).scalar() or 0
        winners = (await session.execute(
            select(func.count(Pattern.id)).where(Pattern.pattern_type.like("winner%"))
        )).scalar() or 0
        rugs = (await session.execute(
            select(func.count(Pattern.id)).where(Pattern.pattern_type.like("rug%"))
        )).scalar() or 0
    return {"total": total, "winners": winners, "rugs": rugs}


async def get_closed_scans_for_analysis() -> list["Scan"]:
    """Returns all closed scans with entry data, suitable for pattern analysis."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Scan).where(
                Scan.status.in_(["win", "break_even", "loss"]),
                Scan.entry_price.is_not(None),
                Scan.peak_multiplier.is_not(None),
            ).order_by(Scan.scanned_at.asc())
        )
        return list(result.scalars().all())


async def get_tokens_by_mints(mints: list[str]) -> "dict[str, Token]":
    """Batch-fetch Token rows by mint address. Returns {mint: Token}."""
    if not mints:
        return {}
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Token).where(Token.mint.in_(mints))
        )
        return {t.mint: t for t in result.scalars().all()}


async def get_token_by_mint(mint: str) -> "Token | None":
    """Returns a single Token by its mint address, or None."""
    async with AsyncSessionLocal() as session:
        return await session.get(Token, mint)


async def get_tier_wallets(max_tier: int = 2) -> list["Wallet"]:
    """Returns wallets with tier 1..max_tier ordered by score desc (max 30)."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Wallet)
            .where(Wallet.tier >= 1, Wallet.tier <= max_tier)
            .order_by(Wallet.score.desc())
            .limit(30)
        )
        return list(result.scalars().all())


async def get_pattern_by_type(pattern_type: str) -> "Pattern | None":
    """Returns a single Pattern row by pattern_type, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pattern).where(Pattern.pattern_type == pattern_type)
        )
        return result.scalar_one_or_none()


async def get_hub_stats() -> dict:
    """Returns aggregated stats for the /hub dashboard."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        scans_today = (await session.execute(
            select(func.count(Scan.id)).where(Scan.scanned_at >= today)
        )).scalar() or 0

        trades_today = (await session.execute(
            select(func.count(Position.id)).where(Position.opened_at >= today)
        )).scalar() or 0

        today_pnl = (await session.execute(
            select(func.coalesce(func.sum(Position.pnl_sol), 0.0))
            .where(Position.closed_at >= today, Position.pnl_sol.is_not(None))
        )).scalar() or 0.0

        alltime_pnl = (await session.execute(
            select(func.coalesce(func.sum(Position.pnl_sol), 0.0))
            .where(Position.pnl_sol.is_not(None))
        )).scalar() or 0.0

        total_closed = (await session.execute(
            select(func.count(Position.id))
            .where(Position.status == "closed", Position.pnl_sol.is_not(None))
        )).scalar() or 0

        wins = (await session.execute(
            select(func.count(Position.id))
            .where(Position.status == "closed", Position.pnl_sol > 0)
        )).scalar() or 0

        win_rate = round(wins / total_closed * 100) if total_closed > 0 else 0

        recent = list((await session.execute(
            select(Position)
            .order_by(Position.opened_at.desc())
            .limit(5)
        )).scalars().all())

        token_count = (await session.execute(
            select(func.count(Token.mint))
        )).scalar() or 0

        last_harvest = (await session.execute(
            select(AgentLog)
            .where(AgentLog.agent_name == "harvester")
            .order_by(AgentLog.run_at.desc())
            .limit(1)
        )).scalar_one_or_none()

        wallet_total = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.tier > 0)
        )).scalar() or 0

        wallet_tier1 = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.tier == 1)
        )).scalar() or 0

        wallet_tier2 = (await session.execute(
            select(func.count(Wallet.address)).where(Wallet.tier == 2)
        )).scalar() or 0

        last_analyst = (await session.execute(
            select(AgentLog)
            .where(AgentLog.agent_name == "wallet_analyst")
            .order_by(AgentLog.run_at.desc())
            .limit(1)
        )).scalar_one_or_none()

        pattern_total = (await session.execute(
            select(func.count(Pattern.id))
        )).scalar() or 0

        pattern_winners = (await session.execute(
            select(func.count(Pattern.id)).where(Pattern.pattern_type.like("winner%"))
        )).scalar() or 0

        pattern_rugs = (await session.execute(
            select(func.count(Pattern.id)).where(Pattern.pattern_type.like("rug%"))
        )).scalar() or 0

        last_pattern_engine = (await session.execute(
            select(AgentLog)
            .where(AgentLog.agent_name == "pattern_engine")
            .order_by(AgentLog.run_at.desc())
            .limit(1)
        )).scalar_one_or_none()

    return {
        "scans_today":        scans_today,
        "trades_today":       trades_today,
        "today_pnl":          float(today_pnl),
        "alltime_pnl":        float(alltime_pnl),
        "win_rate":           win_rate,
        "total_closed":       total_closed,
        "recent_trades":      recent,
        "token_count":        token_count,
        "last_harvest":       last_harvest,
        "wallet_total":       wallet_total,
        "wallet_tier1":       wallet_tier1,
        "wallet_tier2":       wallet_tier2,
        "last_analyst":       last_analyst,
        "pattern_total":      pattern_total,
        "pattern_winners":    pattern_winners,
        "pattern_rugs":       pattern_rugs,
        "last_pattern_engine": last_pattern_engine,
    }


# ── Candidate helpers (Agent 5 — Confidence Engine) ─────────────────────────

async def save_candidate(
    token_address: str,
    token_name: str | None,
    confidence_score: float,
    fingerprint_score: float,
    insider_score: float,
    chart_score: float,
    rug_score: float,
    caller_score: float,
    market_score: float,
    decision: str,
    executed: bool = False,
    source: str | None = None,
    chart_pattern: str | None = None,
) -> "Candidate":
    async with AsyncSessionLocal() as session:
        candidate = Candidate(
            token_address=token_address,
            token_name=token_name,
            confidence_score=confidence_score,
            fingerprint_score=fingerprint_score,
            insider_score=insider_score,
            chart_score=chart_score,
            rug_score=rug_score,
            caller_score=caller_score,
            market_score=market_score,
            source=source,
            chart_pattern=chart_pattern,
            decision=decision,
            executed=executed,
        )
        session.add(candidate)
        await session.commit()
        await session.refresh(candidate)
        return candidate


async def get_candidate_stats_today() -> dict:
    """Returns candidate counts for /hub display."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        scored_today = (await session.execute(
            select(func.count(Candidate.id)).where(Candidate.created_at >= today)
        )).scalar() or 0

        high_conf = (await session.execute(
            select(func.count(Candidate.id)).where(
                Candidate.created_at >= today,
                Candidate.confidence_score >= 80,
            )
        )).scalar() or 0

        executed_today = (await session.execute(
            select(func.count(Candidate.id)).where(
                Candidate.created_at >= today,
                Candidate.executed == True,
            )
        )).scalar() or 0

    return {
        "scored_today":   scored_today,
        "high_conf":      high_conf,
        "executed_today": executed_today,
    }


async def has_caller_scanned(token_address: str) -> bool:
    """Returns True if any caller has scanned this token address."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(Scan.id)).where(Scan.contract_address == token_address)
        )
        return (result.scalar() or 0) > 0


# ── Agent Weights helpers (Agent 6 — Learning Loop) ─────────────────────────

async def get_current_weights() -> "AgentWeights | None":
    """Returns the most recent weights row, or None if no weights exist yet."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AgentWeights).order_by(AgentWeights.id.desc()).limit(1)
        )
        return result.scalar_one_or_none()


async def save_weights(
    fingerprint: float,
    insider: float,
    chart: float,
    rug: float,
    caller: float,
    market: float,
    trades_analyzed: int,
    notes: str | None = None,
) -> "AgentWeights":
    """Appends a new weights row (history is preserved)."""
    async with AsyncSessionLocal() as session:
        row = AgentWeights(
            fingerprint_weight=fingerprint,
            insider_weight=insider,
            chart_weight=chart,
            rug_weight=rug,
            caller_weight=caller,
            market_weight=market,
            trades_analyzed=trades_analyzed,
            notes=notes,
        )
        session.add(row)
        await session.commit()
        await session.refresh(row)
        return row


async def get_closed_positions_since(since_id: int, limit: int = 200) -> list:
    """
    Returns a combined list of closed PaperTrades + closed Positions, oldest first.
    Paper trades are the primary learning substrate; real Positions are included as
    a fallback/augmentation when they exist. `since_id` is retained for signature
    compatibility but is ignored — the caller slices from `last_analyzed`.
    """
    async with AsyncSessionLocal() as session:
        paper_rows = (await session.execute(
            select(PaperTrade)
            .where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
            .order_by(PaperTrade.closed_at.asc())
            .limit(limit)
        )).scalars().all()

        position_rows = (await session.execute(
            select(Position)
            .where(
                Position.status == "closed",
                Position.pnl_sol.is_not(None),
            )
            .order_by(Position.closed_at.asc())
            .limit(limit)
        )).scalars().all()

    combined = list(paper_rows) + list(position_rows)
    combined.sort(key=lambda t: t.closed_at or datetime.min)
    return combined[:limit]


async def get_total_closed_count() -> int:
    """Returns total number of closed trades (paper + real positions with PnL)."""
    async with AsyncSessionLocal() as session:
        paper_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalar() or 0

        position_count = (await session.execute(
            select(func.count(Position.id)).where(
                Position.status == "closed",
                Position.pnl_sol.is_not(None),
            )
        )).scalar() or 0

        return paper_count + position_count


async def get_candidate_by_token(token_address: str) -> "Candidate | None":
    """Returns the most recent candidate row for a token, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Candidate)
            .where(Candidate.token_address == token_address)
            .order_by(Candidate.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


async def update_wallet_tier(address: str, new_tier: int) -> None:
    """Updates a wallet's tier (clamped 0–3)."""
    new_tier = max(0, min(3, new_tier))
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Wallet).where(Wallet.address == address)
        )
        wallet = result.scalar_one_or_none()
        if wallet:
            wallet.tier = new_tier
            wallet.last_updated_at = datetime.utcnow()
            await session.commit()


async def get_weekly_performance() -> dict:
    """Returns performance stats for the last 7 days."""
    cutoff = datetime.utcnow() - timedelta(days=7)
    async with AsyncSessionLocal() as session:
        closed = (await session.execute(
            select(func.count(Position.id)).where(
                Position.status == "closed",
                Position.closed_at >= cutoff,
                Position.pnl_sol.is_not(None),
            )
        )).scalar() or 0

        wins = (await session.execute(
            select(func.count(Position.id)).where(
                Position.status == "closed",
                Position.closed_at >= cutoff,
                Position.pnl_sol > 0,
            )
        )).scalar() or 0

        total_pnl = (await session.execute(
            select(func.coalesce(func.sum(Position.pnl_sol), 0.0)).where(
                Position.status == "closed",
                Position.closed_at >= cutoff,
                Position.pnl_sol.is_not(None),
            )
        )).scalar() or 0.0

    win_rate = round(wins / closed * 100) if closed > 0 else 0
    avg_pnl = round(total_pnl / closed, 4) if closed > 0 else 0.0
    return {
        "trades": closed,
        "wins": wins,
        "win_rate": win_rate,
        "total_pnl": float(total_pnl),
        "avg_pnl": avg_pnl,
    }


# ── AI Trade Params helpers (Agent 6) ────────────────────────────────────────

async def get_trade_params(pattern_type: str) -> "AITradeParams | None":
    """Returns learned trade params for a pattern type, or None."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AITradeParams).where(AITradeParams.pattern_type == pattern_type)
        )
        return result.scalar_one_or_none()


async def get_all_trade_params() -> list["AITradeParams"]:
    """Returns all AI trade param rows."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AITradeParams).order_by(AITradeParams.sample_size.desc())
        )
        return list(result.scalars().all())


async def upsert_trade_params(
    pattern_type: str,
    optimal_tp_x: float,
    optimal_sl_pct: float,
    optimal_position_pct: float = 10.0,
    sample_size: int = 0,
    win_rate: float = 0.0,
    avg_multiple: float = 1.0,
    confidence: float = 0.0,
    trail_sl_trigger_pct: float | None = None,
    trail_sl_enabled: int | None = None,
) -> "AITradeParams":
    """
    Creates or updates trade params for a pattern type.

    `trail_sl_trigger_pct` and `trail_sl_enabled` are preserved on existing
    rows when None is passed — this lets Agent 6 update tp/sl without
    clobbering trailing-stop config that a human set via /setparam.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AITradeParams).where(AITradeParams.pattern_type == pattern_type)
        )
        row = result.scalar_one_or_none()
        if row:
            row.optimal_tp_x = optimal_tp_x
            row.optimal_sl_pct = optimal_sl_pct
            row.optimal_position_pct = optimal_position_pct
            row.sample_size = sample_size
            row.win_rate = win_rate
            row.avg_multiple = avg_multiple
            row.confidence = confidence
            if trail_sl_trigger_pct is not None:
                row.trail_sl_trigger_pct = trail_sl_trigger_pct
            if trail_sl_enabled is not None:
                row.trail_sl_enabled = int(trail_sl_enabled)
            row.updated_at = datetime.utcnow()
        else:
            row = AITradeParams(
                pattern_type=pattern_type,
                optimal_tp_x=optimal_tp_x,
                optimal_sl_pct=optimal_sl_pct,
                optimal_position_pct=optimal_position_pct,
                sample_size=sample_size,
                win_rate=win_rate,
                avg_multiple=avg_multiple,
                confidence=confidence,
                trail_sl_trigger_pct=trail_sl_trigger_pct if trail_sl_trigger_pct is not None else 0.50,
                trail_sl_enabled=int(trail_sl_enabled) if trail_sl_enabled is not None else 0,
            )
            session.add(row)
        await session.commit()
        await session.refresh(row)
        return row


async def seed_ai_trade_params() -> int:
    """
    One-time seed of ai_trade_params with default rows for every known
    pattern_type so the resolver always finds a row. Only inserts rows that
    don't already exist — idempotent and safe to run on every boot.
    Returns count of rows newly inserted.
    """
    # Imported here to avoid a circular import at module load
    from bot.agents.trade_profiles import DEFAULT_AI_TRADE_PARAMS

    inserted = 0
    async with AsyncSessionLocal() as session:
        existing = (await session.execute(
            select(AITradeParams.pattern_type)
        )).scalars().all()
        existing_set = set(existing)

        for ptype, defaults in DEFAULT_AI_TRADE_PARAMS.items():
            if ptype in existing_set:
                continue
            session.add(AITradeParams(
                pattern_type=ptype,
                optimal_tp_x=float(defaults["tp_x"]),
                optimal_sl_pct=float(defaults["sl_pct"]),
                optimal_position_pct=10.0,
                trail_sl_trigger_pct=float(defaults["trail_trigger"]),
                trail_sl_enabled=int(defaults["trail_on"]),
            ))
            inserted += 1

        if inserted:
            await session.commit()

    if inserted:
        logger.info("seed_ai_trade_params: inserted %d default rows", inserted)
    return inserted


async def get_closed_positions_by_source(source: str) -> list["Position"]:
    """Returns closed positions whose token was originally detected via the given source."""
    async with AsyncSessionLocal() as session:
        # Join positions with candidates to find the source
        result = await session.execute(
            select(Position)
            .where(
                Position.status == "closed",
                Position.pnl_sol.is_not(None),
                Position.token_address.in_(
                    select(Candidate.token_address).where(Candidate.source == source)
                ),
            )
            .order_by(Position.closed_at.desc())
            .limit(200)
        )
        return list(result.scalars().all())


# ── Chart pattern stats helpers (Agent 7) ────────────────────────────────────

async def get_chart_pattern_stats_today() -> dict:
    """Returns chart detector stats for /hub display."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    async with AsyncSessionLocal() as session:
        detected = (await session.execute(
            select(func.count(Candidate.id)).where(
                Candidate.created_at >= today,
                Candidate.chart_pattern.is_not(None),
                Candidate.chart_pattern != "none",
            )
        )).scalar() or 0

        confirmed = (await session.execute(
            select(func.count(Candidate.id)).where(
                Candidate.created_at >= today,
                Candidate.chart_score >= 40,
                Candidate.chart_pattern.is_not(None),
                Candidate.chart_pattern != "none",
            )
        )).scalar() or 0

        rejected = detected - confirmed

    return {"detected": detected, "confirmed": confirmed, "rejected": rejected}


async def get_chart_pattern_win_rates() -> list[dict]:
    """Returns win rates per chart pattern type for /patterns display."""
    async with AsyncSessionLocal() as session:
        # Get all distinct chart patterns that have been traded
        patterns = (await session.execute(
            select(Candidate.chart_pattern)
            .where(
                Candidate.chart_pattern.is_not(None),
                Candidate.chart_pattern != "none",
                Candidate.executed == True,
            )
            .distinct()
        )).scalars().all()

        results = []
        for pname in patterns:
            if not pname:
                continue
            # Get positions for tokens with this chart pattern
            total = (await session.execute(
                select(func.count(Position.id)).where(
                    Position.status == "closed",
                    Position.pnl_sol.is_not(None),
                    Position.token_address.in_(
                        select(Candidate.token_address).where(
                            Candidate.chart_pattern == pname
                        )
                    ),
                )
            )).scalar() or 0

            wins = (await session.execute(
                select(func.count(Position.id)).where(
                    Position.status == "closed",
                    Position.pnl_sol > 0,
                    Position.token_address.in_(
                        select(Candidate.token_address).where(
                            Candidate.chart_pattern == pname
                        )
                    ),
                )
            )).scalar() or 0

            wr = round(wins / total * 100) if total > 0 else 0
            results.append({"pattern": pname, "trades": total, "wins": wins, "win_rate": wr})

        results.sort(key=lambda x: x["win_rate"], reverse=True)
        return results


# ── Paper Trade helpers ──────────────────────────────────────────────────────

async def open_paper_trade(
    token_address: str, token_name: str | None,
    entry_mc: float | None, entry_price: float | None,
    paper_sol: float, confidence: float,
    pattern_type: str | None, tp_x: float, sl_pct: float,
) -> "PaperTrade":
    async with AsyncSessionLocal() as session:
        pt = PaperTrade(
            token_address=token_address, token_name=token_name,
            entry_mc=entry_mc, entry_price=entry_price,
            paper_sol_spent=paper_sol, confidence_score=confidence,
            pattern_type=pattern_type, take_profit_x=tp_x,
            stop_loss_pct=sl_pct, status="open",
            peak_mc=entry_mc, peak_multiple=1.0,
        )
        session.add(pt)
        await session.commit()
        await session.refresh(pt)
        return pt


async def get_open_paper_trades() -> list["PaperTrade"]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PaperTrade).where(PaperTrade.status == "open")
        )
        return list(result.scalars().all())


async def has_open_paper_trade(token_address: str) -> bool:
    """Returns True if there's already an open paper trade for this token."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.token_address == token_address,
                PaperTrade.status == "open",
            )
        )
        return (result.scalar() or 0) > 0


async def has_recent_manual_close(token_address: str, within_hours: float = 24.0) -> bool:
    """
    Returns True if this token was manually closed (via /hub Close All
    or similar) in the last `within_hours`. Used by the scanner to
    prevent "rage quit" tokens from being reopened on the next tick
    just because DexScreener still lists them as trending.
    """
    cutoff = datetime.utcnow() - timedelta(hours=within_hours)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.token_address == token_address,
                PaperTrade.close_reason == "manual_close",
                PaperTrade.closed_at >= cutoff,
            )
        )
        return (result.scalar() or 0) > 0


async def get_recent_paper_trades(limit: int = 10) -> list["PaperTrade"]:
    """Returns the last N paper trades regardless of status, newest first."""
    async with AsyncSessionLocal() as session:
        return list((await session.execute(
            select(PaperTrade)
            .order_by(PaperTrade.id.desc())
            .limit(limit)
        )).scalars().all())


async def nuke_paper_trades() -> dict:
    """
    DESTRUCTIVE: wipe every row from paper_trades. Returns a dict with
    counts deleted per status (so the caller can log what was removed)
    plus the offset/starting values that were reset.

    Intended for admin use via /nukepaper confirm when pre-cap bug-era
    data has polluted the learning substrate and a clean slate is the
    cheapest path to honest numbers.
    """
    async with AsyncSessionLocal() as session:
        open_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
        )).scalar() or 0
        closed_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "closed")
        )).scalar() or 0
        total = open_count + closed_count

        await session.execute(
            PaperTrade.__table__.delete()
        )

        # Reset balance bookkeeping so compute_paper_balance reads
        # exactly the starting balance after the wipe
        off_row = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "paper_balance_offset")
        )).scalar_one_or_none()
        if off_row is None:
            session.add(AgentParam(param_name="paper_balance_offset", param_value=0.0))
        else:
            off_row.param_value = 0.0
            off_row.updated_at = datetime.utcnow()

        start_row = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "paper_starting_balance")
        )).scalar_one_or_none()
        starting = 20.0
        if start_row is not None:
            start_row.param_value = starting
            start_row.updated_at = datetime.utcnow()
        else:
            session.add(AgentParam(param_name="paper_starting_balance", param_value=starting))

        await session.commit()

    return {
        "deleted_total": int(total),
        "deleted_open": int(open_count),
        "deleted_closed": int(closed_count),
        "starting_reset_to": starting,
        "offset_reset_to": 0.0,
    }


async def reset_ai_trade_params_samples() -> int:
    """
    Zero out sample_size / wins / losses / win_rate / avg_multiple /
    confidence on every ai_trade_params row. Preserves tp/sl/trail
    config so the seeded defaults remain, but tells Agent 6 there is
    no data to learn from yet — forces it to re-accumulate samples
    from post-nuke trades only.

    Returns the number of rows updated.
    """
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(AITradeParams))).scalars().all()
        count = 0
        for r in rows:
            r.sample_size = 0
            r.win_rate = 0.0
            r.avg_multiple = 1.0
            r.confidence = 0.0
            r.updated_at = datetime.utcnow()
            count += 1
        await session.commit()
    return count


async def close_paper_trade(
    trade_id: int, close_reason: str, pnl_sol: float, peak_mc: float | None, peak_mult: float | None,
) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(PaperTrade).where(PaperTrade.id == trade_id))
        pt = result.scalar_one_or_none()
        if pt:
            pt.status = "closed"
            pt.close_reason = close_reason
            pt.paper_pnl_sol = pnl_sol
            pt.closed_at = datetime.utcnow()
            if peak_mc:
                pt.peak_mc = peak_mc
            if peak_mult:
                pt.peak_multiple = peak_mult
            await session.commit()


async def update_paper_trade_peak(trade_id: int, current_mc: float, peak_mc: float, peak_mult: float) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(PaperTrade).where(PaperTrade.id == trade_id))
        pt = result.scalar_one_or_none()
        if pt:
            pt.peak_mc = peak_mc
            pt.peak_multiple = peak_mult
            await session.commit()


async def update_paper_dead_tracking(
    trade_id: int,
    zero_volume_since=_UNSET,
    last_move_at=_UNSET,
    last_move_mc=_UNSET,
) -> None:
    """
    Update dead-position tracking fields on a paper trade. Pass `_UNSET` to
    leave a field alone; pass `None` to explicitly null it. Used by the
    paper monitor to advance `zero_volume_since` / `last_move_at` state
    each tick without clobbering unrelated fields.
    """
    async with AsyncSessionLocal() as session:
        pt = (await session.execute(
            select(PaperTrade).where(PaperTrade.id == trade_id)
        )).scalar_one_or_none()
        if pt is None:
            return
        if zero_volume_since is not _UNSET:
            pt.zero_volume_since = zero_volume_since
        if last_move_at is not _UNSET:
            pt.last_move_at = last_move_at
        if last_move_mc is not _UNSET:
            pt.last_move_mc = last_move_mc
        await session.commit()


async def get_paper_trade_stats() -> dict:
    """
    Returns paper trade aggregates, split into STRATEGY and META buckets.

    `strategy_*` counts only trades that closed via a bot decision
    (tp_hit / sl_hit / trail_hit / dead_token / expired). These are the
    real performance numbers Agent 6 learns from.

    `meta_*` counts trades closed by human action (manual_close from the
    hub button, reset from /resetbalance). These inflate the balance but
    don't represent strategy skill — a human clicking "close" at a 14x
    peak isn't something the strategy could have done on its own.

    `total_pnl` / `win_rate` / etc. keep their legacy semantics
    (strategy + meta combined) so existing consumers don't break.
    """
    strategy_reasons = list(STRATEGY_CLOSE_REASONS)
    meta_reasons = list(META_CLOSE_REASONS)

    async with AsyncSessionLocal() as session:
        total = (await session.execute(
            select(func.count(PaperTrade.id))
        )).scalar() or 0

        closed = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed", PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalar() or 0

        wins = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed", PaperTrade.paper_pnl_sol > 0,
            )
        )).scalar() or 0

        total_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalar() or 0.0

        # Strategy-only aggregates (what Agent 6 learns from)
        strategy_closed = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.close_reason.in_(strategy_reasons),
            )
        )).scalar() or 0

        strategy_wins = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed",
                PaperTrade.paper_pnl_sol > 0,
                PaperTrade.close_reason.in_(strategy_reasons),
            )
        )).scalar() or 0

        strategy_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.close_reason.in_(strategy_reasons),
            )
        )).scalar() or 0.0

        meta_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.close_reason.in_(meta_reasons),
            )
        )).scalar() or 0.0

        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.opened_at >= today)
        )).scalar() or 0

        today_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.closed_at >= today, PaperTrade.paper_pnl_sol.is_not(None),
            )
        )).scalar() or 0.0

        today_strategy_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.closed_at >= today,
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.close_reason.in_(strategy_reasons),
            )
        )).scalar() or 0.0

        today_meta_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0)).where(
                PaperTrade.closed_at >= today,
                PaperTrade.paper_pnl_sol.is_not(None),
                PaperTrade.close_reason.in_(meta_reasons),
            )
        )).scalar() or 0.0

        open_count = (await session.execute(
            select(func.count(PaperTrade.id)).where(PaperTrade.status == "open")
        )).scalar() or 0

        recent = list((await session.execute(
            select(PaperTrade).where(PaperTrade.status == "closed")
            .order_by(PaperTrade.closed_at.desc()).limit(5)
        )).scalars().all())

    win_rate = round(wins / closed * 100) if closed > 0 else 0
    strategy_win_rate = round(strategy_wins / strategy_closed * 100) if strategy_closed > 0 else 0

    return {
        "total": total, "closed": closed, "wins": wins,
        "win_rate": win_rate, "total_pnl": float(total_pnl),
        "today_count": today_count, "today_pnl": float(today_pnl),
        "open_count": open_count, "recent": recent,
        # Split numbers for honest reporting
        "strategy_closed": strategy_closed,
        "strategy_wins": strategy_wins,
        "strategy_win_rate": strategy_win_rate,
        "strategy_pnl": float(strategy_pnl),
        "meta_pnl": float(meta_pnl),
        "today_strategy_pnl": float(today_strategy_pnl),
        "today_meta_pnl": float(today_meta_pnl),
    }


async def compute_paper_balance(starting: float = 20.0) -> float:
    """
    Compute true paper balance from DB:
    starting - sum(open positions) + sum(closed PnL) + paper_balance_offset

    The offset is applied here (single source of truth) so every caller
    sees the same number without manually re-adding it.
    """
    async with AsyncSessionLocal() as session:
        open_locked = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_sol_spent), 0.0))
            .where(PaperTrade.status == "open")
        )).scalar() or 0.0

        closed_pnl = (await session.execute(
            select(func.coalesce(func.sum(PaperTrade.paper_pnl_sol), 0.0))
            .where(PaperTrade.status == "closed", PaperTrade.paper_pnl_sol.is_not(None))
        )).scalar() or 0.0

        offset_row = (await session.execute(
            select(AgentParam.param_value).where(AgentParam.param_name == "paper_balance_offset")
        )).scalar_one_or_none()
        offset = float(offset_row) if offset_row is not None else 0.0

    return round(starting - float(open_locked) + float(closed_pnl) + offset, 4)


# ── Agent Params helpers ─────────────────────────────────────────────────────

# All defaults — inserted on first run if not present
AGENT_PARAM_DEFAULTS = {
    # Scanner
    "scanner_min_mc": 10000, "scanner_max_mc": 5000000, "scanner_min_liquidity": 5000,
    "scanner_min_ai_score": 40, "scanner_rugcheck_max_risk": 500,
    "scanner_interval_seconds": 15, "scanner_max_candidates": 10,
    # Wallet tiers
    "tier1_min_score": 80, "tier2_min_score": 60, "tier3_min_score": 40,
    "tier1_min_winrate": 0.65, "tier2_min_winrate": 0.45,
    "tier1_min_avg_mult": 5.0, "tier2_min_avg_mult": 2.0, "tier3_min_avg_mult": 1.5,
    "tier1_min_trades": 5, "tier2_min_trades": 3, "tier3_min_trades": 2,
    "wallet_analyst_interval_min": 30,
    # Pattern engine
    "pattern_min_samples": 3, "pattern_interval_hours": 6,
    # Confidence thresholds
    "conf_full_threshold": 80, "conf_half_threshold": 70, "conf_paper_threshold": 20,
    # MC weights — low
    "low_mc_insider": 0.35, "low_mc_fingerprint": 0.28, "low_mc_chart": 0.05,
    "low_mc_rug": 0.20, "low_mc_caller": 0.08, "low_mc_market": 0.04,
    # MC weights — mid
    "mid_mc_insider": 0.30, "mid_mc_fingerprint": 0.25, "mid_mc_chart": 0.15,
    "mid_mc_rug": 0.18, "mid_mc_caller": 0.08, "mid_mc_market": 0.04,
    # MC weights — high
    "high_mc_insider": 0.20, "high_mc_fingerprint": 0.20, "high_mc_chart": 0.30,
    "high_mc_rug": 0.15, "high_mc_caller": 0.10, "high_mc_market": 0.05,
    # Position sizing (% of balance)
    "size_confidence_20": 5, "size_confidence_50": 10,
    "size_confidence_70": 15, "size_confidence_80": 20,
    # Learning loop
    "learning_micro_batch": 3, "learning_full_batch": 5, "learning_major_batch": 15,
    "learning_max_weight_shift": 0.10,
    # Trade mode: 0=off, 1=paper, 2=live
    "trade_mode": 1,  # default to paper
    # Paper balance
    "paper_starting_balance": 20.0,
    "paper_balance_offset": 0.0,
    "paper_reset_v20_done": 0.0,   # one-shot migration flag (legacy)
    "paper_reset_v3_done":  0.0,   # v3: re-reset to 20 after balance inflation
    "sl_floor_20_applied":  0.0,   # raise learned SLs to >=20% once

    # Global trailing-stop config (per-type triggers live in ai_trade_params)
    "trail_sl_enabled":      0.0,  # 0 = off kill switch, 1 = on
    "trail_sl_pct":          0.20, # distance from peak when trailing active

    # Absolute per-trade position size cap in SOL, prevents the
    # compounding-size runaway loop where winners balloon the balance
    # and the next trade opens at an unrealistic size.
    "max_position_sol": 5.0,

    # When a paper trade is manually closed via /hub, don't re-enter
    # the same token mint for this many hours. Prevents the "rage quit
    # reopens next tick" loop when the token stays on DexScreener
    # trending.
    "manual_close_cooldown_hours": 24.0,

    # Self-healing state (Agent 6)
    "last_wr_snapshot_at": 0.0,    # trades_analyzed at last WR snapshot
    "last_wr_snapshot_wr": 0.0,    # win rate at last snapshot

    # Wallet tier thresholds (Agent 2) — realistic levels per user spec
    "tier1_min_wr":       0.45,
    "tier1_min_mult":     2.5,
    "tier1_min_trades":   3,
    "tier1_min_score":    70,
    "tier2_min_wr":       0.35,
    "tier2_min_mult":     1.8,
    "tier2_min_trades":   2,
    "tier2_min_score":    50,
    "tier3_min_wr":       0.20,
    "tier3_min_trades":   1,
    "tier3_min_score":    30,
    "wallet_min_score_to_save": 25,   # was 40; below this → don't save

    # One-shot migration flag for re-scoring existing wallets under new thresholds
    "wallet_rescore_v2_done": 0.0,
}


# Baseline MC-bucket weights — kept as reference for consumers that want
# to render "how different is current from baseline" stats. Agent 6 no
# longer hard-resets to these; instead _self_heal_weights normalizes the
# existing (possibly drifted) weights proportionally to preserve learned
# ratios. The 0.45/0.02 bounds below are DETECTION thresholds only, not
# enforcement — a weight above 0.45 triggers a normalize pass but the
# output may still exceed 0.45 if that's what the learned ratios produce.
MC_WEIGHT_DEFAULTS = {
    "low_mc_": {
        "insider": 0.35, "fingerprint": 0.28, "chart": 0.05,
        "rug": 0.20, "caller": 0.08, "market": 0.04,
    },
    "mid_mc_": {
        "insider": 0.30, "fingerprint": 0.25, "chart": 0.15,
        "rug": 0.18, "caller": 0.08, "market": 0.04,
    },
    "high_mc_": {
        "insider": 0.20, "fingerprint": 0.20, "chart": 0.30,
        "rug": 0.15, "caller": 0.10, "market": 0.05,
    },
}
MC_WEIGHT_DRIFT_HIGH = 0.45   # detection trigger only, not a hard cap
MC_WEIGHT_DRIFT_LOW  = 0.02   # detection trigger only, not a hard cap


async def init_agent_params() -> int:
    """Insert defaults for any missing params. Returns count of new params added."""
    added = 0
    async with AsyncSessionLocal() as session:
        for name, default in AGENT_PARAM_DEFAULTS.items():
            result = await session.execute(
                select(AgentParam).where(AgentParam.param_name == name)
            )
            if result.scalar_one_or_none() is None:
                session.add(AgentParam(param_name=name, param_value=float(default)))
                added += 1
        await session.commit()

    # ── One-shot migration: reset paper balance to 20 SOL ────────────────
    # Runs exactly once per DB (gated by paper_reset_v20_done flag).
    # Force-sets paper_starting_balance=20 and writes paper_balance_offset
    # so compute_paper_balance() returns exactly 20 SOL right after boot,
    # regardless of open trades or historical PnL.
    async with AsyncSessionLocal() as session:
        flag_row = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "paper_reset_v20_done")
        )).scalar_one_or_none()
        already_done = flag_row is not None and flag_row.param_value >= 1.0

    if not already_done:
        async with AsyncSessionLocal() as session:
            # 1. Force starting balance to 20
            row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_starting_balance")
            )).scalar_one_or_none()
            if row is None:
                session.add(AgentParam(param_name="paper_starting_balance", param_value=20.0))
            else:
                row.param_value = 20.0
                row.updated_at = datetime.utcnow()

            # 2. Zero the offset first so compute_paper_balance reflects pure DB state
            off_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_balance_offset")
            )).scalar_one_or_none()
            if off_row is None:
                session.add(AgentParam(param_name="paper_balance_offset", param_value=0.0))
            else:
                off_row.param_value = 0.0
                off_row.updated_at = datetime.utcnow()

            await session.commit()

        # 3. Compute current raw balance with offset=0 and write the offset
        #    that brings it back to exactly 20 SOL
        raw = await compute_paper_balance(20.0)
        needed_offset = round(20.0 - raw, 4)

        async with AsyncSessionLocal() as session:
            off_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_balance_offset")
            )).scalar_one_or_none()
            off_row.param_value = needed_offset
            off_row.updated_at = datetime.utcnow()

            # 4. Flip the flag so this block never runs again
            flag_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_reset_v20_done")
            )).scalar_one_or_none()
            if flag_row is None:
                session.add(AgentParam(param_name="paper_reset_v20_done", param_value=1.0))
            else:
                flag_row.param_value = 1.0
                flag_row.updated_at = datetime.utcnow()

            await session.commit()

        import logging as _logging
        _logging.getLogger(__name__).info(
            "Paper balance reset to 20 SOL (offset=%+.4f, raw_was=%.4f)",
            needed_offset, raw,
        )
        added += 1

    # MC weight healing is handled by learning_loop._self_heal_weights()
    # which runs on every poll tick and normalizes drifted buckets
    # proportionally without overwriting learned ratios. No hard reset.

    # ── One-shot: re-score existing wallets with new tier thresholds ────
    # Run once per DB (gated by wallet_rescore_v2_done flag). Reads every
    # Wallet row and recomputes tier from its stored stats under the new
    # agent_params thresholds. Does NOT touch stats, source, or classifier
    # fields — only updates score + tier.
    async with AsyncSessionLocal() as session:
        rescore_flag = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "wallet_rescore_v2_done")
        )).scalar_one_or_none()
        rescore_done = rescore_flag is not None and rescore_flag.param_value >= 1.0

    if not rescore_done:
        # Import lazily to avoid a circular import on module load
        from bot.agents.wallet_analyst import _score_wallet as _ws

        import logging as _logging
        _log = _logging.getLogger(__name__)

        updated = 0
        async with AsyncSessionLocal() as session:
            wallets = (await session.execute(select(Wallet))).scalars().all()

        for w in wallets:
            try:
                new_score, new_tier = await _ws(
                    wins=w.wins, losses=w.losses, total_trades=w.total_trades,
                    avg_multiple=w.avg_multiple, early_entry_rate=0.0,
                )
            except Exception as exc:
                _log.warning("rescore: failed for %s: %s", w.address[:12], exc)
                continue
            async with AsyncSessionLocal() as session:
                row = (await session.execute(
                    select(Wallet).where(Wallet.address == w.address)
                )).scalar_one_or_none()
                if row is None:
                    continue
                row.score = new_score
                row.tier = new_tier
                row.last_updated_at = datetime.utcnow()
                await session.commit()
            updated += 1

        async with AsyncSessionLocal() as session:
            flag_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "wallet_rescore_v2_done")
            )).scalar_one_or_none()
            if flag_row is None:
                session.add(AgentParam(param_name="wallet_rescore_v2_done", param_value=1.0))
            else:
                flag_row.param_value = 1.0
                flag_row.updated_at = datetime.utcnow()
            await session.commit()

        _log.info("Wallet re-score: %d rows updated under new tier thresholds", updated)
        added += 1

    # ── One-shot: reset paper balance to 20 SOL (v3) ──────────────────
    # Runs once per DB (gated by paper_reset_v3_done). Same mechanism
    # as the v20 migration: closes no trades, just writes an offset
    # that brings the computed balance back to exactly 20 SOL. Used
    # after the compounding-size bug inflated the balance to ~976 SOL.
    # Open trades stay open. Closed trade history is preserved so
    # Agent 6 can still learn from it.
    async with AsyncSessionLocal() as session:
        v3_flag = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "paper_reset_v3_done")
        )).scalar_one_or_none()
        v3_done = v3_flag is not None and v3_flag.param_value >= 1.0

    if not v3_done:
        # Zero the existing offset, compute raw balance, write new
        # offset so computed balance snaps to exactly 20 SOL.
        async with AsyncSessionLocal() as session:
            off_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_balance_offset")
            )).scalar_one_or_none()
            if off_row is None:
                session.add(AgentParam(param_name="paper_balance_offset", param_value=0.0))
            else:
                off_row.param_value = 0.0
                off_row.updated_at = datetime.utcnow()
            await session.commit()

        raw = await compute_paper_balance(20.0)
        needed_offset = round(20.0 - raw, 4)

        async with AsyncSessionLocal() as session:
            off_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_balance_offset")
            )).scalar_one_or_none()
            off_row.param_value = needed_offset
            off_row.updated_at = datetime.utcnow()

            flag_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "paper_reset_v3_done")
            )).scalar_one_or_none()
            if flag_row is None:
                session.add(AgentParam(param_name="paper_reset_v3_done", param_value=1.0))
            else:
                flag_row.param_value = 1.0
                flag_row.updated_at = datetime.utcnow()
            await session.commit()

        import logging as _logging
        _logging.getLogger(__name__).info(
            "Paper balance v3 reset to 20 SOL (offset=%+.4f, raw_was=%.4f)",
            needed_offset, raw,
        )
        added += 1

    # ── One-shot: raise SL floor on learned ai_trade_params rows ──────
    # Prior Agent 6 runs tightened optimal_sl_pct down to the 10% floor
    # on almost every pattern_type (pre-nuke sl_hit_rate was 90%+). At
    # 10% SL, pump.fun tokens insta-stopout on normal price wiggle and
    # no trade gets room to run. User chose to raise the floor to 20%.
    # This block lifts every row currently below 20% to exactly 20%,
    # leaves rows already above 20% alone, and flips the flag so it
    # only runs once.
    async with AsyncSessionLocal() as session:
        sl_flag = (await session.execute(
            select(AgentParam).where(AgentParam.param_name == "sl_floor_20_applied")
        )).scalar_one_or_none()
        sl_done = sl_flag is not None and sl_flag.param_value >= 1.0

    if not sl_done:
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(select(AITradeParams))).scalars().all()
            raised = 0
            for r in rows:
                if (r.optimal_sl_pct or 0) < 20.0:
                    r.optimal_sl_pct = 20.0
                    r.updated_at = datetime.utcnow()
                    raised += 1

            flag_row = (await session.execute(
                select(AgentParam).where(AgentParam.param_name == "sl_floor_20_applied")
            )).scalar_one_or_none()
            if flag_row is None:
                session.add(AgentParam(param_name="sl_floor_20_applied", param_value=1.0))
            else:
                flag_row.param_value = 1.0
                flag_row.updated_at = datetime.utcnow()
            await session.commit()

        import logging as _logging
        _logging.getLogger(__name__).info(
            "SL floor migration: raised %d ai_trade_params rows to 20%%", raised,
        )
        added += 1

    return added


async def get_param(name: str) -> float:
    """Get a single param value, falls back to default."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AgentParam.param_value).where(AgentParam.param_name == name)
        )
        val = result.scalar_one_or_none()
    if val is not None:
        return val
    return AGENT_PARAM_DEFAULTS.get(name, 0.0)


async def get_params(*names: str) -> dict[str, float]:
    """Get multiple params at once."""
    result = {}
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(AgentParam).where(AgentParam.param_name.in_(names))
        )).scalars().all()
        for r in rows:
            result[r.param_name] = r.param_value
    for n in names:
        if n not in result:
            result[n] = AGENT_PARAM_DEFAULTS.get(n, 0.0)
    return result


async def get_all_params() -> dict[str, float]:
    """Get all params."""
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(select(AgentParam))).scalars().all()
    result = dict(AGENT_PARAM_DEFAULTS)
    for r in rows:
        result[r.param_name] = r.param_value
    return result


async def set_param(name: str, value: float, reason: str | None = None,
                    trades: int | None = None, win_rate: float | None = None) -> None:
    """Set a param value and log the change."""
    old_value = await get_param(name)
    if abs(old_value - value) < 0.0001:
        return  # no change

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(AgentParam).where(AgentParam.param_name == name)
        )
        row = result.scalar_one_or_none()
        if row:
            row.param_value = value
            row.updated_at = datetime.utcnow()
        else:
            session.add(AgentParam(param_name=name, param_value=value))

        # Log the change
        session.add(ParamChange(
            param_name=name, old_value=old_value, new_value=value,
            reason=reason, trades_analyzed=trades, win_rate=win_rate,
        ))
        await session.commit()


async def get_recent_param_changes(limit: int = 10) -> list["ParamChange"]:
    """Returns the most recent param changes."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ParamChange).order_by(ParamChange.changed_at.desc()).limit(limit)
        )
        return list(result.scalars().all())


# ── Post-close tracking helpers ──────────────────────────────────────────────

async def get_recently_closed_paper_trades(hours: int = 25) -> list["PaperTrade"]:
    """Returns paper trades closed within last N hours that still need post-close tracking."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PaperTrade).where(
                PaperTrade.status == "closed",
                PaperTrade.closed_at >= cutoff,
                PaperTrade.mc_24h_after.is_(None),  # not yet fully tracked
            )
        )
        return list(result.scalars().all())


async def update_paper_post_close(
    trade_id: int, mc_1h: float | None = None, mc_4h: float | None = None,
    mc_6h: float | None = None, mc_24h: float | None = None,
    peak_after: float | None = None,
    sold_too_early: bool | None = None, sold_too_late: bool | None = None,
) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(PaperTrade).where(PaperTrade.id == trade_id))
        pt = result.scalar_one_or_none()
        if not pt:
            return
        if mc_1h is not None:
            pt.mc_1h_after = mc_1h
        if mc_4h is not None:
            pt.mc_4h_after = mc_4h
        if mc_6h is not None:
            pt.mc_6h_after = mc_6h
        if mc_24h is not None:
            pt.mc_24h_after = mc_24h
        if peak_after is not None:
            pt.peak_after_close = max(pt.peak_after_close or 0, peak_after)
        if sold_too_early is not None:
            pt.sold_too_early = sold_too_early
        if sold_too_late is not None:
            pt.sold_too_late = sold_too_late
        await session.commit()


async def get_post_close_stats() -> dict:
    """Returns aggregate post-close stats for learning."""
    async with AsyncSessionLocal() as session:
        total = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.status == "closed", PaperTrade.mc_24h_after.is_not(None),
            )
        )).scalar() or 0

        too_early = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.sold_too_early == True,
            )
        )).scalar() or 0

        too_late = (await session.execute(
            select(func.count(PaperTrade.id)).where(
                PaperTrade.sold_too_late == True,
            )
        )).scalar() or 0

    return {
        "total_tracked": total,
        "sold_too_early": too_early,
        "sold_too_late": too_late,
        "early_pct": round(too_early / total * 100) if total > 0 else 0,
        "late_pct": round(too_late / total * 100) if total > 0 else 0,
    }
