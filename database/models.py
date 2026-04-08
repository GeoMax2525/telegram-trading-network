"""
database/models.py — Async database using SQLAlchemy.

Uses PostgreSQL (asyncpg) when DATABASE_URL is set (Railway production),
falls back to SQLite (aiosqlite) when DATABASE_URL is not set (local dev).

Tables:
  scans            — logs every /scan command with token info, AI score, and caller details.
  callers          — approved caller Telegram user IDs (managed via /addcaller).
  keybot_settings  — per-user KeyBot trading presets.
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
