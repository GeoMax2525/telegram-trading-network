"""
health.py — liveness heartbeat + watchdog.

Each critical loop calls beat("name") every cycle. A watchdog loop checks the
last beat of every registered loop and pages ADMIN_IDS if any loop goes stale
past its threshold — so a 3am crash, a wedged stream, or a stalled monitor is
noticed in minutes instead of from missing trades.

In-memory by design: if the whole process dies, the watchdog dies too — that
case is covered by Railway's own restart + the startup ping below (a restart
with no startup ping for a while = the operator notices the bot is flapping).
For true external dead-man's-switch monitoring, add an uptime pinger later.
"""

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# name -> {"last": monotonic_ts, "stale_after_s": float}
_beats: dict[str, dict] = {}
# name -> bool (already alerted while stale, so we page once per stall)
_alerted: dict[str, bool] = {}


def register(name: str, stale_after_s: float) -> None:
    """Register a loop with how long it may go silent before it's 'stale'."""
    _beats[name] = {"last": time.monotonic(), "stale_after_s": float(stale_after_s)}
    _alerted[name] = False


def beat(name: str) -> None:
    """Record a heartbeat for a loop. Auto-registers with a 10min default."""
    b = _beats.get(name)
    if b is None:
        _beats[name] = {"last": time.monotonic(), "stale_after_s": 600.0}
    else:
        b["last"] = time.monotonic()
    if _alerted.get(name):
        _alerted[name] = False  # recovered


async def _alert(text: str) -> None:
    try:
        from bot.config import ADMIN_IDS
        import bot.state as _st
        bot_ref = getattr(_st, "bot", None)
        if bot_ref is None:
            return
        for uid in ADMIN_IDS:
            try:
                await bot_ref.send_message(uid, text)
            except Exception:
                pass
    except Exception:
        pass


def health_snapshot() -> list[dict]:
    """Per-loop status for /health."""
    now = time.monotonic()
    out = []
    for name, b in sorted(_beats.items()):
        age = now - b["last"]
        out.append({
            "name": name,
            "age_s": age,
            "stale_after_s": b["stale_after_s"],
            "stale": age > b["stale_after_s"],
        })
    return out


async def watchdog_loop() -> None:
    """Check every registered loop's heartbeat; page admins on a stall."""
    await asyncio.sleep(180)  # let loops register + warm up
    await _alert("✅ Bot started — watchdog online.")
    while True:
        try:
            now = time.monotonic()
            for name, b in list(_beats.items()):
                age = now - b["last"]
                if age > b["stale_after_s"]:
                    if not _alerted.get(name):
                        _alerted[name] = True
                        await _alert(
                            f"⚠️ HEALTH: loop '{name}' is STALE — no heartbeat for "
                            f"{age/60:.1f}min (limit {b['stale_after_s']/60:.0f}min). "
                            f"It may be wedged or crashed."
                        )
        except Exception as exc:
            logger.debug("watchdog: %s", exc)
        await asyncio.sleep(60)
