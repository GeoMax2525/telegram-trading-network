"""
style.py — Echo's Sega Genesis / Ecco the Dolphin (1992) message aesthetic.

Every Echo message is a retro "game screen": a monospace code-block box, short
structured lines, bold numbers, heavy blue/ocean emoji, premium + mysterious
tone (never childish). All public-facing copy is built here so the look stays
consistent across alerts, milestones, and the themed menus.
"""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def box(title: str, lines: list[str]) -> str:
    """Wrap content in the retro game-screen box (monospace code block)."""
    body = "\n".join([f"🐬 ECHO — {title} 🐬", "", *lines])
    return f"```\n{body}\n```"


def fmt_mc(mc: float) -> str:
    if mc >= 1_000_000:
        return f"${mc/1_000_000:.1f}M"
    if mc >= 1_000:
        return f"${mc/1_000:.0f}K"
    return f"${mc:.0f}"


# ── Keyboards ───────────────────────────────────────────────────────────────
def kb_copy(ca: str) -> InlineKeyboardMarkup:
    """One-tap copy-CA + dive-deeper buttons under an alert."""
    try:
        from aiogram.types import CopyTextButton
        copy_btn = InlineKeyboardButton(text="📋 Copy Contract Address", copy_text=CopyTextButton(text=ca))
    except Exception:
        copy_btn = InlineKeyboardButton(text="📋 Contract", url=f"https://dexscreener.com/solana/{ca}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [copy_btn],
        [InlineKeyboardButton(text="🌊 Dive Deeper", url=f"https://dexscreener.com/solana/{ca}")],
    ])


def kb_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌊 View Pod", callback_data="echo:pod"),
         InlineKeyboardButton(text="🔵 View Echoers", callback_data="echo:echoers")],
        [InlineKeyboardButton(text="🌀 Run Sonar", callback_data="echo:sonar"),
         InlineKeyboardButton(text="💧 Waves", callback_data="echo:waves")],
    ])


def kb_pod_links() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌊 Full Pod Rankings", callback_data="echo:pod"),
         InlineKeyboardButton(text="🔵 View Echoers", callback_data="echo:echoers")],
    ])


# ── Message builders ────────────────────────────────────────────────────────
def sonar_report(label: str, mc: float, pct: float, quality: str) -> str:
    """The cross-group consensus Entry alert."""
    return box("SONAR REPORT", [
        f"Entry on {label}",
        f"Market Cap: {fmt_mc(mc)}",
        "",
        f"Signal Strength: {int(pct)}% of chats",
        f"Quality: {quality}",
    ])


def sonar_pulse(label: str, x: int) -> str:
    """A milestone follow-up (5x / 10x / ...)."""
    return box("SONAR PULSE", [
        label,
        f"🌊 {x}x from initial call",
    ])


def pod_status(n_signals: int, avg_x: float) -> str:
    return box("POD STATUS", [
        f"Current Signals Detected: {n_signals}",
        f"Top Pod Performance: +{avg_x:.1f}x avg",
    ])


def pod_rankings(groups: list) -> str:
    lines = []
    for i, g in enumerate(groups, 1):
        name = (g.chat_title or str(g.chat_id))[:22]
        lines.append(f"{i}. {name} — {g.points:.0f} pts ({g.wins}W/{g.losses}L)")
    return box("POD RANKINGS", lines or ["The waters are still."])


def echoers(users: list) -> str:
    lines = []
    for i, u in enumerate(users, 1):
        name = f"@{u.username}" if u.username else str(u.user_id)
        lines.append(f"{i}. {name[:22]} — {u.points:.0f} pts ({u.wins}W/{u.losses}L)")
    return box("ECHOERS", lines or ["No echoers yet."])


def sonar_sweep(active: list) -> str:
    """active: list of (label, ath_mult, quality)."""
    lines = [f"Active Signals: {len(active)}", ""]
    for label, mult, quality in active[:12]:
        lines.append(f"{label[:18]} — {mult:.1f}x · {quality.replace(' Quality Signal','')}")
    if not active:
        lines = ["Active Signals: 0", "", "Sonar quiet. The pod waits."]
    return box("SONAR SWEEP", lines)


def dive_menu(n_signals: int) -> str:
    return box("DIVE", [
        "The pod listens. The pod remembers.",
        "",
        f"Signals on sonar: {n_signals}",
        "",
        "Choose your depth.",
    ])


def waves_help() -> str:
    return box("WAVES", [
        "/dive    — surface the main menu",
        "/pod     — pod rankings & points",
        "/echoers — top echoers & points",
        "/sonar   — sweep current signals",
        "/waves   — this screen",
    ])
