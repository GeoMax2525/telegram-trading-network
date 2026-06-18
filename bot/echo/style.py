"""
style.py — Echo's Sega Genesis / Ecco the Dolphin (1992) message aesthetic.

Every Echo message is a retro "game screen": a monospace code-block box, short
structured lines, bold numbers, heavy blue/ocean emoji, premium + mysterious
tone (never childish). All public-facing copy is built here so the look stays
consistent across alerts, milestones, and the themed menus.
"""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def box(title: str, lines: list[str]) -> str:
    """Wrap content in the retro game-screen box: header, separator, content."""
    body = "\n".join([f"🐬 ECCO — {title} 🐬", SEP, "", *lines])
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
def sonar_report(label: str, mc: float, pct: float, quality: str,
                 pod_strength: int, rank: tuple | None = None) -> str:
    """The cross-group consensus Entry alert. `rank` is the receiving group's
    own (position, total) on the pod leaderboard — personalized per group."""
    lines = [
        f"Entry on {label}",
        f"Market Cap: {fmt_mc(mc)}",
        "",
        f"Signal Strength: {int(pct)}% of chats",
        f"Pod Strength: {pod_strength} pods",
        f"Quality: {quality}",
    ]
    if rank:
        r, total = rank
        lines += ["", f"Your Pod Rank: #{r} of {total}"]
    return box("SONAR REPORT", lines)


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


def pod_screen(groups: list, own: dict | None = None, total: int = 0) -> str:
    """PUBLIC pod screen for groups: the network size + leaderboard + (if run in
    a group) that group's own stats. The ONLY cross-group data a chat may see."""
    lines = [f"🌊 ECCO Network: {total} groups", ""]
    if groups:
        for i, g in enumerate(groups, 1):
            lines.append(f"{i}. {(g.chat_title or str(g.chat_id))[:24]}")
            lines.append(f"   {g.wins}W / {g.losses}L ({_wr(g.wins, g.losses)})   {g.points:+.0f} pts")
            lines.append("")
    else:
        lines = ["The waters are still.", ""]
    if own:
        lines += [
            "🔵 YOUR POD",
            own["title"][:24],
            f"Rank: #{own['rank']} of {own['total']}",
            f"Record: {own['wins']}W / {own['losses']}L ({_wr(own['wins'], own['losses'])})   {own['points']:+.0f} pts",
        ]
        if own["top_echoer"]:
            nm, w = own["top_echoer"]
            lines.append(f"Top Echoer: {_handle(nm)} ({w}W)")
    return box("POD RANKINGS", lines)


def pod_rankings(groups: list) -> str:
    lines = []
    for i, g in enumerate(groups, 1):
        lines.append(f"{i}. {(g.chat_title or str(g.chat_id))[:24]}")
        lines.append(f"   {g.wins}W / {g.losses}L ({_wr(g.wins, g.losses)})   {g.points:+.0f} pts")
        lines.append("")
    return box("POD RANKINGS", lines or ["The waters are still."])


def echoers(users: list) -> str:
    lines = []
    for i, u in enumerate(users, 1):
        tag = f"{i}. {_handle(u.username or str(u.user_id))[:16]}"
        lines.append(f"{tag:<20}{u.wins}W / {u.losses}L ({_wr(u.wins, u.losses)})   {u.points:+.0f} pts")
    return box("ECHOERS", lines or ["No echoers yet."])


def sonar_sweep(active: list) -> str:
    """active: list of (label, ath_mult, quality)."""
    lines = [f"Active Signals: {len(active)}", ""]
    for label, mult, quality in active[:12]:
        lines.append(f"{label[:18]} — {mult:.1f}x · {quality.replace(' Quality Signal','')}")
    if not active:
        lines = ["Active Signals: 0", "", "Sonar quiet. The pod waits."]
    return box("SONAR SWEEP", lines)


SEP = "━" * 30


def _wr(w: int, l: int) -> str:
    t = w + l
    return f"{round(100 * w / t)}%" if t else "—"


def _num(n: int) -> str:
    return f"{int(n):,}"


def _handle(name: str) -> str:
    return name if str(name).lstrip("-").isdigit() else f"@{name}"


def hub_dashboard(st: dict, footer: str = "") -> str:
    """The intelligence dashboard — premium retro sonar-console layout. The
    whole report is one monospace box (so columns + separators align); the
    footer sits outside so its commands stay tappable."""
    L = [
        "🐬 ECCO — INTELLIGENCE DASHBOARD 🐬",
        "",
        SEP,
        "     EDGE CONSENSUS CRYPTO ORACLE",
        SEP,
        "",
        "📡 POD STATUS",
        f"Groups: {st['n_groups']:<11}Echoers: {st['n_users']}",
        f"Sightings: {_num(st['n_sightings']):<8}Signals: {st['n_signals']}",
        f"Record: {st['n_wins']}W / {st['n_losses']}L     Win Rate: {_wr(st['n_wins'], st['n_losses'])}",
        "",
        "🏆 TOP GROUPS",
    ]
    if st["top_groups"]:
        for i, g in enumerate(st["top_groups"], 1):
            L.append(f"{i}. {g['title'][:24]}")
            L.append(f"   Record: {g['wins']}W / {g['losses']}L ({_wr(g['wins'], g['losses'])})   {g['points']:+.0f} pts")
            if g["top_echoer"]:
                nm, w = g["top_echoer"]
                L.append(f"   Top Echoer: {_handle(nm)} ({w}W)")
            L.append("")
    else:
        L += ["Clicking through the waves…", "(no pods yet)", ""]
    L.append("🎯 TOP ECHOERS")
    if st["top_users"]:
        for i, u in enumerate(st["top_users"], 1):
            tag = f"{i}. {_handle(u['name'])[:16]}"
            L.append(f"{tag:<20}{u['wins']}W / {u['losses']}L ({_wr(u['wins'], u['losses'])})   {u['points']:+.0f} pts")
    else:
        L.append("(no echoers yet)")
    L += ["", "📡 RECENT SIGNALS"]
    if st["recent"]:
        for r in st["recent"]:
            L.append(f"• {r['name'][:18]} — {r['quality']} — {r['mult']:.1f}x")
    else:
        L.append("• Sonar quiet")
    out = "```\n" + "\n".join(L) + "\n```"
    if footer:
        out += f"\n{footer}"
    return out


def dive_menu(n_signals: int) -> str:
    return box("DIVE", [
        "Edge Consensus Crypto Oracle",
        "",
        "The pod listens. The pod remembers.",
        "",
        f"Signals on sonar: {n_signals}",
        "",
        "Choose your depth.",
    ])


def shill(ref_link: str) -> str:
    """A forwardable recruit-a-group promo with the sharer's referral link baked
    in. Plain message (not a code block) so it reads as a clean post + the link
    stays tappable when pasted/forwarded into other groups."""
    return (
        "🐬 *ECCO* — Edge Consensus Crypto Oracle 🌊\n\n"
        "A *free* multi-group signal bot that cuts through the noise.\n\n"
        "*HOW IT WORKS*\n"
        "ECCO quietly monitors contract calls across many alpha groups. Instead "
        "of posting every random shill, it only fires when the *same CA is called "
        "across 4+ groups* — confirming real momentum before it ever posts.\n\n"
        "Every *SONAR REPORT* includes:\n"
        "📊 Current market cap\n"
        "📈 % of monitored groups already on the call\n"
        "⭐️ Quality rating (High / Medium) from the historical strength of the calling groups\n"
        "📋 One-click CA copy\n"
        "🚀 Live runner updates — *5x, 10x, 25x from the call*\n\n"
        "*WHY IT WORKS*\n"
        "Any single caller can be wrong. But when smart money across many "
        "independent groups converges on the *same coin at the same time*, that's "
        "real conviction — the strongest signal in the game. ECCO measures that "
        "consensus and grades it.\n\n"
        "*WHY ADD ECCO TO YOUR GROUP*\n"
        "🔵 *Free, high-conviction alpha* — members see cross-group consensus, far stronger than any single call\n"
        "🔵 *Near-zero spam* — only fires on 4+ group confirmation, won't annoy your members\n"
        "🔵 *Status + competition* — your group is ranked on the Pod Leaderboard, your best callers climb\n"
        "🔵 *Rug protection* — every signal is on-chain rug-filtered first\n"
        "🔵 *Stickier group* — real value keeps members active\n"
        "🔵 *Costs nothing* — just add ECCO as admin\n\n"
        "ECCO is completely *FREE* and built to benefit every community it joins. "
        "The larger the network grows, the *stronger and smarter the signals "
        "become for everyone.*\n\n"
        "🤝 Share ECCO and reach out to communities you're in — adding it as admin "
        "costs nothing and instantly benefits the chat.\n\n"
        f"➕ *Add ECCO to your group:* {ref_link}"
    )


def rank_screen(echoer: dict, referral: dict) -> str:
    lines = ["🎯 ECHOER (your calls)"]
    if echoer["rank"]:
        lines += [
            f"Rank: #{echoer['rank']} of {echoer['total']}",
            f"Record: {echoer['wins']}W / {echoer['losses']}L ({_wr(echoer['wins'], echoer['losses'])})",
            f"Points: {echoer['points']:+.0f}",
        ]
    else:
        lines.append("No calls tracked yet.")
    lines += ["", "🤝 REFERRALS (groups you brought)"]
    if referral["rank"]:
        lines += [
            f"Rank: #{referral['rank']} of {referral['total_referrers']}",
            f"Qualified groups: {referral['qualified_groups']}",
            f"Total added: {referral['total_groups']}",
        ]
    else:
        lines += ["Not ranked yet.",
                  f"Groups added: {referral['total_groups']}"]
    return box("YOUR RANK", lines)


def welcome() -> str:
    return box("WELCOME", [
        "Edge Consensus Crypto Oracle",
        "",
        "The pod hears every call across the depths.",
        "Add ECCO to your groups to earn rewards —",
        "and share your link so others do too.",
    ])


def referral_screen(stats: dict) -> str:
    lines = [
        "Spread the pod. Earn rewards.",
        "",
        f"Qualified groups: {stats['qualified_groups']}",
        f"Total groups added: {stats['total_groups']}",
    ]
    if stats["rank"]:
        lines.append(f"Referral Rank: #{stats['rank']} of {stats['total_referrers']}")
    lines += ["", "A group counts only with real members + activity.",
              "Credit goes to whoever referred the adder.",
              "", "💎 FUTURE rewards for TOP referrers.",
              "Hold $REVOLT to be eligible for payout."]
    return box("REFERRAL", lines)


def waves_help() -> str:
    return box("WAVES", [
        "Edge Consensus Crypto Oracle",
        "",
        "/dive    — surface the main menu",
        "/pod     — pod rankings & points",
        "/echoers — top echoers & points",
        "/sonar   — sweep current signals",
        "/waves   — this screen",
    ])
