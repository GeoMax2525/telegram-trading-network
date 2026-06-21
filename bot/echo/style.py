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
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="echo:dive")],
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
                 pod_strength: int, rank: tuple | None = None,
                 surge: bool = False) -> str:
    """The cross-group consensus Entry alert. `rank` is the receiving group's
    own (position, total) on the pod leaderboard — personalized per group.
    `surge` = a re-fire as the pod count climbs into a higher tier."""
    lines = [
        (f"🔥 Surging — now {pod_strength} pods on {label}" if surge
         else f"Entry on {label}"),
        f"Market Cap: {fmt_mc(mc)}",
        "",
        f"Signal Strength: {int(pct)}% of chats",
        f"Pod Strength: {pod_strength} pods",
        f"Quality: {quality}",
    ]
    if rank:
        r, total = rank
        lines += ["", f"Your Pod Rank: #{r} of {total}"]
    return box("SONAR SURGE" if surge else "SONAR REPORT", lines)


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
            nm, w, l = own["top_echoer"]
            lines.append(f"Top Echoer: {_handle(nm)} ({w}W/{l}L)")
    return box("POD RANKINGS", lines)


def _caller_row(i: int, u: dict, show_rugs: bool = False) -> str:
    name = _handle((u.get("username") or str(u["user_id"]))[:14])
    calls = u["calls"]
    if show_rugs:
        rugs = u["rugs"]
        rug_rate = f"{round(100*rugs/calls)}%" if calls else "—"
        return f"{i:>2}. {name:<16} {rugs}💀 / {calls} calls ({rug_rate} rug rate)"
    pts_sign = "+" if u.get("pts", 0) >= 0 else ""
    return (f"{i:>2}. {name:<16} {u['wins']}W/{u['losses']}L"
            f"  avg {u['avg_x']:.1f}x")


def rugs_screen(callers: list, group_title: str = "") -> str:
    """Top rug callers in a group (or across the network)."""
    header = f"TOP RUG CALLERS — {group_title[:20]}" if group_title else "TOP RUG CALLERS (NETWORK)"
    if not callers:
        return box(header, ["No rugs recorded yet."])
    lines = []
    for i, u in enumerate(callers[:10], 1):
        lines.append(_caller_row(i, u, show_rugs=True))
        non_rug_xs = []  # avg X excluding rugs — shows if they're still catching runners
        lines[-1] += f"  avg {u['avg_x']:.1f}x"
    return box(header, lines)


def losers_screen(callers: list, group_title: str = "") -> str:
    """Worst performers in a group (or across the network)."""
    header = f"BOTTOM PERFORMERS — {group_title[:20]}" if group_title else "BOTTOM PERFORMERS (NETWORK)"
    if not callers:
        return box(header, ["No resolved calls yet."])
    lines = []
    for i, u in enumerate(callers[:10], 1):
        total = u["calls"]
        loss_rate = f"{round(100*(u['losses'])/total)}%" if total else "—"
        name = _handle((u.get("username") or str(u["user_id"]))[:14])
        lines.append(
            f"{i:>2}. {name:<16} {u['wins']}W/{u['losses']}L"
            f"  avg {u['avg_x']:.1f}x  ({loss_rate} loss rate)"
        )
    return box(header, lines)


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
            L.append(f"   {g['wins']}W / {g['losses']}L ({_wr(g['wins'], g['losses'])})  ·  avg {g.get('avg_x', 0):.1f}x  ·  {g['points']:+.0f} pts")
            if g["top_echoer"]:
                nm, w, l = g["top_echoer"]
                L.append(f"   Top Echoer: {_handle(nm)} ({w}W/{l}L)")
            L.append("")
    else:
        L += ["Clicking through the waves…", "(no pods yet)", ""]
    L.append("🎯 TOP ECHOERS")
    if st["top_users"]:
        for i, u in enumerate(st["top_users"], 1):
            tag = f"{i}. {_handle(u['name'])[:16]}"
            L.append(f"{tag:<20}{u['wins']}W/{u['losses']}L ({_wr(u['wins'], u['losses'])})  avg {u.get('avg_x', 0):.1f}x  {u['points']:+.0f}pts")
    else:
        L.append("(no echoers yet)")
    L.append("")
    L.append("💀 TOP RUGGERS")
    if st.get("top_ruggers"):
        for i, u in enumerate(st["top_ruggers"], 1):
            name = _handle((u.get("username") or str(u["user_id"]))[:14])
            rug_rate = f"{round(100*u['rugs']/u['calls'])}%" if u["calls"] else "—"
            L.append(f"{i}. {name:<16} {u['rugs']}💀 / {u['calls']} calls  {rug_rate}  avg {u['avg_x']:.1f}x")
    else:
        L.append("(no rugs yet)")
    L.append("")
    L.append("📉 WORST PERFORMERS")
    if st.get("top_losers"):
        for i, u in enumerate(st["top_losers"], 1):
            name = _handle((u.get("username") or str(u["user_id"]))[:14])
            loss_rate = f"{round(100*u['losses']/u['calls'])}%" if u["calls"] else "—"
            L.append(f"{i}. {name:<16} {u['wins']}W/{u['losses']}L  avg {u['avg_x']:.1f}x  ({loss_rate} loss rate)")
    else:
        L.append("(no losses yet)")
    L += ["", "📡 RECENT SIGNALS"]
    if st["recent"]:
        for r in st["recent"]:
            L.append(f"• {r['name'][:18]} — {r['quality']} — {r['mult']:.1f}x")
    else:
        L.append("• Sonar quiet")
    L += ["", "📞 RECENT CALLS",
          "🔵tracking  ✅win  ❌loss  💀rug"]
    from datetime import datetime, timezone
    now_ts = datetime.utcnow()
    if st.get("recent_calls"):
        for c in st["recent_calls"]:
            seen = c["seen_at"]
            if seen:
                secs = (now_ts - seen).total_seconds()
                m = secs / 60.0
                ago = (f"{int(m)}m ago" if m < 60 else
                       f"{int(m/60)}h ago" if m < 1440 else f"{int(m/1440)}d ago")
            else:
                ago = "?"
            # Status badge — shows if the call already resolved
            badge = {"win": "✅", "loss": "❌", "rug": "💀", "void": "⬜"}.get(c["status"], "🔵")
            L.append(f"{badge} {c['name'][:10]:<10} {c['group'][:14]:<14} {c['caller'][:12]:<12} {ago}")
    else:
        L.append("• No calls yet")
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


def shill(ref_link: str, contact: str = "") -> str:
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
        "*COMMANDS IN YOUR GROUP*\n"
        "📊 /pod — Pod rankings + your group's stats & top caller\n"
        "🚨 Alerts post *automatically* on 4+ group consensus — no command needed.\n\n"
        "ECCO is completely *FREE* and built to benefit every community it joins. "
        "The larger the network grows, the *stronger and smarter the signals "
        "become for everyone.*\n\n"
        "🤝 Share ECCO and reach out to communities you're in — adding it as admin "
        "costs nothing and instantly benefits the chat.\n\n"
        "💎 *Spread ECCO, earn rewards.*\n\n"
        f"➕ *Add ECCO / get your referral link:* {ref_link}\n\n"
        "ECCO is brought to you by the *$REVOLT ecosystem*."
        + (f" DM {contact} for any inquiries or questions." if contact else "")
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


def welcome(contact: str = "") -> str:
    lines = [
        "Edge Consensus Crypto Oracle",
        "",
        "The pod hears every call across the depths.",
        "",
        "💎 REFERRAL REWARDS — HOW IT WORKS",
        "1. Share your referral link (below).",
        "2. When someone opens ECCO through it",
        "   and adds it to a group as admin, that",
        "   group is credited to YOU.",
        "3. Groups count once they have real",
        "   members + activity — no fake groups.",
        "",
        "Top referrers earn FUTURE $REVOLT rewards.",
        "Hold $REVOLT to be eligible for payout.",
    ]
    if contact:
        lines += ["", f"📩 Questions: {contact}"]
    return box("WELCOME", lines)


def referral_screen(stats: dict, board: list) -> str:
    lines = ["🏆 TOP REFERRERS"]
    if board:
        for i, e in enumerate(board[:5], 1):
            who = _handle(e["username"] or str(e["user_id"]))
            lines.append(f"{i}. {who[:18]} — {e['groups']} grp ({e.get('qualified', 0)} qual)")
    else:
        lines.append("(be the first)")
    lines += ["", "🔵 YOUR STANDING",
              f"Qualified groups: {stats['qualified_groups']}",
              f"Total added: {stats['total_groups']}"]
    if stats["rank"]:
        lines.append(f"Rank: #{stats['rank']} of {stats['total_referrers']}")
    lines += ["", "A group counts with real members + activity.",
              "Credit goes to whoever referred the adder.",
              "", "💎 FUTURE rewards for TOP referrers.",
              "Hold $REVOLT to be eligible for payout."]
    return box("REFERRAL LEADERBOARD", lines)


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
