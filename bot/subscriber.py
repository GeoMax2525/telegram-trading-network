"""
subscriber.py — Subscriber management commands.

Admin commands:
    /adduser <telegram_id>    — activate a subscriber
    /removeuser <telegram_id> — suspend a subscriber
    /subscribers              — list all active subscribers

User commands (DM):
    /start        — register + generate wallet
    /mywallet     — show wallet address + balance
    /myperformance — show personal paper trading stats
"""

import json
import logging
import os

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.config import ADMIN_IDS

logger = logging.getLogger(__name__)
router = Router()


def _generate_wallet() -> tuple[str, str]:
    """Generate a new Solana wallet. Returns (public_key, private_key_base58)."""
    try:
        from solders.keypair import Keypair
        kp = Keypair()
        public = str(kp.pubkey())
        private = kp.to_base58_string()
        return public, private
    except ImportError:
        # Fallback if solders not available
        import secrets
        fake_pub = "REVOLT" + secrets.token_hex(14)
        fake_priv = secrets.token_hex(32)
        return fake_pub, fake_priv


# ── Admin: /adduser ──────────────────────────────────────────────────────────

@router.message(Command("adduser"))
async def cmd_adduser(message: Message):
    logger.info("ADDUSER called by user %s (admin check: %s in %s)",
                message.from_user.id, message.from_user.id, ADMIN_IDS)
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Not authorized.")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /adduser <telegram_id>")
        return

    try:
        tid = int(args[1])
    except ValueError:
        await message.reply("Invalid ID. Use the numeric Telegram ID.")
        return

    from database.models import get_subscriber, set_subscriber_status, create_subscriber

    sub = await get_subscriber(tid)
    if sub:
        await set_subscriber_status(tid, "active")
        await message.reply(f"Reactivated subscriber {tid}.")
    else:
        pub, priv = _generate_wallet()
        await create_subscriber(
            telegram_id=tid,
            username=None,
            wallet_address=pub,
            wallet_key_hash=priv,  # In production, encrypt this
        )
        await message.reply(
            f"Subscriber {tid} added.\n"
            f"Wallet: {pub}\n"
            f"Mode: paper (20 SOL)"
        )


# ── Admin: /removeuser ───────────────────────────────────────────────────────

@router.message(Command("removeuser"))
async def cmd_removeuser(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 2:
        await message.reply("Usage: /removeuser <telegram_id>")
        return

    try:
        tid = int(args[1])
    except ValueError:
        await message.reply("Invalid ID.")
        return

    from database.models import set_subscriber_status
    ok = await set_subscriber_status(tid, "suspended")
    if ok:
        await message.reply(f"Subscriber {tid} suspended.")
    else:
        await message.reply(f"Subscriber {tid} not found.")


# ── Admin: /subscribers ──────────────────────────────────────────────────────

@router.message(Command("subscribers"))
async def cmd_subscribers(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    from database.models import get_all_active_subscribers
    subs = await get_all_active_subscribers()

    if not subs:
        await message.reply("No active subscribers.")
        return

    lines = [f"<b>Active Subscribers ({len(subs)})</b>", ""]
    for s in subs:
        name = s.username or str(s.telegram_id)
        wallet = s.wallet_address[:8] + "..." if s.wallet_address else "none"
        lines.append(
            f"  {name} | {s.tier} | {s.trade_mode} | "
            f"bal={s.paper_balance:.1f} | wallet={wallet}"
        )

    await message.reply("\n".join(lines), parse_mode="HTML")


# ── User: /start (DM registration) ──────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start_dm(message: Message):
    """Register or show status. Works in DMs and groups."""

    uid = message.from_user.id
    username = message.from_user.username

    from database.models import get_subscriber, is_authorized

    # Check if already registered
    sub = await get_subscriber(uid)
    if sub:
        if sub.status == "active":
            await message.reply(
                f"Welcome back! You're already registered.\n\n"
                f"Wallet: <code>{sub.wallet_address}</code>\n"
                f"Mode: {sub.trade_mode}\n"
                f"Balance: {sub.paper_balance:.2f} SOL\n\n"
                f"Use /mywallet to see your wallet info.",
                parse_mode="HTML",
            )
        else:
            await message.reply(
                "Your account is suspended. Contact admin for reactivation."
            )
        return

    # Not registered — check if authorized (admin pre-approved)
    if not await is_authorized(uid):
        await message.reply(
            "Welcome to Revolt AI Trading.\n\n"
            "This bot requires a subscription. Contact the admin to get access."
        )
        return

    # Auto-register if authorized
    pub, priv = _generate_wallet()
    from database.models import create_subscriber
    await create_subscriber(
        telegram_id=uid,
        username=username,
        wallet_address=pub,
        wallet_key_hash=priv,
    )

    await message.reply(
        f"🔮 <b>Welcome to Revolt AI</b>\n\n"
        f"Your trading wallet has been created:\n"
        f"<code>{pub}</code>\n\n"
        f"⚠️ <b>Save your private key NOW</b> (shown once):\n"
        f"<tg-spoiler>{priv}</tg-spoiler>\n\n"
        f"Import this key into Phantom as a backup.\n\n"
        f"📋 You're in <b>paper trading</b> mode with 20 SOL.\n"
        f"The AI will start trading for you automatically.\n\n"
        f"Commands:\n"
        f"/mywallet — wallet info + balance\n"
        f"/myperformance — your trading stats\n"
        f"/keybot — trading settings",
        parse_mode="HTML",
    )

    logger.info("New subscriber registered: %s (%s) wallet=%s",
                username, uid, pub[:12])


# ── User: /mywallet ─────────────────────────────────────────────────────────

@router.message(Command("mywallet"))
async def cmd_mywallet(message: Message):
    if message.chat.type != "private":
        await message.reply("Use this command in DMs with the bot.")
        return

    from database.models import get_subscriber
    sub = await get_subscriber(message.from_user.id)
    if not sub:
        await message.reply("Not registered. Send /start first.")
        return

    # Get SOL balance if live
    balance_str = f"{sub.paper_balance:.2f} SOL (paper)"
    if sub.trade_mode == "live" and sub.wallet_address:
        try:
            from bot.wallet import get_sol_balance
            real_bal = await get_sol_balance(sub.wallet_address)
            if real_bal is not None:
                balance_str = f"{real_bal:.4f} SOL (live)"
        except Exception:
            pass

    await message.reply(
        f"👛 <b>Your Wallet</b>\n\n"
        f"Address: <code>{sub.wallet_address}</code>\n"
        f"Balance: {balance_str}\n"
        f"Mode: {sub.trade_mode}\n"
        f"Tier: {sub.tier}\n"
        f"Status: {sub.status}",
        parse_mode="HTML",
    )


# ── User: /myperformance ─────────────────────────────────────────────────────

@router.message(Command("myperformance"))
async def cmd_myperformance(message: Message):
    if message.chat.type != "private":
        await message.reply("Use this command in DMs with the bot.")
        return

    from database.models import get_subscriber
    sub = await get_subscriber(message.from_user.id)
    if not sub:
        await message.reply("Not registered. Send /start first.")
        return

    await message.reply(
        f"📊 <b>Your Performance</b>\n\n"
        f"Mode: {sub.trade_mode}\n"
        f"Balance: {sub.paper_balance:.2f} SOL\n"
        f"PnL: {sub.paper_pnl:+.2f} SOL\n\n"
        f"<i>Full per-trade stats coming soon.</i>",
        parse_mode="HTML",
    )
