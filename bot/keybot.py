"""
keybot.py — KeyBot settings menu and simulated buy execution.

Phase 1: UI + preset storage only (no real on-chain trading).
"""

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.trading import SOL_MINT, get_jupiter_quote, get_token_balance, execute_swap
from bot.wallet import get_keypair, get_wallet_address, get_sol_balance
from database.models import get_keybot_settings, upsert_keybot_settings

logger = logging.getLogger(__name__)
router = Router()


# ── FSM states ────────────────────────────────────────────────────────────────

class KeyBotStates(StatesGroup):
    waiting_for_wallet = State()


# ── Keyboard builders ─────────────────────────────────────────────────────────

def _main_keyboard(s) -> InlineKeyboardMarkup:
    sol = s.buy_amount_sol if s else 0.5
    tp  = s.take_profit_x  if s else 3.0
    sl  = s.stop_loss_pct  if s else 30.0
    w   = s.wallet_address  if s else None
    w_label = f"👛 Wallet: {w[:6]}…{w[-4:]}" if w else "👛 Wallet Address: Not set"

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f"💰 Buy Amount: {sol} SOL", callback_data="kb:buy_amount"
    ))
    builder.row(InlineKeyboardButton(
        text=f"🎯 Take Profit: {tp}x", callback_data="kb:take_profit"
    ))
    builder.row(InlineKeyboardButton(
        text=f"🛑 Stop Loss: {sl}%", callback_data="kb:stop_loss"
    ))
    builder.row(InlineKeyboardButton(
        text=w_label, callback_data="kb:wallet"
    ))
    builder.row(InlineKeyboardButton(
        text="📊 Open Positions: 0", callback_data="kb:positions"
    ))
    builder.row(InlineKeyboardButton(
        text="❌ Close", callback_data="kb:close"
    ))
    return builder.as_markup()


def _buy_amount_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for amt in (0.1, 0.25, 0.5, 1.0, 2.0):
        builder.button(text=f"{amt} SOL", callback_data=f"kb:set_buy:{amt}")
    builder.adjust(3, 2)
    builder.row(InlineKeyboardButton(text="⬅️ Back", callback_data="kb:menu"))
    return builder.as_markup()


def _take_profit_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for x in (2, 3, 5, 10):
        builder.button(text=f"{x}x", callback_data=f"kb:set_tp:{x}")
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="⬅️ Back", callback_data="kb:menu"))
    return builder.as_markup()


def _stop_loss_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for pct in (10, 20, 30, 50):
        builder.button(text=f"{pct}%", callback_data=f"kb:set_sl:{pct}")
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="⬅️ Back", callback_data="kb:menu"))
    return builder.as_markup()


def _wallet_input_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="⬅️ Back", callback_data="kb:menu"))
    return builder.as_markup()


def _wallet_options_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✏️ Change Wallet",   callback_data="kb:change_wallet"))
    builder.row(InlineKeyboardButton(text="🗑️ Remove Wallet",   callback_data="kb:confirm_remove"))
    builder.row(InlineKeyboardButton(text="⬅️ Back",            callback_data="kb:menu"))
    return builder.as_markup()


def _confirm_remove_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Yes, Remove", callback_data="kb:remove_wallet"),
        InlineKeyboardButton(text="❌ No, Keep it", callback_data="kb:menu"),
    )
    return builder.as_markup()


# ── Menu text ─────────────────────────────────────────────────────────────────

def _menu_text(s, balance: float | None = None) -> str:
    sol = s.buy_amount_sol if s else 0.5
    tp  = s.take_profit_x  if s else 3.0
    sl  = s.stop_loss_pct  if s else 30.0
    w   = s.wallet_address  if s else None

    if w:
        short = f"{w[:6]}…{w[-4:]}"
        bal   = f" | `{balance:.4f} SOL`" if balance is not None else ""
        w_str = f"`{short}`{bal}"
    else:
        w_str = "_Not set_"

    return (
        "⚡ *KEY BOT SETTINGS*\n\n"
        f"💰 Buy Amount:       `{sol} SOL`\n"
        f"🎯 Take Profit:      `{tp}x`\n"
        f"🛑 Stop Loss:        `{sl}%`\n"
        f"👛 Wallet:           {w_str}\n"
        f"📊 Open Positions:   `0`\n"
    )


async def _build_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """Fetches settings + balance and returns (menu_text, keyboard)."""
    s = await get_keybot_settings(user_id)

    # Auto-populate wallet from env WALLET_PRIVATE_KEY if not already set
    if not (s and s.wallet_address):
        addr = get_wallet_address()
        if addr:
            s = await upsert_keybot_settings(user_id, wallet_address=addr)

    balance = await get_sol_balance(s.wallet_address) if (s and s.wallet_address) else None
    return _menu_text(s, balance), _main_keyboard(s)


# ── /keybot command ───────────────────────────────────────────────────────────

@router.message(Command("keybot"))
async def cmd_keybot(message: Message, state: FSMContext):
    await state.clear()
    text, keyboard = await _build_menu(message.from_user.id)
    await message.reply(text, parse_mode="Markdown", reply_markup=keyboard)


# ── Settings callbacks ────────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kb:"))
async def cb_keybot(callback: CallbackQuery, state: FSMContext):
    action  = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    if action == "menu":
        await state.clear()
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer()

    elif action == "buy_amount":
        await callback.message.edit_text(
            "💰 *Buy Amount*\nHow much SOL to spend per trade:",
            parse_mode="Markdown", reply_markup=_buy_amount_keyboard(),
        )
        await callback.answer()

    elif action == "take_profit":
        await callback.message.edit_text(
            "🎯 *Take Profit*\nAuto-sell when the token hits this multiplier:",
            parse_mode="Markdown", reply_markup=_take_profit_keyboard(),
        )
        await callback.answer()

    elif action == "stop_loss":
        await callback.message.edit_text(
            "🛑 *Stop Loss*\nAuto-sell if the token drops this % from entry:",
            parse_mode="Markdown", reply_markup=_stop_loss_keyboard(),
        )
        await callback.answer()

    elif action == "wallet":
        s = await get_keybot_settings(user_id)
        if s and s.wallet_address:
            await callback.message.edit_text(
                "👛 *Wallet Address*\n\nWhat would you like to do?",
                parse_mode="Markdown", reply_markup=_wallet_options_keyboard(),
            )
        else:
            await state.set_state(KeyBotStates.waiting_for_wallet)
            await callback.message.edit_text(
                "👛 *Wallet Address*\n\nType your Solana wallet address and send it:",
                parse_mode="Markdown", reply_markup=_wallet_input_keyboard(),
            )
        await callback.answer()

    elif action == "change_wallet":
        await state.set_state(KeyBotStates.waiting_for_wallet)
        await callback.message.edit_text(
            "👛 *Change Wallet Address*\n\nType your new Solana wallet address and send it:",
            parse_mode="Markdown", reply_markup=_wallet_input_keyboard(),
        )
        await callback.answer()

    elif action == "confirm_remove":
        await callback.message.edit_text(
            "🗑️ *Remove Wallet*\n\nAre you sure you want to remove your wallet address?",
            parse_mode="Markdown", reply_markup=_confirm_remove_keyboard(),
        )
        await callback.answer()

    elif action == "remove_wallet":
        await upsert_keybot_settings(user_id, wallet_address=None)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer("🗑️ Wallet removed")

    elif action == "positions":
        await callback.answer("📊 No open positions yet — coming in Phase 2!", show_alert=True)

    elif action == "close":
        await state.clear()
        await callback.message.delete()
        await callback.answer()

    elif action.startswith("set_buy:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, buy_amount_sol=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer(f"✅ Buy amount set to {val} SOL")

    elif action.startswith("set_tp:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, take_profit_x=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer(f"✅ Take profit set to {val}x")

    elif action.startswith("set_sl:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, stop_loss_pct=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
        await callback.answer(f"✅ Stop loss set to {val}%")

    else:
        await callback.answer()


# ── FSM: wallet address input ─────────────────────────────────────────────────

@router.message(KeyBotStates.waiting_for_wallet)
async def receive_wallet(message: Message, state: FSMContext):
    wallet = (message.text or "").strip()
    if wallet.startswith("/"):
        await state.clear()
        return
    if not (32 <= len(wallet) <= 44) or " " in wallet:
        await message.reply(
            "⚠️ Invalid Solana address. Please try again or tap ⬅️ Back.",
            parse_mode="Markdown",
            reply_markup=_wallet_input_keyboard(),
        )
        return
    await upsert_keybot_settings(message.from_user.id, wallet_address=wallet)
    await state.clear()
    text, keyboard = await _build_menu(message.from_user.id)
    await message.reply(
        "✅ *Wallet saved!*\n\n" + text,
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


# ── ⚡ KeyBot Buy callback (on Trade Cards) ───────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kbbuy:"))
async def cb_keybot_buy(callback: CallbackQuery):
    user_id  = callback.from_user.id
    address  = callback.data.split(":", 1)[1]

    # Validate settings
    s = await get_keybot_settings(user_id)
    if s is None or not s.buy_amount_sol:
        await callback.answer("⚙️ Set up KeyBot first with /keybot", show_alert=True)
        return

    keypair = get_keypair()
    if keypair is None:
        await callback.answer(
            "❌ WALLET_PRIVATE_KEY not configured on the server.", show_alert=True
        )
        return

    # Parse token name from the Trade Card text
    token_name = address[:8] + "…"
    if callback.message and callback.message.text:
        for line in callback.message.text.splitlines():
            if line.startswith("🪙"):
                token_name = line.split("*")[1] if "*" in line else token_name
                break

    await callback.answer("⚡ Executing swap…")
    status_msg = await callback.message.reply("⏳ Getting Jupiter quote…")

    try:
        amount_lamports = int(s.buy_amount_sol * 1_000_000_000)

        # Get best route
        quote = await get_jupiter_quote(address, amount_lamports)
        price_impact = float(quote.get("priceImpactPct", 0))

        # Sign & broadcast
        await status_msg.edit_text("⏳ Signing and sending transaction…")
        signature = await execute_swap(quote, keypair)

        await status_msg.edit_text(
            f"✅ *Swap Executed!*\n\n"
            f"🪙 Token:          `{token_name}`\n"
            f"💰 Spent:          `{s.buy_amount_sol} SOL`\n"
            f"📊 Price Impact:   `{price_impact:.2f}%`\n"
            f"🎯 Take Profit:    `{s.take_profit_x}x`\n"
            f"🛑 Stop Loss:      `-{s.stop_loss_pct}%`\n\n"
            f"🔗 [View on Solscan](https://solscan.io/tx/{signature})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    except Exception as exc:
        logger.error("Swap failed for %s: %s", address, exc)
        await status_msg.edit_text(
            f"❌ *Swap Failed*\n\n`{str(exc)[:300]}`",
            parse_mode="Markdown",
        )


# ── 💸 KeyBot Sell callback (on Trade Cards) ─────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kbsell:"))
async def cb_keybot_sell(callback: CallbackQuery):
    user_id = callback.from_user.id
    address = callback.data.split(":", 1)[1]

    keypair = get_keypair()
    if keypair is None:
        await callback.answer(
            "❌ WALLET_PRIVATE_KEY not configured on the server.", show_alert=True
        )
        return

    wallet_address = str(keypair.pubkey())

    # Parse token name from Trade Card
    token_name = address[:8] + "…"
    if callback.message and callback.message.text:
        for line in callback.message.text.splitlines():
            if line.startswith("🪙"):
                token_name = line.split("*")[1] if "*" in line else token_name
                break

    await callback.answer("💸 Checking balance…")
    status_msg = await callback.message.reply("⏳ Checking token balance…")

    try:
        # Check how many tokens we hold
        token_amount = await get_token_balance(wallet_address, address)
        if token_amount == 0:
            await status_msg.edit_text(
                f"📭 *No tokens to sell*\n\nWallet holds 0 of `{token_name}`.",
                parse_mode="Markdown",
            )
            return

        # Get sell quote: token → SOL (100% of balance)
        await status_msg.edit_text("⏳ Getting Jupiter sell quote…")
        quote = await get_jupiter_quote(
            output_mint=SOL_MINT,
            amount=token_amount,
            input_mint=address,
        )
        price_impact  = float(quote.get("priceImpactPct", 0))
        out_lamports  = int(quote.get("outAmount", 0))
        sol_received  = round(out_lamports / 1_000_000_000, 4)

        # Sign & broadcast
        await status_msg.edit_text("⏳ Signing and sending transaction…")
        signature = await execute_swap(quote, keypair)

        await status_msg.edit_text(
            f"✅ *Sell Executed!*\n\n"
            f"🪙 Token:          `{token_name}`\n"
            f"📦 Sold:           `100% of holdings`\n"
            f"💰 SOL Received:   `~{sol_received} SOL`\n"
            f"📊 Price Impact:   `{price_impact:.2f}%`\n\n"
            f"🔗 [View on Solscan](https://solscan.io/tx/{signature})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    except Exception as exc:
        logger.error("Sell failed for %s: %s", address, exc)
        await status_msg.edit_text(
            f"❌ *Sell Failed*\n\n`{str(exc)[:300]}`",
            parse_mode="Markdown",
        )
