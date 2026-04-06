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

from bot.config import ADMIN_IDS
from database.models import get_keybot_settings, upsert_keybot_settings

logger = logging.getLogger(__name__)
router = Router()


# ── FSM states ────────────────────────────────────────────────────────────────

class KeyBotStates(StatesGroup):
    waiting_for_wallet = State()


# ── Keyboard builders ─────────────────────────────────────────────────────────

def _main_keyboard(s) -> InlineKeyboardMarkup:
    sol    = s.buy_amount_sol if s else 0.5
    tp     = s.take_profit_x  if s else 3.0
    sl     = s.stop_loss_pct  if s else 30.0
    w      = s.wallet_address  if s else None
    w_label = f"👛 {w[:6]}…{w[-4:]}" if w else "👛 Wallet: not set"

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=f"💰 Buy: {sol} SOL",  callback_data="kb:buy_amount"),
        InlineKeyboardButton(text=f"🎯 TP: {tp}x",       callback_data="kb:take_profit"),
    )
    builder.row(
        InlineKeyboardButton(text=f"🛑 SL: {sl}%",       callback_data="kb:stop_loss"),
        InlineKeyboardButton(text=w_label,                callback_data="kb:wallet"),
    )
    builder.row(
        InlineKeyboardButton(text="📊 Open Positions: 0", callback_data="kb:positions"),
    )
    builder.row(
        InlineKeyboardButton(text="❌ Close",              callback_data="kb:close"),
    )
    return builder.as_markup()


def _buy_amount_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for amt in (0.1, 0.25, 0.5, 1.0, 2.0):
        builder.button(text=f"{amt} SOL", callback_data=f"kb:set_buy:{amt}")
    builder.adjust(3, 2)
    builder.row(InlineKeyboardButton(text="← Back", callback_data="kb:menu"))
    return builder.as_markup()


def _take_profit_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for x in (2, 3, 5, 10):
        builder.button(text=f"{x}x", callback_data=f"kb:set_tp:{x}")
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="← Back", callback_data="kb:menu"))
    return builder.as_markup()


def _stop_loss_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for pct in (10, 20, 30, 50):
        builder.button(text=f"{pct}%", callback_data=f"kb:set_sl:{pct}")
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="← Back", callback_data="kb:menu"))
    return builder.as_markup()


# ── Menu text ─────────────────────────────────────────────────────────────────

def _menu_text(s) -> str:
    sol = s.buy_amount_sol if s else 0.5
    tp  = s.take_profit_x  if s else 3.0
    sl  = s.stop_loss_pct  if s else 30.0
    w   = s.wallet_address  if s else None
    w_str = f"`{w}`" if w else "_not set_"
    return (
        "⚡ *KeyBot Settings*\n\n"
        f"💰 Buy Amount:  `{sol} SOL`\n"
        f"🎯 Take Profit: `{tp}x`\n"
        f"🛑 Stop Loss:   `{sl}%`\n"
        f"👛 Wallet:      {w_str}\n"
        f"📊 Positions:   `0 open`\n"
    )


# ── /keybot command ───────────────────────────────────────────────────────────

@router.message(Command("keybot"))
async def cmd_keybot(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Admin only.")
        return
    await state.clear()
    s = await get_keybot_settings(message.from_user.id)
    await message.reply(_menu_text(s), parse_mode="Markdown", reply_markup=_main_keyboard(s))


# ── Settings callbacks ────────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kb:"))
async def cb_keybot(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Admin only.", show_alert=True)
        return

    action   = callback.data.split(":", 1)[1]
    admin_id = callback.from_user.id

    if action == "menu":
        await state.clear()
        s = await get_keybot_settings(admin_id)
        await callback.message.edit_text(
            _menu_text(s), parse_mode="Markdown", reply_markup=_main_keyboard(s)
        )
        await callback.answer()

    elif action == "buy_amount":
        await callback.message.edit_text(
            "💰 *Select Buy Amount*\nHow much SOL to spend per trade:",
            parse_mode="Markdown", reply_markup=_buy_amount_keyboard(),
        )
        await callback.answer()

    elif action == "take_profit":
        await callback.message.edit_text(
            "🎯 *Select Take Profit*\nAuto-sell when the token hits this multiplier:",
            parse_mode="Markdown", reply_markup=_take_profit_keyboard(),
        )
        await callback.answer()

    elif action == "stop_loss":
        await callback.message.edit_text(
            "🛑 *Select Stop Loss*\nAuto-sell if the token drops this % from entry:",
            parse_mode="Markdown", reply_markup=_stop_loss_keyboard(),
        )
        await callback.answer()

    elif action == "wallet":
        await state.set_state(KeyBotStates.waiting_for_wallet)
        await callback.message.edit_text(
            "👛 *Enter Wallet Address*\n\nSend your Solana wallet address:",
            parse_mode="Markdown",
        )
        await callback.answer()

    elif action == "positions":
        await callback.answer("📊 No open positions yet — coming in Phase 2!", show_alert=True)

    elif action == "close":
        await state.clear()
        await callback.message.delete()
        await callback.answer()

    elif action.startswith("set_buy:"):
        val = float(action.split(":", 1)[1])
        s = await upsert_keybot_settings(admin_id, buy_amount_sol=val)
        await callback.message.edit_text(
            _menu_text(s), parse_mode="Markdown", reply_markup=_main_keyboard(s)
        )
        await callback.answer(f"✅ Buy amount → {val} SOL")

    elif action.startswith("set_tp:"):
        val = float(action.split(":", 1)[1])
        s = await upsert_keybot_settings(admin_id, take_profit_x=val)
        await callback.message.edit_text(
            _menu_text(s), parse_mode="Markdown", reply_markup=_main_keyboard(s)
        )
        await callback.answer(f"✅ Take profit → {val}x")

    elif action.startswith("set_sl:"):
        val = float(action.split(":", 1)[1])
        s = await upsert_keybot_settings(admin_id, stop_loss_pct=val)
        await callback.message.edit_text(
            _menu_text(s), parse_mode="Markdown", reply_markup=_main_keyboard(s)
        )
        await callback.answer(f"✅ Stop loss → {val}%")

    else:
        await callback.answer()


# ── FSM: wallet address input ─────────────────────────────────────────────────

@router.message(KeyBotStates.waiting_for_wallet)
async def receive_wallet(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    wallet = (message.text or "").strip()
    if not (32 <= len(wallet) <= 44) or " " in wallet:
        await message.reply("⚠️ Invalid Solana address. Please try again or /keybot to cancel.")
        return
    s = await upsert_keybot_settings(message.from_user.id, wallet_address=wallet)
    await state.clear()
    await message.reply(
        "✅ Wallet saved!\n\n" + _menu_text(s),
        parse_mode="Markdown",
        reply_markup=_main_keyboard(s),
    )


# ── ⚡ KeyBot Buy callback (on Trade Cards) ───────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kbbuy:"))
async def cb_keybot_buy(callback: CallbackQuery):
    address  = callback.data.split(":", 1)[1]
    admin_id = callback.from_user.id

    if admin_id not in ADMIN_IDS:
        await callback.answer("⛔ KeyBot is for admins only.", show_alert=True)
        return

    s = await get_keybot_settings(admin_id)
    if s is None:
        await callback.answer("⚙️ Set up KeyBot first with /keybot", show_alert=True)
        return

    # Extract token name from the Trade Card message text
    token_name = "this token"
    if callback.message and callback.message.text:
        for line in callback.message.text.splitlines():
            if line.startswith("🪙"):
                token_name = line.split("*")[1] if "*" in line else token_name
                break

    await callback.answer()
    await callback.message.reply(
        f"⚡ *KeyBot Simulation*\n\n"
        f"📋 Token:       `{token_name}`\n"
        f"💰 Buy:         `{s.buy_amount_sol} SOL`\n"
        f"🎯 Take Profit: `{s.take_profit_x}x`\n"
        f"🛑 Stop Loss:   `-{s.stop_loss_pct}%`\n\n"
        f"_Phase 1 — simulation only. Real execution coming soon._",
        parse_mode="Markdown",
    )
