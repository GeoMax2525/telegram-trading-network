"""
keybot.py — KeyBot settings menu and simulated buy execution.

Phase 1: UI + preset storage only (no real on-chain trading).
"""

import asyncio
import logging

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.config import CALLER_GROUP_ID, SCAN_TOPIC_ID
from bot.scanner import fetch_live_data, fetch_sol_price_usd
from bot.trading import SOL_MINT, get_ultra_order, get_token_balance, execute_ultra_order
from bot.wallet import get_keypair, get_wallet_address, get_sol_balance
from database.models import (
    get_keybot_settings, upsert_keybot_settings,
    open_position, close_position,
    get_open_positions, get_position_by_id, get_open_position_by_token,
    update_position_entry_mc, debug_all_positions,
)

logger = logging.getLogger(__name__)
router = Router()


# ── FSM states ────────────────────────────────────────────────────────────────

class KeyBotStates(StatesGroup):
    waiting_for_wallet     = State()
    waiting_for_buy_amount = State()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_mc(mc: float) -> str:
    if mc >= 1_000_000_000:
        return f"${mc / 1_000_000_000:.2f}B"
    if mc >= 1_000_000:
        return f"${mc / 1_000_000:.2f}M"
    if mc >= 1_000:
        return f"${mc / 1_000:.1f}K"
    return f"${mc:.0f}"


def _fmt_price(price: float) -> str:
    if price == 0:
        return "$0"
    if price >= 1:
        return f"${price:.2f}"
    if price >= 0.01:
        return f"${price:.4f}"
    if price >= 0.000001:
        return f"${price:.8f}".rstrip("0")
    return f"${price:.2e}"


def _fmt_pct(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}%"


# ── Keyboard builders ─────────────────────────────────────────────────────────

def _main_keyboard(s, positions: list = None) -> InlineKeyboardMarkup:
    sol     = s.buy_amount_sol
    tp      = s.take_profit_x
    sl      = s.stop_loss_pct
    w       = s.wallet_address
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
    # One Close button per open position
    for pos in (positions or []):
        label = pos.token_name[:20] if pos.token_name else pos.token_address[:10]
        builder.row(InlineKeyboardButton(
            text=f"🔫 Close {label}",
            callback_data=f"kbclose:{pos.id}",
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
    builder.row(InlineKeyboardButton(text="✏️ Custom", callback_data="kb:custom_buy"))
    builder.row(InlineKeyboardButton(text="⬅️ Back",   callback_data="kb:menu"))
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

def _menu_text(
    s,
    balance:       float | None = None,
    pos_live_data: list         = None,   # [(Position, live_dict | None), ...]
    sol_price_usd: float        = 0.0,
) -> str:
    sol = s.buy_amount_sol
    tp  = s.take_profit_x
    sl  = s.stop_loss_pct
    w   = s.wallet_address

    if w:
        short = f"{w[:6]}…{w[-4:]}"
        bal   = f" | {balance:.4f} SOL" if balance is not None else ""
        w_str = f"{short}{bal}"
    else:
        w_str = "Not set"

    lines = [
        "🔑 KEY BOT\n",
        f"💰 Buy Amount:  {sol} SOL",
        f"🎯 Take Profit: {tp}x",
        f"🛑 Stop Loss:   {sl}%",
        f"👛 Wallet:      {w_str}",
    ]

    if not pos_live_data:
        return "\n".join(lines)

    lines.append("\n📂 OPEN POSITIONS")
    total_pos_sol = 0.0

    for i, (pos, live) in enumerate(pos_live_data, 1):
        current_mc  = (live or {}).get("market_cap", 0) or 0
        price_usd   = (live or {}).get("price_usd",  0) or 0
        symbol      = (live or {}).get("symbol", pos.token_name or "???")
        pc          = (live or {}).get("price_changes", {})

        if pos.entry_mc and pos.entry_mc > 0 and current_mc > 0:
            ratio           = current_mc / pos.entry_mc
            current_val_sol = pos.amount_sol_spent * ratio
            profit_sol      = current_val_sol - pos.amount_sol_spent
            profit_pct      = (ratio - 1.0) * 100.0
        else:
            current_val_sol = pos.amount_sol_spent
            profit_sol      = 0.0
            profit_pct      = 0.0

        total_pos_sol   += current_val_sol
        current_val_usd  = current_val_sol * sol_price_usd if sol_price_usd else 0.0

        p_sign = "+" if profit_pct >= 0 else ""
        mc_str    = _fmt_mc(current_mc)    if current_mc else "N/A"
        price_str = _fmt_price(price_usd)  if price_usd  else "N/A"

        lines.append(
            f"/{i} ${symbol}\n"
            f"Profit: {p_sign}{profit_pct:.2f}% / {profit_sol:.4f} SOL\n"
            f"Value: ${current_val_usd:.2f} / {current_val_sol:.4f} SOL\n"
            f"Mcap: {mc_str} @ {price_str}\n"
            f"5m: {_fmt_pct(pc.get('m5', 0))}, "
            f"1h: {_fmt_pct(pc.get('h1', 0))}, "
            f"6h: {_fmt_pct(pc.get('h6', 0))}, "
            f"24h: {_fmt_pct(pc.get('h24', 0))}"
        )

    bal_str = f"{balance:.2f}" if balance is not None else "?"
    net_sol = (balance or 0.0) + total_pos_sol
    net_usd = net_sol * sol_price_usd if sol_price_usd else 0.0

    lines.append(f"\nBalance: {bal_str} SOL")
    lines.append(f"Net Worth: {net_sol:.2f} SOL / ${net_usd:.2f}")

    return "\n".join(lines)


async def _build_menu(user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    """
    Loads saved settings + open positions from DB and returns (menu_text, keyboard).

    - First open: creates a row with defaults (buy=0.5 SOL, tp=3x, sl=30%).
    - Subsequent opens: always reads saved values.
    - Open positions are shown inline with live MC fetched in parallel.
    """
    s = await get_keybot_settings(user_id)

    if s is None:
        env_wallet = get_wallet_address()
        kwargs = {}
        if env_wallet:
            kwargs["wallet_address"] = env_wallet
        s = await upsert_keybot_settings(user_id, **kwargs)
    elif not s.wallet_address:
        env_wallet = get_wallet_address()
        if env_wallet:
            s = await upsert_keybot_settings(user_id, wallet_address=env_wallet)

    # Fetch SOL balance + open positions concurrently
    gathered = await asyncio.gather(
        get_sol_balance(s.wallet_address) if s.wallet_address else asyncio.sleep(0),
        get_open_positions(user_id=user_id),
        return_exceptions=True,
    )
    balance_result, positions_result = gathered[0], gathered[1]

    balance = balance_result if isinstance(balance_result, float) else None

    if isinstance(positions_result, Exception):
        logger.error("_build_menu: get_open_positions failed for user %s: %s", user_id, positions_result)
        positions = []
    else:
        positions = positions_result

    logger.info("_build_menu: found %d open positions for user_id=%s", len(positions), user_id)

    # Fetch live data for every position + SOL price concurrently
    pos_live_data: list = []
    sol_price_usd: float = 0.0
    if positions:
        fetch_tasks = [fetch_live_data(pos.token_address) for pos in positions]
        fetch_tasks.append(fetch_sol_price_usd())
        all_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        live_results  = all_results[:-1]
        sol_result    = all_results[-1]
        sol_price_usd = sol_result if isinstance(sol_result, float) else 0.0

        for pos, live in zip(positions, live_results):
            live_dict = live if isinstance(live, dict) else None
            pos_live_data.append((pos, live_dict))
            logger.info(
                "_build_menu: pos id=%d %r entry_mc=%s current_mc=%s",
                pos.id, pos.token_name,
                f"{pos.entry_mc:.0f}" if pos.entry_mc else "None",
                f"{live_dict['market_cap']:.0f}" if live_dict else "None",
            )

    return _menu_text(s, balance, pos_live_data, sol_price_usd), _main_keyboard(s, positions)


# ── /keybot command ───────────────────────────────────────────────────────────

@router.message(Command("keybot"))
async def cmd_keybot(message: Message, state: FSMContext):
    await state.clear()
    await debug_all_positions()   # dump full positions table to Railway logs
    text, keyboard = await _build_menu(message.from_user.id)
    await message.reply(text, reply_markup=keyboard)


# ── Settings callbacks ────────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kb:"))
async def cb_keybot(callback: CallbackQuery, state: FSMContext):
    action  = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    if action == "menu":
        await state.clear()
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()

    elif action == "buy_amount":
        await callback.message.edit_text(
            "💰 *Buy Amount*\nHow much SOL to spend per trade:",
            parse_mode="Markdown", reply_markup=_buy_amount_keyboard(),
        )
        await callback.answer()

    elif action == "custom_buy":
        await state.set_state(KeyBotStates.waiting_for_buy_amount)
        await callback.message.edit_text(
            "✏️ *Custom Buy Amount*\n\nType the amount of SOL you want to spend per trade:\n_(e.g. 0.3 or 1.5)_",
            parse_mode="Markdown",
            reply_markup=_wallet_input_keyboard(),   # reuse the simple Back keyboard
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
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer("🗑️ Wallet removed")

    elif action == "close":
        await state.clear()
        await callback.message.delete()
        await callback.answer()

    elif action.startswith("set_buy:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, buy_amount_sol=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer(f"✅ Buy amount set to {val} SOL")

    elif action.startswith("set_tp:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, take_profit_x=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer(f"✅ Take profit set to {val}x")

    elif action.startswith("set_sl:"):
        val = float(action.split(":", 1)[1])
        await upsert_keybot_settings(user_id, stop_loss_pct=val)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, reply_markup=keyboard)
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
        "✅ Wallet saved!\n\n" + text,
        reply_markup=keyboard,
    )


# ── FSM: custom buy amount input ──────────────────────────────────────────────

@router.message(KeyBotStates.waiting_for_buy_amount)
async def receive_buy_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if raw.startswith("/"):
        await state.clear()
        return
    try:
        val = float(raw.replace(",", "."))
        if val <= 0:
            raise ValueError
    except ValueError:
        await message.reply(
            "⚠️ Please enter a valid number greater than 0 (e.g. `0.3` or `1.5`).",
            parse_mode="Markdown",
            reply_markup=_wallet_input_keyboard(),
        )
        return

    await upsert_keybot_settings(message.from_user.id, buy_amount_sol=val)
    await state.clear()
    text, keyboard = await _build_menu(message.from_user.id)
    await message.reply(
        f"✅ Buy amount set to {val} SOL\n\n" + text,
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

    # Parse token name + ticker from Trade Card line: 🪙 *NAME* ($SYMBOL)
    token_name = address[:8] + "…"
    if callback.message and callback.message.text:
        for line in callback.message.text.splitlines():
            if line.startswith("🪙"):
                parts = line.split("*")
                if len(parts) >= 3:
                    name   = parts[1]           # e.g. "ZECK"
                    ticker = parts[2].strip()   # e.g. "($ZECK)"
                    token_name = f"{name} {ticker}".strip()
                elif len(parts) >= 2:
                    token_name = parts[1]
                break

    await callback.answer("⚡ Executing swap…")
    status_msg = await callback.message.reply("⏳ Getting Jupiter quote…")

    try:
        amount_lamports = int(s.buy_amount_sol * 1_000_000_000)
        wallet_address  = str(keypair.pubkey())

        # Get Ultra order (quote + transaction in one step)
        order            = await get_ultra_order(address, amount_lamports, wallet_address)
        price_impact     = float(order.get("priceImpactPct", 0))
        tokens_received  = str(order.get("outAmount", ""))

        # Sign & submit
        await status_msg.edit_text("⏳ Signing and sending transaction…")
        signature = await execute_ultra_order(order, keypair)

        mc_str = ""
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
        return

    # Save position separately — DB failure must not hide a successful swap
    try:
        live        = await fetch_live_data(address)
        entry_mc    = (live["market_cap"] or None) if live else None
        entry_price = (live["price_usd"]  or None) if live else None
        logger.info("Saving position: user=%s token=%s sol=%.4f entry_mc=%s entry_price=%s tp=%.1fx sl=%.0f%%",
                    user_id, token_name, s.buy_amount_sol, entry_mc, entry_price,
                    s.take_profit_x, s.stop_loss_pct)
        await open_position(
            user_id=user_id,
            token_address=address,
            token_name=token_name,
            amount_sol_spent=s.buy_amount_sol,
            take_profit_x=s.take_profit_x,
            stop_loss_pct=s.stop_loss_pct,
            entry_price=entry_price,
            entry_mc=entry_mc,
            tokens_received=tokens_received,
        )
        logger.info("Position saved for %s", token_name)
    except Exception as db_exc:
        logger.error("Failed to save position for %s: %s", token_name, db_exc)


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

    # Parse token name + ticker from Trade Card line: 🪙 *NAME* ($SYMBOL)
    token_name = address[:8] + "…"
    if callback.message and callback.message.text:
        for line in callback.message.text.splitlines():
            if line.startswith("🪙"):
                parts = line.split("*")
                if len(parts) >= 3:
                    name   = parts[1]
                    ticker = parts[2].strip()
                    token_name = f"{name} {ticker}".strip()
                elif len(parts) >= 2:
                    token_name = parts[1]
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

        # Get Ultra sell order: token → SOL (100% of balance)
        await status_msg.edit_text("⏳ Getting Jupiter sell quote…")
        order = await get_ultra_order(
            output_mint=SOL_MINT,
            amount=token_amount,
            wallet_address=wallet_address,
            input_mint=address,
        )
        price_impact  = float(order.get("priceImpactPct", 0))
        out_lamports  = int(order.get("outAmount", 0))
        sol_received  = round(out_lamports / 1_000_000_000, 4)

        # Sign & submit
        await status_msg.edit_text("⏳ Signing and sending transaction…")
        signature = await execute_ultra_order(order, keypair)

        # Close open position if one exists for this token
        pnl_sol: float | None = None
        pos = await get_open_position_by_token(user_id, address)
        if pos:
            pnl_sol = round(sol_received - pos.amount_sol_spent, 4)
            await close_position(pos.id, "manual", pnl_sol)

        pnl_str = f"\n💹 PnL:            `{pnl_sol:+.4f} SOL`" if pnl_sol is not None else ""
        await status_msg.edit_text(
            f"✅ *Sell Executed!*\n\n"
            f"🪙 Token:          `{token_name}`\n"
            f"📦 Sold:           `100% of holdings`\n"
            f"💰 SOL Received:   `~{sol_received} SOL`\n"
            f"📊 Price Impact:   `{price_impact:.2f}%`"
            f"{pnl_str}\n\n"
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


# ── 🔴 Close Position callback (from Open Positions view) ────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("kbclose:"))
async def cb_close_position(callback: CallbackQuery):
    user_id     = callback.from_user.id
    position_id = int(callback.data.split(":", 1)[1])

    pos = await get_position_by_id(position_id)
    if pos is None or pos.status != "open":
        await callback.answer("Position already closed.", show_alert=True)
        return
    if pos.user_id != user_id:
        await callback.answer("Not your position.", show_alert=True)
        return

    keypair = get_keypair()
    if keypair is None:
        await callback.answer("❌ WALLET_PRIVATE_KEY not configured.", show_alert=True)
        return

    wallet_address = str(keypair.pubkey())
    await callback.answer("🔴 Closing position…")
    status_msg = await callback.message.reply(
        f"⏳ Closing position for *{pos.token_name}*…", parse_mode="Markdown"
    )

    try:
        token_amount = await get_token_balance(wallet_address, pos.token_address)
        if token_amount == 0:
            await close_position(pos.id, "manual", pnl_sol=None)
            await status_msg.edit_text(
                f"📭 *Position Closed*\n\nNo tokens found in wallet for `{pos.token_name}`.",
                parse_mode="Markdown",
            )
            return

        order        = await get_ultra_order(
            output_mint=SOL_MINT,
            amount=token_amount,
            wallet_address=wallet_address,
            input_mint=pos.token_address,
        )
        out_lamports = int(order.get("outAmount", 0))
        sol_received = round(out_lamports / 1_000_000_000, 4)
        signature    = await execute_ultra_order(order, keypair)

        pnl_sol = round(sol_received - pos.amount_sol_spent, 4)
        await close_position(pos.id, "manual", pnl_sol)

        pnl_emoji = "🟢" if pnl_sol >= 0 else "🔴"
        await status_msg.edit_text(
            f"✅ *Position Closed*\n\n"
            f"🪙 Token:          `{pos.token_name}`\n"
            f"💰 SOL Received:   `{sol_received} SOL`\n"
            f"{pnl_emoji} PnL:            `{pnl_sol:+.4f} SOL`\n\n"
            f"🔗 [View on Solscan](https://solscan.io/tx/{signature})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

        # Refresh the main menu (positions now shown inline)
        text, keyboard = await _build_menu(user_id)
        await callback.message.edit_text(text, reply_markup=keyboard)

    except Exception as exc:
        logger.error("Close position %d failed: %s", position_id, exc)
        await status_msg.edit_text(
            f"❌ *Close Failed*\n\n`{str(exc)[:300]}`", parse_mode="Markdown"
        )


# ── Background: position monitor ──────────────────────────────────────────────

async def position_monitor_loop(bot: Bot) -> None:
    """
    Every 5 minutes checks all open positions across all users:
    - If current MC >= entry_mc * take_profit_x → sell + close as tp_hit
    - If current MC <= entry_mc * (1 - stop_loss_pct/100) → sell + close as sl_hit
    Sends a notification to CALLER_GROUP_ID on auto-close.
    """
    await asyncio.sleep(60)   # startup delay
    logger.info("Position monitor started — checking every 5 minutes")
    while True:
        try:
            positions = await get_open_positions()   # all users
            logger.info("Position monitor tick: %d open position(s)", len(positions))
            keypair   = get_keypair()

            for pos in positions:
                try:
                    live = await fetch_live_data(pos.token_address)
                    if not live or not live.get("market_cap"):
                        logger.warning("Position %d (%s): no live data from DexScreener", pos.id, pos.token_name)
                        continue
                    current_mc = live["market_cap"]

                    # Patch entry_mc if it was 0/None at buy time (token not indexed yet)
                    if pos.entry_mc is None or pos.entry_mc <= 0:
                        logger.warning(
                            "Position %d (%s): entry_mc was %s — patching to current MC %s",
                            pos.id, pos.token_name, pos.entry_mc, _fmt_mc(current_mc),
                        )
                        await update_position_entry_mc(pos.id, current_mc)
                        pos.entry_mc = current_mc  # update in-memory so this tick proceeds

                    tp_mc  = pos.entry_mc * pos.take_profit_x
                    sl_mc  = pos.entry_mc * (1 - pos.stop_loss_pct / 100)
                    mult   = round(current_mc / pos.entry_mc, 2)
                    hit_tp = current_mc >= tp_mc
                    hit_sl = current_mc <= sl_mc

                    logger.info(
                        "Position %d (%s): entry_mc=%s current_mc=%s mult=%.2fx "
                        "| TP target=%s (hit=%s) SL target=%s (hit=%s)",
                        pos.id, pos.token_name,
                        _fmt_mc(pos.entry_mc), _fmt_mc(current_mc), mult,
                        _fmt_mc(tp_mc), hit_tp,
                        _fmt_mc(sl_mc), hit_sl,
                    )

                    if not (hit_tp or hit_sl):
                        continue

                    reason = "tp_hit" if hit_tp else "sl_hit"
                    emoji  = "🎯" if hit_tp else "🛑"
                    label  = "TAKE PROFIT HIT" if hit_tp else "STOP LOSS HIT"

                    mult       = round(current_mc / pos.entry_mc, 2)
                    entry_str  = _fmt_mc(pos.entry_mc)
                    exit_str   = _fmt_mc(current_mc)

                    # Auto-sell if server wallet is configured
                    if keypair:
                        wallet_address = str(keypair.pubkey())
                        token_amount   = await get_token_balance(wallet_address, pos.token_address)
                        if token_amount > 0:
                            order        = await get_ultra_order(
                                output_mint=SOL_MINT,
                                amount=token_amount,
                                wallet_address=wallet_address,
                                input_mint=pos.token_address,
                            )
                            out_lamports = int(order.get("outAmount", 0))
                            sol_received = round(out_lamports / 1_000_000_000, 4)
                            signature    = await execute_ultra_order(order, keypair)
                            pnl_sol      = round(sol_received - pos.amount_sol_spent, 4)
                            await close_position(pos.id, reason, pnl_sol)

                            pnl_sign = "+" if pnl_sol >= 0 else ""
                            await bot.send_message(
                                CALLER_GROUP_ID,
                                f"{emoji} *{label}*\n\n"
                                f"🪙 Token: `{pos.token_name}`\n"
                                f"📊 Entry MC: `{entry_str}` | Exit MC: `{exit_str}`\n"
                                f"📈 Result: `{mult}x` | PNL: `{pnl_sign}{pnl_sol:.4f} SOL`\n\n"
                                f"🔗 [View on Solscan](https://solscan.io/tx/{signature})",
                                parse_mode="Markdown",
                                disable_web_page_preview=True,
                                message_thread_id=SCAN_TOPIC_ID,
                            )
                            logger.info(
                                "Auto-closed position %d (%s): %s mult=%.2fx pnl=%.4f SOL",
                                pos.id, pos.token_name, reason, mult, pnl_sol,
                            )
                        else:
                            # No tokens in wallet — mark closed anyway
                            await close_position(pos.id, reason, pnl_sol=None)
                            logger.warning(
                                "Position %d (%s): %s triggered but wallet holds 0 tokens",
                                pos.id, pos.token_name, reason,
                            )
                    else:
                        # No server wallet — notify for manual action
                        await close_position(pos.id, reason, pnl_sol=None)
                        await bot.send_message(
                            CALLER_GROUP_ID,
                            f"{emoji} *{label}* ⚠️ _Manual sell required_\n\n"
                            f"🪙 Token: `{pos.token_name}`\n"
                            f"📊 Entry MC: `{entry_str}` | Exit MC: `{exit_str}`\n"
                            f"📈 Result: `{mult}x`\n\n"
                            f"_No server wallet configured — sell manually._",
                            parse_mode="Markdown",
                            message_thread_id=SCAN_TOPIC_ID,
                        )

                except Exception as pos_exc:
                    logger.error("Position monitor error for pos %d: %s", pos.id, pos_exc)

            if positions:
                logger.info("Position monitor: checked %d open positions", len(positions))

        except Exception as exc:
            logger.error("Position monitor loop error: %s", exc)

        await asyncio.sleep(300)   # 5 minutes
        logger.info("Position monitor: waking up for next tick")
