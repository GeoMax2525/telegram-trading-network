"""
keyboards.py — Inline keyboard builders.

Trade Card buttons:
  📊 Chart         — opens DexScreener chart in browser
  💰 Buy           — opens a buy link (Jupiter aggregator)
  📢 Share Signal  — callback to re-broadcast the card to MAIN_GROUP_ID
  🚩 Flag as Risky — callback to flag the token for manual review

PnL buttons:
  🔄 Refresh       — callback to refresh the PnL message in place
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def trade_card_keyboard(
    dex_url: str,
    contract_address: str,
) -> InlineKeyboardMarkup:
    """
    Builds and returns the inline keyboard attached to every Trade Card.

    :param dex_url:           Full DexScreener URL for the pair.
    :param contract_address:  Token contract address (used in buy/share callbacks).
    """
    builder = InlineKeyboardBuilder()

    # Row 1 — external links
    builder.row(
        InlineKeyboardButton(
            text="📊 Chart",
            url=dex_url if dex_url else f"https://dexscreener.com/solana/{contract_address}",
        ),
        InlineKeyboardButton(
            text="💰 Buy",
            # Jupiter aggregator deeplink for Solana tokens
            url=f"https://jup.ag/swap/SOL-{contract_address}",
        ),
    )

    # Row 2 — action callbacks (handled in handlers.py)
    builder.row(
        InlineKeyboardButton(
            text="📢 Share Signal",
            callback_data=f"share:{contract_address}",
        ),
        InlineKeyboardButton(
            text="🚩 Flag as Risky",
            callback_data=f"flag:{contract_address}",
        ),
    )

    # Row 3 — KeyBot
    builder.row(
        InlineKeyboardButton(
            text="⚡ KeyBot Buy",
            callback_data=f"kbbuy:{contract_address}",
        ),
    )

    return builder.as_markup()


def pnl_keyboard(contract_address: str) -> InlineKeyboardMarkup:
    """Keyboard for /pnl messages — single Refresh button."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔄 Refresh",
            callback_data=f"pnl:{contract_address}",
        )
    )
    return builder.as_markup()


def top_calls_keyboard(active: str = "ALL") -> InlineKeyboardMarkup:
    """Timeframe filter buttons for the Top Calls leaderboard."""
    builder = InlineKeyboardBuilder()
    for tf in ("24H", "1W", "1M", "ALL"):
        label = f"[{tf}]" if tf == active else tf
        builder.button(text=label, callback_data=f"tc:{tf}")
    builder.adjust(4)
    return builder.as_markup()
