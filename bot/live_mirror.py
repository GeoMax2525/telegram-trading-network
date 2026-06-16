"""
live_mirror.py — autonomous LIVE execution that mirrors paper trading 1:1.

Design principle (operator's words): "same exact way as paper, same
everything." The paper system is the brain — it decides every entry and
every exit. This module is a thin execution layer bolted onto the two
chokepoints all paper trades funnel through:

  • open_paper_trade()      → mirror_open()  fires a capped real BUY
  • _finalize_paper_close() → mirror_close() fires the real SELL

So live takes the identical signals, the identical Claude management, and
the identical TP/SL/trail exits as paper — it just executes them on-chain.

EVERYTHING here is gated behind live_trading_armed (default 0 = OFF). With
it off, these functions are no-ops and the bot stays pure paper. Sizing is
independently capped (live_mirror_size_sol + the live_guard per-trade /
daily circuit breakers) so live risk is bounded regardless of paper size.

v1 scope: mirrors ENTRY and FINAL EXIT at full position size. It does NOT
mirror the paper scale-out ladder's partial sells (2x/5x/10x) — the live
position rides full-in / full-out to the final close. A reconcile loop
retries any sell that failed so a bought position can never be left
silently holding (the "ride to zero" failure mode).
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def _armed_cfg() -> dict | None:
    """Return live config if armed, else None."""
    from database.models import get_params
    cfg = await get_params(
        "live_trading_armed", "live_mirror_size_sol",
        "slippage_tolerance_bps", "max_price_impact_pct",
    )
    if float(cfg.get("live_trading_armed") or 0) < 1.0:
        return None
    return cfg


async def mirror_open(paper_trade_id: int, mint: str, subscriber_id, paper_sol: float) -> None:
    """Mirror a paper OPEN with a capped real buy. No-op unless armed.
    Only mirrors HQ admin trades (never subscriber relay rows)."""
    if subscriber_id is not None:
        return
    cfg = await _armed_cfg()
    if cfg is None:
        return

    from bot.live_guard import live_preflight, record_live_buy
    from bot.wallet import get_keypair
    from bot.trading import get_ultra_order, execute_ultra_order
    from database.models import AsyncSessionLocal, LiveMirror

    size = float(cfg.get("live_mirror_size_sol") or 0.05)
    ok, why = await live_preflight(size)
    if not ok:
        logger.info("live_mirror: skip BUY %s — %s", (mint or "?")[:8], why)
        return

    keypair = get_keypair()
    if keypair is None:
        logger.error("live_mirror: ARMED but WALLET_PRIVATE_KEY not set — cannot buy %s", (mint or "?")[:8])
        return

    try:
        lamports = int(size * 1_000_000_000)
        wallet = str(keypair.pubkey())
        slip = int(float(cfg.get("slippage_tolerance_bps") or 500))
        order = await get_ultra_order(mint, lamports, wallet, slippage_bps=slip)
        impact = abs(float(order.get("priceImpactPct", 0)))
        max_impact = float(cfg.get("max_price_impact_pct") or 10.0)
        if impact > max_impact:
            logger.info("live_mirror: skip BUY %s — price impact %.1f%% > %.1f%%",
                        (mint or "?")[:8], impact, max_impact)
            return
        sig = await execute_ultra_order(order, keypair)
        record_live_buy(size)
        async with AsyncSessionLocal() as session:
            session.add(LiveMirror(
                paper_trade_id=paper_trade_id, mint=mint, sol_spent=size,
                tokens_bought=str(order.get("outAmount", "")), buy_sig=sig,
                status="open",
            ))
            await session.commit()
        logger.info("live_mirror: LIVE BUY %s — %.3f SOL  sig=%s", (mint or "?")[:8], size, (sig or "")[:10])
    except Exception as exc:
        logger.error("live_mirror: LIVE BUY FAILED %s: %s", (mint or "?")[:8], exc)


async def _execute_sell(mint: str, keypair) -> str | None:
    """Sell the entire on-chain token balance back to SOL. Returns sig or None
    if nothing to sell. Raises on execution failure (caller handles retry)."""
    from bot.trading import get_token_balance, get_ultra_order, execute_ultra_order, SOL_MINT
    from database.models import get_param

    wallet = str(keypair.pubkey())
    bal = await get_token_balance(wallet, mint)
    if not bal or bal <= 0:
        return None
    # Wider slippage on exit — getting OUT matters more than price on a dump.
    slip = int(float(await get_param("slippage_tolerance_bps") or 500))
    slip = max(slip, 1000)
    order = await get_ultra_order(SOL_MINT, bal, wallet, input_mint=mint, slippage_bps=slip)
    return await execute_ultra_order(order, keypair)


async def mirror_close(paper_trade_id: int, mint: str) -> None:
    """Mirror a paper CLOSE with a real sell of the full live position.
    No-op if this trade was never mirrored live. On sell failure the row is
    marked 'failed' for the reconcile loop to retry — never left silently open."""
    from database.models import AsyncSessionLocal, LiveMirror, select
    from bot.wallet import get_keypair

    async with AsyncSessionLocal() as session:
        lm = (await session.execute(
            select(LiveMirror).where(
                LiveMirror.paper_trade_id == paper_trade_id,
                LiveMirror.status.in_(("open", "failed")),
            )
        )).scalar_one_or_none()
    if lm is None:
        return  # not a live-mirrored trade

    keypair = get_keypair()
    if keypair is None:
        logger.error("live_mirror: CLOSE but no wallet — %s LEFT HOLDING, reconcile will retry", (mint or "?")[:8])
        await _mark(lm.id, "failed")
        return

    try:
        sig = await _execute_sell(mint, keypair)
        await _mark(lm.id, "closed", sell_sig=sig)
        logger.info("live_mirror: LIVE SELL %s done  sig=%s", (mint or "?")[:8], (sig or "none")[:10])
    except Exception as exc:
        logger.error("live_mirror: LIVE SELL FAILED %s: %s — marked failed, reconcile will retry", (mint or "?")[:8], exc)
        await _mark(lm.id, "failed")


async def _mark(lm_id: int, status: str, sell_sig: str | None = None) -> None:
    from database.models import AsyncSessionLocal, LiveMirror
    async with AsyncSessionLocal() as session:
        lm = await session.get(LiveMirror, lm_id)
        if lm:
            lm.status = status
            if status == "closed":
                lm.closed_at = datetime.utcnow()
                if sell_sig:
                    lm.sell_sig = sell_sig
            await session.commit()


async def live_mirror_reconcile_loop() -> None:
    """Safety net: retry any live position whose sell failed (status='failed').
    Without this a network blip on the exit would leave real tokens held with
    no auto-exit — the one failure mode that can ride a live bag to zero."""
    import asyncio
    await asyncio.sleep(120)
    while True:
        try:
            from database.models import AsyncSessionLocal, LiveMirror, select
            from bot.wallet import get_keypair
            async with AsyncSessionLocal() as session:
                stuck = list((await session.execute(
                    select(LiveMirror).where(LiveMirror.status == "failed")
                )).scalars().all())
            if stuck:
                keypair = get_keypair()
                if keypair is not None:
                    for lm in stuck:
                        try:
                            sig = await _execute_sell(lm.mint, keypair)
                            await _mark(lm.id, "closed", sell_sig=sig)
                            logger.info("live_mirror: RECONCILE sold %s", (lm.mint or "?")[:8])
                        except Exception as exc:
                            logger.warning("live_mirror: reconcile retry failed %s: %s", (lm.mint or "?")[:8], exc)
        except Exception as exc:
            logger.debug("live_mirror: reconcile loop error: %s", exc)
        await asyncio.sleep(90)
