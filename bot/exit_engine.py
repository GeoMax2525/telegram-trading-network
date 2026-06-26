"""
exit_engine.py — the PURE exit-decision state machine.

This is the heart of the system extracted as a pure function: given a position's
state and the resolved params, decide what to do. NO I/O — no DB, no Telegram,
no network. That makes it fully unit-testable (see tests/test_exit_engine.py),
which is exactly what the tangled in-monitor logic never was.

The monitor (bot/agents/paper_monitor.py) handles the I/O-dependent pre-checks
(dead_api / dead_token, which need fresh liquidity + consecutive-failure
tracking), builds a PositionState, calls decide_exit(), and EXECUTES the
returned ExitDecision (DB write, close card, live mirror).

Policy precedence (must match the monitor exactly):
  1. expired / stale        (time cleanup — skipped for 4am let-runners)
  2. hard_timeout           (4h absolute — skipped for 4am)
  3. no_momentum eject      (~90s DOA — skipped for 4am)
  4. bundle_time_exit       (bundle flat in dump window)
  5. time_stop              (5min flat — skipped for 4am)
  6. breakeven_stop         (peak>=trigger, round-tripped to <=1x)
  7. profit_trail           (peak>=trigger, gave back the trail width)
  8. scale_out ladder       (40% @ 2x, 25% @ 5x)
  9. tp_hit                 (hit take-profit)
 10. trail_hit              (trailing stop)
 11. sl_hit / moonbag       (stop-loss; 4am sells down to a moonbag instead)
 (else) hold
"""

from dataclasses import dataclass, field


@dataclass
class PositionState:
    # Price / multiples
    entry_mc: float
    current_mc: float
    current_mult: float
    peak_mult: float
    liq_usd: float | None          # for rug classification (None = unknown)
    # Timing
    age_hours: float
    # Position
    remaining_pct: float           # 0-100, how much of the position is still in
    realized_pnl: float            # SOL already realized from scale-outs
    size_sol: float                # original position size
    # Identity / flags
    is_tg_signal: bool             # 4am source
    is_bundle: bool                # bundle/concentration trade
    let_run: bool                  # 4am "let runners run" active
    subscriber: bool = False       # relay row (no scale-out / moonbag)
    ladder_disabled: bool = False  # Claude let it ride (no auto-ladder)
    trail_override: float | None = None   # Claude per-position trail width
    ride: bool = False             # runner source (4am/algo): no fixed TP cap, 2x floor


@dataclass
class ExitDecision:
    action: str                    # "hold" | "close" | "scale_out"
    reason: str = ""               # close reason, or "scale_out"
    sell_pct: float = 0.0          # for scale_out / moonbag: % of position sold now
    new_remaining: float = 0.0     # resulting remaining_pct after a partial
    new_realized: float = 0.0      # resulting realized_pnl after a partial
    pnl: float = 0.0               # realized PnL of a full close
    note: str = ""                 # human note for the card
    silent: bool = False           # time-cleanup closes don't broadcast
    meta: dict = field(default_factory=dict)


def dynamic_trail_pct(peak_m: float, is_tg_signal: bool, let_run: bool,
                      trail_override: float | None,
                      moonbag_trail_pct: float = 0.60) -> float:
    """Trail width by tier — wide for 4am runners (fat tail), tight for scanner
    scalps. Mirrors paper_monitor._dynamic_trail_pct exactly."""
    if trail_override is not None and 0.05 <= trail_override <= 0.80:
        return float(trail_override)
    if is_tg_signal and let_run:
        if peak_m < 2.0:
            return 0.35
        if peak_m < 5.0:
            return 0.45
        return float(moonbag_trail_pct)
    if peak_m < 3.0:
        return 0.30
    if peak_m < 6.0:
        return 0.28
    return 0.20


def decide_exit(pos: PositionState, p: dict) -> ExitDecision:
    """Pure exit decision. `p` is the resolved params dict (same keys the
    monitor reads). Returns an ExitDecision the caller executes."""
    cur = pos.current_mult
    peak = pos.peak_mult
    age = pos.age_hours
    remaining = pos.remaining_pct
    realized = pos.realized_pnl
    sol = pos.size_sol
    skip_fast = pos.is_tg_signal and pos.let_run

    def _close(reason, mult=None, note="", silent=False) -> ExitDecision:
        m = cur if mult is None else mult
        remaining_sol = sol * (remaining / 100.0)
        pnl = round(realized + remaining_sol * (m - 1.0), 4)
        return ExitDecision(action="close", reason=reason, pnl=pnl,
                            note=note, silent=silent)

    # 1. expired / stale (time cleanup) — 4am runners survive these.
    expired_h = float(p.get("expired_exit_hours", 4.0) or 4.0)
    expired_thr = float(p.get("expired_exit_threshold", 1.20) or 1.20)
    stale_h = float(p.get("stale_exit_hours", 2.0) or 2.0)
    stale_thr = float(p.get("stale_exit_threshold", 1.05) or 1.05)
    if not pos.let_run:
        if age >= expired_h and cur < expired_thr:
            return _close("expired", silent=True)
        if age >= stale_h and cur < stale_thr:
            return _close("stale", silent=True)

    # 2. hard timeout (absolute 4h) — 4am skips.
    hard_h = float(p.get("hard_timeout_hours", 4.0) or 4.0)
    if age >= hard_h and not pos.let_run:
        return _close("hard_timeout")

    # 3. no-momentum DOA eject (~90s) — 4am skips.
    eject_after_h = float(p.get("entry_eject_after_sec", 90.0) or 90.0) / 3600.0
    eject_peak = float(p.get("entry_eject_peak_mult", 1.10) or 1.10)
    ts_min = float(p.get("time_stop_minutes", 5.0) or 5.0)
    if (not skip_fast and eject_after_h <= age < (ts_min / 60.0)
            and peak < eject_peak and cur < 1.0):
        return _close("no_momentum", note="Momentum died early — no follow-through.")

    # 4. bundle time-exit — cut a FLAT/RED bundle before the dump, but let a
    #    GREEN one ride (a bundle up 13% and climbing is working, not flat).
    #    Bundles use ONLY this time-cut (excluded from the generic time_stop).
    if pos.is_bundle:
        b_min = float(p.get("bundle_time_exit_min", 15.0) or 15.0)
        b_flat = float(p.get("bundle_flat_mult", 1.05) or 1.05)
        if age >= (b_min / 60.0) and cur < b_flat:
            return _close("bundle_time_exit", note="Flat/red in the dump window — cut early.")

    # 5. time stop (5min flat) — scanner only (bundles handled above, 4am skips).
    ts_thr = float(p.get("time_stop_threshold", 1.50) or 1.50)
    if (not skip_fast and not pos.is_bundle
            and age >= (ts_min / 60.0) and peak < ts_thr and cur < ts_thr):
        return _close("time_stop", silent=True)

    # 6. breakeven stop — DISABLED for 4am let-runners by default.
    #    A 1.4x breakeven was closing fat-tail runners at 1.0x the instant they
    #    wobbled, BEFORE the moonbag could ever form (capture ~0.13% of peak).
    #    For 4am the moonbag (25% rides) + the wide profit-trail are the
    #    protection — a breakeven stop fights the whole "let runners run"
    #    thesis. Scanner KEEPS breakeven (it's a scalp source). Re-enable for
    #    4am with /setparam tg_breakeven_enabled 1.
    be_trigger = float(p.get("breakeven_trigger", 2.0) or 2.0)
    be_applies = (not skip_fast) or (float(p.get("tg_breakeven_enabled", 0.0) or 0.0) >= 0.5)
    if be_applies and peak >= be_trigger and cur <= 1.0:
        return _close("breakeven_stop", note=f"Peak {peak:.1f}x — protected the entry.")

    # 6.5 RUNNER 2x PROFIT FLOOR — once a runner (4am/algo) has ridden to >=3x,
    #     never let the remaining chunk close below 2x. Capital-preservation
    #     guarantee on the un-capped runner; the wide trail rides the big ones.
    if pos.ride:
        floor_arm = float(p.get("runner_floor_arm", 3.0) or 3.0)
        floor_lock = float(p.get("runner_floor_lock", 2.0) or 2.0)
        if peak >= floor_arm and cur <= floor_lock:
            return _close("profit_floor", note=f"Peak {peak:.1f}x — locked the {floor_lock:.0f}x floor.")

    # 7. profit trail.
    if pos.is_tg_signal:
        pt_trigger = float(p.get("tg_signal_trail_trigger", 1.5) or 1.5)
    else:
        pt_trigger = float(p.get("profit_trail_trigger", 2.0) or 2.0)
    pt_pct = dynamic_trail_pct(peak, pos.is_tg_signal, pos.let_run, pos.trail_override,
                               float(p.get("tg_moonbag_trail_pct", 0.60) or 0.60))
    if peak >= pt_trigger and cur <= peak * (1.0 - pt_pct):
        return _close("profit_trail", note=f"Peak {peak:.1f}x — trailed the runner.")

    # 8. scale-out ladder (HQ only; not subscribers; not if Claude disabled it).
    if not pos.subscriber and not pos.ladder_disabled:
        if remaining > 80 and cur >= 2.0:
            sell_pct = 40.0
            sold = sol * (sell_pct / 100.0) * (cur - 1.0)
            return ExitDecision(action="scale_out", reason="scale_out", sell_pct=sell_pct,
                                new_remaining=remaining - sell_pct,
                                new_realized=round(realized + sold, 4),
                                note=f"Sold 40% at {cur:.1f}x")
        if 50 < remaining <= 80 and cur >= 5.0:
            sell_pct = 25.0
            sold = sol * (sell_pct / 100.0) * (cur - 1.0)
            return ExitDecision(action="scale_out", reason="scale_out", sell_pct=sell_pct,
                                new_remaining=remaining - sell_pct,
                                new_realized=round(realized + sold, 4),
                                note=f"Sold 25% at {cur:.1f}x")

    # 9. take-profit — SKIPPED for runner sources (4am/algos): the scale-out
    #    ladder + trail + 2x floor ride the tail instead of capping at a fixed TP.
    tp_x = float(p.get("_tp_x") or 0)
    ride_on = pos.ride and float(p.get("runner_ride_enabled", 1.0) or 0) >= 0.5
    if tp_x > 0 and cur >= tp_x and not ride_on:
        return _close("tp_hit", note="🎯 Target reached.")

    # 10. trailing stop.
    if pos.is_tg_signal:
        trig = float(p.get("tg_signal_trail_trigger", 1.5) or 1.5)
    else:
        trig = 1.0 + float(p.get("_trail_trigger_raw") or 0.3)
    trail_pct = dynamic_trail_pct(peak, pos.is_tg_signal, pos.let_run, pos.trail_override,
                                  float(p.get("tg_moonbag_trail_pct", 0.60) or 0.60))
    if peak >= trig and cur <= peak * (1.0 - trail_pct):
        return _close("trail_hit", note=f"Peak {peak:.1f}x — locked the trail.")

    # 11. stop-loss (+ 4am moonbag).
    sl_threshold = 1.0 - (float(p.get("_sl_pct") or 30.0) / 100.0)
    catastrophic = 0.50
    grace_min = 10.0 if pos.is_tg_signal else 5.0
    if age < (grace_min / 60.0):
        should_sl = cur <= catastrophic
    else:
        should_sl = cur <= sl_threshold
    if should_sl:
        slip = float(p.get("paper_stop_slippage_pct", 15.0) or 15.0) / 100.0
        fill_mult = cur * (1.0 - slip)
        moonbag_pct = (float(p.get("tg_moonbag_pct", 25.0) or 0)
                       if (pos.is_tg_signal and pos.let_run) else 0.0)
        if moonbag_pct > 0 and remaining > moonbag_pct + 0.5:
            sell_pct = remaining - moonbag_pct
            sold = sol * (sell_pct / 100.0) * (fill_mult - 1.0)   # negative
            return ExitDecision(action="scale_out", reason="moonbag", sell_pct=sell_pct,
                                new_remaining=moonbag_pct,
                                new_realized=round(realized + sold, 4),
                                note=f"Cut {sell_pct:.0f}% at {cur:.2f}x — {moonbag_pct:.0f}% rides the moon")
        if moonbag_pct > 0 and remaining <= moonbag_pct + 0.5:
            return ExitDecision(action="hold", reason="moonbag_riding")
        # Normal full SL close (slippage-adjusted fill).
        remaining_sol = sol * (remaining / 100.0)
        loss = round(-remaining_sol * (1.0 - fill_mult), 4)
        return ExitDecision(action="close", reason="sl_hit",
                            pnl=round(realized + loss, 4), note="🛑 SL triggered.")

    return ExitDecision(action="hold")
