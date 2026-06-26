"""
test_exit_engine.py — locks the exit-decision state machine.

These tests ARE the specification of intended exit behavior. If a future change
breaks one, that's a deliberate policy change that must be reviewed — not a
silent bug shipped to a money path (the failure mode that hid the 4am-runner
dump bug for weeks).

Run:  pytest tests/test_exit_engine.py -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.exit_engine import PositionState, decide_exit, dynamic_trail_pct


# Baseline params (mirror the defaults the monitor resolves).
P = {
    "expired_exit_hours": 4.0, "expired_exit_threshold": 1.20,
    "stale_exit_hours": 2.0, "stale_exit_threshold": 1.05,
    "hard_timeout_hours": 4.0,
    "entry_eject_after_sec": 90.0, "entry_eject_peak_mult": 1.10,
    "time_stop_minutes": 5.0, "time_stop_threshold": 1.50,
    "bundle_time_exit_min": 15.0, "bundle_flat_mult": 1.05,
    "breakeven_trigger": 2.0,
    "profit_trail_trigger": 2.0, "tg_signal_trail_trigger": 1.5,
    "tg_moonbag_pct": 25.0, "tg_moonbag_trail_pct": 0.60,
    "paper_stop_slippage_pct": 15.0,
    "_tp_x": 8.0, "_sl_pct": 20.0, "_trail_trigger_raw": 0.3,
}


def mk(**kw) -> PositionState:
    """PositionState with sane defaults; override per test."""
    base = dict(
        entry_mc=50_000, current_mc=50_000, current_mult=1.0, peak_mult=1.0,
        liq_usd=20_000, age_hours=0.5, remaining_pct=100.0, realized_pnl=0.0,
        size_sol=0.3, is_tg_signal=False, is_bundle=False, let_run=False,
    )
    base.update(kw)
    return PositionState(**base)


# ── Holds ────────────────────────────────────────────────────────────────
def test_fresh_position_holds():
    d = decide_exit(mk(current_mult=1.05, peak_mult=1.05, age_hours=0.02), P)
    assert d.action == "hold"


# ── Scanner fast exits ───────────────────────────────────────────────────
def test_no_momentum_eject():
    # ~2min old, never moved, below entry → ejected.
    d = decide_exit(mk(current_mult=0.95, peak_mult=1.05, age_hours=2/60), P)
    assert d.action == "close" and d.reason == "no_momentum"


def test_time_stop_silent_for_scanner():
    d = decide_exit(mk(current_mult=1.1, peak_mult=1.2, age_hours=6/60), P)
    assert d.action == "close" and d.reason == "time_stop" and d.silent is True


def test_expired_scanner():
    d = decide_exit(mk(current_mult=1.1, peak_mult=1.3, age_hours=4.5), P)
    assert d.action == "close" and d.reason == "expired"


# ── Bundle: cut flat/red, let green climbers ride (Lukes fix) ────────────
def test_bundle_flat_cut_after_window():
    # 16min old, flat/red → cut before the dump.
    d = decide_exit(mk(is_bundle=True, current_mult=0.98, peak_mult=1.1,
                       age_hours=16/60), P)
    assert d.action == "close" and d.reason == "bundle_time_exit"


def test_bundle_green_climber_rides():
    # The Lukes case: bundle up 13% and climbing at 16min → NOT cut.
    d = decide_exit(mk(is_bundle=True, current_mult=1.13, peak_mult=1.13,
                       age_hours=16/60), P)
    assert d.action == "hold"


def test_bundle_not_cut_by_generic_time_stop_early():
    # A green bundle at 6min is not cut by the scanner time_stop.
    d = decide_exit(mk(is_bundle=True, current_mult=1.13, peak_mult=1.13,
                       age_hours=6/60), P)
    assert d.action == "hold"


# ── 4am "let runners run" — the critical fix ─────────────────────────────
def test_4am_skips_no_momentum():
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=0.95, peak_mult=1.05, age_hours=2/60), P)
    assert d.action == "hold"  # a 4am dip is NOT cut early


def test_4am_skips_time_stop():
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=1.1, peak_mult=1.2, age_hours=10/60), P)
    assert d.action == "hold"


def test_4am_survives_expired_and_hard_timeout():
    # 6h old, mildly down (above the -20% SL) — scanner would be expired/
    # hard_timeout here; 4am rides it for the late runner.
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=0.9, peak_mult=1.1, age_hours=6.0), P)
    assert d.action == "hold"


def test_4am_moonbag_on_sl():
    # 4am, dropped to SL, full position in → sells down to the 25% moonbag.
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=0.7, peak_mult=1.1, age_hours=0.3,
                       remaining_pct=100.0), P)
    assert d.action == "scale_out" and d.reason == "moonbag"
    assert d.new_remaining == 25.0
    assert d.sell_pct == 75.0


def test_4am_moonbag_rides_no_further_sl():
    # Already at the moonbag size and dropping → it rides, never SL'd.
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=0.3, peak_mult=1.1, age_hours=1.0,
                       remaining_pct=25.0), P)
    assert d.action == "hold" and d.reason == "moonbag_riding"


def test_scanner_catastrophic_sl_within_grace():
    # Documents the real SL path: a catastrophic early rug (<90s, so before the
    # no_momentum window; within grace, so the catastrophic -50% SL applies).
    # Scanner gets a full stop-loss, no moonbag. (For scanner tokens that pump
    # then fade, trail/time_stop pre-empt the SL — this is the case SL is for.)
    d = decide_exit(mk(current_mult=0.4, peak_mult=1.0, age_hours=72/3600),
                    {**P, "_sl_pct": 20.0})
    assert d.action == "close" and d.reason == "sl_hit"


# ── Profit protection ────────────────────────────────────────────────────
def test_breakeven_stop():
    d = decide_exit(mk(current_mult=1.0, peak_mult=2.5, age_hours=0.5), P)
    assert d.action == "close" and d.reason == "breakeven_stop"


def test_profit_trail_scanner():
    # Scanner peaked 2.5x, trail width 0.30 → stop at 1.75x; now 1.7x → trail.
    d = decide_exit(mk(current_mult=1.7, peak_mult=2.5, age_hours=0.5), P)
    assert d.action == "close" and d.reason == "profit_trail"


# ── Scale-out ladder ─────────────────────────────────────────────────────
def test_scale_out_40_at_2x():
    d = decide_exit(mk(current_mult=2.0, peak_mult=2.0, age_hours=0.3,
                       remaining_pct=100.0), P)
    assert d.action == "scale_out" and d.sell_pct == 40.0
    assert d.new_remaining == 60.0
    assert d.new_realized > 0  # banked profit on the slice


def test_scale_out_25_at_5x():
    d = decide_exit(mk(current_mult=5.0, peak_mult=5.0, age_hours=0.3,
                       remaining_pct=60.0), P)
    assert d.action == "scale_out" and d.sell_pct == 25.0
    assert d.new_remaining == 35.0


def test_subscriber_no_scale_out():
    d = decide_exit(mk(current_mult=2.0, peak_mult=2.0, age_hours=0.3,
                       remaining_pct=100.0, subscriber=True), P)
    assert d.action != "scale_out"  # relay rows ride full-in/full-out


# ── Take profit & trail ──────────────────────────────────────────────────
def test_tp_hit():
    # Past TP, but scale-out only triggers >80% remaining; at 50% it hits TP.
    d = decide_exit(mk(current_mult=8.0, peak_mult=8.0, age_hours=0.5,
                       remaining_pct=50.0), P)
    assert d.action == "close" and d.reason == "tp_hit"


# ── dump-then-moon: the scenario the whole 4am fix exists for ─────────────
def test_4am_dump_then_recover_is_not_prematurely_cut():
    # 4am token dips hard early (would be no_momentum/time_stop for scanner)
    # but 4am holds it so it can recover.
    early = decide_exit(mk(is_tg_signal=True, let_run=True,
                           current_mult=0.6, peak_mult=1.0, age_hours=3/60), P)
    assert early.action == "hold"
    # ...later it moons to 10x — scale-out ladder banks it.
    moon = decide_exit(mk(is_tg_signal=True, let_run=True,
                          current_mult=10.0, peak_mult=10.0, age_hours=2.0,
                          remaining_pct=100.0), P)
    assert moon.action == "scale_out"


# ── Trail-width policy ───────────────────────────────────────────────────
def test_trail_width_4am_wider_than_scanner():
    assert dynamic_trail_pct(10.0, True, True, None) == 0.60   # 4am runner: wide
    assert dynamic_trail_pct(10.0, False, False, None) == 0.20  # scanner: tight


def test_claude_trail_override_respected():
    assert dynamic_trail_pct(10.0, True, True, 0.50) == 0.50


# ── Precedence: breakeven fires before profit_trail at exactly 1.0x ───────
def test_breakeven_precedence_over_trail():
    d = decide_exit(mk(current_mult=1.0, peak_mult=3.0, age_hours=0.5), P)
    assert d.reason == "breakeven_stop"  # not profit_trail (scanner)


# ── Capture fix: breakeven must NOT fire on 4am let-runners ───────────────
def test_breakeven_skipped_for_4am():
    # 4am runner peaked 2.5x then round-tripped to 1.0x — must NOT close via
    # breakeven; the trail/moonbag protect it instead (this was capping capture).
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=1.0, peak_mult=2.5, age_hours=0.5), P)
    assert d.reason != "breakeven_stop"


def test_breakeven_reenabled_for_4am_by_param():
    p = dict(P); p["tg_breakeven_enabled"] = 1.0
    d = decide_exit(mk(is_tg_signal=True, let_run=True,
                       current_mult=1.0, peak_mult=2.5, age_hours=0.5), p)
    assert d.reason == "breakeven_stop"


# ── Runner ride: no fixed TP cap + 2x profit floor ───────────────────────
def test_runner_no_tp_cap():
    # Runner at 5x with an 8x... no, _tp_x=5: a ride source must NOT tp-close.
    p = dict(P); p["_tp_x"] = 5.0
    d = decide_exit(mk(is_tg_signal=True, let_run=True, ride=True,
                       current_mult=5.0, peak_mult=5.0, age_hours=0.5,
                       remaining_pct=35.0), p)
    assert d.reason != "tp_hit"


def test_scanner_keeps_tp_cap():
    # Non-ride (scanner) at/above TP still takes profit. remaining_pct=60 so the
    # scale-out ladder (which precedes TP) doesn't fire first.
    p = dict(P); p["_tp_x"] = 3.0
    d = decide_exit(mk(is_tg_signal=False, ride=False, remaining_pct=60.0,
                       current_mult=3.0, peak_mult=3.0, age_hours=0.5), p)
    assert d.action == "close" and d.reason == "tp_hit"


def test_runner_2x_floor():
    # Rode to 3x then faded to 2x → lock the 2x floor (not give it all back).
    d = decide_exit(mk(is_tg_signal=True, let_run=True, ride=True,
                       current_mult=2.0, peak_mult=3.2, age_hours=0.5,
                       remaining_pct=35.0), P)
    assert d.action == "close" and d.reason == "profit_floor"


def test_runner_floor_not_armed_below_3x():
    # Peaked only 2.5x → floor not armed yet; 2x pullback should NOT floor-close.
    d = decide_exit(mk(is_tg_signal=True, let_run=True, ride=True,
                       current_mult=2.0, peak_mult=2.5, age_hours=0.5,
                       remaining_pct=35.0), P)
    assert d.reason != "profit_floor"


if __name__ == "__main__":
    import subprocess
    raise SystemExit(subprocess.call(["pytest", "-v", __file__]))
