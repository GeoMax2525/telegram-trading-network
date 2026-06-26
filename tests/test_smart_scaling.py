"""Unit tests for SmartScalingExitManager (runs without pytest via importlib)."""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.smart_scaling_exit import SmartScalingExitManager, default_config


def _mgr(ttype="4am", **kw):
    return SmartScalingExitManager().start_position(1.0, ttype, **kw)


def test_scale1_sells_28_and_sets_165_stop():
    m = _mgr("4am")
    assert m.on_price_update(1.5)["action"] is None
    r = m.on_price_update(2.0)
    assert r["action"] == "scale" and r["sell_pct"] == 28.0
    assert abs(r["new_stop"] - 1.65) < 1e-9
    assert abs(m.current_position_pct - 72.0) < 1e-9


def test_runner_left_is_38pct_after_three_scales():
    m = _mgr("4am")
    m.on_price_update(2.0); m.on_price_update(4.5); m.on_price_update(8.0)
    assert abs(m.current_position_pct - 38.0) < 1e-9
    assert m._runner_active is True


def test_stop_ratchets_up_only():
    m = _mgr("4am")
    m.on_price_update(2.0)            # stop → 1.65
    m.on_price_update(4.5)            # stop → 4.5 (current)
    assert m.current_stop_price >= 4.5
    # a lower-price tick must never lower the stop
    m.on_price_update(3.0)
    assert m.current_stop_price >= 4.5


def test_runner_exits_on_trail_not_tp():
    m = _mgr("4am")
    for x in [2.0, 4.5, 8.0, 12.0]:
        m.on_price_update(x)
    # peak 12x, 35% trail → stop ~7.8x; but milestone lock from 8x is 8.0 → max
    r = m.on_price_update(7.9)
    # 7.9 is above the 8.0 lock? no — 7.9 < 8.0 → exit at the locked 8x floor
    assert r["action"] == "exit"


def test_conservative_uses_two_tiers():
    m = _mgr("scanner")
    assert m.on_price_update(2.0)["sell_pct"] == 35.0
    assert m.on_price_update(5.0)["sell_pct"] == 20.0
    assert m._runner_active is True  # only two tiers in conservative


def test_let_run_skips_midladder_stop():
    m = _mgr("4am", is_let_run=True)
    m.on_price_update(2.0)           # stop set to 1.65, runner NOT active
    # drop to 1.5 (below 1.65 stop) — let_run must NOT exit mid-ladder
    r = m.on_price_update(1.5)
    assert r["action"] != "exit"


def test_rehydrate_restores_state():
    m = _mgr("4am")
    # pretend two tiers already fired and a stop is locked at 4.5x, 50% left
    m.rehydrate(remaining_pct=50.0, peak_mult=4.5, current_stop=4.5, scale_tier=2)
    assert m.current_position_pct == 50.0
    assert m.scales_done == [2.0, 4.5]
    # next tick at 8x should fire tier 3 (12%)
    r = m.on_price_update(8.0)
    assert r["action"] == "scale" and r["sell_pct"] == 12.0


def test_sell_fraction_of_remaining_is_correct():
    m = _mgr("4am")
    r1 = m.on_price_update(2.0)      # 28% of original, holding was 100 → 0.28
    assert abs(r1["sell_fraction_of_remaining"] - 0.28) < 1e-6
    r2 = m.on_price_update(4.5)      # 22% of original, holding was 72 → 22/72
    assert abs(r2["sell_fraction_of_remaining"] - (22.0 / 72.0)) < 1e-6


def test_default_config_bundle_is_wider():
    assert default_config("bundle")["runner_trail_pct"] > default_config("4am")["runner_trail_pct"]


if __name__ == "__main__":
    import importlib.util
    g = {k: v for k, v in globals().items() if k.startswith("test_")}
    p = f = 0
    for name, fn in g.items():
        try:
            fn(); p += 1
        except Exception as e:
            f += 1; print("FAIL", name, "->", repr(e))
    print(f"{p} passed, {f} failed of {len(g)}")
