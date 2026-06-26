"""
smart_scaling_exit.py — REVOLT bot modular exit manager.

Owns ONLY the profit side of an exit on a single position: tiered scale-outs,
ratcheting the stop UP after each sale, and a wide trailing stop on the final
runner. It does NOT own the hard safety rules (momentum eject, hard timeout,
bundle_time_exit, base stop-loss, moonbag) — those stay in the monitor and run
alongside this. Mental model:

    "The price moved. Take profit in tiers, lock each gain by ratcheting the
     stop up, and let the final runner ride on a wide trail."

Runs in MULTIPLE space: start with entry_price=1.0 and feed current_mult into
on_price_update(); stops come out as multiples (1.65 = 1.65x), which maps
directly onto how the bot already tracks trades.

State lives on the DB row between polls (the monitor is stateless per tick), so
call rehydrate() after start_position() to restore mid-trade state, then persist
current_position_pct / current_stop_price / len(scales_done) back afterwards.

Config is INJECTABLE (pass `config=` from the DB-backed ScalingConfig table) so
the learning loop can tune it per trade type without code changes. The CONFIGS
dict below is only the seed default.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# SEED CONFIG — defaults only. The live values live in the ScalingConfig table
# and are tuned by scaling_optimizer. Every strategy number is here, per type.
#
#   at       : multiple of entry that triggers the scale
#   sell_pct : PERCENTAGE POINTS OF THE ORIGINAL position to sell at this tier
#              (28 + 22 + 12 = 62% sold → 38% runner). NOT % of remaining.
#   stop     : ("mult", x)  → stop = entry * x   (fixed profit lock)
#              ("current",) → stop = the live price now (lock the gain)
#   runner_trail_pct : trailing-stop width for the final runner (% below peak)
# ─────────────────────────────────────────────────────────────────────────────

CONFIGS = {
    "high_conviction": {                       # 4am / high_conviction / algo
        "scales": [
            {"at": 2.0, "sell_pct": 28.0, "stop": ("mult", 1.65)},
            {"at": 4.5, "sell_pct": 22.0, "stop": ("current",)},
            {"at": 8.0, "sell_pct": 12.0, "stop": ("current",)},
        ],
        "runner_trail_pct": 0.35,
    },
    "conservative": {                          # scanner / normal
        "scales": [
            {"at": 2.0, "sell_pct": 35.0, "stop": ("mult", 1.50)},
            {"at": 5.0, "sell_pct": 20.0, "stop": ("current",)},
        ],
        "runner_trail_pct": 0.26,
    },
}

TYPE_ALIASES = {
    "4am": "high_conviction", "high_conviction": "high_conviction",
    "algo": "high_conviction", "bundle": "high_conviction",
    "scanner": "conservative", "normal": "conservative",
}

BUNDLE_TRAIL_BONUS = 0.05            # bundles dump harder → wider runner trail
CONVICTION_TRAIL_PER_POINT = 0.10   # widen trail per +1.0 conviction_score
MIN_TRAIL_PCT, MAX_TRAIL_PCT = 0.15, 0.60


def default_config(trade_type: str) -> dict:
    """The seed config for a trade type (used to seed the DB table)."""
    key = TYPE_ALIASES.get(str(trade_type).lower(), "conservative")
    base = CONFIGS[key]
    cfg = {"scales": [dict(s) for s in base["scales"]],
           "runner_trail_pct": base["runner_trail_pct"]}
    if str(trade_type).lower() == "bundle":
        cfg["runner_trail_pct"] = round(cfg["runner_trail_pct"] + BUNDLE_TRAIL_BONUS, 4)
    return cfg


@dataclass
class ScaleEvent:
    """One logged scale / trail / exit event — fuel for the learning loop."""
    timestamp: str
    kind: str               # "scale" | "trail" | "exit"
    price: float            # multiple-space "price" (= current_mult)
    multiple: float
    sell_pct: float         # % of ORIGINAL position
    new_stop: Optional[float]
    remaining_pct: float
    reason: str


class SmartScalingExitManager:
    """Tiered scale-out + ratcheting stop + wide runner trail, per trade type."""

    def __init__(self):
        self._reset()

    # ── state ────────────────────────────────────────────────────────────────
    def _reset(self):
        self.active = False
        self.entry_price = 0.0
        self.trade_type = None
        self.is_high_conviction = False
        self.is_let_run = False
        self.conviction_score = 1.0
        self.current_position_pct = 100.0
        self.current_stop_price = None        # None until the first scale sets it
        self.peak_price = 0.0
        self.scales_done = []                 # list of `at` multiples already hit
        self._cfg = None
        self._runner_active = False
        self.events: list[ScaleEvent] = []

    # ── initialise a position ────────────────────────────────────────────────
    def start_position(self, entry_price, trade_type, conviction_score=1.0,
                       is_let_run=False, overrides=None, config=None):
        """Begin tracking. Pass `config` (a {scales, runner_trail_pct} dict from
        the DB-backed ScalingConfig) to use tuned values; otherwise seed defaults.
        `overrides` patches the resolved config for one specific trade."""
        self._reset()
        self.active = True
        self.entry_price = float(entry_price)
        self.peak_price = float(entry_price)
        self.trade_type = trade_type
        self.conviction_score = float(conviction_score)
        self.is_let_run = bool(is_let_run)
        self.is_high_conviction = (
            TYPE_ALIASES.get(str(trade_type).lower()) == "high_conviction")
        self._cfg = self._build_config(trade_type, config, overrides)
        return self

    def _build_config(self, trade_type, config, overrides):
        cfg = config if config else default_config(trade_type)
        # copy so we never mutate the caller's / global dict
        cfg = {"scales": [dict(s) for s in cfg["scales"]],
               "runner_trail_pct": float(cfg["runner_trail_pct"])}
        # very high conviction → let the runner breathe a little more
        if self.conviction_score > 1.0:
            cfg["runner_trail_pct"] = min(
                cfg["runner_trail_pct"]
                + (self.conviction_score - 1.0) * CONVICTION_TRAIL_PER_POINT,
                MAX_TRAIL_PCT)
        if overrides:
            if "runner_trail_pct" in overrides:
                cfg["runner_trail_pct"] = float(overrides["runner_trail_pct"])
            if "scales" in overrides:
                cfg["scales"] = [dict(s) for s in overrides["scales"]]
        cfg["runner_trail_pct"] = max(MIN_TRAIL_PCT, min(cfg["runner_trail_pct"], MAX_TRAIL_PCT))
        return cfg

    def rehydrate(self, remaining_pct, peak_mult, current_stop, scale_tier):
        """Restore mid-trade state from the DB so the manager can run statelessly
        across polls. `scale_tier` = how many tiers have already fired."""
        self.current_position_pct = float(remaining_pct if remaining_pct is not None else 100.0)
        self.peak_price = max(self.peak_price, float(peak_mult or 1.0))
        self.current_stop_price = (float(current_stop)
                                   if current_stop not in (None, 0) else None)
        tier = int(scale_tier or 0)
        self.scales_done = [s["at"] for s in self._cfg["scales"][:tier]]
        self._runner_active = tier >= len(self._cfg["scales"])
        return self

    # ── MAIN: feed every price tick here ─────────────────────────────────────
    def on_price_update(self, current_price: float, age_minutes: float = None) -> dict:
        """Process one tick. Returns one of:
            {"action":"scale", sell_pct, sell_fraction_of_remaining, new_stop, remaining_pct, reason}
            {"action":"trail", new_stop, ...}   # stop ratcheted up, no sell
            {"action":"exit",  sell_pct, ...}   # stop hit → close the remainder
            {"action":None, ...}                # hold
        """
        hold = {"action": None, "sell_pct": 0.0,
                "new_stop": self.current_stop_price, "reason": ""}
        if not self.active or self.entry_price <= 0:
            return hold

        current_price = float(current_price)
        self.peak_price = max(self.peak_price, current_price)     # peak only rises
        mult = current_price / self.entry_price

        # 1) SCALE-OUT — next un-done tier whose trigger we've reached.
        nxt = self._next_scale()
        if nxt is not None and mult >= nxt["at"]:
            return self._do_scale(nxt, current_price, mult)

        # 2) STOP HIT — only once a stop exists (set by the first scale). Before
        #    that, the external hard-rules layer owns the downside. A let_run
        #    trade skips the MID-LADDER stop and only exits on the runner trail
        #    (or the bot's hard rules) — that's what "let it run" buys you.
        if (self.current_stop_price is not None
                and current_price <= self.current_stop_price
                and (self._runner_active or not self.is_let_run)):
            return self._do_exit(current_price, mult)

        # 3) RUNNER TRAIL — once every tier is sold, ratchet the trailing stop UP.
        #    The runner uses ONLY this % trail (and the locked floor from the last
        #    ratchet, whichever is higher). Never a hard milestone stop.
        if self._runner_active:
            trailed = self.peak_price * (1.0 - self._cfg["runner_trail_pct"])
            if self.current_stop_price is None or trailed > self.current_stop_price:
                self.current_stop_price = trailed
                ev = self._log("trail", current_price, mult, 0.0,
                               f"Runner trail {int(self._cfg['runner_trail_pct']*100)}% "
                               f"of peak {self.peak_price:.2f}x → stop {trailed:.4f}")
                return {"action": "trail", "sell_pct": 0.0,
                        "new_stop": self.current_stop_price, "reason": ev.reason}
        return hold

    # ── internals ────────────────────────────────────────────────────────────
    def _next_scale(self):
        for s in self._cfg["scales"]:
            if s["at"] not in self.scales_done:
                return s
        return None

    def _do_scale(self, scale, price, mult):
        sell_pct = scale["sell_pct"]                       # % of ORIGINAL position
        self.current_position_pct = max(self.current_position_pct - sell_pct, 0.0)
        self.scales_done.append(scale["at"])
        new_stop = self._stop_from_rule(scale["stop"], price)
        self.current_stop_price = (new_stop if self.current_stop_price is None
                                   else max(self.current_stop_price, new_stop))
        if self._next_scale() is None:
            self._runner_active = True
        reason = (f"Scale @ {mult:.2f}x — sold {sell_pct:.0f}% (orig), "
                  f"stop→{self.current_stop_price:.3f}x, "
                  f"{self.current_position_pct:.0f}% left"
                  + (" [runner active]" if self._runner_active else ""))
        ev = self._log("scale", price, mult, sell_pct, reason)
        return {
            "action": "scale",
            "sell_pct": sell_pct,
            "sell_fraction_of_remaining": self._frac_of_remaining(sell_pct),
            "new_stop": self.current_stop_price,
            "remaining_pct": self.current_position_pct,
            "scale_tier": len(self.scales_done),
            "reason": ev.reason,
        }

    def _stop_from_rule(self, rule, price):
        mode = rule[0]
        if mode == "mult":
            return self.entry_price * rule[1]
        if mode == "current":
            return price
        raise ValueError(f"unknown stop rule {rule!r}")

    def _frac_of_remaining(self, sell_pct_of_orig):
        """'% of original' → 'fraction of what we currently HOLD' (what a swap
        actually executes)."""
        held_before = self.current_position_pct + sell_pct_of_orig
        return round(sell_pct_of_orig / held_before, 6) if held_before > 0 else 0.0

    def _do_exit(self, price, mult):
        sold = self.current_position_pct
        reason = (f"Stop hit @ {mult:.2f}x (≤ stop {self.current_stop_price:.3f}x) "
                  f"— close remaining {sold:.0f}%")
        ev = self._log("exit", price, mult, sold, reason)
        self.current_position_pct = 0.0
        self.active = False
        return {"action": "exit", "sell_pct": sold,
                "sell_fraction_of_remaining": 1.0,
                "new_stop": self.current_stop_price, "remaining_pct": 0.0,
                "reason": ev.reason}

    def _log(self, kind, price, mult, sell_pct, reason):
        ev = ScaleEvent(
            timestamp=datetime.utcnow().isoformat(timespec="seconds"),
            kind=kind, price=price, multiple=round(mult, 4), sell_pct=sell_pct,
            new_stop=self.current_stop_price,
            remaining_pct=self.current_position_pct, reason=reason,
        )
        self.events.append(ev)
        return ev

    def get_trade_log(self):
        """All events as plain dicts — easy to persist for the learning loop."""
        return [ev.__dict__ for ev in self.events]


# ─────────────────────────────────────────────────────────────────────────────
# Example usage
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mgr = SmartScalingExitManager().start_position(1.0, "4am")
    for m in [1.0, 1.5, 2.0, 3.3, 4.5, 8.0, 12.0, 9.5, 7.8]:   # multiples
        res = mgr.on_price_update(m)
        if res["action"]:
            print(f"{m:5.2f}x  {res['action'].upper():5s}  {res['reason']}")
    print("\nlog:")
    for ev in mgr.get_trade_log():
        print(ev)
