"""
test_claude_engineer_proactive.py — locks the safety-critical behavior of
Phase 3 (self-initiated code shipping): the confidence gate (only
high-confidence PROPOSE ever attempts anything), the notify branching for
all four scan outcomes, and — the property this whole design hinges on —
that the proactive sub-cap can never consume the reactive path's slot.

Uses an isolated SQLite DB (same pattern as the manual verification done
throughout this session) rather than the real dev DB. Claude API calls
and git/subprocess work are mocked via monkeypatch — this suite never
hits the network or touches a real repo.

Async code invoked via asyncio.run() inside plain sync test functions,
same convention as the other claude_engineer test files (no
pytest-asyncio dependency, matches what CI installs).

Run:  pytest tests/test_claude_engineer_proactive.py -v
"""

import asyncio
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import database.models as m
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

import bot.agents.claude_engineer as eng


def _isolated_db():
    """Point database.models at a fresh, isolated in-memory-ish SQLite DB
    for this test run. Mirrors the manual isolation pattern used
    throughout this session -- never touches the real dev/prod DB."""
    test_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    m.engine = test_engine
    m.AsyncSessionLocal = async_sessionmaker(test_engine, expire_on_commit=False)

    async def _create():
        async with test_engine.begin() as conn:
            await conn.run_sync(m.Base.metadata.create_all)

    asyncio.run(_create())
    return test_engine


# ── _build_scan_digest: pure function, no DB ────────────────────────────────

class _FakeReview:
    def __init__(self, review_json, trade_count=10, applied=False):
        self.generated_at = datetime.utcnow()
        self.trade_count = trade_count
        self.review_json = review_json
        self.applied = applied


class _FakeAttempt:
    def __init__(self, status, gate_results_json="[]", requested_by=None, description="test"):
        self.requested_at = datetime.utcnow()
        self.requested_by = requested_by
        self.description = description
        self.status = status
        self.rolled_back = False
        self.gate_results_json = gate_results_json


class _FakeScan:
    def __init__(self, action="NONE", worth_mentioning=False):
        self.scanned_at = datetime.utcnow()
        self.action = action
        self.description = "prior idea"
        self.worth_mentioning = worth_mentioning
        self.reason = "prior reason"


def test_build_scan_digest_shape():
    reviews = [_FakeReview('{"summary": "ok day", "recommendations": [], "no_change_explanation": "nothing to change"}')]
    attempts = [_FakeAttempt("failed_gates", gate_results_json='[{"gate": "syntax", "passed": false}]')]
    scans = [_FakeScan()]
    digest = eng._build_scan_digest(reviews, attempts, scans)
    assert digest["recent_reviews"][0]["summary"] == "ok day"
    assert digest["recent_engineer_attempts"][0]["first_failed_gate"] == "syntax"
    assert len(digest["recent_scans"]) == 1


def test_build_scan_digest_handles_malformed_review_json():
    reviews = [_FakeReview("not valid json{{{")]
    digest = eng._build_scan_digest(reviews, [], [])
    assert digest["recent_reviews"][0]["summary"] is None  # doesn't crash, just empty


def test_build_scan_digest_empty_inputs():
    digest = eng._build_scan_digest([], [], [])
    assert digest == {"recent_reviews": [], "recent_engineer_attempts": [], "recent_scans": []}


# ── _preflight cap arithmetic: the core safety property ─────────────────────

def test_proactive_cap_exhausted_does_not_block_reactive():
    """The property this whole design hinges on: a background loop that
    already used its own carved-out slot can NEVER block a human's
    direct request."""
    _isolated_db()

    async def scenario():
        await m.log_engineer_attempt(
            requested_by=eng.PROACTIVE_SENTINEL, description="proactive used its slot",
            status="shipped", files_changed=[], commit_sha="x", revert_sha=None,
            gate_results=[], cost_usd=0.05, duration_sec=5, rolled_back=False,
        )
        cfg = {"claude_engineer_enabled": 1.0, "claude_engineer_max_changes_per_day": 2,
               "claude_engineer_daily_budget_usd": 3.0}

        # A second proactive attempt should be blocked by its sub-cap.
        ok, _ = await eng._preflight(cfg, "fake-pat", sentinel=eng.PROACTIVE_SENTINEL,
                                       per_source_max=1, per_source_budget=1.0)
        assert not ok

        # But reactive (no sentinel) must still have room.
        ok, _ = await eng._preflight(cfg, "fake-pat")
        assert ok

    asyncio.run(scenario())


def test_shared_cap_exhausted_blocks_both():
    _isolated_db()

    async def scenario():
        for i in range(2):
            await m.log_engineer_attempt(
                requested_by=555, description=f"reactive #{i}", status="shipped",
                files_changed=[], commit_sha=f"x{i}", revert_sha=None,
                gate_results=[], cost_usd=0.05, duration_sec=5, rolled_back=False,
            )
        cfg = {"claude_engineer_enabled": 1.0, "claude_engineer_max_changes_per_day": 2,
               "claude_engineer_daily_budget_usd": 3.0}
        ok, reason = await eng._preflight(cfg, "fake-pat")
        assert not ok and "daily change limit" in reason

    asyncio.run(scenario())


def test_daily_budget_enforced_independent_of_count_cap():
    _isolated_db()

    async def scenario():
        await m.log_engineer_attempt(
            requested_by=555, description="expensive", status="shipped",
            files_changed=[], commit_sha="z", revert_sha=None,
            gate_results=[], cost_usd=3.5, duration_sec=5, rolled_back=False,
        )
        # count=1, well under a generous count cap -- budget is the only limiter
        cfg = {"claude_engineer_enabled": 1.0, "claude_engineer_max_changes_per_day": 10,
               "claude_engineer_daily_budget_usd": 3.0}
        ok, reason = await eng._preflight(cfg, "fake-pat")
        assert not ok and "budget" in reason

    asyncio.run(scenario())


def test_preflight_requires_enabled_flag():
    _isolated_db()

    async def scenario():
        cfg = {"claude_engineer_enabled": 0.0}
        ok, reason = await eng._preflight(cfg, "fake-pat")
        assert not ok and reason == "claude_engineer_enabled=0"

    asyncio.run(scenario())


def test_preflight_requires_pat():
    _isolated_db()

    async def scenario():
        cfg = {"claude_engineer_enabled": 1.0, "claude_engineer_max_changes_per_day": 2,
               "claude_engineer_daily_budget_usd": 3.0}
        ok, reason = await eng._preflight(cfg, "")
        assert not ok and "PAT" in reason

    asyncio.run(scenario())


# ── propose_code_change_proactive: confidence gating + notify branching ─────

def _base_cfg():
    return {
        "claude_engineer_enabled": 1.0, "claude_engineer_proactive_enabled": 1.0,
        "claude_engineer_max_changes_per_day": 2.0, "claude_engineer_daily_budget_usd": 3.0,
        "claude_engineer_proactive_max_per_day": 1.0, "claude_engineer_proactive_daily_budget_usd": 1.0,
        "claude_engineer_max_gate_retries": 3.0, "claude_engineer_max_turns_per_attempt": 12.0,
        "claude_engineer_max_files_changed": 4.0, "claude_engineer_max_lines_changed": 150.0,
        "claude_engineer_health_poll_max_wait_sec": 600.0,
    }


def test_medium_confidence_propose_never_attempts(monkeypatch):
    """A PROPOSE at anything less than high confidence must be treated
    identically to NONE -- never kicks off an attempt."""
    _isolated_db()
    notify_calls = []
    attempt_calls = []

    async def fake_get_params(*names):
        return _base_cfg()

    async def fake_scan():
        return {"action": "PROPOSE", "description": "risky change", "confidence": "medium",
                "reason": "not fully sure", "worth_mentioning": True}

    async def fake_notify(text, chat_id=None):
        notify_calls.append(text)

    async def fake_run_attempt(*a, **kw):
        attempt_calls.append(a)
        return {"attempt_id": 1, "status": "shipped", "cost_usd": 0.1}

    monkeypatch.setattr("database.models.get_params", fake_get_params)
    monkeypatch.setattr(eng, "run_opportunity_scan", fake_scan)
    monkeypatch.setattr(eng, "_run_attempt", fake_run_attempt)
    monkeypatch.setattr("bot.ecconos.announce.post_as_ecconos", fake_notify)

    asyncio.run(eng.propose_code_change_proactive())

    assert attempt_calls == [], "medium confidence must NEVER trigger an attempt"
    assert len(notify_calls) == 1  # worth_mentioning still surfaces the idea
    assert "risky change" not in notify_calls[0] or "noticed" in notify_calls[0].lower()


def test_none_not_worth_mentioning_produces_zero_messages(monkeypatch):
    _isolated_db()
    notify_calls = []

    async def fake_get_params(*names):
        return _base_cfg()

    async def fake_scan():
        return {"action": "NONE", "description": "", "confidence": "low",
                "reason": "nothing stood out", "worth_mentioning": False}

    async def fake_notify(text, chat_id=None):
        notify_calls.append(text)

    monkeypatch.setattr("database.models.get_params", fake_get_params)
    monkeypatch.setattr(eng, "run_opportunity_scan", fake_scan)
    monkeypatch.setattr("bot.ecconos.announce.post_as_ecconos", fake_notify)

    asyncio.run(eng.propose_code_change_proactive())

    assert notify_calls == [], "the common case (nothing notable) must produce NO message"


def test_high_confidence_propose_attempts_and_notifies_twice(monkeypatch):
    """High-confidence PROPOSE should: notify once before starting
    ('working on something'), run the attempt, and _run_attempt's own
    notify (mocked here) handles the completion message separately."""
    _isolated_db()
    monkeypatch.setenv("ECCONOS_ENGINEER_GITHUB_PAT", "fake-pat-for-test")
    notify_calls = []
    attempt_calls = []

    async def fake_get_params(*names):
        return _base_cfg()

    async def fake_scan():
        return {"action": "PROPOSE", "description": "add a missing null check", "confidence": "high",
                "reason": "saw this fail 3 times in recent attempts", "worth_mentioning": True}

    async def fake_notify(text, chat_id=None):
        notify_calls.append(text)

    async def fake_run_attempt(description, requested_by, notify, cfg, pat):
        attempt_calls.append((description, requested_by))
        return {"attempt_id": 42, "status": "shipped", "cost_usd": 0.15}

    monkeypatch.setattr("database.models.get_params", fake_get_params)
    monkeypatch.setattr(eng, "run_opportunity_scan", fake_scan)
    monkeypatch.setattr(eng, "_run_attempt", fake_run_attempt)
    monkeypatch.setattr("bot.ecconos.announce.post_as_ecconos", fake_notify)

    asyncio.run(eng.propose_code_change_proactive())

    assert len(attempt_calls) == 1
    assert attempt_calls[0] == ("add a missing null check", eng.PROACTIVE_SENTINEL)
    assert len(notify_calls) == 1  # the "working on something" pre-announcement
    assert "add a missing null check" in notify_calls[0]


def test_capped_high_confidence_propose_surfaces_idea_but_does_not_attempt(monkeypatch):
    """High confidence, but the proactive slot is already used today --
    must surface the idea rather than silently dropping it, and must NOT attempt."""
    _isolated_db()
    monkeypatch.setenv("ECCONOS_ENGINEER_GITHUB_PAT", "fake-pat-for-test")
    notify_calls = []
    attempt_calls = []

    async def scenario():
        # Pre-consume the proactive sub-cap (max_per_day=1)
        await m.log_engineer_attempt(
            requested_by=eng.PROACTIVE_SENTINEL, description="already used today's slot",
            status="shipped", files_changed=[], commit_sha="x", revert_sha=None,
            gate_results=[], cost_usd=0.05, duration_sec=5, rolled_back=False,
        )

        async def fake_get_params(*names):
            return _base_cfg()

        async def fake_scan():
            return {"action": "PROPOSE", "description": "another good idea", "confidence": "high",
                    "reason": "genuinely confident", "worth_mentioning": True}

        async def fake_notify(text, chat_id=None):
            notify_calls.append(text)

        async def fake_run_attempt(*a, **kw):
            attempt_calls.append(a)
            return {"attempt_id": 1, "status": "shipped", "cost_usd": 0.1}

        import unittest.mock as um
        with um.patch("database.models.get_params", fake_get_params), \
             um.patch.object(eng, "run_opportunity_scan", fake_scan), \
             um.patch.object(eng, "_run_attempt", fake_run_attempt), \
             um.patch("bot.ecconos.announce.post_as_ecconos", fake_notify):
            await eng.propose_code_change_proactive()

    asyncio.run(scenario())

    assert attempt_calls == [], "capped means it must not actually attempt"
    assert len(notify_calls) == 1
    assert "another good idea" in notify_calls[0]
    assert "next window" in notify_calls[0].lower() or "used" in notify_calls[0].lower()


def test_proactive_noop_when_underlying_pipeline_disabled(monkeypatch):
    """claude_engineer_enabled=0 must block proactive too, even if
    claude_engineer_proactive_enabled=1 -- strict superset gate. Must not
    even spend a scan call."""
    _isolated_db()
    scan_calls = []

    async def fake_get_params(*names):
        cfg = _base_cfg()
        cfg["claude_engineer_enabled"] = 0.0
        return cfg

    async def fake_scan():
        scan_calls.append(1)
        return {"action": "NONE", "description": "", "confidence": "low", "reason": "", "worth_mentioning": False}

    monkeypatch.setattr("database.models.get_params", fake_get_params)
    monkeypatch.setattr(eng, "run_opportunity_scan", fake_scan)

    asyncio.run(eng.propose_code_change_proactive())

    assert scan_calls == [], "must not spend a scan call while the underlying pipeline is off"
