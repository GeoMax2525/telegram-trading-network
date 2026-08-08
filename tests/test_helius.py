"""
test_helius.py — locks the RPC-fallback behavior in bot/helius.py: when the
primary (Helius) RPC exhausts its retries, rpc_call/rpc_batch should try
FALLBACK_RPC_URL (if configured) before giving up, so one provider's rate
limit or quota exhaustion can't fully stall trade execution or balance
checks. With no fallback configured, behavior must be unchanged (default).

Run:  pytest tests/test_helius.py -v
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bot.helius as helius


def _mk_retry_stub(responses: dict[str, object]):
    """responses maps url -> return value (or an Exception instance to raise)."""
    calls: list[tuple[str, str]] = []  # (url, label)

    async def _stub(method, url, *, json=None, retries=4, label="helius"):
        calls.append((url, label))
        result = responses.get(url, None)
        if isinstance(result, Exception):
            raise result
        return result

    return _stub, calls


def test_rpc_call_no_fallback_configured_returns_none_on_exhaustion(monkeypatch):
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "")
    stub, calls = _mk_retry_stub({})  # primary always returns None
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_call("getBalance", ["addr"]))

    assert result is None
    assert len(calls) == 1  # never tried a fallback -- none configured


def test_rpc_call_falls_back_when_primary_exhausted(monkeypatch):
    monkeypatch.setattr(helius, "HELIUS_RPC_URL", "https://primary.example/rpc")
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "https://fallback.example/rpc")
    ok_response = {"jsonrpc": "2.0", "id": 1, "result": {"value": 42}}
    stub, calls = _mk_retry_stub({
        "https://primary.example/rpc": None,
        "https://fallback.example/rpc": ok_response,
    })
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_call("getBalance", ["addr"]))

    assert result == ok_response
    assert [c[0] for c in calls] == ["https://primary.example/rpc", "https://fallback.example/rpc"]


def test_rpc_call_does_not_use_fallback_when_primary_succeeds(monkeypatch):
    monkeypatch.setattr(helius, "HELIUS_RPC_URL", "https://primary.example/rpc")
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "https://fallback.example/rpc")
    ok_response = {"jsonrpc": "2.0", "id": 1, "result": {"value": 42}}
    stub, calls = _mk_retry_stub({"https://primary.example/rpc": ok_response})
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_call("getBalance", ["addr"]))

    assert result == ok_response
    assert len(calls) == 1  # fallback never touched when primary works


def test_rpc_call_returns_none_when_both_primary_and_fallback_exhausted(monkeypatch):
    monkeypatch.setattr(helius, "HELIUS_RPC_URL", "https://primary.example/rpc")
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "https://fallback.example/rpc")
    stub, calls = _mk_retry_stub({
        "https://primary.example/rpc": None,
        "https://fallback.example/rpc": None,
    })
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_call("getBalance", ["addr"]))

    assert result is None
    assert len(calls) == 2


def test_rpc_batch_falls_back_when_primary_exhausted(monkeypatch):
    monkeypatch.setattr(helius, "HELIUS_RPC_URL", "https://primary.example/rpc")
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "https://fallback.example/rpc")
    ok_response = [{"jsonrpc": "2.0", "id": 1, "result": 1}]
    stub, calls = _mk_retry_stub({
        "https://primary.example/rpc": None,
        "https://fallback.example/rpc": ok_response,
    })
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_batch([{"method": "getBalance"}]))

    assert result == ok_response
    assert [c[0] for c in calls] == ["https://primary.example/rpc", "https://fallback.example/rpc"]


def test_rpc_batch_no_fallback_configured_returns_empty_list(monkeypatch):
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "")
    stub, calls = _mk_retry_stub({})
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_batch([{"method": "getBalance"}]))

    assert result == []
    assert len(calls) == 1


def test_rpc_call_json_rpc_error_in_body_does_not_trigger_fallback(monkeypatch):
    """A JSON-RPC-level error (HTTP 200, error in body) is a real answer from
    the primary, not exhaustion -- must not spend a fallback call on it."""
    monkeypatch.setattr(helius, "HELIUS_RPC_URL", "https://primary.example/rpc")
    monkeypatch.setattr(helius, "FALLBACK_RPC_URL", "https://fallback.example/rpc")
    error_response = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "invalid params"}}
    stub, calls = _mk_retry_stub({"https://primary.example/rpc": error_response})
    monkeypatch.setattr(helius, "_request_with_retry", stub)

    result = asyncio.run(helius.rpc_call("getBalance", ["addr"]))

    assert result is None
    assert len(calls) == 1  # only the primary was called -- error is a real response, not exhaustion
