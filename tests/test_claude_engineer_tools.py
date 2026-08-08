"""
test_claude_engineer_tools.py — locks the safety-critical behavior of
Ecconos's code-writing tool executor: path containment, the hard file
blocklist, and absolute-path rejection. These enforcements exist in code
specifically because this pipeline ships with zero human review — a bug
here is a real sandbox-escape or self-modification risk, not just a UX
issue.

Async methods invoked via asyncio.run() inside plain sync test functions
(same convention as test_claude_engineer_gates.py — no pytest-asyncio
dependency, matches what CI actually installs).

Run:  pytest tests/test_claude_engineer_tools.py -v
"""

import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.agents.claude_engineer_tools import (
    EngineerToolExecutor, ENGINEER_HARD_BLOCKED_FILES, _normalize_rel, _resolve_and_contain,
)


def _mk_executor() -> EngineerToolExecutor:
    return EngineerToolExecutor(tempfile.mkdtemp())


def test_normal_write_succeeds():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("write_file", {"path": "bot/agents/new_thing.py", "content": "x = 1\n"}))
    assert r.get("ok") is True
    assert "bot/agents/new_thing.py" in ex.changed_files


def test_every_hard_blocked_file_is_refused():
    for blocked in ENGINEER_HARD_BLOCKED_FILES:
        ex = _mk_executor()
        r = asyncio.run(ex.execute("write_file", {"path": blocked, "content": "evil = True"}))
        assert "error" in r and "hard-blocked" in r["error"], f"{blocked} was not refused: {r}"
        assert blocked not in ex.changed_files


def test_relative_path_traversal_refused():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("write_file", {"path": "../../etc/passwd", "content": "evil"}))
    assert "error" in r and "escapes workspace" in r["error"]


def test_deeply_nested_traversal_refused():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("write_file", {"path": "a/b/c/../../../../../../etc/passwd", "content": "evil"}))
    assert "error" in r


def test_unix_absolute_path_rejected():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("write_file", {"path": "/etc/passwd", "content": "evil"}))
    assert "error" in r and "absolute paths are rejected" in r["error"]


def test_windows_absolute_path_rejected():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("write_file", {"path": "C:\\Windows\\evil.py", "content": "evil"}))
    assert "error" in r and "absolute paths are rejected" in r["error"]


def test_read_file_returns_real_content():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "readme_test.py", "content": "hello = 1\n"}))
    r = asyncio.run(ex.execute("read_file", {"path": "readme_test.py"}))
    assert r.get("content") == "hello = 1\n"


def test_read_file_traversal_refused():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("read_file", {"path": "../../../etc/shadow"}))
    assert "error" in r


def test_list_files_finds_written_file():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "sub/dir/thing.py", "content": "1\n"}))
    r = asyncio.run(ex.execute("list_files", {"path": "."}))
    assert "sub/dir/thing.py" in r["files"]


def test_finish_change_records_state():
    ex = _mk_executor()
    assert ex.finished is None
    r = asyncio.run(ex.execute("finish_change", {"summary": "did a thing", "commit_message": "fix: thing"}))
    assert r.get("ok") is True
    assert ex.finished == {"summary": "did a thing", "commit_message": "fix: thing"}


# ── edit_file: added after a real sandbox test showed write_file's
# full-content-required interface can fail on large files (the model's
# response got cut off mid-write repeatedly, since write_file requires
# reproducing the ENTIRE file on every call) ────────────────────────────────

def test_edit_file_replaces_unique_match():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "target.py", "content": "x = 1\ny = 2\nz = 3\n"}))
    r = asyncio.run(ex.execute("edit_file", {"path": "target.py", "old_string": "y = 2", "new_string": "y = 99"}))
    assert r.get("ok") is True
    content = asyncio.run(ex.execute("read_file", {"path": "target.py"}))["content"]
    assert content == "x = 1\ny = 99\nz = 3\n"


def test_edit_file_rejects_nonexistent_file():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("edit_file", {"path": "nope.py", "old_string": "a", "new_string": "b"}))
    assert "error" in r and "does not exist" in r["error"]


def test_edit_file_rejects_missing_old_string():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "target.py", "content": "hello\n"}))
    r = asyncio.run(ex.execute("edit_file", {"path": "target.py", "old_string": "not present", "new_string": "x"}))
    assert "error" in r and "not found" in r["error"]


def test_edit_file_rejects_ambiguous_old_string():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "target.py", "content": "dup\ndup\n"}))
    r = asyncio.run(ex.execute("edit_file", {"path": "target.py", "old_string": "dup", "new_string": "x"}))
    assert "error" in r and "appears 2 times" in r["error"]


def test_edit_file_respects_hard_blocklist():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("edit_file", {"path": "database/models.py", "old_string": "a", "new_string": "b"}))
    assert "error" in r and "hard-blocked" in r["error"]


def test_edit_file_respects_path_containment():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("edit_file", {"path": "../../etc/passwd", "old_string": "a", "new_string": "b"}))
    assert "error" in r and "escapes workspace" in r["error"]


def test_edit_file_tracks_changed_files():
    ex = _mk_executor()
    asyncio.run(ex.execute("write_file", {"path": "target.py", "content": "a = 1\n"}))
    ex.changed_files.clear()  # write_file already tracked it; reset to isolate edit_file's own tracking
    asyncio.run(ex.execute("edit_file", {"path": "target.py", "old_string": "a = 1", "new_string": "a = 2"}))
    assert "target.py" in ex.changed_files


def test_unknown_tool_returns_error():
    ex = _mk_executor()
    r = asyncio.run(ex.execute("delete_everything", {}))
    assert "error" in r


def test_normalize_rel_rejects_absolute_forms():
    assert _normalize_rel("/etc/passwd") is None
    assert _normalize_rel("C:\\evil.py") is None
    assert _normalize_rel("bot/agents/foo.py") == "bot/agents/foo.py"


def test_resolve_and_contain_blocks_escape():
    root = tempfile.mkdtemp()
    assert _resolve_and_contain(root, "../../../etc/passwd") is None
    assert _resolve_and_contain(root, "safe/path.py") is not None
