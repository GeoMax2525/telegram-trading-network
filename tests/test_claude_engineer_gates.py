"""
test_claude_engineer_gates.py — locks the behavior of the 6-gate test
sequence Ecconos's self-shipping code pipeline runs before ever pushing
anything (see bot/agents/claude_engineer_gates.py).

Includes a regression test that reproduces the EXACT fastmcp/httpx
dependency conflict from the real incident earlier this session (adding
fastmcp to requirements.txt without checking it resolved took the whole
live bot offline) -- proving gate 2 (dependency_dry_run) would have
actually caught it before it ever shipped.

Async gate functions are invoked via asyncio.run() inside plain sync test
functions, deliberately not using pytest-asyncio -- CI (.github/workflows/
tests.yml) only installs plain `pytest`, and this suite is also what
claude_engineer's own gate 5 runs against itself, so it must work with
exactly that dependency set.

Run:  pytest tests/test_claude_engineer_gates.py -v
"""

import asyncio
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.agents.claude_engineer_gates import (
    gate_diff_size, gate_syntax, gate_dependency_dry_run, gate_imports,
    _path_to_module, _truncate,
)


def _mk_git_repo() -> str:
    d = tempfile.mkdtemp()
    subprocess.run(["git", "init", "-q"], cwd=d, check=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=d, check=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=d, check=True)
    with open(os.path.join(d, "base.py"), "w") as f:
        f.write("x = 1\n")
    subprocess.run(["git", "add", "-A"], cwd=d, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=d, check=True)
    return d


# ── gate_diff_size ───────────────────────────────────────────────────────────

def test_diff_size_rejects_too_many_files():
    d = _mk_git_repo()
    for i in range(6):
        with open(os.path.join(d, f"new_{i}.py"), "w") as f:
            f.write("y = 1\n")
    result = asyncio.run(gate_diff_size(d, max_files=4, max_lines=1000))
    assert not result.passed
    assert "files" in result.output


def test_diff_size_accepts_small_change():
    d = _mk_git_repo()
    with open(os.path.join(d, "small.py"), "w") as f:
        f.write("z = 1\n")
    result = asyncio.run(gate_diff_size(d, max_files=4, max_lines=150))
    assert result.passed


def test_diff_size_rejects_no_changes():
    d = _mk_git_repo()
    result = asyncio.run(gate_diff_size(d, max_files=4, max_lines=150))
    assert not result.passed


# ── gate_syntax ──────────────────────────────────────────────────────────────

def test_syntax_catches_real_syntax_error():
    d = _mk_git_repo()
    with open(os.path.join(d, "broken.py"), "w") as f:
        f.write("def foo(:\n    pass\n")
    result = asyncio.run(gate_syntax(d, ["broken.py"]))
    assert not result.passed
    assert "broken.py" in result.output


def test_syntax_passes_valid_file():
    d = _mk_git_repo()
    with open(os.path.join(d, "fine.py"), "w") as f:
        f.write("def foo():\n    return 1\n")
    result = asyncio.run(gate_syntax(d, ["fine.py"]))
    assert result.passed


def test_syntax_ignores_deleted_files():
    d = _mk_git_repo()
    result = asyncio.run(gate_syntax(d, ["never_existed.py"]))
    assert result.passed


# ── gate_dependency_dry_run: THE regression test ────────────────────────────

def test_dependency_dry_run_catches_the_real_fastmcp_incident():
    """Reproduces the exact conflict from the real incident: httpx==0.27.2
    pinned alongside fastmcp>=3.0 (which needs httpx>=0.28.1) is
    unresolvable. This is the check that would have caught it before it
    ever reached production, if it had existed that day."""
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "requirements.txt"), "w") as f:
        f.write("httpx==0.27.2\nfastmcp>=3.0\n")
    result = asyncio.run(gate_dependency_dry_run(d, sys.executable))
    assert not result.passed, (
        "dependency_dry_run should have FAILED on the known-conflicting "
        "httpx==0.27.2 + fastmcp>=3.0 pairing -- if this now passes, "
        "either the conflict was resolved upstream or this gate is broken"
    )


def test_dependency_dry_run_passes_resolvable_requirements():
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "requirements.txt"), "w") as f:
        f.write("python-dotenv==1.2.2\n")
    result = asyncio.run(gate_dependency_dry_run(d, sys.executable))
    assert result.passed


# ── gate_imports / _path_to_module ──────────────────────────────────────────

def test_path_to_module_conversion():
    assert _path_to_module("bot/agents/foo.py") == "bot.agents.foo"
    assert _path_to_module("main.py") == "main"
    assert _path_to_module("bot/agents/__init__.py") == "bot.agents"
    assert _path_to_module("requirements.txt") is None
    assert _path_to_module("../escape.py") is None
    assert _path_to_module("./sneaky.py") is None


def test_gate_imports_catches_broken_import():
    d = _mk_git_repo()
    with open(os.path.join(d, "bad_import.py"), "w") as f:
        f.write("import this_module_does_not_exist_anywhere\n")
    result = asyncio.run(gate_imports(d, sys.executable, ["bad_import.py"]))
    assert not result.passed


def test_gate_imports_passes_clean_module():
    d = _mk_git_repo()
    with open(os.path.join(d, "clean_mod.py"), "w") as f:
        f.write("import json\nx = json.dumps({})\n")
    result = asyncio.run(gate_imports(d, sys.executable, ["clean_mod.py"]))
    assert result.passed


# ── misc ─────────────────────────────────────────────────────────────────────

def test_truncate_keeps_short_text_unchanged():
    assert _truncate("short") == "short"


def test_truncate_truncates_long_text():
    long_text = "x" * 10_000
    out = _truncate(long_text)
    assert len(out) < len(long_text)
    assert "truncated" in out
