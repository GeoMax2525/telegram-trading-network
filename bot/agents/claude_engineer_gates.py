"""
bot/agents/claude_engineer_gates.py — Phase 2: the test gate for Ecconos's
self-shipping code pipeline.

Six checks, cheapest first, ALL must pass before anything gets committed
or pushed. This directly replicates the exact verification sequence a
human did by hand before every push during the fastmcp/dependency
incident chain earlier this same session (see bot/agents/claude_engineer.py
for the full orchestration and why each of these exists).

Pure, side-effect-scoped functions: given a workspace path (+ changed file
list where relevant), run one check, return a GateResult. No Claude calls,
no DB writes — independently unit-testable (see
tests/test_claude_engineer_gates.py), including a regression test that
reproduces the exact fastmcp/websockets conflict from earlier today, to
prove gate 2 (dependency_dry_run) would have actually caught it.
"""

import ast
import asyncio
import os
from dataclasses import dataclass

# Max characters of subprocess output kept per gate result — long pytest/pip
# output shouldn't blow up a DB row or the retry-feedback prompt sent back
# to Claude.
_MAX_OUTPUT_CHARS = 4000

GATE_ORDER = [
    "diff_size", "syntax", "dependency_dry_run",
    "dependency_install", "imports", "test_suite",
]


@dataclass
class GateResult:
    gate: str
    passed: bool
    output: str = ""


def _truncate(text: str) -> str:
    text = text or ""
    if len(text) <= _MAX_OUTPUT_CHARS:
        return text
    return text[:_MAX_OUTPUT_CHARS] + f"\n... [truncated, {len(text)} chars total]"


async def _run(cmd: list[str], cwd: str, timeout: float) -> tuple[int, str]:
    """Run a subprocess, return (returncode, combined stdout+stderr).
    Same asyncio.create_subprocess_exec pattern used elsewhere in this repo
    (bot/agents/gmgn_agent.py)."""
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, stdout.decode(errors="replace")
    except asyncio.TimeoutError:
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        return -1, f"TIMEOUT after {timeout}s running: {' '.join(cmd)}"
    except Exception as exc:
        return -1, f"subprocess error running {' '.join(cmd)}: {exc}"


# ── Gate 0: diff size — cheapest, no subprocess, reject before spending any
# real time on a change too large to trust unattended in v1 ─────────────────

async def gate_diff_size(workspace: str, max_files: int, max_lines: int) -> GateResult:
    # `git diff --stat` alone only shows modified TRACKED files -- brand
    # new files from write_file are untracked and invisible to it until
    # staged. `git add -A` first so new + modified + deleted all show up
    # (found via a real failing test, not assumed).
    add_code, add_out = await _run(["git", "add", "-A"], cwd=workspace, timeout=30)
    if add_code != 0:
        return GateResult("diff_size", False, _truncate(f"git add -A failed:\n{add_out}"))
    code, out = await _run(["git", "diff", "--cached", "--stat"], cwd=workspace, timeout=30)
    if code != 0:
        return GateResult("diff_size", False, _truncate(f"git diff --cached --stat failed:\n{out}"))
    file_lines = [l for l in out.splitlines() if "|" in l]
    n_files = len(file_lines)
    total_changed = sum(l.count("+") + l.count("-") for l in file_lines)
    if n_files == 0:
        return GateResult("diff_size", False, "no changes detected (did finish_change run with nothing written?)")
    if n_files > max_files:
        return GateResult("diff_size", False, f"touches {n_files} files, max is {max_files}\n{out}")
    if total_changed > max_lines:
        return GateResult("diff_size", False, f"~{total_changed} changed lines (diffstat markers), max is {max_lines}\n{out}")
    return GateResult("diff_size", True, _truncate(out))


# ── Gate 1: ast.parse every changed .py file — cheap, catches syntax errors
# before spending any subprocess/install time ────────────────────────────────

async def gate_syntax(workspace: str, changed_files: list[str]) -> GateResult:
    errors = []
    for rel_path in changed_files:
        if not rel_path.endswith(".py"):
            continue
        full_path = os.path.join(workspace, rel_path)
        if not os.path.isfile(full_path):
            continue  # deleted file — nothing to parse
        try:
            with open(full_path, encoding="utf-8") as f:
                ast.parse(f.read(), filename=rel_path)
        except SyntaxError as exc:
            errors.append(f"{rel_path}: {exc}")
    if errors:
        return GateResult("syntax", False, _truncate("\n".join(errors)))
    return GateResult("syntax", True, "all changed .py files parse cleanly")


# ── Gate 2: pip install --dry-run — the exact check that would have caught
# today's real fastmcp/websockets incident before it ever reached production ─

async def gate_dependency_dry_run(workspace: str, venv_python: str) -> GateResult:
    code, out = await _run(
        [venv_python, "-m", "pip", "install", "--dry-run", "-r", "requirements.txt"],
        cwd=workspace, timeout=120,
    )
    return GateResult("dependency_dry_run", code == 0, _truncate(out))


# ── Gate 3: real pip install — only meaningful to run after gate 2 passes,
# needed so gates 4/5 run against correctly-resolved dependencies ───────────

async def gate_dependency_install(workspace: str, venv_python: str) -> GateResult:
    code, out = await _run(
        [venv_python, "-m", "pip", "install", "-r", "requirements.txt"],
        cwd=workspace, timeout=300,
    )
    return GateResult("dependency_install", code == 0, _truncate(out) if code != 0 else "installed cleanly")


# ── Gate 4: import every changed module ─────────────────────────────────────

def _path_to_module(rel_path: str) -> str | None:
    """bot/agents/foo.py -> bot.agents.foo ; main.py -> main ; None for
    non-.py files or anything that looks like it'd escape the package tree."""
    if not rel_path.endswith(".py"):
        return None
    rel_path = rel_path.replace("\\", "/")
    if rel_path.endswith("/__init__.py"):
        rel_path = rel_path[: -len("/__init__.py")]
        if not rel_path:
            return None
    else:
        rel_path = rel_path[: -len(".py")]
    parts = rel_path.split("/")
    if any(p in ("", "..", ".") or p.startswith(".") for p in parts):
        return None
    return ".".join(parts)


async def gate_imports(workspace: str, venv_python: str, changed_files: list[str]) -> GateResult:
    errors = []
    for rel_path in changed_files:
        full_path = os.path.join(workspace, rel_path)
        if not os.path.isfile(full_path):
            continue
        module = _path_to_module(rel_path)
        if not module:
            continue
        code, out = await _run(
            [venv_python, "-c", f"import {module}"], cwd=workspace, timeout=30,
        )
        if code != 0:
            errors.append(f"import {module}:\n{out}")
    if errors:
        return GateResult("imports", False, _truncate("\n\n".join(errors)))
    return GateResult("imports", True, "all changed modules import cleanly")


# ── Gate 5: full existing test suite. pytest itself isn't a production
# dependency (matches .github/workflows/tests.yml's own "pip install pytest"
# step — not added to requirements.txt for the same reason) ─────────────────

async def gate_test_suite(workspace: str, venv_python: str) -> GateResult:
    install_code, install_out = await _run(
        [venv_python, "-m", "pip", "install", "pytest"], cwd=workspace, timeout=60,
    )
    if install_code != 0:
        return GateResult("test_suite", False, _truncate(f"failed to install pytest:\n{install_out}"))
    code, out = await _run(
        [venv_python, "-m", "pytest", "tests/", "-v"], cwd=workspace, timeout=180,
    )
    return GateResult("test_suite", code == 0, _truncate(out))
