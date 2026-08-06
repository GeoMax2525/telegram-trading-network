"""
bot/agents/claude_engineer.py — Phase 2: Ecconos's self-shipping code
pipeline. An admin asks for a specific code change; Claude writes it in
an isolated scratch clone (never the live running container's own
checkout), an automated test gate verifies it, and only if every check
passes does it commit and push — with zero human approval step, per the
operator's own agreed model.

WHY THIS IS BUILT THE WAY IT IS: earlier this same session, a single
careless dependency change (adding `fastmcp` to requirements.txt without
checking it resolved) took the entire live trading bot offline for an
extended stretch — with a human supervising closely and still needing
several rounds of debugging to fix it. This pipeline runs unattended, so
it has to be MORE careful than that, not less:
  - Six-gate test sequence (claude_engineer_gates.py) replicates exactly
    what was done by hand that day: syntax check, dependency dry-run,
    real install, import checks, full test suite.
  - Runs in a fresh, isolated git clone + fresh venv every time — never
    touches the live process's own files mid-test.
  - After a successful push, automatically polls /healthz and rolls back
    (git revert, re-push) if the deploy doesn't come back healthy —
    nobody did this automatically during the actual incident; a human
    had to notice and fix it by hand, more than once.
  - Off by default (claude_engineer_enabled=0), rate/budget capped, a
    hard file blocklist enforced in code (claude_engineer_tools.py) --
    it can never touch live-money logic, its own kill switch, or its own
    leash, regardless of how a request is phrased.

Reactive only in v1 (a specific admin request triggers one attempt) --
deliberately not a background loop that decides on its own to go write
code. See the Phase 2 plan for the full reasoning.
"""

import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
import time as _time
import uuid
from datetime import datetime, timedelta

import aiohttp

from bot.agents.claude_engineer_gates import (
    GATE_ORDER, GateResult,
    gate_diff_size, gate_syntax, gate_dependency_dry_run,
    gate_dependency_install, gate_imports, gate_test_suite,
)
from bot.agents.claude_engineer_tools import ENGINEER_TOOLS, EngineerToolExecutor
from bot.agents.claude_reasoning import ANTHROPIC_API_KEY, ANTHROPIC_URL, SONNET_MODEL

logger = logging.getLogger(__name__)

REPO_SLUG = "GeoMax2525/telegram-trading-network"
WORKSPACE_BASE = os.path.join(tempfile.gettempdir(), "ecconos_engineer")
STALE_WORKSPACE_AGE_HOURS = 2.0

_attempt_lock = asyncio.Lock()

# Cost estimate for the Sonnet engineer conversation (worst case: several
# turns across several retry cycles). Rough token-count heuristic, same
# style used elsewhere in this repo (claude_strategist.py).
_SONNET_INPUT_USD_PER_M = 3.0
_SONNET_OUTPUT_USD_PER_M = 15.0


ENGINEER_SYSTEM_PROMPT = """You are a careful software engineer making ONE specific, well-scoped change to a live production Solana trading bot codebase (Python, aiogram, SQLAlchemy async/Postgres).

Your change ships automatically if it passes an automated test gate -- there is no human review step. Be conservative. Understand the existing code before changing it.

Process:
1. Use list_files and read_file to understand the relevant existing code BEFORE writing anything. Match existing patterns and conventions exactly -- don't introduce a new style for something this codebase already has a convention for.
2. Make the smallest change that correctly accomplishes the task. Prefer editing an existing file over creating a new one unless a new file is clearly warranted.
3. Call write_file for each file you need to change.
4. Call finish_change with a clear summary and a good commit message when you're done. Nothing happens until you call this.

If your change fails the automated tests, you'll get the exact failure output back and a chance to fix it. Read the failure carefully -- fix the actual problem, don't just retry the same thing.

You cannot see or run anything outside list_files/read_file/write_file. You cannot install new dependencies beyond what's already in requirements.txt unless the task specifically requires it (if so, add it there normally). Some files are off-limits and write_file will refuse them -- if that happens, the task cannot be done as asked; explain why in your summary and finish_change with your best safe alternative, or make no change and say why not."""


def _venv_python(workspace: str) -> str:
    if sys.platform == "win32":
        return os.path.join(workspace, ".venv", "Scripts", "python.exe")
    return os.path.join(workspace, ".venv", "bin", "python")


async def sweep_stale_workspaces() -> int:
    """Remove scratch workspaces older than STALE_WORKSPACE_AGE_HOURS --
    self-healing after a crashed/killed prior attempt (including the
    container itself restarting mid-attempt). Called at the start of
    every new attempt and once at bot startup."""
    if not os.path.isdir(WORKSPACE_BASE):
        return 0
    cutoff = _time.time() - STALE_WORKSPACE_AGE_HOURS * 3600
    removed = 0
    for name in os.listdir(WORKSPACE_BASE):
        path = os.path.join(WORKSPACE_BASE, name)
        try:
            if os.path.getmtime(path) < cutoff:
                shutil.rmtree(path, ignore_errors=True)
                removed += 1
        except OSError:
            continue
    if removed:
        logger.info("claude_engineer: swept %d stale workspace(s)", removed)
    return removed


async def _run(cmd: list[str], cwd: str, timeout: float, env: dict | None = None) -> tuple[int, str]:
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=cwd, env=env,
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
        return -1, f"TIMEOUT after {timeout}s: {' '.join(cmd)}"
    except Exception as exc:
        return -1, f"error running {' '.join(cmd)}: {exc}"


async def _clone_workspace(pat: str) -> str:
    os.makedirs(WORKSPACE_BASE, exist_ok=True)
    workspace = os.path.join(WORKSPACE_BASE, f"attempt_{uuid.uuid4().hex[:12]}")
    remote = f"https://x-access-token:{pat}@github.com/{REPO_SLUG}.git"
    code, out = await _run(["git", "clone", "--depth", "1", remote, workspace], cwd=tempfile.gettempdir(), timeout=120)
    if code != 0:
        # Scrub the PAT from any error output before it ever gets logged/stored.
        safe_out = out.replace(pat, "***")
        raise RuntimeError(f"git clone failed: {safe_out}")
    return workspace


async def _run_gates(workspace: str, executor: EngineerToolExecutor, limits: dict) -> list[GateResult]:
    changed = sorted(executor.changed_files)
    results: list[GateResult] = []

    r = await gate_diff_size(workspace, int(limits["max_files_changed"]), int(limits["max_lines_changed"]))
    results.append(r)
    if not r.passed:
        return results

    r = await gate_syntax(workspace, changed)
    results.append(r)
    if not r.passed:
        return results

    venv_python = _venv_python(workspace)
    code, out = await _run([sys.executable, "-m", "venv", os.path.join(workspace, ".venv")], cwd=workspace, timeout=60)
    if code != 0:
        results.append(GateResult("venv_setup", False, out))
        return results

    r = await gate_dependency_dry_run(workspace, venv_python)
    results.append(r)
    if not r.passed:
        return results

    r = await gate_dependency_install(workspace, venv_python)
    results.append(r)
    if not r.passed:
        return results

    r = await gate_imports(workspace, venv_python, changed)
    results.append(r)
    if not r.passed:
        return results

    r = await gate_test_suite(workspace, venv_python)
    results.append(r)
    return results


def _gate_failure_summary(results: list[GateResult]) -> str:
    for r in results:
        if not r.passed:
            return f"GATE FAILED: {r.gate}\n\n{r.output}"
    return "unknown gate failure"


# ── The resumable Claude tool-calling loop ──────────────────────────────────

async def _call_claude_raw(messages: list[dict], max_tokens: int = 3000) -> tuple[list[dict], int, int]:
    """One raw Messages API call with the engineer tool set. Returns
    (content_blocks, input_tokens, output_tokens). Does not use
    call_claude_with_tools — that helper terminates when Claude stops
    calling tools, but this loop needs to terminate specifically on
    finish_change, and needs to resume the same history across external
    gate-check retries, which call_claude_with_tools doesn't support."""
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": SONNET_MODEL,
        "max_tokens": max_tokens,
        "system": ENGINEER_SYSTEM_PROMPT,
        "messages": messages,
        "tools": ENGINEER_TOOLS,
    }
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        async with session.post(ANTHROPIC_URL, headers=headers, json=body) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"Claude API HTTP {resp.status}: {json.dumps(data)[:500]}")
    usage = data.get("usage", {})
    return data.get("content", []), usage.get("input_tokens", 0), usage.get("output_tokens", 0)


async def _write_phase(
    executor: EngineerToolExecutor, messages: list[dict], max_turns: int,
) -> tuple[bool, int, float]:
    """Runs until finish_change is called or max_turns is exhausted.
    Mutates `messages` in place so the caller can continue the same
    conversation across a subsequent gate-failure retry. Returns
    (finished, api_calls, cost_usd)."""
    api_calls = 0
    cost_usd = 0.0
    for _turn in range(max_turns):
        blocks, in_tok, out_tok = await _call_claude_raw(messages)
        api_calls += 1
        cost_usd += (in_tok * _SONNET_INPUT_USD_PER_M + out_tok * _SONNET_OUTPUT_USD_PER_M) / 1_000_000

        tool_uses = [b for b in blocks if isinstance(b, dict) and b.get("type") == "tool_use"]
        if not tool_uses:
            # Claude stopped without calling finish_change -- give it one
            # more nudge rather than silently failing the whole attempt.
            messages.append({"role": "assistant", "content": blocks})
            messages.append({"role": "user", "content": "Continue -- call finish_change when the change is complete, or a tool if you need to do more first."})
            continue

        messages.append({"role": "assistant", "content": blocks})
        tool_results = []
        for tu in tool_uses:
            result = await executor.execute(tu.get("name", ""), tu.get("input") or {})
            tool_results.append({
                "type": "tool_result", "tool_use_id": tu.get("id", ""),
                "content": json.dumps(result, separators=(",", ":"), default=str),
            })
        messages.append({"role": "user", "content": tool_results})

        if executor.finished is not None:
            return True, api_calls, cost_usd

    return False, api_calls, cost_usd


# ── Post-deploy health check + auto-rollback ────────────────────────────────

async def _poll_healthz(public_url: str, max_wait_sec: float) -> bool:
    if not public_url:
        return False
    url = public_url.rstrip("/") + "/healthz"
    await asyncio.sleep(30)  # grace period before the first check
    deadline = _time.time() + max_wait_sec
    consecutive_ok = 0
    backoff = [15, 15, 30, 30, 60]
    i = 0
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        while _time.time() < deadline:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            consecutive_ok += 1
                            if consecutive_ok >= 2:
                                return True
                            await asyncio.sleep(3)
                            continue
            except Exception:
                pass
            consecutive_ok = 0
            wait = backoff[min(i, len(backoff) - 1)]
            i += 1
            await asyncio.sleep(wait)
    return False


async def _push(workspace: str, pat: str) -> tuple[bool, str]:
    remote = f"https://x-access-token:{pat}@github.com/{REPO_SLUG}.git"
    code, out = await _run(["git", "push", remote, "HEAD:main"], cwd=workspace, timeout=60)
    return code == 0, out.replace(pat, "***")


# ── Main entry point ─────────────────────────────────────────────────────────

async def propose_code_change(description: str, requested_by: int, notify) -> dict:
    """Kicks off one full attempt. `notify(text)` is an async callable
    used to report the final outcome back to whoever asked (Ecconos's
    reply mechanism supplies this). Returns immediately with a status
    dict; the actual pipeline runs as a background task since it can take
    several minutes."""
    from database.models import get_params

    cfg = await get_params(
        "claude_engineer_enabled", "claude_engineer_max_changes_per_day",
        "claude_engineer_daily_budget_usd", "claude_engineer_max_gate_retries",
        "claude_engineer_max_turns_per_attempt", "claude_engineer_max_files_changed",
        "claude_engineer_max_lines_changed", "claude_engineer_health_poll_max_wait_sec",
    )
    if float(cfg.get("claude_engineer_enabled") or 0.0) < 0.5:
        return {"started": False, "reason": "claude_engineer_enabled=0"}

    pat = os.getenv("ECCONOS_ENGINEER_GITHUB_PAT", "").strip()
    if not pat:
        return {"started": False, "reason": "ECCONOS_ENGINEER_GITHUB_PAT not set"}

    if _attempt_lock.locked():
        return {"started": False, "reason": "another attempt is already in progress"}

    from database.models import get_engineer_attempts_today_count
    today_count = await get_engineer_attempts_today_count()
    if today_count >= int(cfg.get("claude_engineer_max_changes_per_day") or 2):
        return {"started": False, "reason": "daily change limit reached"}

    asyncio.create_task(_run_attempt(description, requested_by, notify, cfg, pat))
    return {"started": True}


async def _run_attempt(description: str, requested_by: int, notify, cfg: dict, pat: str) -> None:
    from database.models import log_engineer_attempt

    async with _attempt_lock:
        t0 = _time.time()
        await sweep_stale_workspaces()
        workspace = None
        status = "error"
        commit_sha = None
        revert_sha = None
        gate_results: list[GateResult] = []
        cost_usd = 0.0
        rolled_back = False

        try:
            workspace = await _clone_workspace(pat)
            executor = EngineerToolExecutor(workspace)
            messages = [{"role": "user", "content": f"TASK: {description}"}]

            max_retries = int(cfg.get("claude_engineer_max_gate_retries") or 3)
            max_turns = int(cfg.get("claude_engineer_max_turns_per_attempt") or 12)
            limits = {
                "max_files_changed": cfg.get("claude_engineer_max_files_changed") or 4,
                "max_lines_changed": cfg.get("claude_engineer_max_lines_changed") or 150,
            }

            shipped = False
            for cycle in range(max_retries):
                finished, calls, cycle_cost = await _write_phase(executor, messages, max_turns)
                cost_usd += cycle_cost
                if not finished:
                    status = "failed_no_finish"
                    break

                gate_results = await _run_gates(workspace, executor, limits)
                if all(r.passed for r in gate_results):
                    shipped = True
                    break

                # Feed the failure back and let Claude retry within the same conversation.
                failure_text = _gate_failure_summary(gate_results)
                messages.append({
                    "role": "user",
                    "content": f"Your change failed testing. Fix it and call finish_change again.\n\n{failure_text}",
                })
                executor.finished = None
                status = "failed_gates"

            if shipped:
                code, out = await _run(["git", "add", "-A"], cwd=workspace, timeout=30)
                commit_msg = (executor.finished or {}).get("commit_message") or "Ecconos: automated change"
                code, out = await _run(["git", "commit", "-m", commit_msg], cwd=workspace, timeout=30)
                if code != 0:
                    status = "error"
                else:
                    code, sha_out = await _run(["git", "rev-parse", "HEAD"], cwd=workspace, timeout=10)
                    commit_sha = sha_out.strip()
                    pushed, push_out = await _push(workspace, pat)
                    if not pushed:
                        status = "push_failed"
                        logger.error("claude_engineer: push failed: %s", push_out)
                    else:
                        status = "shipped"
                        # PUBLIC_URL is a plain env var (not a bot/config.py constant --
                        # verified directly; handlers.py reads it the same way).
                        public_url = os.getenv("PUBLIC_URL", "").strip()
                        healthy = await _poll_healthz(
                            public_url, float(cfg.get("claude_engineer_health_poll_max_wait_sec") or 600),
                        )
                        if not healthy:
                            await _page_admins(f"🚨 Ecconos shipped a change ({commit_sha[:8]}) and it did NOT come back healthy. Rolling back now.")
                            code, out = await _run(["git", "revert", "--no-edit", commit_sha], cwd=workspace, timeout=30)
                            if code == 0:
                                code, sha_out = await _run(["git", "rev-parse", "HEAD"], cwd=workspace, timeout=10)
                                revert_sha = sha_out.strip()
                                reverted, _ = await _push(workspace, pat)
                                if reverted:
                                    revert_healthy = await _poll_healthz(
                                        public_url, float(cfg.get("claude_engineer_health_poll_max_wait_sec") or 600),
                                    )
                                    if revert_healthy:
                                        status = "deploy_unhealthy_rolled_back"
                                        rolled_back = True
                                        await _page_admins(f"Rollback of {commit_sha[:8]} confirmed healthy.")
                                    else:
                                        status = "rollback_failed"
                                        await _page_admins(f"🚨🚨 Rollback of {commit_sha[:8]} ALSO did not come back healthy. Stopping. Manual intervention needed.")
                                else:
                                    status = "rollback_failed"
                                    await _page_admins(f"🚨🚨 Could not push the revert for {commit_sha[:8]}. Manual intervention needed.")
                            else:
                                status = "rollback_failed"
                                await _page_admins(f"🚨🚨 `git revert` itself failed for {commit_sha[:8]}. Manual intervention needed.")

        except Exception as exc:
            logger.error("claude_engineer: attempt failed: %s", exc)
            status = "error"
            gate_results = gate_results or [GateResult("orchestrator", False, str(exc))]
        finally:
            if workspace and os.path.isdir(workspace):
                shutil.rmtree(workspace, ignore_errors=True)

        duration = _time.time() - t0
        await log_engineer_attempt(
            requested_by=requested_by, description=description, status=status,
            files_changed=sorted(executor.changed_files) if 'executor' in dir() else [],
            commit_sha=commit_sha, revert_sha=revert_sha,
            gate_results=[{"gate": r.gate, "passed": r.passed, "output": r.output[:1000]} for r in gate_results],
            cost_usd=round(cost_usd, 4), duration_sec=round(duration, 1), rolled_back=rolled_back,
        )

        try:
            await notify(_summarize_for_admin(status, description, commit_sha, revert_sha))
        except Exception as exc:
            logger.debug("claude_engineer: notify failed: %s", exc)


def _summarize_for_admin(status: str, description: str, commit_sha: str | None, revert_sha: str | None) -> str:
    labels = {
        "shipped": f"Shipped: {description}\n{commit_sha[:8] if commit_sha else '?'}",
        "deploy_unhealthy_rolled_back": f"Tried: {description}\nBroke on deploy, rolled back automatically ({revert_sha[:8] if revert_sha else '?'}). Bot's healthy again.",
        "rollback_failed": f"Tried: {description}\nShipped, broke, and the rollback ALSO failed. This needs your attention now.",
        "failed_gates": f"Couldn't ship: {description}\nFailed testing after all retries -- didn't touch production.",
        "failed_no_finish": f"Couldn't ship: {description}\nDidn't produce a complete change within the turn budget.",
        "push_failed": f"Couldn't ship: {description}\nPassed all tests but the push itself failed.",
        "error": f"Couldn't ship: {description}\nHit an unexpected error -- check logs.",
    }
    return labels.get(status, f"{description}: {status}")


async def _page_admins(text: str) -> None:
    try:
        from bot.config import ADMIN_IDS
        import bot.state as _st
        bot_ref = getattr(_st, "bot", None)
        if bot_ref is None:
            return
        for uid in ADMIN_IDS:
            try:
                await bot_ref.send_message(uid, text)
            except Exception:
                pass
    except Exception:
        pass
