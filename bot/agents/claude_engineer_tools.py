"""
bot/agents/claude_engineer_tools.py — Phase 2: the file-operation tools
Claude gets when writing a code change for Ecconos's self-shipping
pipeline (bot/agents/claude_engineer.py).

Every tool is path-contained to one isolated scratch git clone (never the
live running container's own checkout) and enforces a hard file
blocklist — both checked in CODE here, not left to prompt instruction,
since this pipeline ships with zero human review.
"""

import logging
import os

logger = logging.getLogger(__name__)

# Files this pipeline can never write to, regardless of what it's asked to
# do or how well-reasoned the request sounds. Enforced here, in the tool
# executor itself — not something a clever prompt can talk its way around.
ENGINEER_HARD_BLOCKED_FILES = frozenset({
    "mcp_server/server.py",        # WRITE_BLOCKLIST + all live write tools
    "bot/live_mirror.py",          # live-money arming/execution logic
    "bot/live_guard.py",           # live circuit breakers
    "database/models.py",          # WRITE_BLOCKLIST-adjacent gates, AGENT_PARAM_DEFAULTS,
                                    # trade_mode/live_trading_armed columns
    "bot/agents/claude_engineer.py",        # can't edit its own leash
    "bot/agents/claude_engineer_gates.py",  # can't edit its own leash
    "bot/agents/claude_engineer_tools.py",  # can't edit its own leash
    "Dockerfile",                  # build-process changes = today's exact failure mode
    ".github/workflows/tests.yml", # can't weaken its own CI safety net
    "main.py",                     # wires every loop incl. kill-switch checks;
                                    # an import-time failure here is fatal at boot
})

_MAX_READ_BYTES = 500_000
_SKIP_DIR_NAMES = {".git", ".venv", "__pycache__", "node_modules"}


def _resolve_and_contain(workspace_root: str, rel_path: str) -> str | None:
    """Resolve rel_path against workspace_root; return the absolute path
    ONLY if it's still inside workspace_root. Returns None on any
    traversal attempt (../, absolute paths, symlink escape, etc.)."""
    root = os.path.realpath(workspace_root)
    candidate = os.path.realpath(os.path.join(root, rel_path))
    if candidate != root and not candidate.startswith(root + os.sep):
        return None
    return candidate


def _normalize_rel(rel_path: str) -> str | None:
    """Returns the normalized relative path, or None if rel_path is
    absolute (Unix or Windows style) -- rejected outright rather than
    silently reinterpreted as relative. An absolute path staying
    "contained" by accident is still confusing, unpredictable behavior
    for security-adjacent code; reject it explicitly instead."""
    p = rel_path.replace("\\", "/")
    if p.startswith("/") or (len(p) >= 2 and p[1] == ":"):  # "/x" or "C:x"
        return None
    return p


class EngineerToolExecutor:
    """One instance per attempt. Tracks which files were actually written
    (changed_files) so the orchestrator can hand that list to the import
    gate without re-deriving it from git diff."""

    def __init__(self, workspace_root: str):
        self.workspace_root = workspace_root
        self.changed_files: set[str] = set()
        self.finished: dict | None = None  # set by finish_change

    async def execute(self, name: str, tool_input: dict) -> dict:
        if name == "list_files":
            return self._list_files(tool_input.get("path", "."))
        if name == "read_file":
            return self._read_file(tool_input.get("path", ""))
        if name == "write_file":
            return self._write_file(tool_input.get("path", ""), tool_input.get("content", ""))
        if name == "finish_change":
            self.finished = {
                "summary": tool_input.get("summary", ""),
                "commit_message": tool_input.get("commit_message", ""),
            }
            return {"ok": True, "note": "change marked finished"}
        return {"error": f"unknown tool '{name}'"}

    def _list_files(self, path: str) -> dict:
        rel = _normalize_rel(path)
        if rel is None:
            return {"error": "absolute paths are rejected -- use a path relative to the repo root"}
        full = _resolve_and_contain(self.workspace_root, rel)
        if full is None:
            return {"error": "path escapes workspace"}
        if not os.path.isdir(full):
            return {"error": f"not a directory: {path}"}
        out = []
        for dirpath, dirnames, filenames in os.walk(full):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIR_NAMES]
            for fn in filenames:
                if fn.endswith(".db") or fn.endswith(".pyc"):
                    continue
                abs_fp = os.path.join(dirpath, fn)
                rel_fp = os.path.relpath(abs_fp, self.workspace_root).replace("\\", "/")
                out.append(rel_fp)
        out.sort()
        return {"files": out}

    def _read_file(self, path: str) -> dict:
        rel = _normalize_rel(path)
        if rel is None:
            return {"error": "absolute paths are rejected -- use a path relative to the repo root"}
        full = _resolve_and_contain(self.workspace_root, rel)
        if full is None:
            return {"error": "path escapes workspace"}
        if not os.path.isfile(full):
            return {"error": f"not a file: {path}"}
        size = os.path.getsize(full)
        if size > _MAX_READ_BYTES:
            return {"error": f"file too large ({size} bytes, max {_MAX_READ_BYTES})"}
        try:
            with open(full, encoding="utf-8") as f:
                return {"content": f.read()}
        except UnicodeDecodeError:
            return {"error": "file is not valid UTF-8 text (binary?)"}

    def _write_file(self, path: str, content: str) -> dict:
        rel = _normalize_rel(path)
        if rel is None:
            return {"error": "absolute paths are rejected -- use a path relative to the repo root"}
        if rel in ENGINEER_HARD_BLOCKED_FILES:
            return {
                "error": f"'{rel}' is hard-blocked -- this pipeline can never "
                         f"write to it, regardless of reasoning. Not something "
                         f"a different explanation will change.",
            }
        full = _resolve_and_contain(self.workspace_root, rel)
        if full is None:
            return {"error": "path escapes workspace"}
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(content)
        self.changed_files.add(rel)
        return {"ok": True, "path": rel, "bytes_written": len(content.encode("utf-8"))}


ENGINEER_TOOLS = [
    {
        "name": "list_files",
        "description": "List files under a directory in the workspace (recursive). Skips .git/.venv/__pycache__/node_modules and binary db files.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Directory path, relative to repo root. Defaults to \".\" (repo root)."}},
        },
    },
    {
        "name": "read_file",
        "description": "Read a text file's full content, path relative to repo root.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Write (create or overwrite) a text file, path relative to repo root. Refuses hard-blocked files and any path outside the repo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "finish_change",
        "description": "Call this when the change is complete and ready for testing. This is the explicit stop signal -- the pipeline will not proceed to testing until you call this.",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {"type": "string", "description": "What changed and why, for the log."},
                "commit_message": {"type": "string", "description": "Git commit message to use if all gates pass."},
            },
            "required": ["summary", "commit_message"],
        },
    },
]
