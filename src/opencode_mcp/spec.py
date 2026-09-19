"""Spec-driven catalog: operationId -> method/path, groups, search.

Future-proofing: tools are NOT hardcoded per endpoint. At startup we read the
live /doc OpenAPI; the generic `opencode_api` tool routes any operationId.
If opencode adds endpoints, they appear automatically. If it removes one,
callers get a clear error naming the opencode version.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# operationId prefix -> group. Order matters (longest prefix first).
GROUP_PREFIXES: list[tuple[str, str]] = [
    ("v2.session.", "v2-session"),
    ("v2.", "v2"),
    ("experimental.session.", "experimental"),
    ("experimental.workspace.", "experimental"),
    ("experimental.", "experimental"),
    ("session.", "session"),
    ("permission.", "permission"),
    ("question.", "question"),
    ("tui.", "tui"),
    ("pty.", "pty"),
    ("vcs.", "vcs"),
    ("find.", "find"),
    ("file.", "file"),
    ("tool.", "tool"),
    ("mcp.", "mcp"),
    ("provider.", "provider"),
    ("config.", "config"),
    ("project.", "project"),
    ("global.", "global"),
    ("command.", "command"),
    ("auth.", "auth"),
    ("app.", "app"),
    ("path.", "message"),  # path.get is tiny; lump with misc below via fallback
    ("v2", "v2"),
    ("sync.", "sync"),
    ("worktree.", "experimental"),
    ("instance.", "global"),
]

# Groups hidden from default docs (noisy / TUI-only / unstable).
NOISY_GROUPS = {"tui", "pty", "sync", "experimental", "v2", "v2-session"}

# SSE streaming ops must not be called via generic request tool (would hang).
SSE_OPERATIONS = {"event.subscribe", "global.event", "v2.event.subscribe", "v2.session.events"}


@dataclass
class Operation:
    """One opencode endpoint: how to call it and which group it belongs to."""

    id: str
    method: str
    path: str
    summary: str
    description: str
    group: str

    @property
    def tool_name(self) -> str:
        """MCP-style name for an operationId: session.prompt -> opencode_session_prompt."""
        return "opencode_" + self.id.replace(".", "_")


def group_of(operation_id: str) -> str:
    """Bucket an operationId (session.prompt -> 'session') via longest-prefix match."""
    for prefix, group in GROUP_PREFIXES:
        if operation_id.startswith(prefix):
            return group
    # fallback: first dotted segment
    return operation_id.split(".")[0] if "." in operation_id else "misc"


def build_catalog(spec: dict[str, Any]) -> dict[str, Operation]:
    """Flatten live OpenAPI paths into {operationId: Operation}; skips entries without ids."""
    catalog: dict[str, Operation] = {}
    for path, methods in (spec.get("paths") or {}).items():
        if not isinstance(methods, dict):
            continue
        for method, op in methods.items():
            if not isinstance(op, dict) or "operationId" not in op:
                continue
            oid = op["operationId"]
            catalog[oid] = Operation(
                id=oid,
                method=method.upper(),
                path=path,
                summary=str(op.get("summary", ""))[:160],
                description=str(op.get("description", ""))[:300],
                group=group_of(oid),
            )
    return catalog


def search_catalog(catalog: dict[str, Operation], query: str, limit: int = 20) -> list[Operation]:
    """AND-search ops over id+summary+path; id matches rank first, then alphabetical."""
    tokens = query.lower().split()
    if not tokens:
        return sorted(catalog.values(), key=lambda o: o.id)[:limit]
    scored: list[tuple[int, Operation]] = []
    for op in catalog.values():
        hay = f"{op.id} {op.summary} {op.path}".lower()
        if all(t in hay for t in tokens):
            # prefer id matches, then summary
            score = 0 if all(t in op.id.lower() for t in tokens) else 1
            scored.append((score, op))
    scored.sort(key=lambda t: (t[0], t[1].id))
    return [op for _, op in scored[:limit]]


def visible_groups(tool_groups_env: str) -> set[str] | None:
    """None means show all (OPENCODE_TOOL_GROUPS=all)."""
    v = tool_groups_env.strip().lower()
    if v in ("all", "*"):
        return None
    return {g.strip() for g in v.split(",") if g.strip()}
