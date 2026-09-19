"""MCP server: curated core tools + generic full passthrough + journal resources.

Transport: stdio only. Backend: managed `opencode serve` subprocess.
Lossless rule: the SQLite journal is truth; SSE + notifications are hints.
Context rule: curated lean tools resident; all 160+ upstream ops reachable
via one generic `opencode_api` + `opencode_spec_search` (progressive discovery).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastmcp import FastMCP

from . import __version__
from .client import OpencodeClient, OpencodeError
from .config import Config
from .journal import Journal
from .spec import SSE_OPERATIONS, build_catalog, search_catalog, visible_groups
from .sse import run_consumer

log = logging.getLogger("opencode-mcp")

mcp = FastMCP(name="opencode-mcp")

MAX_FILE_PAGE_CHARS = 50_000
MAX_MODELS_PER_PROVIDER = 40


class State:
    """Process-wide singleton: config, backend client, journal, spec catalog.

    ensure() lazily boots everything once (backend + catalog + SSE consumer);
    shutdown() stops SSE and kills the managed serve. All tools call ensure()
    first, so the server works with any FastMCP lifespan behavior.
    """
    def __init__(self) -> None:
        self.config = Config()
        self.client = OpencodeClient(self.config)
        self.journal = Journal(self.config.journal_path)
        self.catalog: dict = {}
        self._ready = False
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._sse_task: asyncio.Task | None = None

    async def ensure(self) -> None:
        """Idempotent boot: start serve, build catalog, launch SSE consumer (once)."""
        async with self._lock:
            if self._ready:
                return
            await self.client.start()
            self.catalog = build_catalog(self.client.spec)
            if not self.config.disable_sse:
                self._sse_task = asyncio.create_task(
                    run_consumer(self.client, self.journal, self._stop)
                )
            else:
                await self.journal.append("", "mcp.sse.disabled", {}, "mcp")
            self._ready = True

    async def shutdown(self) -> None:
        """Signal SSE stop, cancel consumer, close backend (called on stdio exit)."""
        self._stop.set()
        if self._sse_task is not None:
            self._sse_task.cancel()
        await self.client.close()


STATE = State()


# -- helpers -------------------------------------------------------------
def _trim(text: Any, limit: int) -> str:
    """Cap text at limit chars with a “…[truncated N chars]” tail (context saver)."""
    s = text if isinstance(text, str) else str(text)
    return s if len(s) <= limit else s[:limit] + f"\n…[truncated {len(s) - limit} chars]"


def _cap_obj(obj: Any, limit: int) -> Any:
    """Bound a structured result so one huge response cannot flood the agent's context."""
    try:
        raw = json.dumps(obj)
    except (TypeError, ValueError):
        return {"result": _trim(obj, limit)}
    if len(raw) <= limit:
        return obj
    return {"_truncated": True, "_chars": len(raw), "_preview": raw[:limit]}


def _parts_text(parts: Any, limit: int) -> str:
    """Flatten message parts to readable text: text verbatim, tools/files as [tag] lines.

    When the flattened form overflows the budget the assistant's text parts win, so a
    long reasoning block can never truncate away the actual reply.
    """
    if not isinstance(parts, list):
        return _trim(parts, limit)
    chunks: list[str] = []
    reply: list[str] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        t = p.get("type", "")
        if t == "text" and p.get("text"):
            chunks.append(str(p["text"]))
            reply.append(str(p["text"]))
        elif t in ("tool", "tool-result", "step", "reasoning") and p.get("text"):
            chunks.append(f"[{t}] {p['text']}")
        elif t == "file" and (p.get("path") or p.get("url")):
            chunks.append(f"[file] {p.get('path') or p.get('url')}")
        elif t:
            chunks.append(f"[{t}]")
    joined = "\n".join(chunks)
    if len(joined) > limit and reply:
        return _trim("\n".join(reply), limit)
    return _trim(joined, limit)


def _compact_message(msg: Any, max_chars: int) -> dict[str, Any]:
    """Shrink {info, parts} to agent-friendly shape (full JSON behind verbose)."""
    if not isinstance(msg, dict):
        return {"result": _trim(msg, max_chars)}
    info = msg.get("info", {}) if isinstance(msg.get("info"), dict) else {}
    parts = msg.get("parts", [])
    return {
        "message_id": info.get("id") or info.get("messageID"),
        "role": info.get("role"),
        "model": info.get("modelID") or info.get("model"),
        "agent": info.get("agent"),
        "error": info.get("error"),
        "text": _parts_text(parts, max_chars),
        "parts_count": len(parts) if isinstance(parts, list) else 0,
    }


def _model_arg(model: str | None) -> dict[str, str] | None:
    """Split 'provider/model' into opencode's {providerID, modelID} body shape (None if unset)."""
    if not model:
        return None
    provider, sep, mid = model.partition("/")
    if not sep or not provider or not mid:
        raise ValueError(
            f"model must be 'provider/model' (e.g. 'zai-coding-plan/glm-5.3'), got {model!r}"
        )
    return {"providerID": provider, "modelID": mid}


def _prompt_body(
    text: str,
    model: str | None,
    agent: str | None,
    variant: str | None,
) -> dict[str, Any]:
    """Build a session.prompt body: text part + optional model/agent/reasoning variant."""
    body: dict[str, Any] = {"parts": [{"type": "text", "text": text}]}
    m = _model_arg(model)
    if m:
        body["model"] = m
    if agent:
        body["agent"] = agent
    if variant:
        body["variant"] = variant
    return body


# -- health ---------------------------------------------------------------
@mcp.tool()
async def opencode_health() -> dict[str, Any]:
    """Check managed opencode server health, version, spec size, journal depth."""
    await STATE.ensure()
    data = await STATE.client.health()
    data["mcp_version"] = __version__
    tail = await STATE.journal.list(after_seq=0, limit=1)
    data["journal_max_seq"] = tail["max_seq"]
    return data


# -- sessions --------------------------------------------------------------
@mcp.tool()
async def opencode_session_create(title: str | None = None) -> dict[str, Any]:
    """Create a new opencode session. Returns session with id."""
    await STATE.ensure()
    body: dict[str, Any] = {}
    if title:
        body["title"] = title
    return await STATE.client.request("POST", "/session", body=body)


@mcp.tool()
async def opencode_session_list(limit: int = 20) -> list[Any]:
    """List recent opencode sessions (newest first, capped)."""
    await STATE.ensure()
    sessions = await STATE.client.request("GET", "/session")
    if isinstance(sessions, list):
        return sessions[: max(1, min(limit, 100))]
    return sessions


@mcp.tool()
async def opencode_session_get(session_id: str) -> dict[str, Any]:
    """Get session details by id (ses_*)."""
    await STATE.ensure()
    return await STATE.client.request("GET", "/session/{sessionID}", path_params={"sessionID": session_id})


@mcp.tool()
async def opencode_session_delete(session_id: str) -> dict[str, Any]:
    """Delete a session and its data."""
    await STATE.ensure()
    ok = await STATE.client.request(
        "DELETE", "/session/{sessionID}", path_params={"sessionID": session_id}
    )
    return {"deleted": ok}


@mcp.tool()
async def opencode_session_status(session_id: str | None = None) -> dict[str, Any]:
    """Get status (idle/busy) for all sessions, or one session if id given."""
    await STATE.ensure()
    statuses = await STATE.client.request("GET", "/session/status")
    if session_id and isinstance(statuses, dict):
        # opencode only reports sessions that are doing something; absence means idle.
        return {"session_id": session_id, "status": statuses.get(session_id, {"type": "idle"})}
    return statuses if isinstance(statuses, dict) else {"status": statuses}


# -- prompting (sync + async task bridge) -----------------------------------
@mcp.tool()
async def opencode_prompt(
    session_id: str,
    text: str,
    model: str | None = None,
    agent: str | None = None,
    variant: str | None = None,
) -> dict[str, Any]:
    """Send a prompt and WAIT for the reply. Default to max effort (explicit strong
    model + max variant) unless the user specifies effort. Long jobs: use prompt_async."""
    await STATE.ensure()
    body = _prompt_body(text, model, agent, variant)
    msg = await STATE.client.request(
        "POST", "/session/{sessionID}/message", path_params={"sessionID": session_id}, body=body
    )
    compact = _compact_message(msg, STATE.config.max_chars)
    compact["session_id"] = session_id
    if isinstance(msg, dict) and isinstance(msg.get("info"), dict):
        await STATE.journal.append(
            session_id, "mcp.prompt.completed", {"messageID": msg["info"].get("id")}, "mcp"
        )
    return compact


@mcp.tool()
async def opencode_prompt_async(
    session_id: str,
    text: str,
    model: str | None = None,
    agent: str | None = None,
    variant: str | None = None,
) -> dict[str, Any]:
    """Send a prompt WITHOUT waiting. Default to max effort (explicit strong model +
    max variant) unless the user specifies effort. Returns task handle; poll task_result
    + events_list until idle, reply to permissions_pending, finish with events_verify."""
    await STATE.ensure()
    body = _prompt_body(text, model, agent, variant)
    await STATE.client.request(
        "POST", "/session/{sessionID}/prompt_async", path_params={"sessionID": session_id}, body=body
    )
    seq = (await STATE.journal.list(after_seq=0, limit=1))["max_seq"]
    await STATE.journal.append(session_id, "mcp.prompt.submitted", {"text": text[:500]}, "mcp")
    return {"task_id": f"{session_id}:async", "session_id": session_id, "resume_seq": seq}


@mcp.tool()
async def opencode_task_result(session_id: str, limit: int = 5) -> dict[str, Any]:
    """Get latest assistant reply(s) for a session (use after prompt_async)."""
    await STATE.ensure()
    msgs = await STATE.client.request(
        "GET",
        "/session/{sessionID}/message",
        path_params={"sessionID": session_id},
        query={"limit": max(1, min(limit, 50))},
    )
    if not isinstance(msgs, list):
        return {"session_id": session_id, "messages": _trim(msgs, STATE.config.max_chars)}
    out = [_compact_message(m, STATE.config.max_chars) for m in msgs[::-1][:limit]]
    status = await STATE.client.request("GET", "/session/status")
    state = status.get(session_id, {}) if isinstance(status, dict) else {}
    return {"session_id": session_id, "session_state": state, "messages": out}


@mcp.tool()
async def opencode_task_cancel(session_id: str) -> dict[str, Any]:
    """Abort a running session (cancels the in-flight prompt)."""
    await STATE.ensure()
    ok = await STATE.client.request(
        "POST", "/session/{sessionID}/abort", path_params={"sessionID": session_id}
    )
    await STATE.journal.append(session_id, "mcp.prompt.aborted", {}, "mcp")
    return {"aborted": ok, "session_id": session_id}


@mcp.tool()
async def opencode_ask(
    text: str,
    title: str | None = None,
    model: str | None = None,
    agent: str | None = None,
    variant: str | None = None,
) -> dict[str, Any]:
    """One-shot: create a session and prompt it. Default to max effort (explicit strong
    model + max variant) unless the user specifies effort."""
    await STATE.ensure()
    session = await STATE.client.request("POST", "/session", body={"title": title} if title else {})
    sid = session.get("id", "") if isinstance(session, dict) else ""
    body = _prompt_body(text, model, agent, variant)
    msg = await STATE.client.request(
        "POST", "/session/{sessionID}/message", path_params={"sessionID": sid}, body=body
    )
    compact = _compact_message(msg, STATE.config.max_chars)
    compact["session_id"] = sid
    return compact


@mcp.tool()
async def opencode_messages(session_id: str, limit: int = 20) -> dict[str, Any]:
    """List messages in a session (compact text form)."""
    await STATE.ensure()
    msgs = await STATE.client.request(
        "GET",
        "/session/{sessionID}/message",
        path_params={"sessionID": session_id},
        query={"limit": max(1, min(limit, 100))},
    )
    if not isinstance(msgs, list):
        return {"session_id": session_id, "messages": []}
    return {
        "session_id": session_id,
        "messages": [_compact_message(m, STATE.config.max_chars) for m in msgs],
    }


# -- lossless event journal --------------------------------------------------
@mcp.tool()
async def opencode_events_list(
    after_seq: int = 0, limit: int = 50, session_id: str | None = None
) -> dict[str, Any]:
    """Read journaled opencode events from cursor. AUTHORITATIVE (polling is truth).

    Store next_seq and resume from it after any disconnect — zero loss.
    """
    await STATE.ensure()
    return await STATE.journal.list(after_seq=after_seq, limit=limit, session_id=session_id or None)


@mcp.tool()
async def opencode_events_verify(from_seq: int, to_seq: int) -> dict[str, Any]:
    """Assert no gaps in seq range [from_seq, to_seq]. Proves lossless resume."""
    await STATE.ensure()
    return await STATE.journal.verify(from_seq, to_seq)


# -- permissions (must not stall sessions) ------------------------------------
@mcp.tool()
async def opencode_permissions_pending() -> list[Any]:
    """List pending permission/question requests across sessions."""
    await STATE.ensure()
    try:
        pending = await STATE.client.request("GET", "/permission")
    except OpencodeError:
        pending = []
    out = pending if isinstance(pending, list) else [pending]
    # also surface recent permission events from journal for context
    return out[:50]


@mcp.tool()
async def opencode_permission_reply(request_id: str, response: str) -> dict[str, Any]:
    """Reply to a permission request: response 'allow'/'deny' (or 'once'/'always' if offered)."""
    await STATE.ensure()
    norm = response.strip().lower()
    mapping = {"allow": "allow", "always": "allow", "once": "allow", "deny": "deny", "reject": "deny"}
    body: dict[str, Any] = {"response": mapping.get(norm, norm)}
    ok = await STATE.client.request(
        "POST", "/permission/{requestID}/reply", path_params={"requestID": request_id}, body=body
    )
    await STATE.journal.append("", "mcp.permission.replied", {"requestID": request_id}, "mcp")
    return {"replied": ok}


# -- files ---------------------------------------------------------------------
@mcp.tool()
async def opencode_read_file(
    path: str, offset: int = 0, limit: int = 200, full: bool = False
) -> dict[str, Any]:
    """Read a file from the project (paginated; page capped at 50KB unless full=true)."""
    await STATE.ensure()
    STATE.config.assert_path_allowed(path)
    data = await STATE.client.request("GET", "/file/content", query={"path": path})
    content = ""
    if isinstance(data, dict):
        content = str(data.get("content", data))
    else:
        content = str(data)
    lines = content.splitlines()
    page = lines[offset : offset + max(1, min(limit, 500))]
    truncated = False
    if not full:
        text = "\n".join(page)
        if len(text) > MAX_FILE_PAGE_CHARS:
            page = text[:MAX_FILE_PAGE_CHARS].splitlines()
            truncated = True
    return {
        "path": path,
        "total_lines": len(lines),
        "offset": offset,
        "lines": page,
        "truncated": truncated,
    }


@mcp.tool()
async def opencode_search_text(pattern: str, limit: int = 30) -> list[Any]:
    """Search text in project files (ripgrep-backed)."""
    await STATE.ensure()
    res = await STATE.client.request("GET", "/find", query={"pattern": pattern})
    return res[:limit] if isinstance(res, list) else res


@mcp.tool()
async def opencode_find_files(query: str, limit: int = 30) -> list[Any]:
    """Find files/dirs by fuzzy name."""
    await STATE.ensure()
    res = await STATE.client.request(
        "GET", "/find/file", query={"query": query, "limit": max(1, min(limit, 200))}
    )
    return res[:limit] if isinstance(res, list) else res


# -- discovery -------------------------------------------------------------------
@mcp.tool()
async def opencode_agents() -> list[Any]:
    """List available opencode agents (build/plan/custom)."""
    await STATE.ensure()
    agents = await STATE.client.request("GET", "/agent")
    return agents if isinstance(agents, list) else [agents]


@mcp.tool()
async def opencode_models() -> dict[str, Any]:
    """List configured providers and their model ids (full detail via opencode_api)."""
    await STATE.ensure()
    try:
        data = await STATE.client.request("GET", "/config/providers")
    except OpencodeError:
        data = await STATE.client.request("GET", "/provider")
    providers = data.get("providers") if isinstance(data, dict) else None
    if not isinstance(providers, list):
        return _cap_obj(data, STATE.config.max_chars)
    defaults = data.get("default") if isinstance(data.get("default"), dict) else {}
    out = []
    for p in providers:
        if not isinstance(p, dict):
            continue
        models = p.get("models")
        ids = sorted(models) if isinstance(models, dict) else []
        entry: dict[str, Any] = {
            "id": p.get("id"),
            "name": p.get("name"),
            "default": defaults.get(p.get("id")),
            "model_count": len(ids),
            "models": ids[:MAX_MODELS_PER_PROVIDER],
        }
        if len(ids) > MAX_MODELS_PER_PROVIDER:
            entry["models_omitted"] = len(ids) - MAX_MODELS_PER_PROVIDER
        out.append(entry)
    return _cap_obj({"providers": out}, STATE.config.max_chars)


@mcp.tool()
async def opencode_spec_search(query: str = "", limit: int = 20) -> list[dict[str, Any]]:
    """Search the live opencode API catalog (all 160+ ops). Use before opencode_api."""
    await STATE.ensure()
    groups = visible_groups(STATE.config.tool_groups)
    ops = search_catalog(STATE.catalog, query, limit=max(1, min(limit, 50)))
    out = []
    for op in ops:
        if groups is not None and op.group not in groups and query.lower() not in op.id.lower():
            continue
        out.append(
            {"operation": op.id, "method": op.method, "path": op.path,
             "group": op.group, "summary": op.summary}
        )
    return out


@mcp.tool()
async def opencode_api(
    operation: str,
    path_params: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> Any:
    """Generic passthrough to ANY opencode operation by id (full API coverage).

    Find ids via opencode_spec_search. SSE streams are rejected (use events_list).
    Example: {"operation": "session.fork", "path_params": {"sessionID": "ses_x"}, "body": {}}.
    """
    await STATE.ensure()
    op = STATE.catalog.get(operation)
    if op is None:
        known = sorted(STATE.catalog)[:5]
        raise ValueError(
            f"unknown operation {operation!r} (opencode {STATE.client.version}); "
            f"try opencode_spec_search. e.g. {known}"
        )
    if operation in SSE_OPERATIONS:
        raise ValueError(f"{operation} is an SSE stream; use opencode_events_list instead")
    res = await STATE.client.request(op.method, op.path, path_params=path_params, query=query, body=body)
    if isinstance(res, str):
        return {"result": _trim(res, STATE.config.max_chars)}
    if isinstance(res, list):
        if len(res) > 50:
            return {"result": res[:50], "truncated": len(res) - 50}
        return {"result": res}
    if isinstance(res, dict):
        return _cap_obj(res, STATE.config.max_chars)
    return {"result": res}


# -- resources (subscribable reads over the journal) -------------------------------
@mcp.resource("opencode://health")
async def resource_health() -> str:
    """Resource view of backend health (same data as opencode_health, for subscribers)."""
    await STATE.ensure()
    return json.dumps(await STATE.client.health())


@mcp.resource("opencode://sessions/{session_id}/events")
async def resource_session_events(session_id: str) -> str:
    """Latest journaled events for a session (clients: poll events_list for lossless tail)."""
    await STATE.ensure()
    data = await STATE.journal.tail(
        limit=STATE.config.events_default_limit, session_id=session_id
    )
    return json.dumps(data)


@mcp.resource("opencode://sessions/{session_id}/messages")
async def resource_session_messages(session_id: str) -> str:
    """Resource view of a session's recent messages (compact form, errors inline)."""
    await STATE.ensure()
    try:
        msgs = await STATE.client.request(
            "GET", "/session/{sessionID}/message",
            path_params={"sessionID": session_id}, query={"limit": 20},
        )
        compact = [_compact_message(m, 2000) for m in msgs] if isinstance(msgs, list) else msgs
        return json.dumps({"session_id": session_id, "messages": compact})
    except OpencodeError as e:
        return json.dumps({"session_id": session_id, "error": str(e)[:500]})


def main() -> None:
    """stdio entrypoint: run MCP loop, then best-effort kill the managed serve."""
    logging.basicConfig(level=logging.WARNING, format="%(name)s: %(message)s")
    try:
        mcp.run()
    finally:
        # best-effort cleanup of managed serve on stdio exit
        try:
            import anyio

            async def _down() -> None:
                await STATE.shutdown()

            anyio.run(_down)
        except Exception as e:  # noqa: BLE001 - exit path must not raise
            log.debug("shutdown cleanup failed: %s", e)


if __name__ == "__main__":
    main()
