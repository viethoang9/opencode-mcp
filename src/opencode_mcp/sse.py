"""Persistent SSE consumer -> journal bridge.

Connects to opencode's /event and /global/event streams, parses SSE frames
tolerantly (opencode emits `data: <json>` frames), extracts session id, and
appends EVERY frame to the journal. Auto-reconnects with backoff; never drops
silently — reconnects are journaled as marker events so agents can see them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from .client import OpencodeClient
from .journal import Journal

log = logging.getLogger("opencode-mcp.sse")


def _unwrap(payload: Any) -> Any:
    """Strip opencode's frame envelope ({"payload": ...} / {directory,project,payload}).

    Envelope context (directory/project) is preserved as _directory_* keys so
    session routing still works after unwrapping.
    """
    if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        inner = payload["payload"]
        if set(payload) <= {"payload", "directory", "project"}:
            inner = dict(inner)
            for k in ("directory", "project"):
                if payload.get(k) is not None:
                    inner.setdefault(f"_directory_{k}", payload[k])
            return inner
        if set(payload) == {"payload"}:
            return inner
    return payload


def _session_of(payload: Any) -> str:
    """Extract owning session id (ses_*) from a frame; '' for global frames."""
    payload = _unwrap(payload)
    if isinstance(payload, dict):
        for key in ("sessionID", "sessionId", "session_id"):
            if payload.get(key):
                return str(payload[key])
        props = payload.get("properties")
        if isinstance(props, dict):
            for key in ("sessionID", "sessionId", "session_id", "id"):
                if props.get(key):
                    return str(props[key])
            info = props.get("info")
            if isinstance(info, dict) and info.get("id"):
                sid = str(info["id"])
                if sid.startswith("ses"):
                    return sid
        sync = payload.get("syncEvent")
        if isinstance(sync, dict):
            aggregate = sync.get("aggregateID")
            if isinstance(aggregate, str) and aggregate.startswith("ses"):
                return aggregate
            data = sync.get("data")
            if isinstance(data, dict) and data.get("sessionID"):
                return str(data["sessionID"])
    return ""


def _etype_of(payload: Any) -> str:
    """Extract event type for journal indexing; 'message' when the frame has none."""
    payload = _unwrap(payload)
    if isinstance(payload, dict):
        sync = payload.get("syncEvent")
        if isinstance(sync, dict) and isinstance(sync.get("type"), str) and sync["type"]:
            return str(sync["type"])[:120]
        t = payload.get("type") or payload.get("event") or ""
        if isinstance(t, str) and t:
            return t[:120]
    return "message"


async def _consume_stream(
    client: OpencodeClient,
    path: str,
    source: str,
    journal: Journal,
    on_event: Callable[[dict[str, Any]], Awaitable[None]] | None,
    stop: asyncio.Event,
) -> None:
    """Hold one SSE stream open forever: parse data: frames -> journal; backoff-reconnect.

    Reconnects are journaled as mcp.sse.reconnect markers (never silent gaps).
    The optional on_event hook fires best-effort hint callbacks (must not raise).
    """
    backoff = 1.0
    assert client._client is not None
    while not stop.is_set():
        try:
            async with client._client.stream("GET", path) as resp:
                if resp.status_code != 200:
                    raise RuntimeError(f"{path} -> {resp.status_code}")
                backoff = 1.0
                buf = ""
                async for chunk in resp.aiter_text():
                    if stop.is_set():
                        break
                    buf += chunk
                    while "\n\n" in buf:
                        frame, buf = buf.split("\n\n", 1)
                        data_lines = [
                            line[5:].strip()
                            for line in frame.splitlines()
                            if line.startswith("data:")
                        ]
                        if not data_lines:
                            continue
                        raw = "\n".join(data_lines)
                        try:
                            payload = json.loads(raw)
                        except (json.JSONDecodeError, ValueError):
                            payload = {"_raw": raw[:4000]}
                        etype = _etype_of(payload)
                        sid = _session_of(payload)
                        clean = _unwrap(payload)
                        await journal.append(sid, etype, clean if isinstance(clean, dict) else {"_raw": clean}, source)
                        if on_event is not None:
                            try:
                                await on_event({"type": etype, "session_id": sid})
                            except Exception as e:  # noqa: BLE001 - hint must not kill consumer
                                log.debug("on_event hook failed: %s", e)
        except Exception as e:  # noqa: BLE001 - reconnect loop
            if stop.is_set():
                break
            log.warning("sse %s error (%s); reconnect in %.1fs", path, str(e)[:200], backoff)
            try:
                await journal.append("", "mcp.sse.reconnect", {"path": path, "error": str(e)[:300]}, "mcp")
            except Exception as marker_error:  # noqa: BLE001 - reconnect must proceed
                log.debug("could not journal reconnect marker: %s", marker_error)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def run_consumer(
    client: OpencodeClient,
    journal: Journal,
    stop: asyncio.Event,
    on_event: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> None:
    """Run both consumers (/event + /global/event) until stop is set."""
    await asyncio.gather(
        _consume_stream(client, "/event", "event", journal, on_event, stop),
        _consume_stream(client, "/global/event", "global-event", journal, on_event, stop),
    )
