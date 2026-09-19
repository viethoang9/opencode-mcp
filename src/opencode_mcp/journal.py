"""Durable lossless event journal (SQLite).

The live SSE stream is NOT the source of truth — this journal is. Every SSE
frame is appended with a monotonic seq. MCP tools read with cursor
(after_seq, limit), so a client crash/restart resumes without gaps.
Notifications (resource updates) are hints only.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any


class Journal:
    """Durable event store: the source of truth clients resume from after any disconnect.

    Writes run in threads (sqlite3 is sync) under an asyncio lock; reads are
    cursor-based (after_seq/limit) and capped at 200 rows per page.
    """
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    # -- sync helpers run in threads ------------------------------------
    def _connect(self) -> sqlite3.Connection:
        """Open DB (creating dirs/table/index), WAL mode + FULL sync for crash safety."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        con = sqlite3.connect(self.path, timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=FULL")
        con.execute(
            """CREATE TABLE IF NOT EXISTS events(
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id TEXT NOT NULL DEFAULT '',
              type TEXT NOT NULL DEFAULT '',
              source TEXT NOT NULL DEFAULT '',
              payload TEXT NOT NULL DEFAULT '{}',
              ts REAL NOT NULL)"""
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_session_seq ON events(session_id, seq)")
        con.commit()
        return con

    def _append_sync(self, session_id: str, etype: str, payload: dict[str, Any], source: str) -> int:
        """Insert one event row; returns its monotonic seq. Payloads capped at 200KB."""
        con = self._connect()
        try:
            cur = con.execute(
                "INSERT INTO events(session_id,type,source,payload,ts) VALUES(?,?,?,?,?)",
                (session_id or "", etype or "", source or "", json.dumps(payload)[:200000], time.time()),
            )
            con.commit()
            return int(cur.lastrowid or 0)
        finally:
            con.close()

    def _list_sync(
        self, after_seq: int, limit: int, session_id: str | None
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch one page (optionally per-session) plus global max seq for has_more."""
        con = self._connect()
        try:
            if session_id:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " WHERE seq>? AND session_id=? ORDER BY seq ASC LIMIT?",
                    (after_seq, session_id, limit),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " WHERE seq>? ORDER BY seq ASC LIMIT?",
                    (after_seq, limit),
                ).fetchall()
            max_seq = con.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0]
            out = [
                {
                    "seq": r[0],
                    "session_id": r[1],
                    "type": r[2],
                    "source": r[3],
                    "payload": json.loads(r[4]) if r[4] else {},
                    "ts": r[5],
                }
                for r in rows
            ]
            return out, int(max_seq)
        finally:
            con.close()

    # -- async API --------------------------------------------------------
    async def append(
        self, session_id: str, etype: str, payload: dict[str, Any], source: str = "sse"
    ) -> int:
        """Thread-safe append; returns seq the caller stores as its resume cursor."""
        async with self._lock:
            return await asyncio.to_thread(self._append_sync, session_id or "", etype, payload, source)

    async def list(
        self, after_seq: int = 0, limit: int = 50, session_id: str | None = None
    ) -> dict[str, Any]:
        """Cursor page: {events, next_seq (resume here), max_seq, has_more}."""
        limit = max(1, min(limit, 200))
        events, max_seq = await asyncio.to_thread(self._list_sync, after_seq, limit, session_id)
        next_seq = events[-1]["seq"] if events else after_seq
        return {
            "events": events,
            "next_seq": next_seq,
            "max_seq": max_seq,
            "has_more": next_seq < max_seq,
        }

    async def verify(self, from_seq: int, to_seq: int) -> dict[str, Any]:
        """Assert no gaps in [from_seq, to_seq]; proves lossless resume."""
        data = await self.list(after_seq=from_seq - 1, limit=(to_seq - from_seq + 1) + 5)
        seqs = [e["seq"] for e in data["events"] if e["seq"] <= to_seq]
        expected = list(range(from_seq, to_seq + 1))
        missing = [s for s in expected if s not in seqs]
        return {"ok": not missing, "missing": missing, "have": len(seqs), "expected": len(expected)}
