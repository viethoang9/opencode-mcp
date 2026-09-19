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

MAX_PAYLOAD_CHARS = 200_000
MAX_MISSING_REPORTED = 100


def _row(r: tuple) -> dict[str, Any]:
    """Map a DB row to the event dict clients consume."""
    return {
        "seq": r[0],
        "session_id": r[1],
        "type": r[2],
        "source": r[3],
        "payload": json.loads(r[4]) if r[4] else {},
        "ts": r[5],
    }


def _encode(payload: dict[str, Any]) -> str:
    """Serialize a payload, replacing (never slicing) oversized JSON so reads stay valid."""
    raw = json.dumps(payload)
    if len(raw) <= MAX_PAYLOAD_CHARS:
        return raw
    return json.dumps(
        {"_truncated": True, "_original_chars": len(raw), "_head": raw[:2000]}
    )


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
                (session_id or "", etype or "", source or "", _encode(payload), time.time()),
            )
            con.commit()
            return int(cur.lastrowid or 0)
        finally:
            con.close()

    def _list_sync(
        self, after_seq: int, limit: int, session_id: str | None
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch one page plus the max seq of the same scope, so has_more can terminate."""
        con = self._connect()
        try:
            if session_id:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " WHERE seq>? AND session_id=? ORDER BY seq ASC LIMIT?",
                    (after_seq, session_id, limit),
                ).fetchall()
                max_seq = con.execute(
                    "SELECT COALESCE(MAX(seq),0) FROM events WHERE session_id=?", (session_id,)
                ).fetchone()[0]
            else:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " WHERE seq>? ORDER BY seq ASC LIMIT?",
                    (after_seq, limit),
                ).fetchall()
                max_seq = con.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0]
            return [_row(r) for r in rows], int(max_seq)
        finally:
            con.close()

    def _tail_sync(
        self, limit: int, session_id: str | None
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch the newest rows of a scope, returned oldest-first within the page."""
        con = self._connect()
        try:
            if session_id:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " WHERE session_id=? ORDER BY seq DESC LIMIT?",
                    (session_id, limit),
                ).fetchall()
                max_seq = con.execute(
                    "SELECT COALESCE(MAX(seq),0) FROM events WHERE session_id=?", (session_id,)
                ).fetchone()[0]
            else:
                rows = con.execute(
                    "SELECT seq,session_id,type,source,payload,ts FROM events"
                    " ORDER BY seq DESC LIMIT?",
                    (limit,),
                ).fetchall()
                max_seq = con.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0]
            return [_row(r) for r in reversed(rows)], int(max_seq)
        finally:
            con.close()

    def _seqs_sync(self, from_seq: int, to_seq: int) -> set[int]:
        """Every seq present in [from_seq, to_seq]; unpaged so verify() sees the whole range."""
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT seq FROM events WHERE seq>=? AND seq<=?", (from_seq, to_seq)
            ).fetchall()
            return {int(r[0]) for r in rows}
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

    async def tail(self, limit: int = 50, session_id: str | None = None) -> dict[str, Any]:
        """Newest page for 'latest events' views (events stay oldest-first within the page)."""
        limit = max(1, min(limit, 200))
        events, max_seq = await asyncio.to_thread(self._tail_sync, limit, session_id)
        return {
            "events": events,
            "next_seq": events[-1]["seq"] if events else 0,
            "max_seq": max_seq,
            "has_more": False,
        }

    async def verify(self, from_seq: int, to_seq: int) -> dict[str, Any]:
        """Assert no gaps in [from_seq, to_seq]; proves lossless resume."""
        present = await asyncio.to_thread(self._seqs_sync, from_seq, to_seq)
        expected = max(0, to_seq - from_seq + 1)
        missing = [s for s in range(from_seq, to_seq + 1) if s not in present]
        return {
            "ok": not missing,
            "missing": missing[:MAX_MISSING_REPORTED],
            "missing_count": len(missing),
            "have": len(present),
            "expected": expected,
        }
