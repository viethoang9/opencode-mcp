import pytest

from opencode_mcp.journal import Journal


@pytest.mark.asyncio
async def test_journal_append_list_verify(tmp_path):
    j = Journal(str(tmp_path / "j.db"))
    s1 = await j.append("ses_a", "message.part.updated", {"i": 1})
    s2 = await j.append("ses_a", "session.idle", {"i": 2})
    await j.append("ses_b", "message.part.updated", {"i": 3})
    assert (s2 - s1) == 1

    page = await j.list(after_seq=0, limit=10)
    assert page["max_seq"] == 3
    assert page["next_seq"] == 3
    assert page["has_more"] is False

    scoped = await j.list(after_seq=0, limit=10, session_id="ses_a")
    assert len(scoped["events"]) == 2

    tail = await j.list(after_seq=s1, limit=10)
    assert [e["seq"] for e in tail["events"]] == [s2, s2 + 1]

    v = await j.verify(1, 3)
    assert v["ok"] and not v["missing"]

    v2 = await j.verify(1, 7)
    assert not v2["ok"] and v2["missing"] == [4, 5, 6, 7]


@pytest.mark.asyncio
async def test_session_filtered_page_reports_session_max(tmp_path):
    """has_more must reflect the filtered session, not unrelated newer sessions."""
    j = Journal(str(tmp_path / "j.db"))
    await j.append("ses_a", "tick", {})
    await j.append("ses_b", "tick", {})
    await j.append("ses_b", "tick", {})

    page = await j.list(after_seq=0, limit=10, session_id="ses_a")
    assert page["max_seq"] == 1
    assert page["next_seq"] == 1
    assert page["has_more"] is False


@pytest.mark.asyncio
async def test_session_filtered_empty_page_terminates(tmp_path):
    """An exhausted session cursor must not advertise more (else callers spin forever)."""
    j = Journal(str(tmp_path / "j.db"))
    await j.append("ses_a", "tick", {})
    await j.append("ses_b", "tick", {})

    page = await j.list(after_seq=1, limit=10, session_id="ses_a")
    assert page["events"] == []
    assert page["has_more"] is False


@pytest.mark.asyncio
async def test_verify_range_wider_than_page_limit(tmp_path):
    """verify() must not inherit list()'s 200-row cap and report present events missing."""
    j = Journal(str(tmp_path / "j.db"))
    for i in range(300):
        await j.append("ses_x", "tick", {"i": i})

    v = await j.verify(1, 300)
    assert v["ok"]
    assert v["missing"] == []
    assert v["have"] == 300


@pytest.mark.asyncio
async def test_verify_caps_missing_list(tmp_path):
    """A huge empty range must not return an unbounded missing list."""
    j = Journal(str(tmp_path / "j.db"))
    await j.append("ses_x", "tick", {})

    v = await j.verify(1, 5000)
    assert v["missing_count"] == 4999
    assert len(v["missing"]) <= 100


@pytest.mark.asyncio
async def test_oversized_payload_stays_readable(tmp_path):
    """Truncating serialized JSON would corrupt the row; the read must still work."""
    j = Journal(str(tmp_path / "j.db"))
    seq = await j.append("ses_x", "big", {"blob": "x" * 300_000})

    page = await j.list(after_seq=seq - 1, limit=1)
    assert page["events"][0]["payload"]["_truncated"] is True


@pytest.mark.asyncio
async def test_tail_returns_newest_page_oldest_first(tmp_path):
    """'Latest events' views need the tail; list(after_seq=0) returns the oldest page."""
    j = Journal(str(tmp_path / "j.db"))
    for i in range(10):
        await j.append("ses_x", "tick", {"i": i})

    page = await j.tail(limit=3, session_id="ses_x")
    assert [e["payload"]["i"] for e in page["events"]] == [7, 8, 9]


@pytest.mark.asyncio
async def test_tail_scopes_to_session(tmp_path):
    j = Journal(str(tmp_path / "j.db"))
    await j.append("ses_a", "tick", {"i": 0})
    for i in range(5):
        await j.append("ses_b", "tick", {"i": i})

    page = await j.tail(limit=10, session_id="ses_a")
    assert [e["payload"]["i"] for e in page["events"]] == [0]


@pytest.mark.asyncio
async def test_journal_lossless_resume(tmp_path):
    """Simulate client crash: resume from stored cursor loses nothing."""
    j = Journal(str(tmp_path / "j.db"))
    for i in range(5):
        await j.append("ses_x", "tick", {"i": i})
    first = await j.list(after_seq=0, limit=2)
    cursor = first["next_seq"]
    rest = await j.list(after_seq=cursor, limit=10)
    assert [e["payload"]["i"] for e in rest["events"]] == [2, 3, 4]
    assert (await j.verify(1, 5))["ok"]
