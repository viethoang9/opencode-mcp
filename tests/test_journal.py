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
