"""Frame parsing: session attribution and event typing for opencode SSE envelopes."""

from opencode_mcp.sse import _etype_of, _session_of, _unwrap

SID = "ses_f473c05a2ffeaQMPwtmeznggM4"

# opencode wraps some events in a sync envelope: the session lives on
# syncEvent.aggregateID, not on sessionID/properties like ordinary frames.
SYNC_FRAME = {
    "type": "sync",
    "syncEvent": {
        "id": "evt_0b8c4543d0010YlXa9vLIFUZxu",
        "type": "session.updated.1",
        "seq": 6,
        "aggregateID": SID,
        "data": {"sessionID": SID, "info": {"id": SID, "slug": "quiet-canyon"}},
    },
    "id": "evt_0b8c4543d0010YlXa9vLIFUZxu",
}


def test_session_of_sync_frame():
    assert _session_of(SYNC_FRAME) == SID


def test_session_of_sync_frame_in_envelope():
    framed = {"directory": "/tmp/p", "project": "abc", "payload": SYNC_FRAME}
    assert _session_of(framed) == SID


def test_session_of_sync_frame_without_aggregate_id():
    frame = {"type": "sync", "syncEvent": {"type": "x.1", "data": {"sessionID": SID}}}
    assert _session_of(frame) == SID


def test_session_of_sync_frame_for_non_session_aggregate():
    frame = {"type": "sync", "syncEvent": {"type": "project.updated.1", "aggregateID": "prj_1"}}
    assert _session_of(frame) == ""


def test_etype_of_sync_frame_uses_inner_type():
    assert _etype_of(SYNC_FRAME) == "session.updated.1"


def test_session_of_ordinary_frame():
    frame = {"type": "message.part.delta", "properties": {"sessionID": SID}}
    assert _session_of(frame) == SID


def test_session_of_frame_with_info_id():
    frame = {"type": "session.updated", "properties": {"info": {"id": SID}}}
    assert _session_of(frame) == SID


def test_session_of_global_frame():
    assert _session_of({"type": "server.heartbeat"}) == ""


def test_etype_of_ordinary_frame():
    assert _etype_of({"type": "message.part.delta"}) == "message.part.delta"


def test_etype_of_untyped_frame():
    assert _etype_of({"_raw": "garbage"}) == "message"


def test_unwrap_keeps_envelope_context():
    framed = {"directory": "/tmp/p", "project": "abc", "payload": SYNC_FRAME}
    assert _unwrap(framed)["_directory_directory"] == "/tmp/p"
