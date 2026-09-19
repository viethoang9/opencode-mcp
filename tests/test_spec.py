from opencode_mcp.spec import (
    SSE_OPERATIONS,
    build_catalog,
    group_of,
    search_catalog,
    visible_groups,
)

FAKE_SPEC = {
    "paths": {
        "/session": {"post": {"operationId": "session.create", "summary": "Create session"}},
        "/session/{sessionID}/message": {
            "post": {"operationId": "session.prompt", "summary": "Send message"}
        },
        "/event": {"get": {"operationId": "event.subscribe", "summary": "Subscribe"}},
        "/tui/submit-prompt": {"post": {"operationId": "tui.submitPrompt", "summary": "TUI"}},
    }
}


def test_group_of():
    assert group_of("session.create") == "session"
    assert group_of("tui.submitPrompt") == "tui"
    assert group_of("v2.session.prompt") == "v2-session"
    assert group_of("weird") == "misc"


def test_build_catalog_skips_non_ops():
    cat = build_catalog(FAKE_SPEC)
    assert set(cat) == {"session.create", "session.prompt", "event.subscribe", "tui.submitPrompt"}
    assert cat["session.prompt"].method == "POST"
    assert cat["session.prompt"].tool_name == "opencode_session_prompt"


def test_search_prefers_id_match():
    cat = build_catalog(FAKE_SPEC)
    hits = search_catalog(cat, "session")
    assert hits[0].id.startswith("session.")
    assert search_catalog(cat, "nope") == []


def test_visible_groups():
    assert visible_groups("all") is None
    assert visible_groups("session,message") == {"session", "message"}


def test_sse_ops_blocked():
    assert "event.subscribe" in SSE_OPERATIONS
