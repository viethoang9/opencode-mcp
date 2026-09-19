"""Variant (reasoning-effort) passthrough: model/agent/variant reach the request body."""

import pytest

from opencode_mcp import server
from opencode_mcp.journal import Journal
from opencode_mcp.server import _prompt_body


def test_prompt_body_omits_unset():
    body = _prompt_body("hi", None, None, None)
    assert body == {"parts": [{"type": "text", "text": "hi"}]}


def test_prompt_body_full():
    body = _prompt_body("hi", "zai-coding-plan/glm-5.3-highspeed", "build", "max")
    assert body["model"] == {"providerID": "zai-coding-plan", "modelID": "glm-5.3-highspeed"}
    assert body["agent"] == "build"
    assert body["variant"] == "max"


@pytest.mark.asyncio
async def test_async_tool_forwards_variant(monkeypatch, tmp_path):
    """opencode_prompt_async must forward variant verbatim to opencode (no validation here)."""
    seen: dict = {}

    async def fake_ensure() -> None:
        return None

    async def fake_request(method, path, **kw):
        seen.update(method=method, path=path, body=kw.get("body"))
        return True

    monkeypatch.setattr(server.STATE, "ensure", fake_ensure)
    monkeypatch.setattr(server.STATE.client, "request", fake_request)
    monkeypatch.setattr(server.STATE, "journal", Journal(str(tmp_path / "j.db")))

    res = await server.opencode_prompt_async(
        "ses_test", "do it well", model="zai-coding-plan/glm-5.3", variant="max"
    )
    assert seen["body"]["variant"] == "max"
    assert seen["body"]["model"] == {"providerID": "zai-coding-plan", "modelID": "glm-5.3"}
    assert res["session_id"] == "ses_test"
