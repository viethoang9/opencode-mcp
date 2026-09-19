"""Tool-layer behaviour: output shaping, status mapping, model parsing, path containment."""

import pytest

from opencode_mcp import server
from opencode_mcp.journal import Journal
from opencode_mcp.server import _cap_obj, _model_arg, _parts_text


@pytest.fixture
def stub_backend(monkeypatch, tmp_path):
    """Replace the managed backend with a scripted responder."""
    calls: dict = {}

    async def fake_ensure() -> None:
        return None

    monkeypatch.setattr(server.STATE, "ensure", fake_ensure)
    monkeypatch.setattr(server.STATE, "journal", Journal(str(tmp_path / "j.db")))

    def respond(table):
        async def fake_request(method, path, **kw):
            calls.setdefault("seen", []).append((method, path, kw))
            return table[path] if isinstance(table, dict) else table

        monkeypatch.setattr(server.STATE.client, "request", fake_request)
        return calls

    return respond


# -- reply text must survive truncation ------------------------------------
def test_parts_text_keeps_order_within_budget():
    parts = [{"type": "reasoning", "text": "why"}, {"type": "text", "text": "ok"}]
    assert _parts_text(parts, 1000) == "[reasoning] why\nok"


def test_parts_text_keeps_reply_when_reasoning_overflows():
    """A long reasoning block must not push the actual answer out of the budget."""
    parts = [
        {"type": "reasoning", "text": "z" * 5000},
        {"type": "text", "text": "FINAL ANSWER"},
    ]
    out = _parts_text(parts, 200)
    assert "FINAL ANSWER" in out


# -- oversized structured results ------------------------------------------
def test_cap_obj_passes_small_payloads_through():
    assert _cap_obj({"a": 1}, 1000) == {"a": 1}


def test_cap_obj_truncates_large_payloads():
    capped = _cap_obj({"blob": "x" * 50_000}, 1000)
    assert capped["_truncated"] is True
    assert capped["_chars"] > 1000


# -- model parsing ----------------------------------------------------------
def test_model_arg_splits_on_first_slash_only():
    assert _model_arg("openrouter/z-ai/glm-5.2:free") == {
        "providerID": "openrouter",
        "modelID": "z-ai/glm-5.2:free",
    }


def test_model_arg_rejects_model_without_provider():
    """Silently dropping the model would run the prompt on an unintended default."""
    with pytest.raises(ValueError):
        _model_arg("glm-5.3")


# -- status mapping ---------------------------------------------------------
@pytest.mark.asyncio
async def test_session_status_absent_means_idle(stub_backend):
    """opencode only reports busy sessions; absence is idle, not unknown."""
    stub_backend({"/session/status": {}})
    res = await server.opencode_session_status("ses_x")
    assert res["status"] == {"type": "idle"}


@pytest.mark.asyncio
async def test_session_status_reports_busy(stub_backend):
    stub_backend({"/session/status": {"ses_x": {"type": "busy"}}})
    res = await server.opencode_session_status("ses_x")
    assert res["status"] == {"type": "busy"}


# -- provider listing must stay small ---------------------------------------
@pytest.mark.asyncio
async def test_models_summarises_large_provider_catalog(stub_backend):
    """The raw /config/providers payload is megabytes; the tool must stay compact."""
    stub_backend(
        {
            "/config/providers": {
                "providers": [
                    {
                        "id": "openrouter",
                        "name": "OpenRouter",
                        "models": {f"m{i}": {"cost": {"input": 1}} for i in range(372)},
                    }
                ],
                "default": {"openrouter": "m1"},
            }
        }
    )

    res = await server.opencode_models()
    provider = res["providers"][0]
    assert provider["id"] == "openrouter"
    assert provider["default"] == "m1"
    assert provider["model_count"] == 372
    assert len(provider["models"]) == 40
    assert provider["models_omitted"] == 332


# -- path containment -------------------------------------------------------
@pytest.mark.asyncio
async def test_read_file_rejects_path_outside_allowed_dirs(stub_backend, tmp_path):
    stub_backend({"/file/content": {"content": "secret"}})
    server.STATE.config.directory = str(tmp_path)
    server.STATE.config.allowed_dirs = [str(tmp_path)]

    with pytest.raises(ValueError):
        await server.opencode_read_file("../../../../etc/passwd")


@pytest.mark.asyncio
async def test_read_file_allows_path_inside_project(stub_backend, tmp_path):
    stub_backend({"/file/content": {"content": "line1\nline2"}})
    server.STATE.config.directory = str(tmp_path)
    server.STATE.config.allowed_dirs = [str(tmp_path)]

    res = await server.opencode_read_file("src/app.py")
    assert res["lines"] == ["line1", "line2"]
