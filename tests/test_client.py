"""Backend adapter: path substitution, transport error mapping, boot failure recovery."""

import httpx
import pytest

from opencode_mcp import client as client_mod
from opencode_mcp.client import OpencodeClient, OpencodeError
from opencode_mcp.config import Config


def test_substitute_leaves_plain_ids_intact():
    assert OpencodeClient._substitute("/session/{sessionID}", {"ses" "sionID": "ses_abc"}) == (
        "/session/ses_abc"
    )


def test_substitute_quotes_path_separators():
    """An id containing / must not escape its path segment."""
    out = OpencodeClient._substitute("/session/{sessionID}", {"sessionID": "a/../../global/config"})
    assert out == "/session/a%2F..%2F..%2Fglobal%2Fconfig"


def test_substitute_quotes_query_delimiter():
    out = OpencodeClient._substitute("/session/{sessionID}", {"sessionID": "x?admin=1"})
    assert out == "/session/x%3Fadmin%3D1"


@pytest.mark.asyncio
async def test_request_wraps_transport_errors():
    """A dead backend must surface as OpencodeError, not a raw httpx exception."""

    class DeadTransport:
        async def request(self, *args, **kwargs):
            raise httpx.ConnectError("connection refused")

    c = OpencodeClient(Config())
    c._client = DeadTransport()

    with pytest.raises(OpencodeError):
        await c.request("GET", "/global/health")


@pytest.mark.asyncio
async def test_failed_start_does_not_poison_client(monkeypatch):
    """A failed boot must clean up and let the next start() retry, not early-return."""
    attempts = []

    class FakeProc:
        returncode = None

        def poll(self):
            return None

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    async def never_ready(self):
        attempts.append(1)
        raise OpencodeError("not ready")

    monkeypatch.setattr(client_mod.shutil, "which", lambda b: "/bin/true")
    monkeypatch.setattr(client_mod.subprocess, "Popen", lambda *a, **k: FakeProc())
    monkeypatch.setattr(OpencodeClient, "_wait_ready_locked", never_ready)

    c = OpencodeClient(Config())
    for _ in range(2):
        with pytest.raises(OpencodeError):
            await c.start()

    assert len(attempts) == 2
    assert c.proc is None
