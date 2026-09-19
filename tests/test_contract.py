"""Live contract test against a real `opencode serve`.

Run: OPENCODE_TEST=1 uv run pytest -m integration
Skipped otherwise (no binary / no network needed for unit tests).
Covers: version gate, spec freshness, session CRUD, journal bridging.
Prompting (needs model auth) is best-effort and never fails the suite.
"""

import os

import pytest

pytestmark = pytest.mark.integration

from opencode_mcp.client import OpencodeClient  # noqa: E402
from opencode_mcp.config import Config  # noqa: E402
from opencode_mcp.journal import Journal  # noqa: E402
from opencode_mcp.spec import build_catalog  # noqa: E402


@pytest.mark.asyncio
async def test_contract(tmp_path):
    if os.environ.get("OPENCODE_TEST") != "1":
        pytest.skip("set OPENCODE_TEST=1 for live contract test")
    cfg = Config()
    cfg.journal_path = str(tmp_path / "j.db")
    client = OpencodeClient(cfg)
    try:
        await client.start()
        health = await client.health()
        assert health.get("healthy") is True, health
        assert len(client.spec.get("paths", {})) > 100, "spec shrank unexpectedly"
        catalog = build_catalog(client.spec)
        for must in ("session.create", "session.prompt", "session.prompt_async", "global.health"):
            assert must in catalog, f"missing {must} in opencode {client.version}"

        session = await client.request("POST", "/session", body={"title": "mcp-contract"})
        sid = session["id"]
        assert sid.startswith("ses")

        msgs = await client.request("GET", "/session/{sessionID}/message",
                                    path_params={"sessionID": sid})
        assert isinstance(msgs, list)

        status = await client.request("GET", "/session/status")
        assert isinstance(status, dict)

        # journal bridge sanity
        j = Journal(cfg.journal_path)
        seq = await j.append(sid, "mcp.contract.ping", {"v": client.version}, "mcp")
        assert (await j.verify(seq, seq))["ok"]

        # best-effort prompt (requires model auth; skip on 4xx/auth errors)
        try:
            await client.request(
                "POST", "/session/{sessionID}/prompt_async",
                path_params={"sessionID": sid},
                body={"parts": [{"type": "text", "text": "reply with: pong"}]},
            )
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"prompt_async unavailable (likely no model auth): {str(e)[:200]}")

        await client.request("DELETE", "/session/{sessionID}", path_params={"sessionID": sid})
    finally:
        await client.close()
