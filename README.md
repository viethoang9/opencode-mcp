# opencode-mcp

MCP server (stdio) letting **other agents use [opencode](https://github.com/anomalyco/opencode)** — verified against opencode `1.18.31` (162 spec paths).

- **Managed backend:** spawns its own `opencode serve` on a free port, waits for `/global/health`, shuts it down on exit. No pre-running server needed.
- **Full API coverage, lean context:** 23 curated tools (**~1.9K tokens, LEAN** — less than filesystem's 2.4K, far below GitHub's 11K / Notion's 17K) + generic `opencode_api` / `opencode_spec_search` driven by the **live `/doc` OpenAPI spec**, so opencode updates surface automatically.
- **Lossless events:** persistent SSE (`/event` + `/global/event`) → SQLite journal with cursor resume. `opencode_events_list` is truth; notifications are hints only. `opencode_events_verify` proves zero gaps.

## Install

```bash
uv sync
```

Requires the `opencode` binary on PATH (`OPENCODE_BINARY` overrides).

## Use

Claude Code / Cursor / opencode itself (`opencode.json`):

```json
{ "mcp": { "opencode": {
  "type": "local",
  "command": ["uvx", "--from", "/path/to/opencode-mcp", "opencode-mcp"]
} } }
```

Run directly (stdio):

```bash
uv run opencode-mcp
```

## Typical agent loop

1. `opencode_ask(text)` for one-shots, or `opencode_session_create` + `opencode_prompt`.
2. Long jobs: `opencode_prompt_async` → poll `opencode_task_result` / `opencode_events_list(after_seq=last_seq)` → `opencode_task_cancel` if needed.
3. Permission stall? `opencode_permissions_pending` → `opencode_permission_reply`.
4. Anything exotic: `opencode_spec_search(query)` → `opencode_api(operation, path_params, query, body)`.

Resume after any disconnect: re-call `opencode_events_list` from your stored `next_seq`, then `opencode_events_verify(from_seq, to_seq)`.

## Config (env)

| Var | Default | Purpose |
|---|---|---|
| `OPENCODE_BINARY` | `opencode` | binary path |
| `OPENCODE_PORT` | `0` (auto) | fixed port if needed |
| `OPENCODE_SERVER_PASSWORD` | — | basic-auth passthrough |
| `OPENCODE_DIRECTORY` / `OPENCODE_ALLOWED_DIRS` | cwd | working dir + allowlist |
| `OPENCODE_TOOL_GROUPS` | core groups | `all` exposes every spec group in search |
| `OPENCODE_MAX_CHARS` | `8000` | per-result text cap |
| `OPENCODE_EVENTS_LIMIT` | `50` | default page size |
| `OPENCODE_JOURNAL_PATH` | `~/.local/share/opencode-mcp/journal.db` | durable journal |
| `OPENCODE_DISABLE_SSE` | — | set `1` to disable live consumer |
| `OPENCODE_MIN_VERSION` | `1.0.0` | compat floor (warns, never hard-blocks) |

## Context cost

```bash
uv run python scripts/tokens.py   # static defs census, budget 9000 (currently ~1938)
```

Design: lean hand-written schemas for the hot path; the 160+ op tail lives behind one 207-token `opencode_api` tool (progressive discovery). Full transcripts/file contents are paginated and truncated by default.

## Surviving opencode updates

- Single HTTP adapter (`client.py`); tools never touch HTTP directly.
- Catalog rebuilt from live `/doc` every start; new ops appear in `opencode_spec_search` with no code change.
- Version gate warns on `< OPENCODE_MIN_VERSION`; unknown ops error naming the running version.
- Nightly CI (`contract-nightly.yml`) runs the live contract test against `latest` and `dev`.

## Dev

```bash
uv run pytest tests/test_journal.py tests/test_spec.py -q
OPENCODE_TEST=1 uv run pytest tests/test_contract.py -q   # needs opencode binary
```
