"""Token census: static context cost of tool definitions (mcp-context-cost method).

Spawns nothing: reads tool schemas from the FastMCP app object, serializes
{name, description, inputSchema} as JSON, counts with tiktoken o200k if
present else len//4. Fails CI if total exceeds budget.
"""

from __future__ import annotations

import asyncio
import json
import sys

BUDGET_TOKENS = 9000


def count(text: str) -> int:
    """Count tokens via tiktoken o200k, else len//4 fallback (accurate ~10%)."""
    try:
        import tiktoken

        return len(tiktoken.get_encoding("o200k_base").encode(text))
    except Exception:  # noqa: BLE001 - fallback estimator
        return len(text) // 4


async def collect() -> list[dict]:
    """Gather {name, tokens} per tool from the live FastMCP app object (no subprocess)."""
    from opencode_mcp.server import mcp

    tools = await mcp.list_tools()
    out = []
    for t in tools:
        schema = getattr(t, "parameters", None) or getattr(t, "inputSchema", None) or {}
        if not isinstance(schema, dict):
            try:
                schema = schema.model_dump()  # pydantic v2
            except Exception:  # noqa: BLE001
                schema = {}
        blob = json.dumps(
            {"name": t.name, "description": t.description, "inputSchema": schema}
        )
        out.append({"name": t.name, "tokens": count(blob), "len": len(blob)})
    return out


def main() -> int:
    rows = asyncio.run(collect())
    total = sum(r["tokens"] for r in rows)
    for r in sorted(rows, key=lambda r: -r["tokens"]):
        print(f"{r['name']:32s} {r['tokens']:6d} tok")
    print(f"TOTAL {len(rows)} tools: {total} tokens "
          f"({total/2000:.1f}% of 200K, {total/1280:.1f}% of 128K)")
    if total > BUDGET_TOKENS:
        print(f"OVER BUDGET ({BUDGET_TOKENS}); slim schemas or move tools behind opencode_api")
        return 1
    print("within budget")
    return 0


if __name__ == "__main__":
    sys.exit(main())
