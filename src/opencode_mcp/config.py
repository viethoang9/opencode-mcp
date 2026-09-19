"""Environment-driven configuration. All knobs are env vars so agents can scope safely."""

from __future__ import annotations

import os
import socket


def _free_port() -> int:
    """Pick an unused localhost TCP port (bind port 0, read back, release)."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Config:
    """All runtime knobs, read from env vars with safe defaults.

    Centralizes `OPENCODE_*` parsing so the rest of the code never touches
    os.environ directly. Port 0 means "auto-pick a free port".
    """
    def __init__(self) -> None:
        self.binary: str = os.environ.get("OPENCODE_BINARY", "opencode")
        self.host: str = os.environ.get("OPENCODE_HOST", "127.0.0.1")
        _port = os.environ.get("OPENCODE_PORT", "0")
        self.port: int = int(_port) if _port.strip() else 0
        if not self.port:
            self.port = _free_port()
        self.username: str = os.environ.get("OPENCODE_SERVER_USERNAME", "opencode")
        self.password: str = os.environ.get("OPENCODE_SERVER_PASSWORD", "")
        self.directory: str = os.environ.get("OPENCODE_DIRECTORY", os.getcwd())
        allowed = os.environ.get("OPENCODE_ALLOWED_DIRS", "")
        self.allowed_dirs: list[str] = [d for d in allowed.split(":") if d] or [self.directory]
        self.start_timeout_s: float = float(os.environ.get("OPENCODE_START_TIMEOUT", "30"))
        self.request_timeout_s: float = float(os.environ.get("OPENCODE_REQUEST_TIMEOUT", "120"))
        self.min_version: str = os.environ.get("OPENCODE_MIN_VERSION", "1.0.0")
        self.journal_path: str = os.environ.get(
            "OPENCODE_JOURNAL_PATH",
            os.path.expanduser("~/.local/share/opencode-mcp/journal.db"),
        )
        # Comma-separated op groups to expose via generic tool docs; "all" = everything.
        self.tool_groups: str = os.environ.get(
            "OPENCODE_TOOL_GROUPS",
            "session,message,event,permission,file,find,agent,config,provider,project,global",
        )
        self.max_chars: int = int(os.environ.get("OPENCODE_MAX_CHARS", "8000"))
        self.events_default_limit: int = int(os.environ.get("OPENCODE_EVENTS_LIMIT", "50"))
        self.disable_sse: bool = os.environ.get("OPENCODE_DISABLE_SSE", "").lower() in (
            "1",
            "true",
            "yes",
        )

    @property
    def base_url(self) -> str:
        """HTTP root of the managed `opencode serve` (e.g. http://127.0.0.1:4096)."""
        return f"http://{self.host}:{self.port}"

    def _contain(self, target: str, label: str) -> str:
        """Return realpath of target if it sits inside an allowed dir, else raise."""
        resolved = os.path.realpath(target)
        for allowed in self.allowed_dirs:
            a = os.path.realpath(allowed)
            if resolved == a or resolved.startswith(a.rstrip("/") + "/"):
                return resolved
        raise ValueError(f"{label} not allowed: {resolved} (allowed: {self.allowed_dirs})")

    def assert_dir_allowed(self, directory: str | None) -> str:
        """Resolve working directory, enforcing allowlist (prevents dir-escape)."""
        return self._contain(directory or self.directory, "directory")

    def assert_path_allowed(self, path: str) -> str:
        """Resolve a file path (relative to the project root), enforcing the allowlist."""
        target = path if os.path.isabs(path) else os.path.join(self.directory, path)
        return self._contain(target, "path")
