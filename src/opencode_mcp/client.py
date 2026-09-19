"""Managed `opencode serve` subprocess + HTTP wrapper.

Only module that knows opencode HTTP paths. Everything else goes through it,
so upstream renames/restructures touch one file.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import time
from typing import Any
from urllib.parse import quote

import httpx
from packaging.version import Version

from .config import Config

log = logging.getLogger("opencode-mcp.client")


class OpencodeError(RuntimeError):
    """Raised for any managed-backend failure (binary missing, HTTP 4xx/5xx, timeout)."""


class OpencodeClient:
    """Owns the `opencode serve` subprocess and speaks its HTTP API.

    Sole module that knows opencode's paths; callers use request() or the
    generic opencode_api tool, so upstream renames touch only this file.
    start() is idempotent and lock-guarded (safe under concurrent tool calls).
    """
    def __init__(self, config: Config) -> None:
        self.config = config
        self.proc: subprocess.Popen | None = None
        self.version: str = "unknown"
        self.compat_warning: str = ""
        self.spec: dict[str, Any] = {}
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    # -- lifecycle ------------------------------------------------------
    async def start(self) -> None:
        """Boot managed backend: spawn serve, wait ready, record version, load /doc spec."""
        async with self._lock:
            if self.proc is not None:
                return
            binary = self.config.binary
            if not shutil.which(binary) and not os.path.exists(binary):
                raise OpencodeError(
                    f"opencode binary not found: {binary!r} (set OPENCODE_BINARY)"
                )
            env = dict(os.environ)
            env["OPENCODE_DISABLE_AUTOUPDATE"] = env.get("OPENCODE_DISABLE_AUTOUPDATE", "1")
            cmd = [
                binary,
                "serve",
                "--port",
                str(self.config.port),
                "--hostname",
                self.config.host,
            ]
            log.warning("spawning: %s (cwd=%s)", " ".join(cmd), self.config.directory)
            self.proc = subprocess.Popen(  # noqa: ASYNC220 - returns at once; readiness awaited below
                cmd,
                cwd=self.config.directory,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            auth = (
                (self.config.username, self.config.password)
                if self.config.password
                else None
            )
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                auth=auth,
                timeout=self.config.request_timeout_s,
            )
            try:
                await self._wait_ready_locked()
                await self._fetch_version_locked()
                await self._fetch_spec_locked()
            except BaseException:
                await self.close()
                raise

    async def _wait_ready_locked(self) -> None:
        """Poll /global/health until 200 or timeout; fail fast if the process dies."""
        assert self._client is not None
        deadline = time.time() + self.config.start_timeout_s
        last: str = ""
        while time.time() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise OpencodeError(f"opencode serve exited early (code={self.proc.returncode})")
            try:
                r = await self._client.get("/global/health")
                if r.status_code == 200:
                    return
                last = f"http={r.status_code}"
            except Exception as e:  # noqa: BLE001 - transient during boot
                last = str(e)
            await asyncio.sleep(0.4)
        raise OpencodeError(f"opencode serve not ready after {self.config.start_timeout_s}s: {last}")

    async def _fetch_version_locked(self) -> None:
        """Read server version for the compat gate; sets compat_warning if too old."""
        assert self._client is not None
        try:
            r = await self._client.get("/global/health")
            self.version = r.json().get("version", "unknown")
        except Exception:  # noqa: BLE001
            self.version = "unknown"
        try:
            if self.version != "unknown" and Version(self.version) < Version(self.config.min_version):
                self.compat_warning = (
                    f"opencode {self.version} < minimum {self.config.min_version}; "
                    "some tools may fail; please upgrade opencode."
                )
                log.warning(self.compat_warning)
        except Exception as e:  # noqa: BLE001 - non-PEP440 dev versions
            log.debug("version compare skipped for %r: %s", self.version, e)

    async def _fetch_spec_locked(self) -> None:
        """Download live OpenAPI from /doc; degrades gracefully (generic tool only)."""
        assert self._client is not None
        try:
            r = await self._client.get("/doc")
            r.raise_for_status()
            spec = r.json()
            if isinstance(spec, dict) and "paths" in spec:
                self.spec = spec
                log.warning(
                    "loaded opencode spec: %d paths, version=%s",
                    len(spec["paths"]),
                    self.version,
                )
            else:
                log.warning("/doc did not return OpenAPI paths; generic tool degraded")
        except Exception as e:  # noqa: BLE001 - serve stays usable without spec
            log.warning("could not fetch /doc spec: %s", e)

    async def close(self) -> None:
        """Shut down HTTP pool, then terminate (kill after 5s) the serve subprocess."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as e:  # noqa: BLE001 - shutdown must not raise
                log.debug("http pool close failed: %s", e)
            self._client = None
        if self.proc is not None:
            try:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            except Exception as e:  # noqa: BLE001 - shutdown must not raise
                log.debug("serve termination failed: %s", e)
            self.proc = None

    # -- low-level request ----------------------------------------------
    @staticmethod
    def _substitute(path: str, params: dict[str, Any] | None) -> str:
        """Fill OpenAPI path templates: /session/{sessionID} + {sessionID: ses_x}.

        Values are percent-encoded so an id can never escape its path segment.
        """
        if params:
            for k, v in params.items():
                path = path.replace("{" + k + "}", quote(str(v), safe=""))
        return path

    async def request(
        self,
        method: str,
        path: str,
        *,
        path_params: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
        body: Any = None,
    ) -> Any:
        """One generic HTTP call: auto-starts backend, maps 204->True, 4xx/5xx->OpencodeError."""
        if self._client is None:
            await self.start()
        assert self._client is not None
        url = self._substitute(path, path_params)
        q = {k: v for k, v in (query or {}).items() if v is not None}
        try:
            r = await self._client.request(method, url, params=q or None, json=body)
        except httpx.HTTPError as e:
            raise OpencodeError(f"{method} {url} -> transport error: {e}") from e
        if r.status_code == 204:
            return True
        if r.status_code >= 400:
            try:
                detail = r.text[:2000]
            except Exception:  # noqa: BLE001
                detail = f"http={r.status_code}"
            raise OpencodeError(f"{method} {url} -> {r.status_code}: {detail}")
        ctype = r.headers.get("content-type", "")
        if "text/event-stream" in ctype:
            return r.text
        try:
            return r.json()
        except (json.JSONDecodeError, ValueError):
            return r.text

    async def health(self) -> dict[str, Any]:
        """Health + version + loaded spec size + compat warning (powers opencode_health)."""
        data = await self.request("GET", "/global/health")
        if not isinstance(data, dict):
            data = {"healthy": True, "version": str(data)}
        data.setdefault("spec_paths", len(self.spec.get("paths", {})))
        if self.compat_warning:
            data["compat_warning"] = self.compat_warning
        return data
