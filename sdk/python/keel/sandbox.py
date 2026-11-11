"""Client for keel-sandbox and a ready-made ``run_python`` tool.

Running code has no outside effect beyond its own container, so the tool is
effect "none" (safe to retry). In dry-run forks a canned result is returned.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from .telemetry import inject_headers
from .tools import ToolRegistry, tool


class SandboxClient:
    def __init__(self, url: str | None = None, timeout: float = 150.0):
        self.url = (url or os.environ.get("KEEL_SANDBOX_URL", "http://localhost:8095")).rstrip("/")
        self.http = httpx.AsyncClient(timeout=timeout)

    async def execute(self, code: str, files: dict[str, str] | None = None, timeout: float = 30.0) -> dict:
        r = await self.http.post(self.url + "/execute", headers=inject_headers(), json={"language": "python", "code": code,
                                                              "files": files or {}, "timeout": timeout})
        if r.status_code >= 500:
            raise RuntimeError(f"sandbox unavailable: {r.text[:200]}")
        r.raise_for_status()
        return r.json()


_client: SandboxClient | None = None


def client() -> SandboxClient:
    global _client
    if _client is None:
        _client = SandboxClient()
    return _client


def register_sandbox_tool(registry: ToolRegistry, files_provider: Any = None, name: str = "run_python") -> Any:
    """Register ``run_python(code)``. ``files_provider()`` may supply input files
    (e.g. the CSV a data-analysis agent was asked about)."""

    async def _dry(code: str, **_: Any) -> dict:
        return {"stdout": "[dry run: sandbox not called]", "stderr": "", "exit_code": 0, "files": {}}

    @tool(name=name, effect="none", risk="medium", timeout=150, registry=registry, dry_run=_dry)
    async def run_python(code: str) -> dict:
        """Run Python 3 code in an isolated sandbox (no network, 512 MB, 30 s). Input files are in /tmp;
        write output files to /tmp/out. Returns stdout, stderr and exit_code."""
        files = files_provider() if files_provider else None
        r = await client().execute(code, files)
        return {"stdout": r.get("stdout", "")[-8000:], "stderr": r.get("stderr", "")[-4000:],
                "exit_code": r.get("exit_code"), "timed_out": r.get("timed_out", False),
                "files": {k: v[:4000] for k, v in (r.get("files") or {}).items()}}

    return run_python
