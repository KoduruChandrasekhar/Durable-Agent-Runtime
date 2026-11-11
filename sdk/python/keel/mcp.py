"""MCP (Model Context Protocol) tools in the keel registry.

At worker start-up, connect to the configured MCP servers with the official
MCP Python SDK, list their tools and register each one with its JSON schema.
MCP tools then go through validation, policy, idempotency keys and journaling
like any other tool.

Effects: MCP has no idempotency keys, so a tool is registered only if its
retry safety is known, from the server's annotations (readOnlyHint ->
effect "none", idempotentHint -> "idempotent") or from an explicit ``effects``
mapping. Anything else is refused rather than retried blindly.

MCP servers are untrusted: their outputs are tool outputs (untrusted text),
and their tool *descriptions* reach the model too, so a run whose LLM call
includes MCP tool definitions becomes tainted.
"""

from __future__ import annotations

import json
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from .tools import EFFECTS, ToolRegistry, ToolSpec

log = logging.getLogger("keel.mcp")


def _attr(obj: Any, *names: str, default: Any = None) -> Any:
    """Read a field under either the MCP SDK 1.x (camelCase) or 2.x (snake_case) name."""
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return default


@dataclass
class MCPServerConfig:
    name: str
    command: str | None = None          # stdio transport
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None
    url: str | None = None              # streamable HTTP transport
    effects: dict[str, str] = field(default_factory=dict)   # tool -> none|idempotent|reconcile
    risks: dict[str, str] = field(default_factory=dict)
    prefix: str = ""


class MCPToolset:
    def __init__(self, configs: list[MCPServerConfig], registry: ToolRegistry):
        self.configs = configs
        self.registry = registry
        self.stack = AsyncExitStack()
        self.sessions: dict[str, Any] = {}
        self.refused: list[str] = []
        self.registered: list[str] = []

    async def connect(self) -> MCPToolset:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        for cfg in self.configs:
            if cfg.command:
                params = StdioServerParameters(command=cfg.command, args=cfg.args, env=cfg.env)
                read, write = await self.stack.enter_async_context(stdio_client(params))
            elif cfg.url:
                from mcp.client import streamable_http
                client = (getattr(streamable_http, "streamable_http_client", None)  # mcp >= 2
                          or streamable_http.streamablehttp_client)  # type: ignore[attr-defined]
                read, write, *_ = await self.stack.enter_async_context(client(cfg.url))
            else:
                raise ValueError(f"MCP server {cfg.name}: need command or url")
            session = await self.stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            self.sessions[cfg.name] = session
            listed = await session.list_tools()
            for t in listed.tools:
                self._register(cfg, session, t)
        return self

    def _register(self, cfg: MCPServerConfig, session: Any, t: Any) -> None:
        ann = getattr(t, "annotations", None)
        effect = cfg.effects.get(t.name)
        if effect is None and ann is not None:
            if _attr(ann, "readOnlyHint", "read_only_hint", default=False):
                effect = "none"
            elif _attr(ann, "idempotentHint", "idempotent_hint", default=False):
                effect = "idempotent"
        name = cfg.prefix + t.name
        if effect not in EFFECTS or effect == "reconcile":
            self.refused.append(name)
            log.warning("refusing MCP tool %s: no retry-safe effect declared (annotate readOnlyHint/"
                        "idempotentHint or configure effects)", name)
            return
        risk = cfg.risks.get(t.name) or ("low" if effect == "none" else
                                         "high" if ann is not None and _attr(ann, "destructiveHint", "destructive_hint", default=False)
                                         else "medium")

        async def call(**kwargs: Any) -> Any:
            kwargs.pop("key", None)
            res = await session.call_tool(t.name, kwargs)
            parts = []
            for c in res.content:
                txt = getattr(c, "text", None)
                parts.append(txt if txt is not None else c.model_dump(mode="json"))
            out: Any = parts[0] if len(parts) == 1 else parts
            if isinstance(out, str):
                try:
                    out = json.loads(out)
                except ValueError:
                    pass
            if _attr(res, "isError", "is_error", default=False):
                raise RuntimeError(f"MCP tool {t.name} failed: {out}")
            return out

        spec = ToolSpec(name=name, description=t.description or name, fn=call, effect=effect, risk=risk,
                        raw_schema=_attr(t, "inputSchema", "input_schema") or {"type": "object", "properties": {}},
                        source="mcp",
                        meta={"server": cfg.name})
        self.registry.add(spec)
        self.registered.append(name)

    async def close(self) -> None:
        await self.stack.aclose()
