"""LLM messages with trust labels, the normalized reply, and clients for the
model gateway (production) and an in-process mock (tests, simulation)."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field

from .errors import BudgetExceeded, LLMError
from .telemetry import inject_headers

Trust = Literal["trusted", "user", "untrusted", "extracted"]


class ToolCall(BaseModel):
    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class Message(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = None
    tool_calls: list[ToolCall] | None = None
    tool_call_id: str | None = None
    name: str | None = None
    # Where the text came from. Defaults: system=trusted, user=user,
    # tool=untrusted (tool outputs can carry injected instructions).
    trust: Trust | None = None
    source: str | None = None

    def effective_trust(self) -> str:
        if self.trust:
            return self.trust
        return {"system": "trusted", "user": "user", "tool": "untrusted", "assistant": "trusted"}[self.role]

    def wire(self) -> dict:
        d: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls:
            d["tool_calls"] = [tc.model_dump() for tc in self.tool_calls]
        if self.tool_call_id:
            d["tool_call_id"] = self.tool_call_id
        if self.name:
            d["name"] = self.name
        return d


def system(text: str) -> Message:
    return Message(role="system", content=text, trust="trusted")


def user(text: str) -> Message:
    return Message(role="user", content=text, trust="user")


def untrusted(text: str, source: str = "external", role: Literal["user", "system"] = "user") -> Message:
    """Text from web pages, emails, documents, tool outputs or retrieved passages."""
    return Message(role=role, content=text, trust="untrusted", source=source)


def trusted(text: str, role: Literal["user", "system"] = "user") -> Message:
    """Text the developer vouches for (e.g. structured fields formatted by code)."""
    return Message(role=role, content=text, trust="trusted")


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0


class LLMReply(BaseModel):
    text: str | None = None
    tool_calls: list[ToolCall] = Field(default_factory=list)
    usage: Usage = Field(default_factory=Usage)
    cost_usd: float = 0.0
    model: str = ""
    provider: str = ""
    cache_hit: str | None = None
    latency_ms: float = 0.0
    retries: int = 0
    fallback_used: bool = False
    finish_reason: str = "stop"
    tainted: bool = False

    @property
    def message(self) -> Message:
        return Message(role="assistant", content=self.text, tool_calls=self.tool_calls or None,
                       trust="untrusted" if self.tainted else "trusted")

    def parsed(self) -> Any:
        """Parse the reply text as JSON (for response_format=json)."""
        text = (self.text or "").strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text[text.find("\n") + 1:] if "\n" in text else text
        return json.loads(text)


class ChatRequest(BaseModel):
    run_id: str = ""
    tenant_id: str = "default"
    step_key: str = ""
    model: str
    messages: list[dict]
    tools: list[dict] | None = None
    temperature: float = 0.0
    max_tokens: int = 1024
    response_format: dict | None = None
    stream: bool = False
    cacheable: bool | None = None
    metadata: dict = Field(default_factory=dict)


LLMClient = Callable[[ChatRequest], Awaitable[LLMReply]]


class GatewayClient:
    """Calls the keel model gateway's POST /v1/chat.

    Retries, fallbacks, rate limits, budgets and caching happen in the
    gateway; this client only maps its errors into SDK exceptions.
    """

    def __init__(self, url: str | None = None, timeout: float = 120.0, api_key: str | None = None):
        self.url = (url or os.environ.get("KEEL_GATEWAY_URL", "http://localhost:8090")).rstrip("/")
        headers = {}
        key = api_key or os.environ.get("KEEL_GATEWAY_KEY")
        if key:
            headers["Authorization"] = f"Bearer {key}"
        self._client = httpx.AsyncClient(timeout=timeout, headers=headers,
                                         limits=httpx.Limits(max_connections=512, max_keepalive_connections=128))

    async def __call__(self, req: ChatRequest) -> LLMReply:
        try:
            r = await self._client.post(self.url + "/v1/chat", json=req.model_dump(exclude_none=True),
                                        headers=inject_headers({"Idempotency-Key": f"{req.run_id}:{req.step_key}"}))
        except httpx.TimeoutException as e:
            raise LLMError("timeout", str(e) or "gateway timeout", retryable=True) from e
        except httpx.HTTPError as e:
            raise LLMError("unavailable", str(e) or type(e).__name__, retryable=True) from e
        if r.status_code == 200:
            return LLMReply.model_validate(r.json())
        try:
            err = r.json().get("error", {})
        except ValueError:
            err = {}
        code = err.get("code", f"http_{r.status_code}")
        msg = err.get("message", r.text[:300])
        if code == "budget_exceeded" or r.status_code == 402:
            raise BudgetExceeded(msg)
        retryable = r.status_code >= 500 or code in ("rate_limited", "overloaded", "timeout", "unavailable")
        ra = r.headers.get("Retry-After")
        raise LLMError(code, msg, retryable=retryable, retry_after=float(ra) if ra else None)

    async def aclose(self) -> None:
        await self._client.aclose()


class MockLLM:
    """In-process scripted model for unit tests and the simulation.

    ``brain(request) -> dict`` returns {"text": ..., "tool_calls": [...]}; the
    mock adds usage, a price and optional latency/failures from a seeded RNG.
    """

    def __init__(self, brain: Callable[[ChatRequest], dict], *, clock: Any = None, latency_s: float = 0.0,
                 error_rate: float = 0.0, rng: Any = None, price_per_1k: float = 0.002):
        self.brain = brain
        self.clock = clock
        self.latency_s = latency_s
        self.error_rate = error_rate
        self.rng = rng
        self.price_per_1k = price_per_1k
        self.calls = 0
        self.tokens = 0

    async def __call__(self, req: ChatRequest) -> LLMReply:
        self.calls += 1
        if self.latency_s and self.clock is not None:
            await self.clock.sleep(self.latency_s)
        if self.error_rate and self.rng is not None and self.rng.random() < self.error_rate:
            raise LLMError("overloaded", "mock provider overloaded", retryable=True)
        out = self.brain(req)
        in_tok = sum(len(str(m.get("content") or "")) // 4 + 4 for m in req.messages)
        text = out.get("text")
        calls = [ToolCall(id=c.get("id") or f"call_{uuid.uuid5(uuid.NAMESPACE_URL, req.run_id + req.step_key + str(i)).hex[:12]}",
                          name=c["name"], arguments=c.get("arguments", {}))
                 for i, c in enumerate(out.get("tool_calls") or [])]
        out_tok = len(text or "") // 4 + 8 * len(calls) + 1
        self.tokens += in_tok + out_tok
        return LLMReply(text=text, tool_calls=calls, usage=Usage(input_tokens=in_tok, output_tokens=out_tok),
                        cost_usd=round((in_tok + out_tok) / 1000 * self.price_per_1k, 8),
                        model=req.model, provider="mock")
