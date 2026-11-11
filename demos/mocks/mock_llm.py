"""OpenAI-compatible mock LLM server for load, chaos and eval runs.

Two "providers" share the process: /v1 (primary) and /fallback/v1 (the
hosted fallback in outage tests). Each has its own chaos settings: latency,
error rate (429/503 with Retry-After) and a timed outage window.

    uvicorn demos.mocks.mock_llm:app --port 8000
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from . import brain


class Target:
    def __init__(self, name: str) -> None:
        self.name = name
        lat = os.environ.get("KEEL_MOCK_LLM_LATENCY_MS", "0,0").split(",")
        self.latency = (float(lat[0]), float(lat[-1]))
        self.error_rate = float(os.environ.get("KEEL_MOCK_LLM_ERROR_RATE", "0"))
        self.error_statuses = [503, 429]
        self.outage_until = 0.0
        self.outage_status = 503
        self.calls = 0
        self.errors = 0
        self.tokens = 0
        self.rate_limited_seen = 0
        self.rng = random.Random()


TARGETS = {"primary": Target("primary"), "fallback": Target("fallback")}
app = FastAPI(title="mock-llm")


def _to_brain(body: dict) -> dict:
    msgs = []
    for m in body.get("messages", []):
        mm = {"role": m.get("role"), "content": m.get("content")}
        if m.get("tool_calls"):
            mm["tool_calls"] = [{"id": tc.get("id"), "name": tc["function"]["name"],
                                 "arguments": json.loads(tc["function"].get("arguments") or "{}")}
                                for tc in m["tool_calls"]]
        if m.get("tool_call_id"):
            mm["tool_call_id"] = m["tool_call_id"]
        msgs.append(mm)
    rf = body.get("response_format")
    return {"model": body.get("model", "mock-small"), "messages": msgs, "tools": body.get("tools") or [],
            "temperature": body.get("temperature", 0), "response_format": rf}


def _usage(body: dict, text: str | None, calls: list) -> tuple[int, int]:
    pin = sum(len(str(m.get("content") or "")) // 4 + 4 for m in body.get("messages", []))
    pin += len(json.dumps(body.get("tools") or [])) // 4
    pout = len(text or "") // 4 + sum(len(json.dumps(c.get("arguments", {}))) // 4 + 8 for c in calls) + 1
    return pin, pout


async def _handle(target: Target, req: Request) -> Any:
    body = await req.json()
    target.calls += 1
    lo, hi = target.latency
    if hi > 0:
        await asyncio.sleep(target.rng.uniform(lo, hi) / 1000)
    if time.time() < target.outage_until:
        target.errors += 1
        return JSONResponse({"error": {"message": "mock outage", "type": "overloaded"}}, status_code=target.outage_status)
    if target.error_rate and target.rng.random() < target.error_rate:
        target.errors += 1
        status = target.rng.choice(target.error_statuses)
        headers = {"Retry-After": "0.5"} if status == 429 else {}
        return JSONResponse({"error": {"message": f"mock error {status}"}}, status_code=status, headers=headers)
    out = brain.respond(_to_brain(body))
    text = out.get("text")
    calls = out.get("tool_calls") or []
    tcs = [{"id": c.get("id") or "call_" + uuid.uuid4().hex[:12], "type": "function",
            "function": {"name": c["name"], "arguments": json.dumps(c.get("arguments", {}))}} for c in calls]
    pin, pout = _usage(body, text, calls)
    target.tokens += pin + pout
    model = body.get("model", "mock")
    if body.get("stream"):
        async def gen() -> Any:
            cid = "chatcmpl-" + uuid.uuid4().hex[:10]
            for w in (text or "").split(" "):
                chunk = {"id": cid, "model": model, "choices": [{"index": 0, "delta": {"content": w + " "}}]}
                yield f"data: {json.dumps(chunk)}\n\n"
                await asyncio.sleep(0.01)
            if tcs:
                delta = {"tool_calls": [{"index": i, "id": t["id"], "function": t["function"]} for i, t in enumerate(tcs)]}
                yield f"data: {json.dumps({'id': cid, 'model': model, 'choices': [{'index': 0, 'delta': delta}]})}\n\n"
            fin = {"id": cid, "model": model, "choices": [{"index": 0, "delta": {},
                                                          "finish_reason": "tool_calls" if tcs else "stop"}],
                   "usage": {"prompt_tokens": pin, "completion_tokens": pout}}
            yield f"data: {json.dumps(fin)}\n\n"
            yield "data: [DONE]\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream")
    msg: dict[str, Any] = {"role": "assistant", "content": text}
    if tcs:
        msg["tool_calls"] = tcs
    return {"id": "chatcmpl-" + uuid.uuid4().hex[:10], "object": "chat.completion", "model": model,
            "choices": [{"index": 0, "message": msg, "finish_reason": "tool_calls" if tcs else "stop"}],
            "usage": {"prompt_tokens": pin, "completion_tokens": pout, "total_tokens": pin + pout}}


@app.post("/v1/chat/completions")
async def primary(req: Request) -> Any:
    return await _handle(TARGETS["primary"], req)


@app.post("/fallback/v1/chat/completions")
async def fallback(req: Request) -> Any:
    return await _handle(TARGETS["fallback"], req)


@app.post("/admin/chaos")
async def chaos(cfg: dict) -> dict:
    names = ["primary", "fallback"] if cfg.get("target", "primary") == "both" else [cfg.get("target", "primary")]
    for n in names:
        t = TARGETS[n]
        if "error_rate" in cfg:
            t.error_rate = float(cfg["error_rate"])
        if "error_statuses" in cfg:
            t.error_statuses = list(cfg["error_statuses"])
        if "latency_ms_min" in cfg or "latency_ms_max" in cfg:
            t.latency = (float(cfg.get("latency_ms_min", 0)), float(cfg.get("latency_ms_max", 0)))
        if "outage_s" in cfg:
            t.outage_until = time.time() + float(cfg["outage_s"])
            t.outage_status = int(cfg.get("outage_status", 503))
    if "gullibility" in cfg:
        brain.GULLIBILITY = float(cfg["gullibility"])
    return await stats()


@app.get("/admin/stats")
async def stats() -> dict:
    return {n: {"calls": t.calls, "errors": t.errors, "tokens": t.tokens, "error_rate": t.error_rate,
                "latency_ms": t.latency, "outage_remaining_s": max(0.0, t.outage_until - time.time())}
            for n, t in TARGETS.items()}


@app.post("/admin/reset")
async def reset() -> dict:
    for n in TARGETS:
        TARGETS[n] = Target(n)
    brain.GULLIBILITY = 1.0
    return {"ok": True}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
