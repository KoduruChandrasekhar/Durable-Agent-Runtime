"""The long-running research agent.

1. Plans sub-questions, then runs one child workflow per sub-question in
   parallel (batched).
2. Children search the web and read pages; page text is untrusted, so the
   runs become tainted.
3. Notes go into a vector memory (the mock web service's /memory index).
4. It sleeps between batches to respect search-API rate limits (durable timers).
5. It asks a human to confirm or edit the outline (approval with edits).
6. It writes the report, and stops cleanly if the budget runs out.
7. Long histories roll over with ctx.continue_as_new.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx
from keel import Context, StepError, tool, workflow
from keel.errors import LLMError
from keel.llm import system, untrusted, user
from keel.tools import ToolRegistry
from pydantic import BaseModel, Field

TOOLS = ToolRegistry()
_http: httpx.AsyncClient | None = None


def web() -> httpx.AsyncClient:
    global _http
    if _http is None:
        base = os.environ.get("KEEL_WEB_URL", os.environ.get("KEEL_MOCKS_URL", "http://localhost:9000") + "/web")
        _http = httpx.AsyncClient(base_url=base, timeout=10)
    return _http


@tool(effect="none", risk="low", registry=TOOLS)
async def web_search(query: str, n: int = 3) -> list[dict]:
    """Search the web; returns page ids, titles and urls."""
    r = await web().get("/search", params={"q": query, "n": n})
    r.raise_for_status()
    return r.json()


@tool(effect="none", risk="low", registry=TOOLS)
async def fetch_page(page_id: str) -> dict:
    """Fetch a page's text (untrusted content)."""
    r = await web().get(f"/pages/{page_id}")
    r.raise_for_status()
    return r.json()


@tool(effect="idempotent", risk="low", registry=TOOLS)
async def remember(namespace: str, text: str, *, key: str) -> dict:
    """Store a note in the agent's vector memory."""
    r = await web().post("/memory", json={"namespace": namespace, "text": text}, headers={"Idempotency-Key": key})
    r.raise_for_status()
    return r.json()


@tool(effect="none", risk="medium", registry=TOOLS)
async def confirm_outline(title: str, sections: list[str]) -> dict:
    """Propose the report outline; a human confirms or edits it (policy: research-outline-needs-review)."""
    return {"title": title, "sections": sections}


async def _dry_publish(title: str, body: str, **_: Any) -> dict:
    return {"report_id": "rep_dryrun", "title": title}


@tool(effect="idempotent", risk="high", registry=TOOLS, dry_run=_dry_publish)
async def publish_report(title: str, body: str, *, key: str) -> dict:
    """Publish the finished report."""
    r = await web().post("/reports", json={"title": title, "body": body}, headers={"Idempotency-Key": key})
    r.raise_for_status()
    return r.json()


class Question(BaseModel):
    question: str
    max_pages: int = 2
    namespace: str
    model: str = "router:default"


class ResearchInput(BaseModel):
    topic: str
    batch_size: int = 2
    pause_s: float = 60.0
    max_pages: int = 2
    model: str = "router:default"
    max_history: int = 400
    # Carried across continue_as_new:
    questions: list[str] = Field(default_factory=list)
    next_index: int = 0
    collected: list[dict] = Field(default_factory=list)
    memory_ns: str = ""
    rollovers: int = 0


@workflow(name="research_question", version="1", max_steps=60)
async def research_question(ctx: Context, q: Question) -> dict:
    results = await ctx.tool(web_search(query=q.question, n=q.max_pages))
    notes: list[str] = []
    sources: list[str] = []
    for r in results:
        page = await ctx.tool(fetch_page(page_id=r["page_id"]), key=r["page_id"])
        reply = await ctx.llm(
            [system('[agent:research:notes] Summarize the page into short factual notes. '
                    'Reply with JSON {"notes": ["..."]}.'),
             untrusted(page["text"], source=page["url"])],
            model=q.model, key=r["page_id"], response_format={"type": "json_object"})
        try:
            page_notes = [str(n) for n in reply.parsed().get("notes", [])]
        except (ValueError, AttributeError):
            page_notes = []
        notes += page_notes
        sources.append(page["url"])
        if page_notes:
            await ctx.tool(remember(namespace=q.namespace, text=" ".join(page_notes)), key=r["page_id"])
    return {"question": q.question, "notes": notes, "sources": sources}


@workflow(name="research_agent", version="1", max_steps=600)
async def research_agent(ctx: Context, inp: ResearchInput) -> dict:
    ns = inp.memory_ns or f"research-{ctx.run_id}"
    questions = list(inp.questions)
    if not questions:
        plan = await ctx.llm([system('[agent:research:plan] Break the topic into 3-4 sub-questions. '
                                     'Reply with JSON {"sub_questions": ["..."]}.'), user(inp.topic)],
                             model=inp.model, response_format={"type": "json_object"})
        try:
            questions = [str(q) for q in plan.parsed()["sub_questions"]][:6]
        except (ValueError, KeyError, TypeError):
            questions = [inp.topic]
    collected = list(inp.collected)
    i = inp.next_index
    while i < len(questions):
        batch = questions[i:i + inp.batch_size]
        results = await asyncio.gather(*(
            ctx.child(research_question, Question(question=q, max_pages=inp.max_pages, namespace=ns, model=inp.model),
                      key=f"q{i + j}")
            for j, q in enumerate(batch)))
        collected += results
        i += len(batch)
        if i < len(questions):
            await ctx.sleep(seconds=inp.pause_s)  # stay under the search API's rate limit
            if ctx.history_length > inp.max_history:
                ctx.continue_as_new(inp.model_copy(update={
                    "questions": questions, "next_index": i, "collected": collected, "memory_ns": ns,
                    "rollovers": inp.rollovers + 1}))
    notes = "\n".join(f"- {n}" for r in collected for n in r.get("notes", []))
    sources = sorted({s for r in collected for s in r.get("sources", [])})
    try:
        outline = (await ctx.llm([system('[agent:research:outline] Propose a report outline as JSON '
                                         '{"title": "...", "sections": ["..."]}.'),
                                  untrusted(f"Topic: {inp.topic}\nNotes:\n{notes}", source="research_notes")],
                                 model=inp.model, response_format={"type": "json_object"})).parsed()
    except ValueError:
        outline = {"title": f"Report: {inp.topic}", "sections": ["Findings"]}
    confirmed = await ctx.tool(confirm_outline(title=str(outline.get("title") or inp.topic),
                                               sections=[str(s) for s in outline.get("sections") or ["Findings"]]))
    try:
        report = await ctx.llm([system("[agent:research:write] Write the report following the outline."),
                                untrusted(f"Outline: {confirmed}\nNotes:\n{notes}", source="research_notes")],
                               model=inp.model, max_tokens=2000)
    except StepError as e:
        if e.error_type in ("BudgetExceeded",):
            return {"status": "budget_exhausted", "topic": inp.topic, "questions": len(questions),
                    "notes": notes.count("\n") + 1 if notes else 0, "rollovers": inp.rollovers}
        raise
    except LLMError:
        raise
    body = f"# {confirmed['title']}\n\n" + (report.text or "") + "\n\nSources:\n" + "\n".join(sources)
    published = await ctx.tool(publish_report(title=confirmed["title"], body=body))
    return {"status": "published", "report_id": published.get("report_id"), "title": confirmed["title"],
            "sections": confirmed["sections"], "questions": len(questions), "sources": len(sources),
            "rollovers": inp.rollovers}


ALL = [research_agent, research_question]
