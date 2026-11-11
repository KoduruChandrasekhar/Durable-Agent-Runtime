"""Optional third demo: a data-analysis agent that writes Python and runs it
in the sandbox to answer a question about a CSV."""

from __future__ import annotations

import contextvars

from pydantic import BaseModel

from keel import Context, workflow
from keel.llm import system, user
from keel.sandbox import register_sandbox_tool
from keel.tools import ToolRegistry, tool_result_messages

TOOLS = ToolRegistry()
# The dataset travels with the workflow's context into the tool's step.
DATASET: contextvars.ContextVar[str] = contextvars.ContextVar("keel_dataset", default="")
run_python = register_sandbox_tool(TOOLS, files_provider=lambda: {"data.csv": DATASET.get()})


class AnalysisInput(BaseModel):
    question: str
    csv: str
    model: str = "router:default"


@workflow(name="analysis_agent", version="1", max_steps=40)
async def analysis_agent(ctx: Context, inp: AnalysisInput) -> dict:
    DATASET.set(inp.csv)
    messages = [system("[agent:analysis] Answer the question about /tmp/data.csv by writing Python 3 and "
                       "running it with run_python. Then answer in one sentence."), user(inp.question)]
    for _ in range(min(6, ctx.limits.max_turns)):
        reply = await ctx.llm(messages, model=inp.model, tools=[run_python])
        if not reply.tool_calls:
            return {"answer": reply.text, "turns": len(messages)}
        results = await ctx.tools(reply.tool_calls)
        messages += [reply.message, *tool_result_messages(reply.tool_calls, results)]
    ctx.fail("turn limit reached")
    return {}
