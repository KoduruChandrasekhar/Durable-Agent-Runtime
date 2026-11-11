"""Worker process serving every demo workflow.

    python -m demos.worker
Env: KEEL_SERVER (grpc addr), KEEL_GATEWAY_URL, KEEL_MOCKS_URL / KEEL_*_URL,
KEEL_MAX_CONCURRENT, KEEL_MCP_SERVERS (JSON list of MCPServerConfig dicts),
KEEL_METRICS_PORT.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from bench import workflows as bench_workflows
from keel.tools import ToolRegistry
from keel.worker import Worker

from .analysis_agent import agent as analysis
from .refund_agent import agent as refund
from .refund_agent import tools as refund_tools
from .research_agent import agent as research


def build_registry() -> ToolRegistry:
    return refund_tools.TOOLS.merge(research.TOOLS).merge(analysis.TOOLS)


async def main() -> None:
    logging.basicConfig(level=os.environ.get("KEEL_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    tools = build_registry()
    mcp = None
    if os.environ.get("KEEL_MCP_SERVERS"):
        from keel.mcp import MCPServerConfig, MCPToolset
        cfgs = [MCPServerConfig(**c) for c in json.loads(os.environ["KEEL_MCP_SERVERS"])]
        mcp = await MCPToolset(cfgs, tools).connect()
        logging.info("MCP tools registered: %s (refused: %s)", mcp.registered, mcp.refused)
    workflows = [*refund.ALL, *research.ALL, analysis.analysis_agent, *bench_workflows.ALL]
    w = Worker(workflows=workflows, tools=tools, max_concurrent=int(os.environ.get("KEEL_MAX_CONCURRENT", "100")),
               dev_mode=os.environ.get("KEEL_DEV_MODE") == "1")
    try:
        await w.run()
    finally:
        if mcp is not None:
            await mcp.close()


if __name__ == "__main__":
    asyncio.run(main())
