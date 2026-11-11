"""keel: durable execution for AI agents.

    from keel import workflow, Context, tool

    @workflow(name="support_agent", version="3")
    async def support_agent(ctx: Context, ticket: Ticket) -> Outcome:
        reply = await ctx.llm([...], model="router:support", tools=TOOLS)
        ...
"""

from .errors import (
    ApprovalRejected,
    BudgetExceeded,
    ChildRunFailed,
    EffectDeclarationError,
    KeelError,
    LimitExceeded,
    LLMError,
    LoopDetected,
    MissingStepKeyError,
    NonDeterminismError,
    NonDeterministicCallError,
    OutputCheckFailed,
    PolicyDenied,
    SignalTimeout,
    StepError,
    Suspend,
    ToolLimitExceeded,
    ToolValidationError,
    WorkflowFailed,
)
from .guard import SecretStore, check_output, mask_pii
from .llm import (
    ChatRequest,
    GatewayClient,
    LLMReply,
    Message,
    MockLLM,
    ToolCall,
    system,
    trusted,
    untrusted,
    user,
)
from .retry import RetryPolicy
from .tools import ToolRegistry, ToolResult, tool, tool_result_messages
from .workflow import Context, Limits, workflow

__all__ = [
    "ApprovalRejected", "BudgetExceeded", "ChatRequest", "ChildRunFailed", "Context", "EffectDeclarationError",
    "GatewayClient", "KeelError", "LLMError", "LLMReply", "LimitExceeded", "Limits", "LoopDetected", "Message",
    "MissingStepKeyError", "MockLLM", "NonDeterminismError", "NonDeterministicCallError", "OutputCheckFailed",
    "PolicyDenied", "RetryPolicy", "SecretStore", "SignalTimeout", "StepError", "Suspend", "ToolCall",
    "ToolLimitExceeded", "ToolRegistry", "ToolResult", "ToolValidationError", "WorkflowFailed", "check_output",
    "mask_pii", "system", "tool", "tool_result_messages", "trusted", "untrusted", "user", "workflow",
]
__version__ = "0.1.0"
