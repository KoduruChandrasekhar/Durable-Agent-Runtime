"""Exceptions raised by the keel SDK.

Two families:

* ``KeelError`` subclasses are ordinary ``Exception`` s that workflow code may
  catch (a step that failed permanently, a policy denial, a budget limit).
* ``ControlFlow`` subclasses inherit from ``BaseException`` (as
  ``asyncio.CancelledError`` does) so a user's ``except Exception:`` can never
  swallow a suspension, a lost lease or a cancellation.
"""

from __future__ import annotations

from typing import Any


class KeelError(Exception):
    """Base class for errors workflow code can handle."""

    retryable = False


class StepError(KeelError):
    """A step failed permanently (retries exhausted or a non-retryable error).

    On replay the same recorded error is raised again at the same point.
    """

    def __init__(self, step_id: str, error_type: str, message: str, details: dict | None = None):
        super().__init__(f"step {step_id} failed: {error_type}: {message}")
        self.step_id = step_id
        self.error_type = error_type
        self.message = message
        self.details = details or {}

    @classmethod
    def from_record(cls, step_id: str, err: dict) -> StepError:
        return cls(step_id, err.get("type", "Error"), err.get("message", ""), err.get("details"))

    def to_json(self) -> dict:
        return {"type": "StepError", "step_id": self.step_id, "error_type": self.error_type,
                "message": self.message, "details": self.details}


class NonDeterminismError(KeelError):
    """Replay requested a different step than the history recorded.

    The run is parked for an operator instead of silently doing something else.
    """


class NonDeterministicCallError(KeelError):
    """Workflow code (not a step) read the clock, randomness or uuid4 in dev mode."""


class MissingStepKeyError(KeelError):
    """A step was requested from a concurrent branch without an explicit key."""


class EffectDeclarationError(KeelError):
    """A tool was registered without a valid effect declaration."""


class SerializationError(KeelError):
    """A step input or output is not JSON-serializable (failed at record time)."""


class PolicyDenied(KeelError):
    def __init__(self, tool: str, reason: str, rule_ids: list[str] | None = None):
        super().__init__(f"policy denied {tool}: {reason}")
        self.tool = tool
        self.reason = reason
        self.rule_ids = rule_ids or []


class ApprovalRejected(KeelError):
    def __init__(self, tool: str, approval_id: str, decision: str, decided_by: str = "", comment: str = ""):
        super().__init__(f"approval {approval_id} for {tool} was {decision}"
                         + (f" by {decided_by}" if decided_by else "") + (f": {comment}" if comment else ""))
        self.tool = tool
        self.approval_id = approval_id
        self.decision = decision
        self.decided_by = decided_by
        self.comment = comment


class ToolValidationError(KeelError):
    def __init__(self, tool: str, errors: Any):
        super().__init__(f"invalid arguments for {tool}: {errors}")
        self.tool = tool
        self.errors = errors


class ToolNotFound(KeelError):
    pass


class LoopDetected(KeelError):
    pass


class ToolLimitExceeded(KeelError):
    pass


class LimitExceeded(KeelError):
    """A per-run limit (steps, turns or wall-clock time) was reached."""


class BudgetExceeded(KeelError):
    """The gateway could not reserve the estimated cost within the budget."""


class LLMError(KeelError):
    def __init__(self, code: str, message: str, retryable: bool = False, retry_after: float | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = retryable
        self.retry_after = retry_after


class ChildRunFailed(KeelError):
    def __init__(self, child_run_id: str, status: str, error: Any):
        super().__init__(f"child run {child_run_id} {status}: {error}")
        self.child_run_id = child_run_id
        self.status = status
        self.error = error


class WorkflowFailed(KeelError):
    """Raised by ``ctx.fail(...)``."""


class SignalTimeout(KeelError, TimeoutError):
    pass


class OutputCheckFailed(KeelError):
    pass


class ExtractionError(KeelError):
    retryable = True


# --------------------------------------------------------------------- control flow


class ControlFlow(BaseException):
    """Internal signals that must unwind workflow code untouched."""


class Suspend(ControlFlow):
    """The run must leave the worker (durable wait or long retry backoff)."""

    def __init__(self, reason: str = "wait", available_at_ms: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.available_at_ms = available_at_ms


class LeaseLost(ControlFlow):
    """The server rejected our epoch: another worker owns the run now."""


class RunCancelled(ControlFlow):
    """The run was cancelled through the API."""


class ContinueAsNew(ControlFlow):
    def __init__(self, state: Any):
        super().__init__("continue_as_new")
        self.state = state


class WorkerCrash(ControlFlow):
    """Testing only: simulates the worker process dying at this point."""
