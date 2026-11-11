"""Retry policies with exponential backoff and full jitter.

delay = random(0, min(max_interval, initial * multiplier ** (attempt - 1)))

The worker computes the delay once and records it in StepFailed, so replay
uses the same value.
"""

from __future__ import annotations

import random as _random
from dataclasses import dataclass, field
from typing import Any

from . import errors

# Errors that retrying cannot fix.
_ALWAYS_FINAL: tuple[type[BaseException], ...] = (
    errors.PolicyDenied, errors.ApprovalRejected, errors.ToolValidationError, errors.BudgetExceeded,
    errors.LoopDetected, errors.ToolLimitExceeded, errors.LimitExceeded, errors.SerializationError,
    errors.NonDeterminismError, errors.NonDeterministicCallError, errors.EffectDeclarationError,
    errors.WorkflowFailed, errors.ChildRunFailed, errors.OutputCheckFailed, errors.ToolNotFound,
)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    initial: float = 1.0
    multiplier: float = 2.0
    max_interval: float = 60.0
    jitter: bool = True
    non_retryable: tuple[Any, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        object.__setattr__(self, "non_retryable", tuple(self.non_retryable))

    def delay(self, attempt: int, rng: _random.Random | None = None) -> float:
        """Backoff before attempt+1, after `attempt` failures (attempt >= 1)."""
        cap = min(self.max_interval, self.initial * (self.multiplier ** max(0, attempt - 1)))
        if not self.jitter:
            return cap
        return (rng or _random).uniform(0, cap)

    def is_retryable(self, exc: BaseException) -> bool:
        if isinstance(exc, _ALWAYS_FINAL):
            return False
        for nr in self.non_retryable:
            if isinstance(nr, str):
                if type(exc).__name__ == nr:
                    return False
            elif isinstance(exc, nr):
                return False
        # Exceptions may say so themselves (LLMError, HTTP client errors...).
        return getattr(exc, "retryable", None) is not False


DEFAULT = RetryPolicy()
NO_RETRY = RetryPolicy(max_attempts=1)
# Compensations are retried until they succeed (with long backoffs suspending the run).
FOREVER = RetryPolicy(max_attempts=10**9, initial=1.0, max_interval=300.0)
