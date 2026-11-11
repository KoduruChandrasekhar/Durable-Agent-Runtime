"""Development-mode guards against nondeterministic workflow code.

While workflow code (not a step) is running, a context variable is set. In dev
mode, time.time, datetime.now, random.* and uuid.uuid4 are wrapped so that
calling them from workflow code raises NonDeterministicCallError; inside steps
they behave normally. Names imported with ``from x import y`` into a workflow's
module are wrapped too when the @workflow decorator runs.
"""

from __future__ import annotations

import contextvars
import datetime as _dt
import functools
import random
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .errors import NonDeterministicCallError

IN_WORKFLOW: contextvars.ContextVar[bool] = contextvars.ContextVar("keel_in_workflow", default=False)

_installed = False
_ALLOWED_CALLERS = ("logging", "asyncio", "concurrent", "threading", "grpc", "keel", "pydantic", "httpx",
                    "httpcore", "anyio", "opentelemetry", "_pytest", "pytest", "hypothesis", "json", "uuid",
                    "random", "tempfile", "email", "http", "urllib", "ssl", "socket", "selectors", "traceback",
                    "warnings", "inspect", "typing", "importlib", "contextlib", "functools", "datetime")


@contextmanager
def sdk_code() -> Iterator[None]:
    """Mark a region (step bodies, SDK internals) as allowed to be nondeterministic."""
    token = IN_WORKFLOW.set(False)
    try:
        yield
    finally:
        IN_WORKFLOW.reset(token)


def _library_prefixes() -> tuple[str, ...]:
    import os
    import sysconfig
    paths = {os.path.dirname(os.path.abspath(__file__))}
    for k in ("stdlib", "platstdlib", "purelib", "platlib"):
        p = sysconfig.get_paths().get(k)
        if p:
            paths.add(os.path.abspath(p))
    return tuple(os.path.normcase(p) for p in paths)


_LIB_PREFIXES = _library_prefixes()


def _caller_allowed() -> bool:
    """Only calls made directly by user code (not the stdlib, installed
    libraries or the SDK itself) are flagged."""
    import os
    f = sys._getframe(2)
    if f is None:
        return True
    mod = f.f_globals.get("__name__", "")
    if mod.split(".")[0] in _ALLOWED_CALLERS:
        return True
    fn = f.f_code.co_filename
    if fn.startswith("<"):
        return True
    return os.path.normcase(os.path.abspath(fn)).startswith(_LIB_PREFIXES)


def _guard(orig: Any, name: str) -> Any:
    if getattr(orig, "__keel_guard__", False):
        return orig

    @functools.wraps(orig)
    def wrapper(*a: Any, **k: Any) -> Any:
        if IN_WORKFLOW.get() and not _caller_allowed():
            raise NonDeterministicCallError(
                f"{name}() called from workflow code; use ctx.now(), ctx.random() or ctx.uuid(), "
                "or move the call into a step")
        return orig(*a, **k)

    wrapper.__keel_guard__ = True  # type: ignore[attr-defined]
    wrapper.__keel_orig__ = orig  # type: ignore[attr-defined]
    return wrapper


class _GuardedDatetime(_dt.datetime):
    @classmethod
    def now(cls, tz: Any = None) -> Any:  # type: ignore[override]
        if IN_WORKFLOW.get() and not _caller_allowed():
            raise NonDeterministicCallError("datetime.now() called from workflow code; use ctx.now()")
        return _dt.datetime.now(tz)

    @classmethod
    def utcnow(cls) -> Any:  # type: ignore[override]
        if IN_WORKFLOW.get() and not _caller_allowed():
            raise NonDeterministicCallError("datetime.utcnow() called from workflow code; use ctx.now()")
        return _dt.datetime.now(_dt.UTC).replace(tzinfo=None)

    @classmethod
    def today(cls) -> Any:  # type: ignore[override]
        if IN_WORKFLOW.get() and not _caller_allowed():
            raise NonDeterministicCallError("datetime.today() called from workflow code; use ctx.now()")
        return _dt.datetime.today()


_RANDOM_FUNCS = ("random", "randint", "randrange", "choice", "choices", "shuffle", "uniform", "sample",
                 "getrandbits", "gauss")
_TIME_FUNCS = ("time", "time_ns")
_originals: dict[str, Any] = {}


def install() -> None:
    """Install the guards process-wide (idempotent)."""
    global _installed
    if _installed:
        return
    for fn in _TIME_FUNCS:
        _originals["time." + fn] = getattr(time, fn)
        setattr(time, fn, _guard(getattr(time, fn), "time." + fn))
    for fn in _RANDOM_FUNCS:
        _originals["random." + fn] = getattr(random, fn)
        setattr(random, fn, _guard(getattr(random, fn), "random." + fn))
    _originals["uuid.uuid4"] = uuid.uuid4
    uuid.uuid4 = _guard(uuid.uuid4, "uuid.uuid4")  # type: ignore[assignment]
    _installed = True


def guard_module_globals(g: dict) -> None:
    """Wrap nondeterministic names a workflow module imported directly."""
    if not _installed:
        return
    for name, val in list(g.items()):
        if val is _dt.datetime:
            g[name] = _GuardedDatetime
        elif isinstance(val, type(_dt)) and val is _dt:
            continue
        else:
            orig = getattr(val, "__keel_orig__", None) or val
            for key, o in _originals.items():
                if orig is o:
                    g[name] = _guard(o, key)
                    break


def is_installed() -> bool:
    return _installed
