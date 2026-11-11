"""JSON (de)serialization of step inputs and outputs.

Values are converted to JSON *at record time* (so a non-serializable result
fails immediately, not on some later replay), and the live path returns the
round-tripped value so live execution and replay hand workflow code exactly the
same types.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import enum
import functools
import hashlib
import json
import uuid
from typing import Any

from pydantic import BaseModel, TypeAdapter

from .errors import SerializationError


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise SerializationError("NaN/Infinity is not JSON-serializable")
        return value
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise SerializationError(f"dict keys must be strings, got {type(k).__name__}")
            out[k] = to_jsonable(v)
        return out
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, (set, frozenset)):
        raise SerializationError("sets are not JSON-serializable (and their order is not deterministic); use a sorted list")
    raise SerializationError(f"{type(value).__name__} is not JSON-serializable")


@functools.lru_cache(maxsize=512)
def _adapter(tp: Any) -> TypeAdapter:
    return TypeAdapter(tp)


def from_jsonable(value: Any, tp: Any = None) -> Any:
    if tp is None or tp is Any or tp is type(None):
        return value
    try:
        return _adapter(tp).validate_python(value)
    except TypeError:
        return value  # unhashable/unsupported annotation: hand back plain JSON


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def stable_hash(value: Any) -> str:
    try:
        data = canonical_json(to_jsonable(value))
    except SerializationError:
        data = canonical_json(repr(value))
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def loads(data: bytes | str | None) -> Any:
    if not data:
        return None
    return json.loads(data)


def error_json(exc: BaseException, **extra: Any) -> dict:
    """A JSON description of an exception for StepFailed / RunFailed."""
    if hasattr(exc, "to_json"):
        d = exc.to_json()
    else:
        d = {"type": type(exc).__name__, "message": str(exc)[:4000]}
    d.update({k: v for k, v in extra.items() if v is not None})
    return d
