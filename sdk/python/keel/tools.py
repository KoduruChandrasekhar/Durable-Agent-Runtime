"""Typed tool registry with mandatory effect declarations.

Every tool declares how it stays safe under retries:

* ``effect="none"``        read-only; retried freely
* ``effect="idempotent"``  the service deduplicates by the idempotency key
* ``effect="reconcile"``   no keys, but past effects can be looked up; on any
                           re-attempt the runtime calls ``reconcile`` first

A tool without one is rejected at registration, so there is no silent gap.
Tools that act (idempotent / reconcile) must accept a keyword-only ``key``.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, get_type_hints

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from .errors import EffectDeclarationError, ToolNotFound, ToolValidationError
from .llm import ToolCall
from .retry import DEFAULT, RetryPolicy

EFFECTS = ("none", "idempotent", "reconcile")
RISKS = ("low", "medium", "high")
_RESERVED = {"key", "secrets", "ctx"}


@dataclass
class ToolSpec:
    name: str
    description: str
    fn: Callable[..., Any]
    effect: str
    risk: str = "low"
    timeout: float = 30.0
    reconcile: Callable[..., Any] | None = None
    dry_run: Callable[..., Any] | None = None
    args_model: type[BaseModel] | None = None
    output_type: Any = None
    max_per_run: int | None = None
    secrets: tuple[str, ...] = ()
    retry: RetryPolicy = DEFAULT
    trust_output: str = "untrusted"
    source: str = "python"  # python | mcp | http | sandbox
    raw_schema: dict | None = None
    takes_key: bool = False
    takes_secrets: bool = False
    meta: dict = field(default_factory=dict)

    def json_schema(self) -> dict:
        if self.raw_schema is not None:
            return self.raw_schema
        if self.args_model is None:
            return {"type": "object", "properties": {}}
        s = self.args_model.model_json_schema()
        s.pop("title", None)
        return s

    def openai_schema(self) -> dict:
        return {"type": "function", "function": {"name": self.name, "description": self.description,
                                                 "parameters": self.json_schema()}}

    def validate(self, args: dict) -> dict:
        """Validate (and coerce) arguments; raises ToolValidationError."""
        if not isinstance(args, dict):
            raise ToolValidationError(self.name, "arguments must be a JSON object")
        if self.args_model is None:
            if self.raw_schema is not None:
                _check_required(self.name, self.raw_schema, args)
            return dict(args)
        try:
            m = self.args_model.model_validate(args)
        except ValidationError as e:
            errs = [{"loc": ".".join(str(x) for x in er["loc"]), "msg": er["msg"]} for er in e.errors()]
            raise ToolValidationError(self.name, errs) from None
        return m.model_dump(mode="json")


def _check_required(name: str, schema: dict, args: dict) -> None:
    for req in schema.get("required", []) or []:
        if req not in args:
            raise ToolValidationError(name, [{"loc": req, "msg": "field required"}])
    props = schema.get("properties") or {}
    types: dict[str, type | tuple[type, ...]] = {"string": str, "integer": int, "number": (int, float),
                                                 "boolean": bool, "object": dict, "array": list}
    for k, v in args.items():
        if schema.get("additionalProperties") is False and k not in props:
            raise ToolValidationError(name, [{"loc": k, "msg": "unexpected field"}])
        want = types.get((props.get(k) or {}).get("type", ""))
        if want and not isinstance(v, want):
            raise ToolValidationError(name, [{"loc": k, "msg": f"expected {props[k]['type']}"}])


@dataclass
class ToolInvocation:
    """What calling a @tool function returns: a request for ctx.tool()."""

    spec: ToolSpec
    args: dict


class ToolFunction:
    def __init__(self, spec: ToolSpec):
        self.spec = spec
        self.__name__ = spec.name
        self.__doc__ = spec.description

    def __call__(self, *args: Any, **kwargs: Any) -> ToolInvocation:
        sig = inspect.signature(self.spec.fn)
        bound = sig.bind_partial(*args, **kwargs)
        return ToolInvocation(self.spec, {k: v for k, v in bound.arguments.items() if k not in _RESERVED})

    def __repr__(self) -> str:
        return f"<tool {self.spec.name} effect={self.spec.effect} risk={self.spec.risk}>"


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def add(self, spec: ToolSpec) -> ToolSpec:
        _check_effect(spec)
        self._tools[spec.name] = spec
        return spec

    def register(self, fn: Callable[..., Any], **kw: Any) -> ToolFunction:
        return tool(fn, registry=self, **kw)

    def get(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError:
            raise ToolNotFound(f"no tool named {name!r}") from None

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    def names(self) -> list[str]:
        return sorted(self._tools)

    def specs(self) -> list[ToolSpec]:
        return [self._tools[n] for n in sorted(self._tools)]

    def merge(self, other: ToolRegistry) -> ToolRegistry:
        out = ToolRegistry()
        out._tools = {**self._tools, **other._tools}
        return out


DEFAULT_REGISTRY = ToolRegistry()


def _check_effect(spec: ToolSpec) -> None:
    if spec.effect not in EFFECTS:
        raise EffectDeclarationError(
            f"tool {spec.name!r} must declare effect='none', 'idempotent' or 'reconcile' "
            f"(got {spec.effect!r}); the runtime refuses tools whose retries could repeat an effect")
    if spec.effect == "reconcile" and spec.reconcile is None:
        raise EffectDeclarationError(f"tool {spec.name!r} uses effect='reconcile' but has no reconcile function")
    if spec.effect in ("idempotent", "reconcile") and spec.source == "python" and not spec.takes_key:
        raise EffectDeclarationError(f"tool {spec.name!r} has effect={spec.effect!r} so it must accept a "
                                     "keyword-only `key` (the idempotency key run_id:step_id)")
    if spec.risk not in RISKS:
        raise EffectDeclarationError(f"tool {spec.name!r}: risk must be one of {RISKS}")


def _args_model(name: str, fn: Callable[..., Any]) -> tuple[type[BaseModel], Any, bool, bool]:
    sig = inspect.signature(fn)
    try:
        hints = get_type_hints(fn)
    except Exception:
        hints = {}
    fields: dict[str, Any] = {}
    takes_key = takes_secrets = False
    for pname, p in sig.parameters.items():
        if pname == "key":
            takes_key = True
            continue
        if pname == "secrets":
            takes_secrets = True
            continue
        if pname == "ctx" or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        ann = hints.get(pname, Any)
        default = ... if p.default is inspect.Parameter.empty else p.default
        fields[pname] = (ann, default)
    model = create_model(f"{name}_args", __config__=ConfigDict(extra="forbid"), **fields)  # type: ignore[call-overload]
    return model, hints.get("return"), takes_key, takes_secrets


def tool(fn: Callable[..., Any] | None = None, *, name: str | None = None, effect: str | None = None,
         risk: str = "low", timeout: float = 30.0, reconcile: Callable[..., Any] | None = None,
         dry_run: Callable[..., Any] | None = None, max_per_run: int | None = None,
         secrets: tuple[str, ...] | list[str] = (), description: str | None = None,
         retry: RetryPolicy | None = None, trust_output: str = "untrusted",
         registry: ToolRegistry | None = None) -> Any:
    """Declare a tool. ``effect`` is mandatory."""

    def deco(f: Callable[..., Any]) -> ToolFunction:
        tname = name or f.__name__
        if effect is None:
            raise EffectDeclarationError(
                f"tool {tname!r} has no effect declaration; use @tool(effect='none'|'idempotent'|'reconcile')")
        model, out_type, takes_key, takes_secrets = _args_model(tname, f)
        spec = ToolSpec(name=tname, description=(description or inspect.getdoc(f) or tname).strip(),
                        fn=f, effect=effect, risk=risk, timeout=timeout, reconcile=reconcile, dry_run=dry_run,
                        args_model=model, output_type=out_type, max_per_run=max_per_run, secrets=tuple(secrets),
                        retry=retry or DEFAULT, trust_output=trust_output, takes_key=takes_key,
                        takes_secrets=takes_secrets)
        (registry or DEFAULT_REGISTRY).add(spec)
        return ToolFunction(spec)

    if fn is not None:
        return deco(fn)
    return deco


class ToolResult(BaseModel):
    """Outcome of one LLM tool call, ready to send back to the model."""

    call_id: str
    name: str
    ok: bool
    output: Any = None
    error: str | None = None
    error_type: str | None = None

    def content(self) -> str:
        import json
        if self.ok:
            return json.dumps(self.output, ensure_ascii=False, default=str)
        return json.dumps({"error": self.error, "type": self.error_type}, ensure_ascii=False)


def tool_result_messages(calls: list[ToolCall], results: list[Any]) -> list[Any]:
    from .llm import Message
    out = []
    for call, res in zip(calls, results, strict=False):
        if isinstance(res, ToolResult):
            content = res.content()
        else:
            import json
            content = json.dumps(res, ensure_ascii=False, default=str)
        out.append(Message(role="tool", tool_call_id=call.id, name=call.name, content=content, trust="untrusted",
                           source=f"tool:{call.name}"))
    return out
