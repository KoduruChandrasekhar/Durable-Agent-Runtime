"""Python twin of the server's CEL policy engine, used by LocalServer and the
simulation. The conformance suite checks both engines decide identically.

Semantics (same as server/internal/policy):
* rules apply by tool name, tool risk and tenant; ``when`` is CEL (empty = true)
* deny beats require_approval beats allow
* a rule whose condition errors fails closed (counts as matched)
* require_approval rules a human already approved for this call are skipped
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .protocol import PolicyDecision

_DUR = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)$")


def parse_duration_ms(s: str | None) -> int:
    if not s:
        return 0
    total = 0.0
    for part in re.findall(r"\d+(?:\.\d+)?(?:ms|s|m|h|d)", s):
        m = _DUR.match(part)
        if not m:
            raise ValueError(f"bad duration {s!r}")
        n, unit = float(m.group(1)), m.group(2)
        total += n * {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    if total == 0 and s not in ("0", "0s"):
        raise ValueError(f"bad duration {s!r}")
    return int(total)


def _normalize(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _normalize(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_normalize(x) for x in v]
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e15:
        return int(v)
    return v


@dataclass
class Rule:
    id: str
    action: str
    tool: str = ""
    tool_risk: str = ""
    tenants: list[str] = field(default_factory=list)
    when: str = ""
    reason: str = ""
    approvers: list[str] = field(default_factory=list)
    expires_in: str = ""
    on_expiry: str = "reject"
    _prog: Any = None
    _expires_ms: int = 0

    def applies(self, tool: str, risk: str, tenant: str) -> bool:
        if self.tool and self.tool != "*" and self.tool != tool:
            return False
        if self.tool_risk and self.tool_risk != risk:
            return False
        return not (self.tenants and tenant not in self.tenants)

    def describe(self) -> str:
        if self.reason:
            return f"{self.id}: {self.reason}"
        return f"{self.id}: {self.when}" if self.when else self.id


class PolicyEngine:
    def __init__(self, rules: list[Rule], version: str):
        self.rules = rules
        self.version = version

    @classmethod
    def parse(cls, text: str) -> PolicyEngine:
        import celpy
        from celpy import celtypes

        data = yaml.safe_load(text) or []
        if isinstance(data, dict):
            data = data.get("rules") or []
        env = celpy.Environment()
        rules: list[Rule] = []
        seen: set[str] = set()

        def count(run: Any, name: Any) -> Any:
            try:
                counts = run.get(celtypes.StringType("tool_counts"))
                v = counts.get(name) if counts is not None else None
                return celtypes.IntType(int(v) if v is not None else 0)
            except Exception:
                return celtypes.IntType(0)

        for i, raw in enumerate(data):
            r = Rule(**{k: v for k, v in raw.items() if k in Rule.__dataclass_fields__ and not k.startswith("_")})
            if not r.id:
                raise ValueError(f"rule {i}: missing id")
            if r.id in seen:
                raise ValueError(f"rule {r.id}: duplicate id")
            seen.add(r.id)
            if r.action not in ("allow", "deny", "require_approval"):
                raise ValueError(f"rule {r.id}: bad action {r.action!r}")
            r.on_expiry = r.on_expiry or "reject"
            r._expires_ms = parse_duration_ms(r.expires_in)
            expr = r.when.strip() or "true"
            r._prog = env.program(env.compile(expr), functions={"count": count})
            rules.append(r)
        version = hashlib.sha256(text.encode()).hexdigest()[:12]
        return cls(rules, version)

    @classmethod
    def load(cls, path: str | Path) -> PolicyEngine:
        return cls.parse(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def empty(cls) -> PolicyEngine:
        return cls([], "empty")

    def evaluate(self, tool: str, risk: str, tenant: str, args: dict, run: dict,
                 approved_rules: list[str] | None = None) -> PolicyDecision:
        import celpy
        from celpy.evaluation import CELEvalError

        act = {
            "args": celpy.json_to_cel(_normalize(args or {})),
            "run": celpy.json_to_cel(_normalize(run or {})),
            "tool": celpy.json_to_cel({"name": tool, "risk": risk}),
            "tenant": celpy.json_to_cel(tenant),
        }
        approved = set(approved_rules or [])
        denies: list[tuple[Rule, str]] = []
        approvals: list[Rule] = []
        allows: list[Rule] = []
        for r in self.rules:
            if not r.applies(tool, risk, tenant):
                continue
            note = ""
            try:
                out = r._prog.evaluate(act)
                if isinstance(out, CELEvalError):
                    matched, note = True, f" (condition error: {out.args[0] if out.args else out}; failing closed)"
                elif isinstance(out, bool) or type(out).__name__ == "BoolType":
                    matched = bool(out)
                else:
                    matched, note = True, " (condition not boolean; failing closed)"
            except Exception as e:  # noqa: BLE001
                matched, note = True, f" (condition error: {e}; failing closed)"
            if not matched:
                continue
            if r.action == "deny":
                denies.append((r, r.describe() + note))
            elif r.action == "require_approval":
                if r.id in approved and not note:
                    continue
                approvals.append(r)
            else:
                allows.append(r)
        if denies:
            return PolicyDecision("deny", "; ".join(n for _, n in denies), [r.id for r, _ in denies],
                                  policy_version=self.version)
        if approvals:
            approvers: list[str] = []
            expires = 0
            for r in approvals:
                for a in r.approvers:
                    if a not in approvers:
                        approvers.append(a)
                if r._expires_ms and (expires == 0 or r._expires_ms < expires):
                    expires = r._expires_ms
            return PolicyDecision("require_approval", "; ".join(r.describe() for r in approvals),
                                  [r.id for r in approvals], sorted(approvers), expires,
                                  approvals[0].on_expiry, self.version)
        return PolicyDecision("allow", "no rule blocked the call", [r.id for r in allows], policy_version=self.version)
