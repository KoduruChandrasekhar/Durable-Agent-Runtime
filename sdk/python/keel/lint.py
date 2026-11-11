"""Static checks for workflow code (functions decorated with @workflow).

Flags what makes replay take a different path: reading the clock, random
numbers, uuid4, environment variables, file or network I/O, threads, and
iterating over sets (string hashing is randomized per process).

    keel-py lint demos/
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

BANNED_CALLS = {
    "time.time": "use ctx.now()", "time.time_ns": "use ctx.now()", "datetime.now": "use ctx.now()",
    "datetime.utcnow": "use ctx.now()", "datetime.today": "use ctx.now()", "date.today": "use ctx.now()",
    "datetime.datetime.now": "use ctx.now()", "random.random": "use ctx.random()",
    "random.randint": "use ctx.random()", "random.choice": "use ctx.random()", "random.shuffle": "use ctx.random()",
    "random.uniform": "use ctx.random()", "uuid.uuid4": "use ctx.uuid()", "uuid4": "use ctx.uuid()",
    "os.getenv": "read configuration in a step", "open": "do file I/O in a step",
    "requests.get": "do network I/O in a step", "requests.post": "do network I/O in a step",
    "httpx.get": "do network I/O in a step", "httpx.post": "do network I/O in a step",
    "threading.Thread": "workflows must not start threads", "asyncio.to_thread": "run blocking work in a step",
    "time.sleep": "use await ctx.sleep(...)", "asyncio.sleep": "use await ctx.sleep(...) (durable)",
}


@dataclass
class Finding:
    path: str
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return _dotted(node.value) + "." + node.attr
    return ""


def _is_workflow(fn: ast.AST) -> bool:
    if not isinstance(fn, (ast.AsyncFunctionDef, ast.FunctionDef)):
        return False
    for d in fn.decorator_list:
        target = d.func if isinstance(d, ast.Call) else d
        if _dotted(target).split(".")[-1] == "workflow":
            return True
    return False


class _Visitor(ast.NodeVisitor):
    def __init__(self, path: str):
        self.path = path
        self.findings: list[Finding] = []
        self.in_step_lambda = 0

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Lambdas passed to ctx.step run as steps; their bodies may be nondeterministic.
        self.in_step_lambda += 1
        self.generic_visit(node)
        self.in_step_lambda -= 1

    def visit_Call(self, node: ast.Call) -> None:
        name = _dotted(node.func)
        if not self.in_step_lambda:
            for banned, hint in BANNED_CALLS.items():
                if name == banned or (banned.count(".") == 1 and name.endswith("." + banned)):
                    self.findings.append(Finding(self.path, node.lineno, f"{name}() in workflow code: {hint}"))
                    break
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if _dotted(node.value) == "os.environ" and not self.in_step_lambda:
            self.findings.append(Finding(self.path, node.lineno, "os.environ in workflow code: read it in a step"))
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        self._check_iter(node.iter, node.lineno)
        self.generic_visit(node)

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self._check_iter(node.iter, getattr(node.iter, "lineno", 0))
        self.generic_visit(node)

    def _check_iter(self, it: ast.AST, line: int) -> None:
        is_set = isinstance(it, (ast.Set, ast.SetComp)) or (
            isinstance(it, ast.Call) and _dotted(it.func) in ("set", "frozenset"))
        if is_set:
            self.findings.append(Finding(self.path, line, "iterating over a set: order differs between "
                                                          "processes; iterate over sorted(...)"))


def lint_source(src: str, path: str = "<string>") -> list[Finding]:
    tree = ast.parse(src)
    out: list[Finding] = []
    for node in ast.walk(tree):
        if _is_workflow(node):
            v = _Visitor(path)
            for stmt in node.body:  # type: ignore[attr-defined]
                v.visit(stmt)
            out += v.findings
    return out


def lint_paths(paths: list[str]) -> list[Finding]:
    out: list[Finding] = []
    for p in paths:
        pp = Path(p)
        files = [pp] if pp.is_file() else sorted(pp.rglob("*.py"))
        for f in files:
            try:
                out += lint_source(f.read_text(encoding="utf-8"), str(f))
            except SyntaxError as e:
                out.append(Finding(str(f), e.lineno or 0, f"syntax error: {e.msg}"))
    return out


def main(argv: list[str] | None = None) -> int:
    findings = lint_paths(argv or sys.argv[1:] or ["."])
    for f in findings:
        print(f)
    print(f"{len(findings)} finding(s)")
    return 1 if findings else 0
