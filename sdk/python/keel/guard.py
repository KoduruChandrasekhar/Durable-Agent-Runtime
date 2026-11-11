"""Guardrail helpers that run in the worker: secret references and redaction,
PII masking for logs/traces, and output checks before an answer reaches the
user. Policy decisions themselves are made by the server (CEL rules)."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from typing import Any

from .errors import OutputCheckFailed

_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_CARD = re.compile(r"\b(?:\d[ \-]?){12,18}\d\b")
_AADHAAR = re.compile(r"\b[2-9]\d{3}[ \-]?\d{4}[ \-]?\d{4}\b")
_PHONE = re.compile(r"(?<![\w-])\+?\(?\d(?:[ \-()]{0,2}\d){9,12}\b")
_SECRET_PATTERNS = [
    re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"),            # OpenAI-style keys
    re.compile(r"\bsk-ant-[A-Za-z0-9_\-]{16,}"),        # Anthropic keys
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),                # AWS access key ids
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),      # GitHub tokens
    re.compile(r"\bxox[abpr]-[A-Za-z0-9\-]{10,}"),      # Slack tokens
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


def luhn(digits: str) -> bool:
    if len(digits) < 13:
        return False
    total, alt = 0, False
    for ch in reversed(digits):
        n = ord(ch) - 48
        if alt:
            n *= 2
            if n > 9:
                n -= 9
        total += n
        alt = not alt
    return total % 10 == 0


def _digits(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def mask_pii(text: str) -> str:
    """Mask emails, Luhn-valid card numbers, Aadhaar numbers and phone numbers."""
    def email(m: re.Match) -> str:
        local, _, domain = m.group(0).partition("@")
        return (local[:1] + "*" * max(0, len(local) - 1)) + "@" + domain

    def card(m: re.Match) -> str:
        d = _digits(m.group(0))
        return f"[card ****{d[-4:]}]" if luhn(d) else m.group(0)

    def aadhaar(m: re.Match) -> str:
        d = _digits(m.group(0))
        return f"[aadhaar ****{d[-4:]}]" if len(d) == 12 else m.group(0)

    def phone(m: re.Match) -> str:
        d = _digits(m.group(0))
        return f"[phone ****{d[-2:]}]" if 10 <= len(d) <= 13 else m.group(0)

    text = _EMAIL.sub(email, text)
    text = _CARD.sub(card, text)
    text = _AADHAAR.sub(aadhaar, text)
    return _PHONE.sub(phone, text)


def mask_value(v: Any) -> Any:
    if isinstance(v, str):
        return mask_pii(v)
    if isinstance(v, dict):
        return {k: mask_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [mask_value(x) for x in v]
    return v


class SecretStore:
    """Credentials handed to tools by reference at call time. Secret values
    never appear in prompts, events, traces or sandbox environments; every
    event payload is scrubbed for known values before it is appended."""

    def __init__(self, values: dict[str, str] | None = None, env_prefix: str = "KEEL_SECRET_"):
        self._values: dict[str, str] = {}
        for k, v in os.environ.items():
            if k.startswith(env_prefix) and v:
                self._values[k[len(env_prefix):]] = v
        self._values.update(values or {})

    def get(self, name: str) -> str:
        if name not in self._values:
            raise KeyError(f"secret {name!r} is not configured")
        return self._values[name]

    def resolve(self, names: Iterable[str]) -> dict[str, str]:
        return {n: self.get(n) for n in names}

    def known_values(self) -> list[str]:
        return [v for v in self._values.values() if len(v) >= 6]

    def redact(self, value: Any) -> Any:
        secrets = self.known_values()
        if not secrets:
            return value
        return _redact(value, secrets)


def _redact(v: Any, secrets: list[str]) -> Any:
    if isinstance(v, str):
        for s in secrets:
            if s in v:
                v = v.replace(s, "[REDACTED]")
        return v
    if isinstance(v, dict):
        return {k: _redact(x, secrets) for k, x in v.items()}
    if isinstance(v, list):
        return [_redact(x, secrets) for x in v]
    return v


def redact_patterns(text: str) -> str:
    for p in _SECRET_PATTERNS:
        text = p.sub("[REDACTED]", text)
    return text


def check_output(text: str, *, allowed_emails: Iterable[str] = (), secrets: SecretStore | None = None,
                 max_len: int = 20_000, redact: bool = True) -> str:
    """Scan an answer before it goes back to the user: secrets and other
    customers' email addresses are redacted (or rejected with redact=False)."""
    if len(text) > max_len:
        raise OutputCheckFailed(f"answer is {len(text)} chars, over the {max_len} limit")
    allowed = {e.lower() for e in allowed_emails}
    problems: list[str] = []
    out = text
    for p in _SECRET_PATTERNS:
        if p.search(out):
            problems.append("secret pattern")
            out = p.sub("[REDACTED]", out)
    if secrets:
        for s in secrets.known_values():
            if s in out:
                problems.append("known secret")
                out = out.replace(s, "[REDACTED]")
    for m in set(_EMAIL.findall(out)):
        if m.lower() not in allowed:
            problems.append(f"other customer's email {mask_pii(m)}")
            out = out.replace(m, "[REDACTED EMAIL]")
    if problems and not redact:
        raise OutputCheckFailed("; ".join(problems))
    return out
