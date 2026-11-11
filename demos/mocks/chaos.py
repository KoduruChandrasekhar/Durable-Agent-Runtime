"""Chaos modes shared by the mock services: random latency, 5xx before
commit, timeouts, and the nastiest case, fail after commit (apply the effect,
then drop the response)."""

from __future__ import annotations

import asyncio
import random
from dataclasses import asdict, dataclass

from fastapi import HTTPException


@dataclass
class ChaosConfig:
    latency_ms_min: int = 0
    latency_ms_max: int = 0
    error_rate: float = 0.0               # 503 before the effect
    timeout_rate: float = 0.0             # hang before the effect
    fail_after_commit_rate: float = 0.0   # effect applied, then 503
    timeout_after_commit_rate: float = 0.0  # effect applied, then hang past the client timeout
    hang_s: float = 20.0


class Chaos:
    def __init__(self) -> None:
        self.cfg = ChaosConfig()
        self.rng = random.Random()
        self.stats = {"before_errors": 0, "after_commit_failures": 0, "timeouts": 0}

    def configure(self, **kw: object) -> dict:
        for k, v in kw.items():
            if hasattr(self.cfg, k) and v is not None:
                setattr(self.cfg, k, type(getattr(self.cfg, k))(v))
        return asdict(self.cfg)

    def reset(self) -> None:
        self.cfg = ChaosConfig()

    async def before(self) -> None:
        c = self.cfg
        if c.latency_ms_max > 0:
            await asyncio.sleep(self.rng.uniform(c.latency_ms_min, c.latency_ms_max) / 1000)
        if c.timeout_rate and self.rng.random() < c.timeout_rate:
            self.stats["timeouts"] += 1
            await asyncio.sleep(c.hang_s)
        if c.error_rate and self.rng.random() < c.error_rate:
            self.stats["before_errors"] += 1
            raise HTTPException(503, "chaos: service unavailable")

    async def after_commit(self) -> None:
        c = self.cfg
        if c.timeout_after_commit_rate and self.rng.random() < c.timeout_after_commit_rate:
            self.stats["timeouts"] += 1
            await asyncio.sleep(c.hang_s)
        if c.fail_after_commit_rate and self.rng.random() < c.fail_after_commit_rate:
            self.stats["after_commit_failures"] += 1
            raise HTTPException(503, "chaos: effect committed but the response was dropped")
