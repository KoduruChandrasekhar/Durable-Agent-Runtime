"""Shared helpers for the chaos experiments (they run on the host against the
Docker Compose stack)."""

from __future__ import annotations

import os
import random
import subprocess
import time
from dataclasses import dataclass

import httpx

API = os.environ.get("KEEL_URL", "http://127.0.0.1:8080")
MOCKS = os.environ.get("KEEL_MOCKS_URL", "http://127.0.0.1:9000")
MOCK_LLM = os.environ.get("KEEL_MOCK_LLM_URL", "http://127.0.0.1:8000")
DSN = os.environ.get("KEEL_DATABASE_URL", "postgresql://keel:keel@127.0.0.1:55433/keel")
PROJECT = os.environ.get("KEEL_COMPOSE_PROJECT", "keel")


def docker(*args: str, check: bool = False) -> str:
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    r = subprocess.run(["docker", *args], capture_output=True, text=True, env=env)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout.strip()


def containers(service: str, running_only: bool = True) -> list[str]:
    out = docker("ps", *([] if running_only else ["-a"]), "--filter", f"label=com.docker.compose.project={PROJECT}",
                 "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.Names}}")
    return sorted(n for n in out.splitlines() if n)


def hostname(container: str) -> str:
    return docker("inspect", "-f", "{{.Config.Hostname}}", container)


@dataclass
class KillEvent:
    container: str
    at: float


class Killer:
    """Kill -9 a random container of a service every `every` seconds, then
    start it again after `restart_after` seconds (a crashed process being
    restarted by its supervisor)."""

    def __init__(self, service: str, every: float = 3.0, restart_after: float = 2.0, seed: int = 1):
        self.service = service
        self.every = every
        self.restart_after = restart_after
        self.rng = random.Random(seed)
        self.events: list[KillEvent] = []
        self.stopped = False

    def run(self) -> None:
        import threading
        pending: list[tuple[float, str]] = []
        while not self.stopped:
            alive = containers(self.service)
            if len(alive) > 1:
                victim = self.rng.choice(alive)
                docker("kill", "--signal", "KILL", victim)
                self.events.append(KillEvent(victim, time.time()))
                pending.append((time.time() + self.restart_after, victim))
            now = time.time()
            for t, c in list(pending):
                if t <= now:
                    docker("start", c)
                    pending.remove((t, c))
            end = time.time() + self.every
            while time.time() < end and not self.stopped:
                time.sleep(0.2)
                now = time.time()
                for t, c in list(pending):
                    if t <= now:
                        threading.Thread(target=docker, args=("start", c), daemon=True).start()
                        pending.remove((t, c))
        for _, c in pending:
            docker("start", c)


def configure_chaos(*, payments_fail_after_commit: float = 0.0, email_fail_after_commit: float = 0.0,
                    llm_error_rate: float = 0.0, llm_latency_ms: tuple[int, int] | None = None) -> None:
    with httpx.Client(timeout=10) as c:
        c.post(f"{MOCKS}/payments/admin/chaos", json={"fail_after_commit_rate": payments_fail_after_commit})
        c.post(f"{MOCKS}/email/admin/chaos", json={"fail_after_commit_rate": email_fail_after_commit})
        body: dict = {"target": "primary", "error_rate": llm_error_rate}
        if llm_latency_ms:
            body.update(latency_ms_min=llm_latency_ms[0], latency_ms_max=llm_latency_ms[1])
        c.post(f"{MOCK_LLM}/admin/chaos", json=body)


def reset_mocks() -> None:
    with httpx.Client(timeout=30) as c:
        for svc in ("orders", "payments", "email", "crm"):
            c.post(f"{MOCKS}/{svc}/admin/reset")
        c.post(f"{MOCK_LLM}/admin/reset")
