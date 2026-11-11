"""The worker process: long-poll for runs, execute many at once as asyncio
tasks (agents mostly wait on LLMs and tools), heartbeat every lease_ttl/3,
and on SIGTERM stop polling, let running steps finish within a deadline and
hand runs back immediately instead of making them wait for lease expiry.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from typing import Any

from . import determinism
from .backend import GrpcBackend
from .executor import RunExecutor, Services
from .guard import SecretStore
from .llm import GatewayClient
from .protocol import TransientBackendError, WorkflowDescriptor
from .telemetry import METRICS, setup_tracing
from .tools import DEFAULT_REGISTRY, ToolRegistry
from .workflow import WORKFLOWS, WorkflowDef, WorkflowRegistry, workflow_def

log = logging.getLogger("keel.worker")


class Worker:
    def __init__(self, *, server: str | None = None, workflows: list[Any] | None = None,
                 tools: ToolRegistry | None = None, llm: Any = None, max_concurrent: int = 50,
                 worker_id: str | None = None, token: str | None = None, dev_mode: bool | None = None,
                 secrets: SecretStore | None = None, default_model: str | None = None, pollers: int | None = None,
                 metrics_port: int | None = None, backend: Any = None, shutdown_grace_s: float = 20.0,
                 batch_local_steps: bool = True):
        self.registry = WorkflowRegistry()
        for w in workflows or WORKFLOWS.all():
            self.registry.add(w if isinstance(w, WorkflowDef) else workflow_def(w))
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.backend = backend or GrpcBackend(server or os.environ.get("KEEL_SERVER", "localhost:7233"),
                                              token=token or os.environ.get("KEEL_WORKER_TOKEN"))
        dev = dev_mode if dev_mode is not None else os.environ.get("KEEL_DEV_MODE") == "1"
        if dev:
            determinism.install()
        self.services = Services(tools=tools or DEFAULT_REGISTRY, llm=llm or GatewayClient(),
                                 secrets=secrets or SecretStore(), dev_mode=dev,
                                 default_model=default_model or os.environ.get("KEEL_DEFAULT_MODEL", "router:default"),
                                 batch_local_steps=batch_local_steps)
        self.max_concurrent = max_concurrent
        self.pollers = pollers or max(1, min(8, max_concurrent // 8 or 1))
        self.metrics_port = metrics_port if metrics_port is not None else int(os.environ.get("KEEL_METRICS_PORT", "0") or 0)
        self.shutdown_grace_s = shutdown_grace_s
        self.active: dict[str, tuple[asyncio.Task, RunExecutor]] = {}
        self._sem = asyncio.Semaphore(max_concurrent)
        self._stop = asyncio.Event()
        self._draining = False

    @property
    def keys(self) -> list[str]:
        return [d.key for d in self.registry.all()]

    def descriptors(self) -> list[WorkflowDescriptor]:
        return [WorkflowDescriptor(d.name, d.version, d.input_schema) for d in self.registry.all()]

    def stop(self) -> None:
        self._stop.set()

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, RuntimeError, ValueError):
                try:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(self.stop))
                except ValueError:
                    pass

    async def run(self) -> None:
        setup_tracing("keel-worker")
        METRICS.serve(self.metrics_port)
        self._install_signals()
        log.info("worker %s serving %s (concurrency %d)", self.worker_id, self.keys, self.max_concurrent)
        polls = [asyncio.create_task(self._poll_loop(i)) for i in range(self.pollers)]
        await self._stop.wait()
        log.info("worker %s draining %d runs", self.worker_id, len(self.active))
        for p in polls:
            p.cancel()
        await self._drain()

    async def _poll_loop(self, idx: int) -> None:
        descriptors = self.descriptors()
        while not self._stop.is_set():
            await self._sem.acquire()
            try:
                task = await self.backend.poll(self.worker_id, self.keys, descriptors, 30_000)
            except (TransientBackendError, OSError) as e:
                self._sem.release()
                log.warning("poll failed: %s", e)
                await asyncio.sleep(1)
                continue
            except asyncio.CancelledError:
                self._sem.release()
                raise
            if task is None:
                self._sem.release()
                continue
            wd = self.registry.get(task.workflow, task.version)
            if wd is None:  # cannot happen: the server only hands out announced versions
                self._sem.release()
                continue
            ex = RunExecutor(self.backend, task, wd, self.services, self.worker_id)
            if self._draining:
                ex.draining = True
            t = asyncio.create_task(self._execute(ex))
            self.active[task.run_id] = (t, ex)

    async def _execute(self, ex: RunExecutor) -> None:
        METRICS.active(len(self.active) + 1)
        try:
            outcome = await ex.execute()
            log.debug("run %s -> %s", ex.task.run_id, outcome)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("run %s crashed the executor", ex.task.run_id)
        finally:
            self.active.pop(ex.task.run_id, None)
            METRICS.active(len(self.active))
            self._sem.release()

    async def _drain(self) -> None:
        self._draining = True
        for _t, ex in list(self.active.values()):
            ex.draining = True  # the next live step hands the run back
        if self.active:
            done, pending = await asyncio.wait([t for t, _ in self.active.values()], timeout=self.shutdown_grace_s)
            for t in pending:
                found = next((e for tt, e in self.active.values() if tt is t), None)
                t.cancel()
                if found is not None and not found.dead:
                    ex = found
                    ex.mark_dead()
                    try:
                        await self.backend.suspend(ex.task.run_id, ex.epoch, [], 0, "handback")
                    except Exception:  # noqa: BLE001 - lease expiry is the fallback
                        pass
        close = getattr(self.backend, "close", None)
        if close is not None:
            await close()
        log.info("worker %s stopped", self.worker_id)


def run_worker(**kwargs: Any) -> None:
    logging.basicConfig(level=os.environ.get("KEEL_LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(Worker(**kwargs).run())
