"""gRPC client for keel-server's WorkerService.

Transient failures (server restarting, database down, network faults) are
retried with backoff and the same arguments; appends carry the epoch and the
expected sequence number, so a retried append whose first attempt landed is
recognized by the server instead of being written twice.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any

import grpc

from .protocol import (
    AppendResult,
    Event,
    FinishResult,
    HeartbeatResult,
    NewEvent,
    PolicyDecision,
    RealClock,
    RunTask,
    SuspendResult,
    TransientBackendError,
    Wait,
    WorkflowDescriptor,
)
from .serde import dumps, loads
from .telemetry import inject_headers
from .v1 import worker_pb2 as pb
from .v1 import worker_pb2_grpc as pbg

log = logging.getLogger("keel.backend")

_RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED,
              grpc.StatusCode.ABORTED, grpc.StatusCode.UNKNOWN, grpc.StatusCode.INTERNAL}


class GrpcBackend:
    def __init__(self, target: str = "localhost:7233", *, token: str | None = None, max_retry_s: float = 25.0):
        opts = [("grpc.max_receive_message_length", 64 << 20), ("grpc.max_send_message_length", 64 << 20),
                ("grpc.keepalive_time_ms", 20_000), ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.http2.max_pings_without_data", 0)]
        self.channel = grpc.aio.insecure_channel(target, options=opts)
        self.stub = pbg.WorkerServiceStub(self.channel)
        self.clock = RealClock()
        self.md = (("authorization", f"Bearer {token}"),) if token else None
        self.max_retry_s = max_retry_s
        self._rng = random.Random()

    async def close(self) -> None:
        await self.channel.close()

    async def _call(self, method: Any, req: Any, timeout: float, retry: bool = True) -> Any:
        delay = 0.1
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.max_retry_s
        while True:
            try:
                md = tuple(self.md or ()) + tuple(inject_headers().items())
                return await method(req, timeout=timeout, metadata=md or None)
            except grpc.aio.AioRpcError as e:
                if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                    raise ValueError(e.details()) from None
                if not retry or e.code() not in _RETRYABLE or loop.time() > deadline:
                    raise TransientBackendError(f"{e.code().name}: {e.details()}") from None
                log.debug("retrying %s after %s", getattr(method, "_method", method), e.code().name)
            await asyncio.sleep(delay * (0.5 + self._rng.random()))
            delay = min(delay * 2, 3.0)

    # ------------------------------------------------------------------ protocol

    async def poll(self, worker_id: str, workflows: list[str], descriptors: list[WorkflowDescriptor],
                   max_wait_ms: int = 30_000) -> RunTask | None:
        req = pb.PollRequest(worker_id=worker_id, workflows=workflows, max_wait_ms=max_wait_ms,
                             descriptors=[pb.WorkflowDescriptor(name=d.name, version=d.version,
                                                                input_schema=dumps(d.input_schema) if d.input_schema else b"")
                                          for d in descriptors])
        t = await self._call(self.stub.PollRun, req, timeout=max_wait_ms / 1000 + 10, retry=False)
        if not t.has_task:
            return None
        self.clock.observe_server_time(t.server_now_unix_ms)
        return RunTask(
            run_id=t.run_id, epoch=t.epoch, workflow=t.workflow, version=t.version,
            history=[Event(seq=e.seq, type=e.type, step_id=e.step_id, payload=loads(e.payload) or {}, epoch=e.epoch,
                           created_at_ms=e.created_at_unix_ms) for e in t.history],
            input=loads(t.input), tenant_id=t.tenant_id, cancel_requested=t.cancel_requested,
            server_now_ms=t.server_now_unix_ms, lease_ttl_ms=t.lease_ttl_ms or 30_000, dry_run=t.dry_run,
            parent_run_id=t.parent_run_id, next_seq=t.next_seq, created_at_ms=t.created_at_unix_ms,
            overrides=loads(t.overrides) or {})

    async def append(self, run_id: str, epoch: int, expected_seq: int, events: list[NewEvent]) -> AppendResult:
        req = pb.AppendRequest(run_id=run_id, epoch=epoch, expected_seq=expected_seq,
                               events=[pb.Event(type=e.type, step_id=e.step_id, payload=dumps(e.payload)) for e in events])
        r = await self._call(self.stub.AppendEvents, req, timeout=15)
        self.clock.observe_server_time(r.server_now_unix_ms)
        return AppendResult(ok=r.ok, next_seq=r.next_seq, error_code=r.error_code,
                            cancel_requested=r.cancel_requested, server_now_ms=r.server_now_unix_ms)

    async def heartbeat(self, run_id: str, epoch: int, worker_id: str) -> HeartbeatResult:
        r = await self._call(self.stub.Heartbeat, pb.HeartbeatRequest(run_id=run_id, epoch=epoch, worker_id=worker_id),
                             timeout=8)
        self.clock.observe_server_time(r.server_now_unix_ms)
        return HeartbeatResult(ok=r.ok, lease_lost=r.lease_lost, cancel_requested=r.cancel_requested,
                               server_now_ms=r.server_now_unix_ms)

    async def suspend(self, run_id: str, epoch: int, waits: list[Wait], available_at_ms: int, reason: str) -> SuspendResult:
        req = pb.SuspendRequest(run_id=run_id, epoch=epoch, available_at_unix_ms=available_at_ms, reason=reason,
                                waits=[pb.WaitCondition(kind=w.kind, id=w.id, name=w.name, fire_at_unix_ms=w.fire_at_ms)
                                       for w in waits])
        r = await self._call(self.stub.Suspend, req, timeout=15)
        return SuspendResult(ok=r.ok, lease_lost=r.lease_lost, woken=r.woken)

    async def finish(self, run_id: str, epoch: int, expected_seq: int, status: str, result: Any = None,
                     error: Any = None, continue_input: Any = None) -> FinishResult:
        req = pb.FinishRequest(run_id=run_id, epoch=epoch, expected_seq=expected_seq, status=status,
                               result=dumps(result) if result is not None else b"",
                               error=dumps(error) if error is not None else b"",
                               continue_input=dumps(continue_input) if continue_input is not None else b"")
        r = await self._call(self.stub.Finish, req, timeout=15)
        return FinishResult(ok=r.ok, lease_lost=r.lease_lost, error_code=r.error_code, new_run_id=r.new_run_id)

    async def evaluate_policy(self, run_id: str, epoch: int, tool: str, risk: str, args: dict, step_id: str,
                              approved_rules: list[str]) -> PolicyDecision:
        req = pb.PolicyRequest(run_id=run_id, epoch=epoch, tool=tool, tool_risk=risk, args=json.dumps(args).encode(),
                               step_id=step_id, approved_rules=approved_rules)
        r = await self._call(self.stub.EvaluatePolicy, req, timeout=10)
        return PolicyDecision(decision=r.decision, reason=r.reason, rule_ids=list(r.rule_ids),
                              approvers=list(r.approvers), expires_in_ms=r.expires_in_ms,
                              on_expiry=r.on_expiry or "reject", policy_version=r.policy_version,
                              lease_lost=r.lease_lost)
