"""Worker-side metrics (Prometheus) and tracing (OpenTelemetry). Both are
optional: without the libraries, or without an OTLP endpoint, they are no-ops.
Replayed steps never emit spans, so a recovery does not duplicate the trace."""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from typing import Any

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _PROM = True
except Exception:  # pragma: no cover
    _PROM = False

_tracer = None


def setup_tracing(service: str = "keel-worker") -> None:
    """Export spans over OTLP when OTEL_EXPORTER_OTLP_ENDPOINT is set."""
    global _tracer
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except Exception:
        return
    endpoint = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"]
    if endpoint.endswith(":4318"):
        endpoint = endpoint[:-4] + "4317"
    provider = TracerProvider(resource=Resource.create({"service.name": service}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("keel")


def _run_parent(run_id: str) -> Any:
    """A remote parent whose trace id is the run id, so every execution of a
    run (including each recovery after a crash) joins one trace."""
    import uuid as _uuid

    from opentelemetry import trace
    from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
    tid = _uuid.UUID(run_id).int
    sc = SpanContext(trace_id=tid, span_id=(tid & 0xFFFFFFFFFFFFFFFF) or 1, is_remote=True,
                     trace_flags=TraceFlags(TraceFlags.SAMPLED))
    return trace.set_span_in_context(NonRecordingSpan(sc))


@contextlib.contextmanager
def span(name: str, trace_run: str | None = None, **attrs: Any) -> Iterator[None]:
    if _tracer is None:
        yield
        return
    from .guard import mask_value
    parent = _run_parent(trace_run) if trace_run else None
    with _tracer.start_as_current_span(name, context=parent) as s:
        for k, v in attrs.items():
            if v is not None:
                s.set_attribute(f"keel.{k}", v if isinstance(v, (int, float, bool)) else str(mask_value(v)))
        yield


def inject_headers(headers: dict | None = None) -> dict:
    """W3C trace context for outgoing HTTP calls (gateway, sandbox, tools)."""
    headers = dict(headers or {})
    if _tracer is not None:
        try:
            from opentelemetry.propagate import inject
            inject(headers)
        except Exception:  # noqa: BLE001
            pass
    return headers


def current_traceparent() -> str | None:
    if _tracer is None:
        return None
    try:
        from opentelemetry.propagate import inject
        carrier: dict[str, str] = {}
        inject(carrier)
        return carrier.get("traceparent")
    except Exception:
        return None


class _Metrics:
    def __init__(self) -> None:
        self.enabled = _PROM
        if not _PROM:
            return
        self._steps = Counter("keel_worker_steps_total", "Steps executed live", ["kind", "outcome"])
        self._step_dur = Histogram("keel_worker_step_seconds", "Live step durations", ["kind"],
                                   buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
        self._replay = Histogram("keel_replay_duration_seconds", "Time to replay a run's history before going live",
                                 buckets=(0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
        self._replay_len = Histogram("keel_replay_history_events", "History length at claim",
                                     buckets=(1, 5, 10, 25, 50, 100, 250, 1000, 5000, 10000))
        self._runs = Counter("keel_worker_run_outcomes_total", "Run executions by outcome", ["outcome"])
        self._active = Gauge("keel_worker_active_runs", "Runs executing on this worker")
        self._tokens = Counter("keel_worker_llm_tokens_total", "LLM tokens used by this worker", ["direction"])
        self._wasted = Counter("keel_wasted_llm_tokens_total", "Tokens paid twice because a crash interrupted an LLM call")
        self._policy = Counter("keel_worker_policy_decisions_total", "Policy decisions seen", ["decision"])

    def serve(self, port: int) -> None:
        if self.enabled and port:
            start_http_server(port)

    def step(self, kind: str, seconds: float, failed: bool = False) -> None:
        if self.enabled:
            self._steps.labels(kind, "failed" if failed else "ok").inc()
            self._step_dur.labels(kind).observe(seconds)

    def replay_duration(self, seconds: float, events: int) -> None:
        if self.enabled:
            self._replay.observe(seconds)
            self._replay_len.observe(events)

    def run_outcome(self, outcome: str) -> None:
        if self.enabled:
            self._runs.labels(outcome).inc()

    def active(self, n: int) -> None:
        if self.enabled:
            self._active.set(n)

    def llm(self, reply: Any) -> None:
        if self.enabled:
            self._tokens.labels("input").inc(reply.usage.input_tokens)
            self._tokens.labels("output").inc(reply.usage.output_tokens)

    def wasted_tokens(self, n: int) -> None:
        if self.enabled:
            self._wasted.inc(n)

    def policy(self, d: Any) -> None:
        if self.enabled:
            self._policy.labels(d.decision).inc()


METRICS = _Metrics()
