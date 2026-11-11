from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class Event(_message.Message):
    __slots__ = ("seq", "type", "step_id", "payload", "epoch", "created_at_unix_ms")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    seq: int
    type: str
    step_id: str
    payload: bytes
    epoch: int
    created_at_unix_ms: int
    def __init__(self, seq: _Optional[int] = ..., type: _Optional[str] = ..., step_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., epoch: _Optional[int] = ..., created_at_unix_ms: _Optional[int] = ...) -> None: ...

class WorkflowDescriptor(_message.Message):
    __slots__ = ("name", "version", "input_schema")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    input_schema: bytes
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ..., input_schema: _Optional[bytes] = ...) -> None: ...

class PollRequest(_message.Message):
    __slots__ = ("worker_id", "workflows", "max_wait_ms", "descriptors")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    MAX_WAIT_MS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTORS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    workflows: _containers.RepeatedScalarFieldContainer[str]
    max_wait_ms: int
    descriptors: _containers.RepeatedCompositeFieldContainer[WorkflowDescriptor]
    def __init__(self, worker_id: _Optional[str] = ..., workflows: _Optional[_Iterable[str]] = ..., max_wait_ms: _Optional[int] = ..., descriptors: _Optional[_Iterable[_Union[WorkflowDescriptor, _Mapping]]] = ...) -> None: ...

class RunTask(_message.Message):
    __slots__ = ("has_task", "run_id", "epoch", "workflow", "version", "history", "input", "tenant_id", "cancel_requested", "server_now_unix_ms", "lease_ttl_ms", "dry_run", "parent_run_id", "next_seq", "created_at_unix_ms", "overrides", "traceparent")
    HAS_TASK_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    SERVER_NOW_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    LEASE_TTL_MS_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    PARENT_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_SEQ_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    has_task: bool
    run_id: str
    epoch: int
    workflow: str
    version: str
    history: _containers.RepeatedCompositeFieldContainer[Event]
    input: bytes
    tenant_id: str
    cancel_requested: bool
    server_now_unix_ms: int
    lease_ttl_ms: int
    dry_run: bool
    parent_run_id: str
    next_seq: int
    created_at_unix_ms: int
    overrides: bytes
    traceparent: str
    def __init__(self, has_task: _Optional[bool] = ..., run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., workflow: _Optional[str] = ..., version: _Optional[str] = ..., history: _Optional[_Iterable[_Union[Event, _Mapping]]] = ..., input: _Optional[bytes] = ..., tenant_id: _Optional[str] = ..., cancel_requested: _Optional[bool] = ..., server_now_unix_ms: _Optional[int] = ..., lease_ttl_ms: _Optional[int] = ..., dry_run: _Optional[bool] = ..., parent_run_id: _Optional[str] = ..., next_seq: _Optional[int] = ..., created_at_unix_ms: _Optional[int] = ..., overrides: _Optional[bytes] = ..., traceparent: _Optional[str] = ...) -> None: ...

class AppendRequest(_message.Message):
    __slots__ = ("run_id", "epoch", "expected_seq", "events")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SEQ_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    epoch: int
    expected_seq: int
    events: _containers.RepeatedCompositeFieldContainer[Event]
    def __init__(self, run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., expected_seq: _Optional[int] = ..., events: _Optional[_Iterable[_Union[Event, _Mapping]]] = ...) -> None: ...

class AppendResponse(_message.Message):
    __slots__ = ("ok", "next_seq", "error_code", "cancel_requested", "server_now_unix_ms")
    OK_FIELD_NUMBER: _ClassVar[int]
    NEXT_SEQ_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    SERVER_NOW_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    next_seq: int
    error_code: str
    cancel_requested: bool
    server_now_unix_ms: int
    def __init__(self, ok: _Optional[bool] = ..., next_seq: _Optional[int] = ..., error_code: _Optional[str] = ..., cancel_requested: _Optional[bool] = ..., server_now_unix_ms: _Optional[int] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("run_id", "epoch", "worker_id")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    epoch: int
    worker_id: str
    def __init__(self, run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., worker_id: _Optional[str] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("ok", "lease_lost", "cancel_requested", "server_now_unix_ms")
    OK_FIELD_NUMBER: _ClassVar[int]
    LEASE_LOST_FIELD_NUMBER: _ClassVar[int]
    CANCEL_REQUESTED_FIELD_NUMBER: _ClassVar[int]
    SERVER_NOW_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    lease_lost: bool
    cancel_requested: bool
    server_now_unix_ms: int
    def __init__(self, ok: _Optional[bool] = ..., lease_lost: _Optional[bool] = ..., cancel_requested: _Optional[bool] = ..., server_now_unix_ms: _Optional[int] = ...) -> None: ...

class WaitCondition(_message.Message):
    __slots__ = ("kind", "id", "name", "fire_at_unix_ms")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIRE_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    name: str
    fire_at_unix_ms: int
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., name: _Optional[str] = ..., fire_at_unix_ms: _Optional[int] = ...) -> None: ...

class SuspendRequest(_message.Message):
    __slots__ = ("run_id", "epoch", "waits", "available_at_unix_ms", "reason", "expected_seq")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    WAITS_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SEQ_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    epoch: int
    waits: _containers.RepeatedCompositeFieldContainer[WaitCondition]
    available_at_unix_ms: int
    reason: str
    expected_seq: int
    def __init__(self, run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., waits: _Optional[_Iterable[_Union[WaitCondition, _Mapping]]] = ..., available_at_unix_ms: _Optional[int] = ..., reason: _Optional[str] = ..., expected_seq: _Optional[int] = ...) -> None: ...

class SuspendResponse(_message.Message):
    __slots__ = ("ok", "lease_lost", "woken")
    OK_FIELD_NUMBER: _ClassVar[int]
    LEASE_LOST_FIELD_NUMBER: _ClassVar[int]
    WOKEN_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    lease_lost: bool
    woken: bool
    def __init__(self, ok: _Optional[bool] = ..., lease_lost: _Optional[bool] = ..., woken: _Optional[bool] = ...) -> None: ...

class FinishRequest(_message.Message):
    __slots__ = ("run_id", "epoch", "expected_seq", "status", "result", "error", "continue_input")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_SEQ_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CONTINUE_INPUT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    epoch: int
    expected_seq: int
    status: str
    result: bytes
    error: bytes
    continue_input: bytes
    def __init__(self, run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., expected_seq: _Optional[int] = ..., status: _Optional[str] = ..., result: _Optional[bytes] = ..., error: _Optional[bytes] = ..., continue_input: _Optional[bytes] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("ok", "lease_lost", "error_code", "new_run_id")
    OK_FIELD_NUMBER: _ClassVar[int]
    LEASE_LOST_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    lease_lost: bool
    error_code: str
    new_run_id: str
    def __init__(self, ok: _Optional[bool] = ..., lease_lost: _Optional[bool] = ..., error_code: _Optional[str] = ..., new_run_id: _Optional[str] = ...) -> None: ...

class PolicyRequest(_message.Message):
    __slots__ = ("run_id", "epoch", "tool", "tool_risk", "args", "step_id", "approved_rules")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    TOOL_RISK_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_RULES_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    epoch: int
    tool: str
    tool_risk: str
    args: bytes
    step_id: str
    approved_rules: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, run_id: _Optional[str] = ..., epoch: _Optional[int] = ..., tool: _Optional[str] = ..., tool_risk: _Optional[str] = ..., args: _Optional[bytes] = ..., step_id: _Optional[str] = ..., approved_rules: _Optional[_Iterable[str]] = ...) -> None: ...

class PolicyResponse(_message.Message):
    __slots__ = ("decision", "reason", "rule_ids", "approvers", "expires_in_ms", "on_expiry", "policy_version", "lease_lost")
    DECISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RULE_IDS_FIELD_NUMBER: _ClassVar[int]
    APPROVERS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_IN_MS_FIELD_NUMBER: _ClassVar[int]
    ON_EXPIRY_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    LEASE_LOST_FIELD_NUMBER: _ClassVar[int]
    decision: str
    reason: str
    rule_ids: _containers.RepeatedScalarFieldContainer[str]
    approvers: _containers.RepeatedScalarFieldContainer[str]
    expires_in_ms: int
    on_expiry: str
    policy_version: str
    lease_lost: bool
    def __init__(self, decision: _Optional[str] = ..., reason: _Optional[str] = ..., rule_ids: _Optional[_Iterable[str]] = ..., approvers: _Optional[_Iterable[str]] = ..., expires_in_ms: _Optional[int] = ..., on_expiry: _Optional[str] = ..., policy_version: _Optional[str] = ..., lease_lost: _Optional[bool] = ...) -> None: ...
