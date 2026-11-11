"""REST API contract: status codes, error shape, scopes and tenant isolation.

Runs against a keel-server started with these bootstrap keys (see the Makefile
target `api-test`):

  KEEL_API_KEYS="admin-key=ops:root:admin;a-writer=tenant-a:alice:runs:write,runs:read;
                 a-reader=tenant-a:audit:runs:read;a-approver=tenant-a:lead:approvals:decide,runs:read,pii:read;
                 b-writer=tenant-b:bob:runs:write,runs:read"
  KEEL_API_TEST_URL=http://localhost:18081  KEEL_API_TEST_GRPC=localhost:17234
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

URL = os.environ.get("KEEL_API_TEST_URL")
GRPC = os.environ.get("KEEL_API_TEST_GRPC")
pytestmark = pytest.mark.skipif(not URL, reason="KEEL_API_TEST_URL not set")
WF = f"apitest-{uuid.uuid4().hex[:6]}"


def c(key: str | None) -> httpx.Client:
    return httpx.Client(base_url=URL or "", headers={"Authorization": f"Bearer {key}"} if key else {}, timeout=10)


@pytest.fixture(scope="module", autouse=True)
def announce_workflow() -> None:
    """A worker announces the workflow and its input schema over gRPC."""
    import asyncio

    from keel.backend import GrpcBackend
    from keel.protocol import WorkflowDescriptor

    async def go() -> None:
        b = GrpcBackend(GRPC or "localhost:17234")
        schema = {"type": "object", "properties": {"email": {"type": "string"}, "n": {"type": "integer"}},
                  "required": ["email"]}
        await b.poll("api-test-worker", [f"{WF}@1"], [WorkflowDescriptor(WF, "1", schema)], 100)
        await b.close()
    asyncio.run(go())


def err(r: httpx.Response) -> str:
    body = r.json()
    assert set(body) == {"error"} and {"code", "message"} <= set(body["error"]), body
    return body["error"]["code"]


def start(key: str, **body: object) -> httpx.Response:
    return c(key).post("/v1/runs", json={"workflow": WF, "input": {"email": "x@y.com"}, **body})


def test_health_ready_metrics() -> None:
    assert c(None).get("/health").status_code == 200
    assert c(None).get("/ready").status_code == 200
    m = c(None).get("/metrics")
    assert m.status_code == 200 and "keel_lease_expirations_total" in m.text and "keel_pending_runs" in m.text


def test_authentication_and_scopes() -> None:
    assert err(c(None).get("/v1/runs")) == "unauthorized"
    assert c("nope").get("/v1/runs").status_code == 401
    r = start("a-reader")
    assert r.status_code == 403 and err(r) == "forbidden"
    assert c("a-writer").post(f"/v1/approvals/{uuid.uuid4()}", json={"decision": "approve"}).status_code == 403


def test_bad_requests() -> None:
    w = c("a-writer")
    r = w.post("/v1/runs", content=b"{not json", headers={"Content-Type": "application/json"})
    assert r.status_code == 400 and err(r) == "invalid_json"
    assert err(w.post("/v1/runs", json={"input": {}})) == "invalid_request"
    assert err(w.post("/v1/runs", json={"workflow": "never-announced"})) == "unknown_workflow"
    r = w.post("/v1/runs", json={"workflow": WF, "input": {"n": "not-a-number"}})
    assert r.status_code == 400 and err(r) == "invalid_input"


def test_start_key_makes_start_idempotent() -> None:
    key = uuid.uuid4().hex
    r1 = start("a-writer", start_key=key)
    r2 = start("a-writer", start_key=key)
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["run_id"] == r2.json()["run_id"]
    # The same start key in another tenant is a different run.
    r3 = start("b-writer", start_key=key)
    assert r3.status_code == 201 and r3.json()["run_id"] != r1.json()["run_id"]


def test_tenant_isolation() -> None:
    rid = start("a-writer").json()["run_id"]
    b = c("b-writer")
    for resp in (b.get(f"/v1/runs/{rid}"), b.get(f"/v1/runs/{rid}/events"),
                 b.post(f"/v1/runs/{rid}/signals/go", json={"payload": 1}),
                 b.post(f"/v1/runs/{rid}/cancel"), b.post(f"/v1/runs/{rid}/fork", json={"from_seq": 1})):
        assert resp.status_code == 404, resp.text  # indistinguishable from a run that does not exist
    assert all(r["run_id"] != rid for r in b.get("/v1/runs").json()["runs"])
    assert c("a-reader").get(f"/v1/runs/{rid}").status_code == 200
    assert c("admin-key").get(f"/v1/runs/{rid}").status_code == 200


def test_not_found_and_conflict() -> None:
    w = c("a-writer")
    assert err(w.get(f"/v1/runs/{uuid.uuid4()}")) == "not_found"
    assert w.get("/v1/runs/not-a-uuid").status_code == 404
    assert c("a-approver").post("/v1/approvals/not-a-uuid", json={"decision": "approve"}).status_code == 404
    rid = start("a-writer").json()["run_id"]
    assert w.post(f"/v1/runs/{rid}/cancel").status_code == 202
    assert w.post(f"/v1/runs/{rid}/signals/x", json={"payload": 1}).status_code == 202


def test_pending_cap_returns_429_with_retry_after() -> None:
    admin = c("admin-key")
    tenant = f"capped-{uuid.uuid4().hex[:6]}"
    assert admin.put(f"/v1/tenants/{tenant}", json={"max_pending": 2}).status_code == 200
    codes = [admin.post("/v1/runs", json={"workflow": WF, "input": {"email": "a@b.co"}, "tenant": tenant}).status_code
             for _ in range(3)]
    assert codes[:2] == [201, 201] and codes[2] == 429
    r = admin.post("/v1/runs", json={"workflow": WF, "input": {"email": "a@b.co"}, "tenant": tenant})
    assert r.headers.get("Retry-After") and err(r) == "too_many_pending"


def test_pii_is_masked_unless_revealed_with_permission() -> None:
    rid = c("a-writer").post("/v1/runs", json={"workflow": WF, "input": {"email": "priya.s@example.com"}}).json()["run_id"]
    masked = c("a-writer").get(f"/v1/runs/{rid}").json()["input"]["email"]
    assert masked != "priya.s@example.com" and masked.endswith("@example.com")
    assert c("a-writer").get(f"/v1/runs/{rid}?reveal_pii=true").json()["input"]["email"] != "priya.s@example.com"
    assert c("a-approver").get(f"/v1/runs/{rid}?reveal_pii=true").json()["input"]["email"] == "priya.s@example.com"
    evs = c("a-writer").get(f"/v1/runs/{rid}/events").json()["events"]
    assert "priya.s@example.com" not in str(evs)


def test_admin_can_create_scoped_keys() -> None:
    key = c("admin-key").post("/v1/api-keys", json={"tenant": "tenant-c", "principal": "carol",
                                                     "scopes": ["runs:read"]}).json()["api_key"]
    assert c(key).get("/v1/runs").status_code == 200
    assert c(key).post("/v1/runs", json={"workflow": WF}).status_code == 403
