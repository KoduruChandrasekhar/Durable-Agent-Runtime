package store

// Integration tests against a real PostgreSQL. Set KEEL_TEST_DATABASE_URL
// (CI starts a postgres service container); otherwise they are skipped.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keel-runtime/keel/server/internal/blob"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("KEEL_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, dsn, 60, blobs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `TRUNCATE runs, events, signals, approvals, outbox, workers, workflows, tenant_spend, tenant_counters CASCADE;
		DELETE FROM tenants; INSERT INTO tenants (tenant_id) VALUES ('default')`); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustStart(t *testing.T, s *Store, tenant string) string {
	t.Helper()
	id, created, err := s.StartRun(context.Background(), StartRunParams{TenantID: tenant, Workflow: "wf", Version: "1",
		Input: json.RawMessage(`{"x":1}`)})
	if err != nil || !created {
		t.Fatalf("start: %v created=%v", err, created)
	}
	return id
}

func claim(t *testing.T, s *Store, worker string) *ClaimedRun {
	t.Helper()
	c, err := s.ClaimRun(context.Background(), worker, []string{"wf@1"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func expire(t *testing.T, s *Store, runID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `UPDATE runs SET lease_expires_at = now() - interval '1 second' WHERE run_id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SweepExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
}

func checkLog(t *testing.T, s *Store, runID string) []Event {
	t.Helper()
	evs, _, err := s.ListEvents(context.Background(), "", runID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var last int64
	for i, e := range evs {
		if e.Seq != int64(i+1) {
			t.Fatalf("gap or duplicate: position %d has seq %d", i+1, e.Seq)
		}
		if e.Epoch < last {
			t.Fatalf("seq %d written with epoch %d after epoch %d", e.Seq, e.Epoch, last)
		}
		last = e.Epoch
	}
	return evs
}

func ev(typ, step string) NewEvent {
	return NewEvent{Type: typ, StepID: step, Payload: json.RawMessage(`{"kind":"local"}`)}
}

func TestMigrationsCreateSchemaFromScratch(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public'
		AND table_name IN ('runs','events','signals','approvals','outbox','tenants')`).Scan(&n); err != nil || n != 6 {
		t.Fatalf("tables: %d %v", n, err)
	}
}

// Two appenders with different epochs race on the same run thousands of
// times: only the current epoch ever writes, and sequence numbers never have
// gaps or duplicates.
func TestFencingConcurrentAppenders(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	runID := mustStart(t, s, "default")
	c := claim(t, s, "w0")
	rounds := 1000
	if testing.Short() {
		rounds = 200
	}
	var staleAccepted, currentAccepted, rejected atomic.Int64
	for i := 0; i < rounds; i++ {
		oldEpoch := c.Run.LeaseEpoch
		expire(t, s, runID)
		c = claim(t, s, fmt.Sprintf("w%d", i+1))
		if c == nil || c.Run.LeaseEpoch != oldEpoch+1 {
			t.Fatalf("round %d: reclaim failed", i)
		}
		next := c.Run.NextSeq
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			epoch := oldEpoch
			if g%2 == 1 {
				epoch = c.Run.LeaseEpoch
			}
			wg.Add(1)
			go func(epoch int64, g int) {
				defer wg.Done()
				res, err := s.Append(ctx, runID, epoch, next, []NewEvent{ev("StepStarted", fmt.Sprintf("s%d-%d", i, g))})
				if err != nil {
					t.Error(err)
					return
				}
				switch {
				case res.OK && epoch == oldEpoch:
					staleAccepted.Add(1)
				case res.OK:
					currentAccepted.Add(1)
				default:
					rejected.Add(1)
				}
			}(epoch, g)
		}
		wg.Wait()
		c.Run.NextSeq = next + 1
		// Refresh next_seq for the run as stored (RunResumed was appended at claim).
		r, _ := s.GetRun(ctx, "", runID)
		c.Run.NextSeq = r.NextSeq
	}
	if staleAccepted.Load() != 0 {
		t.Fatalf("a stale epoch wrote %d times", staleAccepted.Load())
	}
	if currentAccepted.Load() != int64(rounds) {
		t.Fatalf("expected exactly one current-epoch append per round, got %d/%d", currentAccepted.Load(), rounds)
	}
	checkLog(t, s, runID)
	t.Logf("rounds=%d current=%d rejected=%d stale=0", rounds, currentAccepted.Load(), rejected.Load())
}

func TestRetriedAppendIsAcknowledgedNotDuplicated(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	runID := mustStart(t, s, "default")
	c := claim(t, s, "w")
	batch := []NewEvent{ev("StepStarted", "a:1"), ev("StepCompleted", "a:1")}
	r1, err := s.Append(ctx, runID, c.Run.LeaseEpoch, c.Run.NextSeq, batch)
	if err != nil || !r1.OK {
		t.Fatal(r1, err)
	}
	// The response was lost; the worker retries with the same expected_seq.
	r2, err := s.Append(ctx, runID, c.Run.LeaseEpoch, c.Run.NextSeq, batch)
	if err != nil || !r2.OK || r2.NextSeq != r1.NextSeq {
		t.Fatalf("retry not acknowledged: %+v %v", r2, err)
	}
	evs := checkLog(t, s, runID)
	if len(evs) != 3 {
		t.Fatalf("expected 3 events, got %d", len(evs))
	}
	// A different batch at the same position is a real conflict.
	r3, _ := s.Append(ctx, runID, c.Run.LeaseEpoch, c.Run.NextSeq, []NewEvent{ev("StepStarted", "b:1")})
	if r3.OK || r3.ErrorCode != "seq_conflict" {
		t.Fatalf("expected seq_conflict, got %+v", r3)
	}
}

func TestLargePayloadsRoundTripThroughBlobStore(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	runID := mustStart(t, s, "default")
	c := claim(t, s, "w")
	big := strings.Repeat("x", 200*1024)
	payload, _ := json.Marshal(map[string]any{"kind": "llm", "output": big})
	res, err := s.Append(ctx, runID, c.Run.LeaseEpoch, c.Run.NextSeq, []NewEvent{{Type: "StepCompleted", StepID: "llm:1", Payload: payload}})
	if err != nil || !res.OK {
		t.Fatal(res, err)
	}
	var stored string
	if err := s.Pool.QueryRow(ctx, `SELECT payload::text FROM events WHERE run_id = $1::uuid AND step_id = 'llm:1'`, runID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stored, `"blob": "sha256:`) || len(stored) > 200 {
		t.Fatalf("payload not offloaded: %s", stored[:min(len(stored), 120)])
	}
	evs := checkLog(t, s, runID)
	var p map[string]any
	_ = json.Unmarshal(evs[len(evs)-1].Payload, &p)
	if p["output"] != big {
		t.Fatal("blob did not round-trip")
	}
}

func TestStartKeyMakesStartIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make([]string, 20)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _, err := s.StartRun(ctx, StartRunParams{TenantID: "default", Workflow: "wf", Version: "1",
				Input: json.RawMessage(`{}`), StartKey: "order-42"})
			if err != nil {
				t.Error(err)
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] || id == "" {
			t.Fatalf("different run ids for one start_key: %v", ids)
		}
	}
}

func TestSignalSentBeforeTheWaitIsDelivered(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	runID := mustStart(t, s, "default")
	c := claim(t, s, "w")
	if _, err := s.Signal(ctx, "default", runID, "approve", json.RawMessage(`{"ok":true}`), "sig-1"); err != nil {
		t.Fatal(err)
	}
	dup, err := s.Signal(ctx, "default", runID, "approve", json.RawMessage(`{"ok":false}`), "sig-1")
	if err != nil || !dup {
		t.Fatalf("duplicate signal id not detected: %v %v", dup, err)
	}
	res, err := s.Suspend(ctx, SuspendParams{RunID: runID, Epoch: c.Run.LeaseEpoch,
		Waits: []Wait{{Kind: "signal", ID: "signal:approve:1", Name: "approve"}}})
	if err != nil || !res.Woken {
		t.Fatalf("buffered signal should wake the run immediately: %+v %v", res, err)
	}
	evs := checkLog(t, s, runID)
	last := evs[len(evs)-1]
	if last.Type != "SignalReceived" || !strings.Contains(string(last.Payload), `"ok": true`) && !strings.Contains(string(last.Payload), `"ok":true`) {
		t.Fatalf("unexpected last event %s %s", last.Type, last.Payload)
	}
}

func TestTimersApprovalsAndChildren(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	runID := mustStart(t, s, "default")
	c := claim(t, s, "w")
	ep := c.Run.LeaseEpoch
	approval := "6b65656c-0000-4000-8000-0000000000a1"
	child := "6b65656c-0000-4000-8000-0000000000c1"
	ap, _ := json.Marshal(map[string]any{"approval_id": approval, "action": map[string]any{"tool": "refund"},
		"reason": "big", "rule_ids": []string{"big"}, "approvers": []string{"leads"}})
	cp, _ := json.Marshal(map[string]any{"child_run_id": child, "workflow": "wf", "version": "1", "input": map[string]any{}})
	res, err := s.Append(ctx, runID, ep, c.Run.NextSeq, []NewEvent{
		{Type: "ApprovalRequested", StepID: "refund:1", Payload: ap}, {Type: "ChildRunStarted", StepID: "child:wf:1", Payload: cp}})
	if err != nil || !res.OK {
		t.Fatal(res, err)
	}
	fire := time.Now().Add(-time.Second).UnixMilli()
	sr, err := s.Suspend(ctx, SuspendParams{RunID: runID, Epoch: ep, Waits: []Wait{
		{Kind: "approval", ID: approval}, {Kind: "child", ID: child}, {Kind: "timer", ID: "timer:1", FireAtMs: fire + 3_600_000}}})
	if err != nil || sr.Woken {
		t.Fatalf("suspend: %+v %v", sr, err)
	}
	// Deciding twice changes nothing the second time.
	if _, changed, err := s.DecideApproval(ctx, "default", approval, "approved", nil, "lead@x", ""); err != nil || !changed {
		t.Fatal(changed, err)
	}
	if _, changed, _ := s.DecideApproval(ctx, "default", approval, "rejected", nil, "lead@x", ""); changed {
		t.Fatal("second decision changed the approval")
	}
	r, _ := s.GetRun(ctx, "", runID)
	if r.Status != StatusPending {
		t.Fatalf("approval should wake the run, status %s", r.Status)
	}
	// The child exists, is pending, and belongs to the parent.
	cr, err := s.GetRun(ctx, "", child)
	if err != nil || cr.ParentRunID != runID {
		t.Fatalf("child: %+v %v", cr, err)
	}
	checkLog(t, s, runID)
}

// While tenant A floods thousands of runs, tenant B's runs start within a few claims.
func TestFairSchedulingAcrossTenants(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `INSERT INTO runs (run_id, tenant_id, workflow_name, workflow_version, status, input, chain_root, next_seq)
		SELECT id, 'tenant-a', 'wf', '1', 'pending', '{}', id, 1 FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, 3000)) g`); err != nil {
		t.Fatal(err)
	}
	var bRuns []string
	for i := 0; i < 10; i++ {
		bRuns = append(bRuns, mustStart(t, s, "tenant-b"))
	}
	firstB := map[string]int{}
	for i := 1; i <= 60; i++ {
		c := claim(t, s, "w")
		if c == nil {
			t.Fatal("nothing claimed")
		}
		if c.Run.TenantID == "tenant-b" {
			firstB[c.Run.RunID] = i
		}
	}
	if len(firstB) != len(bRuns) {
		t.Fatalf("tenant B got only %d of %d runs started in the first 60 claims", len(firstB), len(bRuns))
	}
	maxPos := 0
	for _, p := range firstB {
		maxPos = max(maxPos, p)
	}
	t.Logf("all of tenant B's runs started by claim #%d despite 3000 queued for tenant A", maxPos)
}

func TestTenantRunningCapIsRespected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	two := 2
	if err := s.UpsertTenant(ctx, Tenant{TenantID: "capped", MaxRunning: &two}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		mustStart(t, s, "capped")
	}
	var wg sync.WaitGroup
	var claimed atomic.Int64
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c, _ := s.ClaimRun(ctx, "w", []string{"wf@1"}); c != nil {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	if claimed.Load() != 2 {
		t.Fatalf("cap of 2 running violated: %d claimed", claimed.Load())
	}
}
