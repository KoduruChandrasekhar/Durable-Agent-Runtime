package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keel-runtime/keel/server/internal/metrics"
)

// ---------------------------------------------------------------- start

type StartRunParams struct {
	RunID       string // optional; generated when empty
	TenantID    string
	Workflow    string
	Version     string
	Input       json.RawMessage
	BudgetUSD   *float64
	Priority    int
	StartKey    string
	ParentRunID string
	ChainRoot   string
	DryRun      bool
	Overrides   json.RawMessage
	Extra       map[string]any // merged into the RunStarted payload
}

// StartRun creates a pending run with RunStarted as event 1. A repeated
// start_key returns the existing run with created=false.
func (s *Store) StartRun(ctx context.Context, p StartRunParams) (runID string, created bool, err error) {
	err = s.tx(ctx, func(tx pgx.Tx) error {
		runID, created, err = s.startRunTx(ctx, tx, p, true)
		return err
	})
	if err == nil && created {
		metrics.RunsStarted.WithLabelValues(p.Workflow, p.TenantID).Inc()
	}
	return runID, created, err
}

func (s *Store) startRunTx(ctx context.Context, tx pgx.Tx, p StartRunParams, checkCap bool) (string, bool, error) {
	if p.StartKey != "" {
		var existing string
		err := tx.QueryRow(ctx, `SELECT run_id::text FROM runs WHERE tenant_id = $1 AND start_key = $2`,
			p.TenantID, p.StartKey).Scan(&existing)
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", false, err
		}
	}
	if checkCap {
		var pending, maxPending int
		err := tx.QueryRow(ctx, `SELECT coalesce((SELECT pending FROM tenant_load l WHERE l.tenant_id = t.tenant_id), 0),
			max_pending FROM tenants t WHERE tenant_id = $1`, p.TenantID).Scan(&pending, &maxPending)
		if err == nil && pending >= maxPending {
			return "", false, ErrTooManyPending
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return "", false, err
		}
	}
	runID := p.RunID
	if runID == "" {
		runID = uuid.NewString()
	}
	chainRoot := p.ChainRoot
	if chainRoot == "" {
		chainRoot = runID
	}
	if len(p.Input) == 0 {
		p.Input = json.RawMessage(`{}`)
	}
	var month time.Time
	err := tx.QueryRow(ctx, `
		INSERT INTO runs (run_id, tenant_id, workflow_name, workflow_version, status, input, next_seq,
		                  budget_usd, priority, start_key, parent_run_id, chain_root, dry_run, overrides)
		VALUES ($1::uuid, $2, $3, $4, 'pending', $5::jsonb, 2, $6, $7, $8, $9::uuid, $10::uuid, $11, $12::jsonb)
		ON CONFLICT DO NOTHING
		RETURNING date_trunc('month', created_at)::date`,
		runID, p.TenantID, p.Workflow, p.Version, string(p.Input), p.BudgetUSD, p.Priority,
		nullStr(p.StartKey), nullStr(p.ParentRunID), chainRoot, p.DryRun, nullJSON(p.Overrides)).Scan(&month)
	if errors.Is(err, pgx.ErrNoRows) {
		// Lost a race on start_key (or the run id already exists).
		var existing string
		err2 := tx.QueryRow(ctx, `SELECT run_id::text FROM runs WHERE run_id = $1::uuid OR (tenant_id = $2 AND start_key = $3)`,
			runID, p.TenantID, nullStr(p.StartKey)).Scan(&existing)
		if err2 != nil {
			return "", false, err2
		}
		return existing, false, nil
	}
	if err != nil {
		return "", false, err
	}
	payload := map[string]any{
		"workflow": p.Workflow, "version": p.Version, "input": p.Input,
		"budget_usd": p.BudgetUSD, "priority": p.Priority, "tenant_id": p.TenantID,
	}
	if p.ParentRunID != "" {
		payload["parent_run_id"] = p.ParentRunID
	}
	if p.DryRun {
		payload["dry_run"] = true
	}
	for k, v := range p.Extra {
		payload[k] = v
	}
	if err := s.appendTx(ctx, tx, runID, month, 1, 0, []NewEvent{{Type: "RunStarted", Payload: mustJSON(payload)}}); err != nil {
		return "", false, err
	}
	s.ready()
	return runID, true, nil
}

// ---------------------------------------------------------------- claim

type ClaimedRun struct {
	Run       *Run
	History   []Event
	ServerNow time.Time
}

var errOverCap = errors.New("tenant over running cap")

// ClaimRun leases one runnable run to workerID. Among tenants with ready runs
// and spare capacity it prefers the lowest running/weight, then claims that
// tenant's highest-priority, oldest run with FOR UPDATE SKIP LOCKED so
// concurrent claimers never block each other.
func (s *Store) ClaimRun(ctx context.Context, workerID string, workflows []string) (*ClaimedRun, error) {
	if len(workflows) == 0 {
		return nil, nil
	}
	var out *ClaimedRun
	err := s.tx(ctx, func(tx pgx.Tx) error {
		out = nil
		rows, err := tx.Query(ctx, `
			SELECT t.tenant_id FROM tenants t JOIN tenant_load l USING (tenant_id)
			 WHERE l.pending > 0 AND (t.max_running IS NULL OR l.running < t.max_running)
			 ORDER BY l.running::float8 / GREATEST(t.weight, 1), random()
			 LIMIT 3`)
		if err != nil {
			return err
		}
		tenants, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		tenants = append(tenants, "") // fallback: any tenant with capacity
		var runID, lastRelease string
		var availableAt time.Time
		var reclaimed bool
		for _, t := range tenants {
			err = tx.QueryRow(ctx, `
				WITH next AS (
				  SELECT run_id, available_at, first_claimed_at, last_release FROM runs
				   WHERE status = 'pending' AND available_at <= now() AND workflow_key = ANY($1)
				     AND ($3::text = '' OR tenant_id = $3)
				     AND tenant_id NOT IN (SELECT t.tenant_id FROM tenants t JOIN tenant_load l USING (tenant_id)
				                            WHERE t.max_running IS NOT NULL AND l.running >= t.max_running)
				   ORDER BY priority DESC, available_at
				   LIMIT 1
				   FOR UPDATE SKIP LOCKED)
				UPDATE runs r
				   SET status = 'running', lease_owner = $2, lease_epoch = r.lease_epoch + 1,
				       lease_expires_at = now() + $4 * interval '1 millisecond', updated_at = now(),
				       first_claimed_at = coalesce(r.first_claimed_at, now()),
				       wake_at = NULL, waiting_on = NULL, wait_reason = NULL
				  FROM next WHERE r.run_id = next.run_id
				RETURNING r.run_id::text, next.available_at, next.first_claimed_at IS NOT NULL, coalesce(next.last_release, '')`,
				workflows, workerID, t, s.LeaseTTL.Milliseconds()).Scan(&runID, &availableAt, &reclaimed, &lastRelease)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			break
		}
		if runID == "" {
			return nil
		}
		r, err := lockRun(ctx, tx, runID, "")
		if err != nil {
			return err
		}
		// Strict cap for capped tenants: lock the tenant row (serializing only
		// that tenant's claims), then the committed sum of the counter shards
		// plus our own increment is exact; undo the claim if it overshot.
		var maxRunning *int
		if err := tx.QueryRow(ctx, `SELECT max_running FROM tenants WHERE tenant_id = $1`, r.TenantID).
			Scan(&maxRunning); err != nil {
			return err
		}
		if maxRunning != nil {
			var running int
			if err := tx.QueryRow(ctx, `SELECT 1 FROM tenants WHERE tenant_id = $1 FOR UPDATE`, r.TenantID).Scan(&running); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT coalesce(sum(running), 0) FROM tenant_counters WHERE tenant_id = $1`,
				r.TenantID).Scan(&running); err != nil {
				return err
			}
			if running > *maxRunning {
				return errOverCap
			}
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
			return err
		}
		if reclaimed {
			ev := NewEvent{Type: "RunResumed", Payload: mustJSON(map[string]any{
				"worker_id": workerID, "epoch": r.LeaseEpoch, "reason": lastRelease,
			})}
			if err := s.appendServerTx(ctx, tx, r, []NewEvent{ev}); err != nil {
				return err
			}
		}
		hist, err := s.loadHistory(ctx, tx, r.RunID, r.RunMonth, 0, 0)
		if err != nil {
			return err
		}
		reason := "new"
		if reclaimed {
			reason = lastRelease
		}
		metrics.Claims.WithLabelValues(reason).Inc()
		metrics.QueueWait.Observe(now.Sub(availableAt).Seconds())
		out = &ClaimedRun{Run: r, History: hist, ServerNow: now}
		return nil
	})
	if errors.Is(err, errOverCap) {
		return nil, nil
	}
	return out, err
}

// ---------------------------------------------------------------- append

type AppendResult struct {
	OK              bool
	NextSeq         int64
	ErrorCode       string // lease_lost | seq_conflict | invalid
	CancelRequested bool
	ServerNow       time.Time
}

// Append writes events for a worker. One UPDATE checks the epoch (fencing
// token), the expected sequence number and the status, and locks the run row
// so appends to one run are serialized. Zero rows means the lease is gone or
// someone else wrote: the transaction rolls back and the worker must stop.
func (s *Store) Append(ctx context.Context, runID string, epoch, expectedSeq int64, evs []NewEvent) (AppendResult, error) {
	for _, e := range evs {
		if serverOnlyEvents[e.Type] || e.Type == "" {
			return AppendResult{ErrorCode: "invalid"}, fmt.Errorf("%w: workers cannot append %q", ErrInvalid, e.Type)
		}
		if len(e.Payload) > 0 && !json.Valid(e.Payload) {
			return AppendResult{ErrorCode: "invalid"}, fmt.Errorf("%w: payload of %s is not JSON", ErrInvalid, e.Type)
		}
	}
	var res AppendResult
	err := s.tx(ctx, func(tx pgx.Tx) error {
		res = AppendResult{}
		var tenant string
		var month time.Time
		err := tx.QueryRow(ctx, `
			UPDATE runs SET next_seq = next_seq + $4, updated_at = now()
			 WHERE run_id = $1::uuid AND lease_epoch = $2 AND next_seq = $3 AND status = 'running'
			RETURNING tenant_id, date_trunc('month', created_at)::date, cancel_requested, now(), next_seq`,
			runID, epoch, expectedSeq, len(evs)).Scan(&tenant, &month, &res.CancelRequested, &res.ServerNow, &res.NextSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			var curEpoch, curSeq int64
			var status string
			err2 := tx.QueryRow(ctx, `SELECT lease_epoch, next_seq, status FROM runs WHERE run_id = $1::uuid`, runID).
				Scan(&curEpoch, &curSeq, &status)
			switch {
			case errors.Is(err2, pgx.ErrNoRows):
				res.ErrorCode = "lease_lost"
			case err2 != nil:
				return err2
			case curEpoch != epoch || status != StatusRunning:
				res.ErrorCode = "lease_lost"
				metrics.FencingRejections.Inc()
			case curSeq == expectedSeq+int64(len(evs)) && s.sameAppend(ctx, tx, runID, epoch, expectedSeq, evs):
				// A retried append whose first attempt committed but whose
				// response was lost. Only the holder of this epoch can have
				// written these rows, so acknowledge instead of writing twice.
				res.OK = true
				res.NextSeq = curSeq
				_ = tx.QueryRow(ctx, `SELECT cancel_requested, now() FROM runs WHERE run_id = $1::uuid`, runID).
					Scan(&res.CancelRequested, &res.ServerNow)
			default:
				res.ErrorCode = "seq_conflict"
				res.NextSeq = curSeq
				metrics.SeqConflicts.Inc()
			}
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.appendTx(ctx, tx, runID, month, expectedSeq, epoch, evs); err != nil {
			return err
		}
		if err := s.applyDerived(ctx, tx, runID, tenant, epoch, evs); err != nil {
			return err
		}
		res.OK = true
		return nil
	})
	return res, err
}

func (s *Store) sameAppend(ctx context.Context, tx pgx.Tx, runID string, epoch, from int64, evs []NewEvent) bool {
	rows, err := tx.Query(ctx, `SELECT seq, type, coalesce(step_id,''), epoch FROM events
		WHERE run_id = $1::uuid AND seq >= $2 AND seq < $3 ORDER BY seq`, runID, from, from+int64(len(evs)))
	if err != nil {
		return false
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var seq, ep int64
		var typ, step string
		if rows.Scan(&seq, &typ, &step, &ep) != nil || i >= len(evs) {
			return false
		}
		if ep != epoch || typ != evs[i].Type || step != evs[i].StepID {
			return false
		}
		i++
	}
	return rows.Err() == nil && i == len(evs)
}

// applyDerived maintains tables derived from specific event types inside the
// append transaction, so the event and its consequence commit together (a
// transactional outbox for the runtime's own effects).
func (s *Store) applyDerived(ctx context.Context, tx pgx.Tx, runID, tenant string, epoch int64, evs []NewEvent) error {
	for _, e := range evs {
		var p map[string]any
		if len(e.Payload) > 0 {
			_ = json.Unmarshal(e.Payload, &p)
		}
		switch e.Type {
		case "ApprovalRequested":
			var a struct {
				ApprovalID string          `json:"approval_id"`
				Action     json.RawMessage `json:"action"`
				Reason     string          `json:"reason"`
				RuleIDs    []string        `json:"rule_ids"`
				Approvers  []string        `json:"approvers"`
				ExpiresAt  int64           `json:"expires_at_ms"`
				OnExpiry   string          `json:"on_expiry"`
				Summary    json.RawMessage `json:"summary"`
			}
			if err := json.Unmarshal(e.Payload, &a); err != nil || a.ApprovalID == "" {
				return fmt.Errorf("%w: bad ApprovalRequested", ErrInvalid)
			}
			if a.OnExpiry == "" {
				a.OnExpiry = "reject"
			}
			if len(a.Action) == 0 {
				a.Action = json.RawMessage(`{}`)
			}
			var expires any
			if a.ExpiresAt > 0 {
				expires = time.UnixMilli(a.ExpiresAt).UTC()
			}
			if a.RuleIDs == nil {
				a.RuleIDs = []string{}
			}
			if a.Approvers == nil {
				a.Approvers = []string{}
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO approvals (approval_id, run_id, tenant_id, step_id, action, reason, rule_ids,
				                       approvers, summary, status, on_expiry, expires_at)
				VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6, $7, $8, $9::jsonb, 'pending', $10, $11)
				ON CONFLICT (approval_id) DO NOTHING`,
				a.ApprovalID, runID, tenant, e.StepID, string(a.Action), a.Reason, a.RuleIDs, a.Approvers,
				nullJSON(a.Summary), a.OnExpiry, expires); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO outbox (run_id, step_id, destination, payload, dedupe_key)
				VALUES ($1::uuid, $2, 'approval_webhook', $3::jsonb, $4) ON CONFLICT (dedupe_key) DO NOTHING`,
				runID, e.StepID, string(mustJSON(map[string]any{
					"approval_id": a.ApprovalID, "run_id": runID, "tenant_id": tenant, "action": a.Action,
					"reason": a.Reason, "approvers": a.Approvers,
				})), "approval:"+a.ApprovalID); err != nil {
				return err
			}
		case "ChildRunStarted":
			var c struct {
				ChildRunID string          `json:"child_run_id"`
				Workflow   string          `json:"workflow"`
				Version    string          `json:"version"`
				Input      json.RawMessage `json:"input"`
				BudgetUSD  *float64        `json:"budget_usd"`
			}
			if err := json.Unmarshal(e.Payload, &c); err != nil || c.ChildRunID == "" || c.Workflow == "" {
				return fmt.Errorf("%w: bad ChildRunStarted", ErrInvalid)
			}
			var dry bool
			if err := tx.QueryRow(ctx, `SELECT dry_run FROM runs WHERE run_id = $1::uuid`, runID).Scan(&dry); err != nil {
				return err
			}
			if c.Version == "" {
				v, err := s.latestVersionTx(ctx, tx, c.Workflow)
				if err != nil {
					return err
				}
				c.Version = v
			}
			if _, _, err := s.startRunTx(ctx, tx, StartRunParams{
				RunID: c.ChildRunID, TenantID: tenant, Workflow: c.Workflow, Version: c.Version,
				Input: c.Input, BudgetUSD: c.BudgetUSD, ParentRunID: runID, DryRun: dry,
			}, false); err != nil {
				return err
			}
		case "OutboxMessage":
			dest, _ := p["destination"].(string)
			key, _ := p["dedupe_key"].(string)
			if key == "" {
				key = runID + ":" + e.StepID
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO outbox (run_id, step_id, destination, payload, dedupe_key)
				VALUES ($1::uuid, $2, $3, $4::jsonb, $5) ON CONFLICT (dedupe_key) DO NOTHING`,
				runID, e.StepID, dest, string(e.Payload), key); err != nil {
				return err
			}
		case "RunTainted":
			if _, err := tx.Exec(ctx, `UPDATE runs SET tainted = true WHERE run_id = $1::uuid`, runID); err != nil {
				return err
			}
		case "FactRecorded":
			facts, _ := json.Marshal(p["facts"])
			if _, err := tx.Exec(ctx, `UPDATE runs SET facts = facts || $2::jsonb WHERE run_id = $1::uuid`,
				runID, string(facts)); err != nil {
				return err
			}
		case "StepCompleted":
			kind, _ := p["kind"].(string)
			if ms, ok := p["duration_ms"].(float64); ok {
				metrics.StepDuration.WithLabelValues(kind).Observe(ms / 1000)
			}
		case "StepFailed":
			if r, _ := p["retryable"].(bool); r {
				metrics.StepRetries.Inc()
			}
		case "PolicyDecided":
			decision, _ := p["decision"].(string)
			rules, _ := p["rule_ids"].([]any)
			if len(rules) == 0 {
				metrics.PolicyDecisions.WithLabelValues("", decision).Inc()
			}
			for _, r := range rules {
				rs, _ := r.(string)
				metrics.PolicyDecisions.WithLabelValues(rs, decision).Inc()
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------- heartbeat

func (s *Store) Heartbeat(ctx context.Context, runID string, epoch int64) (ok, cancel bool, now time.Time, err error) {
	err = s.Pool.QueryRow(ctx, `
		UPDATE runs SET lease_expires_at = now() + $3 * interval '1 millisecond'
		 WHERE run_id = $1::uuid AND lease_epoch = $2 AND status = 'running'
		RETURNING cancel_requested, now()`, runID, epoch, s.LeaseTTL.Milliseconds()).Scan(&cancel, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		metrics.FencingRejections.Inc()
		return false, false, time.Now(), nil
	}
	return err == nil, cancel, now, err
}

// ---------------------------------------------------------------- suspend

type SuspendParams struct {
	RunID       string
	Epoch       int64
	Waits       []Wait
	AvailableAt time.Time // retry backoff
	Reason      string    // wait | retry | handback | parked
}

type SuspendResult struct {
	OK        bool
	LeaseLost bool
	Woken     bool
}

// Suspend releases a running run. A wait whose condition is already met
// (buffered signal, decided approval, finished child, due timer) wakes the run
// immediately, so nothing that arrived early is lost.
func (s *Store) Suspend(ctx context.Context, p SuspendParams) (SuspendResult, error) {
	var res SuspendResult
	err := s.tx(ctx, func(tx pgx.Tx) error {
		res = SuspendResult{}
		r, err := lockRun(ctx, tx, p.RunID, "")
		if errors.Is(err, ErrNotFound) {
			res.LeaseLost = true
			return nil
		}
		if err != nil {
			return err
		}
		if r.Status != StatusRunning || r.LeaseEpoch != p.Epoch {
			metrics.FencingRejections.Inc()
			res.LeaseLost = true
			return nil
		}
		res.OK = true
		switch {
		case p.Reason == "handback":
			_, err = tx.Exec(ctx, `UPDATE runs SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
				available_at = now(), last_release = 'handback', updated_at = now() WHERE run_id = $1::uuid`, r.RunID)
			if err != nil {
				return err
			}
			s.ready()
			return nil
		case p.Reason == "parked":
			_, err = tx.Exec(ctx, `UPDATE runs SET status = 'waiting', lease_owner = NULL, lease_expires_at = NULL,
				waiting_on = NULL, wake_at = NULL, wait_reason = 'parked', last_release = 'parked', updated_at = now()
				WHERE run_id = $1::uuid`, r.RunID)
			return err
		case len(p.Waits) == 0:
			avail := p.AvailableAt
			if avail.IsZero() {
				avail = time.Now()
			}
			_, err = tx.Exec(ctx, `UPDATE runs SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
				available_at = GREATEST($2::timestamptz, now()), last_release = 'retry', updated_at = now()
				WHERE run_id = $1::uuid`, r.RunID, avail.UTC())
			if err != nil {
				return err
			}
			s.ready()
			return nil
		}
		if r.CancelRequested {
			res.Woken = true
			return s.wakeTx(ctx, tx, r, nil)
		}
		evs, err := s.checkWaitsTx(ctx, tx, r, p.Waits)
		if err != nil {
			return err
		}
		if len(evs) > 0 {
			res.Woken = true
			return s.wakeTx(ctx, tx, r, evs)
		}
		var wakeAt *time.Time
		for _, w := range p.Waits {
			if w.Kind == "timer" && w.FireAtMs > 0 {
				t := time.UnixMilli(w.FireAtMs).UTC()
				if wakeAt == nil || t.Before(*wakeAt) {
					wakeAt = &t
				}
			}
		}
		reason := waitReason(p.Waits)
		_, err = tx.Exec(ctx, `UPDATE runs SET status = 'waiting', lease_owner = NULL, lease_expires_at = NULL,
			waiting_on = $2::jsonb, wake_at = $3, wait_reason = $4, last_release = 'wait', updated_at = now()
			WHERE run_id = $1::uuid`, r.RunID, string(mustJSON(p.Waits)), wakeAt, reason)
		return err
	})
	return res, err
}

func waitReason(ws []Wait) string {
	seen := map[string]bool{}
	out := ""
	for _, w := range ws {
		if !seen[w.Kind] {
			if out != "" {
				out += ","
			}
			out += w.Kind
			seen[w.Kind] = true
		}
	}
	return out
}

// checkWaitsTx returns the events that satisfy any of the waits right now.
func (s *Store) checkWaitsTx(ctx context.Context, tx pgx.Tx, r *Run, waits []Wait) ([]NewEvent, error) {
	var evs []NewEvent
	var nowMs int64
	if err := tx.QueryRow(ctx, `SELECT (extract(epoch from now()) * 1000)::bigint`).Scan(&nowMs); err != nil {
		return nil, err
	}
	for _, w := range waits {
		switch w.Kind {
		case "timer":
			if w.FireAtMs <= nowMs {
				evs = append(evs, NewEvent{Type: "TimerFired", StepID: w.ID, Payload: mustJSON(map[string]any{
					"timer_id": w.ID, "fire_at_ms": w.FireAtMs, "fired_at_ms": nowMs,
				})})
				metrics.TimersFired.Inc()
			}
		case "signal":
			var id int64
			var payload []byte
			var signalID string
			err := tx.QueryRow(ctx, `SELECT id, payload, signal_id FROM signals
				WHERE run_id = $1::uuid AND name = $2 AND consumed_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE`,
				r.RunID, w.Name).Scan(&id, &payload, &signalID)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE signals SET consumed_at = now(), consumed_by = $2 WHERE id = $1`, id, w.ID); err != nil {
				return nil, err
			}
			if len(payload) == 0 {
				payload = []byte("null")
			}
			evs = append(evs, NewEvent{Type: "SignalReceived", StepID: w.ID, Payload: mustJSON(map[string]any{
				"wait_id": w.ID, "name": w.Name, "payload": json.RawMessage(payload), "signal_id": signalID,
			})})
		case "approval":
			var status, decidedBy, comment, stepID, onExpiry string
			var edited []byte
			var decidedAt *time.Time
			err := tx.QueryRow(ctx, `SELECT status, coalesce(decided_by,''), edited_args, coalesce(comment,''),
				decided_at, step_id, on_expiry FROM approvals WHERE approval_id = $1::uuid`, w.ID).
				Scan(&status, &decidedBy, &edited, &comment, &decidedAt, &stepID, &onExpiry)
			if errors.Is(err, pgx.ErrNoRows) || (err == nil && status == "pending") {
				continue
			}
			if err != nil {
				return nil, err
			}
			p := map[string]any{"approval_id": w.ID, "decision": status, "decided_by": decidedBy,
				"comment": comment, "on_expiry": onExpiry}
			if len(edited) > 0 {
				p["edited_args"] = json.RawMessage(edited)
			}
			if decidedAt != nil {
				p["decided_at_ms"] = decidedAt.UnixMilli()
			}
			evs = append(evs, NewEvent{Type: "ApprovalResolved", StepID: stepID, Payload: mustJSON(p)})
		case "child":
			var finalID, status string
			var result, errJSON []byte
			err := tx.QueryRow(ctx, `SELECT run_id::text, status, result, error FROM runs
				WHERE chain_root = $1::uuid AND status IN ('completed','failed','cancelled') AND continued_as IS NULL
				LIMIT 1`, w.ID).Scan(&finalID, &status, &result, &errJSON)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return nil, err
			}
			evs = append(evs, NewEvent{Type: "ChildRunCompleted", StepID: w.ID, Payload: mustJSON(map[string]any{
				"child_run_id": w.ID, "final_run_id": finalID, "status": status,
				"result": rawOrNull(result), "error": rawOrNull(errJSON),
			})})
		}
	}
	return evs, nil
}

func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	return b
}

// wakeTx appends the wake events and makes the run pending again.
func (s *Store) wakeTx(ctx context.Context, tx pgx.Tx, r *Run, evs []NewEvent) error {
	if err := s.appendServerTx(ctx, tx, r, evs); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE runs SET status = 'pending', available_at = now(), waiting_on = NULL,
		wake_at = NULL, wait_reason = NULL, lease_owner = NULL, lease_expires_at = NULL,
		last_release = CASE WHEN status = 'running' THEN 'wait' ELSE 'wake' END, updated_at = now()
		WHERE run_id = $1::uuid`, r.RunID)
	if err != nil {
		return err
	}
	r.Status = StatusPending
	s.ready()
	return nil
}

func parseWaits(raw json.RawMessage) []Wait {
	var ws []Wait
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &ws)
	}
	return ws
}

// ---------------------------------------------------------------- finish

type FinishParams struct {
	RunID         string
	Epoch         int64
	ExpectedSeq   int64
	Status        string // completed | failed | cancelled | continued
	Result        json.RawMessage
	Error         json.RawMessage
	ContinueInput json.RawMessage
}

type FinishResult struct {
	OK        bool
	LeaseLost bool
	ErrorCode string
	NewRunID  string
}

func (s *Store) Finish(ctx context.Context, p FinishParams) (FinishResult, error) {
	var res FinishResult
	var finalStatus string
	var created time.Time
	err := s.tx(ctx, func(tx pgx.Tx) error {
		res = FinishResult{}
		r, err := lockRun(ctx, tx, p.RunID, "")
		if errors.Is(err, ErrNotFound) {
			res.LeaseLost = true
			return nil
		}
		if err != nil {
			return err
		}
		if r.Status != StatusRunning || r.LeaseEpoch != p.Epoch {
			metrics.FencingRejections.Inc()
			res.LeaseLost = true
			res.ErrorCode = "lease_lost"
			return nil
		}
		if p.ExpectedSeq > 0 && r.NextSeq != p.ExpectedSeq {
			metrics.SeqConflicts.Inc()
			res.ErrorCode = "seq_conflict"
			return nil
		}
		created = r.CreatedAt
		var parent *Run
		if r.ParentRunID != "" && p.Status != "continued" {
			if parent, err = lockRun(ctx, tx, r.ParentRunID, ""); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}
		var evs []NewEvent
		status := p.Status
		switch p.Status {
		case StatusCompleted:
			evs = []NewEvent{{Type: "RunCompleted", Payload: mustJSON(map[string]any{"result": rawOrNull(p.Result)})}}
		case StatusFailed:
			evs = []NewEvent{{Type: "RunFailed", Payload: mustJSON(map[string]any{"error": rawOrNull(p.Error)})}}
		case StatusCancelled:
			evs = []NewEvent{{Type: "RunCancelled", Payload: mustJSON(map[string]any{"error": rawOrNull(p.Error)})}}
		case "continued":
			var budget *float64
			if r.BudgetUSD != nil {
				left := *r.BudgetUSD - r.SpentUSD
				budget = &left
			}
			newID, _, err := s.startRunTx(ctx, tx, StartRunParams{
				TenantID: r.TenantID, Workflow: r.WorkflowName, Version: r.WorkflowVersion,
				Input: p.ContinueInput, BudgetUSD: budget, Priority: r.Priority, ParentRunID: r.ParentRunID,
				ChainRoot: r.ChainRoot, DryRun: r.DryRun, Overrides: r.Overrides,
				Extra: map[string]any{"continued_from": r.RunID},
			}, false)
			if err != nil {
				return err
			}
			res.NewRunID = newID
			p.Result = mustJSON(map[string]any{"continued_as_new": newID})
			status = StatusCompleted
			evs = []NewEvent{
				{Type: "RunContinuedAsNew", Payload: mustJSON(map[string]any{"new_run_id": newID})},
				{Type: "RunCompleted", Payload: mustJSON(map[string]any{"result": p.Result})},
			}
			if _, err := tx.Exec(ctx, `UPDATE runs SET continued_as = $2::uuid WHERE run_id = $1::uuid`, r.RunID, newID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: status %q", ErrInvalid, p.Status)
		}
		if err := s.appendServerTx(ctx, tx, r, evs); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET status = $2, result = $3::jsonb, error = $4::jsonb,
			lease_owner = NULL, lease_expires_at = NULL, finished_at = now(), updated_at = now()
			WHERE run_id = $1::uuid`, r.RunID, status, nullJSON(p.Result), nullJSON(p.Error)); err != nil {
			return err
		}
		finalStatus = status
		// Wake the parent if it is waiting on this child chain.
		if parent != nil && parent.Status == StatusWaiting {
			for _, w := range parseWaits(parent.WaitingOn) {
				if w.Kind == "child" && w.ID == r.ChainRoot {
					cevs, err := s.checkWaitsTx(ctx, tx, parent, parseWaits(parent.WaitingOn))
					if err != nil {
						return err
					}
					if len(cevs) > 0 {
						return s.wakeTx(ctx, tx, parent, cevs)
					}
				}
			}
		}
		res.OK = true
		return nil
	})
	if err == nil && res.OK {
		metrics.RunsFinished.WithLabelValues(finalStatus).Inc()
		metrics.RunDuration.Observe(time.Since(created).Seconds())
	}
	return res, err
}
