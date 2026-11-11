package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/keel-runtime/keel/server/internal/metrics"
)

// Signal buffers an external signal. A run already waiting for that name is
// woken in the same transaction; otherwise the signal stays buffered until a
// wait consumes it. Repeated signal ids are ignored.
func (s *Store) Signal(ctx context.Context, tenant, runID, name string, payload json.RawMessage, signalID string) (duplicate bool, err error) {
	err = s.tx(ctx, func(tx pgx.Tx) error {
		duplicate = false
		r, err := lockRun(ctx, tx, runID, tenant)
		if err != nil {
			return err
		}
		if IsFinal(r.Status) {
			return ErrConflict
		}
		var id int64
		err = tx.QueryRow(ctx, `INSERT INTO signals (run_id, name, payload, signal_id)
			VALUES ($1::uuid, $2, $3::jsonb, $4) ON CONFLICT (run_id, signal_id) DO NOTHING RETURNING id`,
			runID, name, nullJSON(payload), signalID).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			duplicate = true
			return nil
		}
		if err != nil {
			return err
		}
		if r.Status != StatusWaiting {
			return nil
		}
		evs, err := s.checkWaitsTx(ctx, tx, r, parseWaits(r.WaitingOn))
		if err != nil || len(evs) == 0 {
			return err
		}
		return s.wakeTx(ctx, tx, r, evs)
	})
	return duplicate, err
}

type Approval struct {
	ApprovalID string          `json:"approval_id"`
	RunID      string          `json:"run_id"`
	TenantID   string          `json:"tenant_id"`
	StepID     string          `json:"step_id"`
	Action     json.RawMessage `json:"action"`
	Reason     string          `json:"reason"`
	RuleIDs    []string        `json:"rule_ids"`
	Approvers  []string        `json:"approvers"`
	Summary    json.RawMessage `json:"summary,omitempty"`
	Status     string          `json:"status"`
	OnExpiry   string          `json:"on_expiry"`
	EditedArgs json.RawMessage `json:"edited_args,omitempty"`
	Comment    string          `json:"comment,omitempty"`
	DecidedBy  string          `json:"decided_by,omitempty"`
	DecidedAt  *time.Time      `json:"decided_at,omitempty"`
	ExpiresAt  *time.Time      `json:"expires_at,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

const approvalCols = `approval_id::text, run_id::text, tenant_id, step_id, action, reason, rule_ids, approvers,
 summary, status, on_expiry, edited_args, coalesce(comment,''), coalesce(decided_by,''), decided_at, expires_at, created_at`

func scanApproval(row pgx.Row) (*Approval, error) {
	var a Approval
	err := row.Scan(&a.ApprovalID, &a.RunID, &a.TenantID, &a.StepID, (*[]byte)(&a.Action), &a.Reason, &a.RuleIDs,
		&a.Approvers, (*[]byte)(&a.Summary), &a.Status, &a.OnExpiry, (*[]byte)(&a.EditedArgs), &a.Comment,
		&a.DecidedBy, &a.DecidedAt, &a.ExpiresAt, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &a, err
}

// DecideApproval records a human decision. Deciding an already decided
// approval changes nothing and reports changed=false.
func (s *Store) DecideApproval(ctx context.Context, tenant, approvalID, decision string, edited json.RawMessage,
	by, comment string) (a *Approval, changed bool, err error) {
	if decision != "approved" && decision != "rejected" {
		return nil, false, ErrInvalid
	}
	err = s.tx(ctx, func(tx pgx.Tx) error {
		changed = false
		q := `SELECT ` + approvalCols + ` FROM approvals WHERE approval_id = $1::uuid`
		args := []any{approvalID}
		if tenant != "" {
			q += ` AND tenant_id = $2`
			args = append(args, tenant)
		}
		a, err = scanApproval(tx.QueryRow(ctx, q+` FOR UPDATE`, args...))
		if err != nil {
			return err
		}
		if a.Status != "pending" {
			return nil
		}
		a, err = scanApproval(tx.QueryRow(ctx, `UPDATE approvals SET status = $2, edited_args = $3::jsonb,
			decided_by = $4, decided_at = now(), comment = $5 WHERE approval_id = $1::uuid RETURNING `+approvalCols,
			approvalID, decision, nullJSON(edited), by, comment))
		if err != nil {
			return err
		}
		changed = true
		return s.wakeIfWaitingTx(ctx, tx, a.RunID)
	})
	return a, changed, err
}

func (s *Store) wakeIfWaitingTx(ctx context.Context, tx pgx.Tx, runID string) error {
	r, err := lockRun(ctx, tx, runID, "")
	if err != nil {
		return err
	}
	if r.Status != StatusWaiting {
		return nil
	}
	evs, err := s.checkWaitsTx(ctx, tx, r, parseWaits(r.WaitingOn))
	if err != nil || len(evs) == 0 {
		return err
	}
	return s.wakeTx(ctx, tx, r, evs)
}

// Cancel flags a run for cancellation. The worker sees the flag on its next
// heartbeat or append, runs compensations and appends RunCancelled. A
// waiting run is made pending so a worker can do that.
func (s *Store) Cancel(ctx context.Context, tenant, runID string) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		r, err := lockRun(ctx, tx, runID, tenant)
		if err != nil {
			return err
		}
		if IsFinal(r.Status) {
			return ErrConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE runs SET cancel_requested = true, updated_at = now() WHERE run_id = $1::uuid`, runID); err != nil {
			return err
		}
		if r.Status == StatusWaiting {
			return s.wakeTx(ctx, tx, r, nil)
		}
		return nil
	})
}

// Unpark re-queues a run parked for an operator (e.g. after a
// NonDeterminismError once the code was fixed).
func (s *Store) Unpark(ctx context.Context, tenant, runID string) error {
	return s.tx(ctx, func(tx pgx.Tx) error {
		r, err := lockRun(ctx, tx, runID, tenant)
		if err != nil {
			return err
		}
		if r.Status != StatusWaiting || r.WaitReason != "parked" {
			return ErrConflict
		}
		return s.wakeTx(ctx, tx, r, nil)
	})
}

// SweepExpiredLeases returns runs whose lease expired to pending. The next
// claim increments the epoch, fencing off the old holder.
func (s *Store) SweepExpiredLeases(ctx context.Context) (int, error) {
	var n int
	err := s.tx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH expired AS (
			  SELECT run_id FROM runs WHERE status = 'running' AND lease_expires_at < now()
			   ORDER BY lease_expires_at LIMIT 500 FOR UPDATE SKIP LOCKED)
			UPDATE runs r SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL,
			       available_at = now(), last_release = 'lease_expired', updated_at = now()
			  FROM expired WHERE r.run_id = expired.run_id RETURNING r.run_id`)
		if err != nil {
			return err
		}
		n = 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if n > 0 {
			s.ready()
			return nil
		}
		return nil
	})
	if n > 0 {
		metrics.LeaseExpirations.Add(float64(n))
	}
	return n, err
}

// FireTimers wakes waiting runs whose earliest timer is due, appending
// TimerFired (and anything else that became satisfiable).
func (s *Store) FireTimers(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT run_id::text FROM runs WHERE status = 'waiting' AND wake_at <= now()
		ORDER BY wake_at LIMIT 1000`)
	if err != nil {
		return 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	// A bounded pool of independent transactions: the first benchmark showed
	// one-at-a-time firing falling ~40 s behind with thousands of sleepers.
	var mu sync.Mutex
	var firstErr error
	fired := 0
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, id := range ids {
		sem <- struct{}{}
		wg.Add(1)
		go func(id string) {
			defer func() { <-sem; wg.Done() }()
			woke := false
			err := s.tx(ctx, func(tx pgx.Tx) error {
				r, err := scanRun(tx.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE run_id = $1::uuid
				AND status = 'waiting' AND wake_at <= now() FOR UPDATE SKIP LOCKED`, id))
				if errors.Is(err, ErrNotFound) {
					return nil
				}
				if err != nil {
					return err
				}
				evs, err := s.checkWaitsTx(ctx, tx, r, parseWaits(r.WaitingOn))
				if err != nil {
					return err
				}
				if len(evs) == 0 {
					_, err = tx.Exec(ctx, `UPDATE runs SET wake_at = NULL WHERE run_id = $1::uuid`, id)
					return err
				}
				woke = true
				return s.wakeTx(ctx, tx, r, evs)
			})
			mu.Lock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if err == nil && woke {
				fired++
			}
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	return fired, firstErr
}

// ExpireApprovals resolves approvals past their expiry as "expired"; the
// rule's on_expiry action is applied by the workflow when it resumes.
func (s *Store) ExpireApprovals(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT approval_id::text FROM approvals
		WHERE status = 'pending' AND expires_at <= now() ORDER BY expires_at LIMIT 500`)
	if err != nil {
		return 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		err := s.tx(ctx, func(tx pgx.Tx) error {
			var runID string
			err := tx.QueryRow(ctx, `UPDATE approvals SET status = 'expired', decided_by = 'system:expiry',
				decided_at = now() WHERE approval_id = $1::uuid AND status = 'pending' RETURNING run_id::text`, id).Scan(&runID)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			n++
			return s.wakeIfWaitingTx(ctx, tx, runID)
		})
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
