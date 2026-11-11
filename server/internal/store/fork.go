package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var terminalEvents = map[string]bool{
	"RunCompleted": true, "RunFailed": true, "RunCancelled": true, "RunContinuedAsNew": true,
}

// Fork creates a new run whose journal is a copy of the source run's events
// up to fromSeq, then continues live (with overrides such as a different
// model). Replay is deterministic, so the copied steps return their recorded
// results and no earlier side effect repeats. Forks are dry runs by default:
// effectful tools are replaced by mocks.
func (s *Store) Fork(ctx context.Context, tenant, runID string, fromSeq int64, overrides json.RawMessage, dryRun bool) (string, error) {
	var newID string
	err := s.tx(ctx, func(tx pgx.Tx) error {
		src, err := lockRun(ctx, tx, runID, tenant)
		if err != nil {
			return err
		}
		evs, err := s.loadHistory(ctx, tx, runID, src.RunMonth, 0, 0)
		if err != nil {
			return err
		}
		var copied []Event
		var maxEpoch int64
		tainted := false
		facts := map[string]any{}
		for _, e := range evs {
			if fromSeq > 0 && e.Seq > fromSeq {
				break
			}
			if terminalEvents[e.Type] {
				break
			}
			copied = append(copied, e)
			if e.Epoch > maxEpoch {
				maxEpoch = e.Epoch
			}
			// Derived state is rebuilt from the copied prefix only.
			switch e.Type {
			case "RunTainted":
				tainted = true
			case "FactRecorded":
				var p struct {
					Facts map[string]any `json:"facts"`
				}
				if json.Unmarshal(e.Payload, &p) == nil {
					for k, v := range p.Facts {
						facts[k] = v
					}
				}
			}
		}
		if len(copied) == 0 {
			return ErrInvalid
		}
		input := src.Input
		var ov map[string]json.RawMessage
		if len(overrides) > 0 {
			_ = json.Unmarshal(overrides, &ov)
			if in, ok := ov["input"]; ok && len(copied) == 1 {
				input = in
			}
		}
		newID = uuid.NewString()
		lastSeq := copied[len(copied)-1].Seq
		var month time.Time
		err = tx.QueryRow(ctx, `
			INSERT INTO runs (run_id, tenant_id, workflow_name, workflow_version, status, input, next_seq,
			                  lease_epoch, budget_usd, priority, chain_root, dry_run, overrides, forked_from, tainted, facts)
			VALUES ($1::uuid, $2, $3, $4, 'pending', $5::jsonb, $6, $7, $8, $9, $1::uuid, $10, $11::jsonb, $12::uuid, $13, $14::jsonb)
			RETURNING date_trunc('month', created_at)::date`,
			newID, src.TenantID, src.WorkflowName, src.WorkflowVersion, string(input), lastSeq+2, maxEpoch,
			src.BudgetUSD, src.Priority, dryRun, nullJSON(overrides), src.RunID, tainted,
			string(mustJSON(facts))).Scan(&month)
		if err != nil {
			return err
		}
		for i, e := range copied {
			if err := s.appendTx(ctx, tx, newID, month, int64(i)+1, e.Epoch, []NewEvent{{Type: e.Type, StepID: e.StepID, Payload: e.Payload}}); err != nil {
				return err
			}
		}
		fork := NewEvent{Type: "RunForked", Payload: mustJSON(map[string]any{
			"source_run_id": src.RunID, "from_seq": lastSeq, "overrides": rawOrNull(overrides), "dry_run": dryRun,
		})}
		if err := s.appendTx(ctx, tx, newID, month, lastSeq+1, maxEpoch, []NewEvent{fork}); err != nil {
			return err
		}
		s.ready()
		return nil
	})
	return newID, err
}
