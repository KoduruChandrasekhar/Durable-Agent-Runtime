// Package budget enforces per-run and per-tenant daily budgets with
// reserve-then-settle: the estimated maximum cost is reserved atomically
// before the call and the real cost settled after, so parallel calls can
// never overspend.
package budget

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrExceeded = errors.New("budget exceeded")

type Ledger interface {
	Reserve(ctx context.Context, runID, tenant string, est float64) error
	Settle(ctx context.Context, runID, tenant string, est, actual float64) error
}

type Postgres struct{ Pool *pgxpool.Pool }

// Reserve holds `est` against the run's budget and the tenant's daily budget
// in one transaction; either both hold or neither does.
func (p *Postgres) Reserve(ctx context.Context, runID, tenant string, est float64) error {
	return pgx.BeginFunc(ctx, p.Pool, func(tx pgx.Tx) error {
		if runID != "" {
			tag, err := tx.Exec(ctx, `UPDATE runs SET reserved_usd = reserved_usd + $2
				WHERE run_id = $1::uuid AND (budget_usd IS NULL OR spent_usd + reserved_usd + $2 <= budget_usd)`, runID, est)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var exists bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid)`, runID).Scan(&exists); err != nil {
					return err
				}
				if exists {
					return ErrExceeded
				}
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO tenant_spend (tenant_id, day) VALUES ($1, current_date)
			ON CONFLICT DO NOTHING`, tenant); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE tenant_spend ts SET reserved_usd = ts.reserved_usd + $2
			WHERE ts.tenant_id = $1 AND ts.day = current_date AND (
			  SELECT t.daily_budget_usd IS NULL OR ts.spent_usd + ts.reserved_usd + $2 <= t.daily_budget_usd
			  FROM tenants t WHERE t.tenant_id = $1
			) IS NOT FALSE`, tenant, est)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrExceeded
		}
		return nil
	})
}

// Settle releases the reservation and records the real cost.
func (p *Postgres) Settle(ctx context.Context, runID, tenant string, est, actual float64) error {
	return pgx.BeginFunc(ctx, p.Pool, func(tx pgx.Tx) error {
		if runID != "" {
			if _, err := tx.Exec(ctx, `UPDATE runs SET reserved_usd = GREATEST(reserved_usd - $2, 0),
				spent_usd = spent_usd + $3 WHERE run_id = $1::uuid`, runID, est, actual); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE tenant_spend SET reserved_usd = GREATEST(reserved_usd - $2, 0),
			spent_usd = spent_usd + $3 WHERE tenant_id = $1 AND day = current_date`, tenant, est, actual)
		return err
	})
}

// Noop is used when the gateway runs without a database (tests, demos).
type Noop struct{}

func (Noop) Reserve(context.Context, string, string, float64) error         { return nil }
func (Noop) Settle(context.Context, string, string, float64, float64) error { return nil }
