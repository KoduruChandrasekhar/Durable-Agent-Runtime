// Package store is the only code that touches PostgreSQL. Every write to a
// run's event log goes through Append (or the server-side wake paths), which
// check the lease epoch and expected sequence number in the same transaction
// that inserts the events.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"

	"github.com/keel-runtime/keel/server/internal/blob"
	"github.com/keel-runtime/keel/server/internal/metrics"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var (
	ErrNotFound       = errors.New("not found")
	ErrConflict       = errors.New("conflict")
	ErrTooManyPending = errors.New("tenant pending queue is full")
	ErrInvalid        = errors.New("invalid request")
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusWaiting   = "waiting"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

func IsFinal(status string) bool {
	return status == StatusCompleted || status == StatusFailed || status == StatusCancelled
}

type Store struct {
	Pool     *pgxpool.Pool
	DSN      string
	Blobs    blob.Store
	LeaseTTL time.Duration

	readyCh chan struct{}
}

func Open(ctx context.Context, dsn string, maxConns int32, blobs blob.Store) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool, DSN: dsn, Blobs: blobs, LeaseTTL: 30 * time.Second, readyCh: make(chan struct{}, 1)}, nil
}

func (s *Store) Close() { s.Pool.Close() }

// Migrate applies the embedded migrations, holding a PostgreSQL advisory lock
// so several replicas starting at once do not race.
func (s *Store) Migrate(ctx context.Context) error {
	db, err := sql.Open("pgx", s.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(database.DialectPostgres, db, sub, goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

// ---------------------------------------------------------------- types

type Run struct {
	RunID           string          `json:"run_id"`
	TenantID        string          `json:"tenant_id"`
	WorkflowName    string          `json:"workflow"`
	WorkflowVersion string          `json:"version"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           json.RawMessage `json:"error,omitempty"`
	NextSeq         int64           `json:"next_seq"`
	LeaseOwner      string          `json:"lease_owner,omitempty"`
	LeaseEpoch      int64           `json:"lease_epoch"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	AvailableAt     time.Time       `json:"available_at"`
	WakeAt          *time.Time      `json:"wake_at,omitempty"`
	WaitingOn       json.RawMessage `json:"waiting_on,omitempty"`
	WaitReason      string          `json:"wait_reason,omitempty"`
	LastRelease     string          `json:"last_release,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	Priority        int             `json:"priority"`
	BudgetUSD       *float64        `json:"budget_usd,omitempty"`
	SpentUSD        float64         `json:"spent_usd"`
	ReservedUSD     float64         `json:"reserved_usd"`
	Tainted         bool            `json:"tainted"`
	Facts           json.RawMessage `json:"facts,omitempty"`
	DryRun          bool            `json:"dry_run"`
	Overrides       json.RawMessage `json:"overrides,omitempty"`
	ForkedFrom      string          `json:"forked_from,omitempty"`
	StartKey        string          `json:"start_key,omitempty"`
	ParentRunID     string          `json:"parent_run_id,omitempty"`
	ChainRoot       string          `json:"chain_root"`
	ContinuedAs     string          `json:"continued_as,omitempty"`
	FirstClaimedAt  *time.Time      `json:"first_claimed_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	RunMonth        time.Time       `json:"-"`
}

const runCols = `run_id::text, tenant_id, workflow_name, workflow_version, status, input, result, error,
 next_seq, coalesce(lease_owner,''), lease_epoch, lease_expires_at, available_at, wake_at, waiting_on,
 coalesce(wait_reason,''), coalesce(last_release,''), cancel_requested, priority, budget_usd::float8,
 spent_usd::float8, reserved_usd::float8, tainted, facts, dry_run, overrides, coalesce(forked_from::text,''),
 coalesce(start_key,''), coalesce(parent_run_id::text,''), chain_root::text, coalesce(continued_as::text,''),
 first_claimed_at, finished_at, created_at, updated_at, date_trunc('month', created_at)::date`

func scanRun(row pgx.Row) (*Run, error) {
	var r Run
	err := row.Scan(&r.RunID, &r.TenantID, &r.WorkflowName, &r.WorkflowVersion, &r.Status,
		(*[]byte)(&r.Input), (*[]byte)(&r.Result), (*[]byte)(&r.Error),
		&r.NextSeq, &r.LeaseOwner, &r.LeaseEpoch, &r.LeaseExpiresAt, &r.AvailableAt, &r.WakeAt,
		(*[]byte)(&r.WaitingOn), &r.WaitReason, &r.LastRelease, &r.CancelRequested, &r.Priority,
		&r.BudgetUSD, &r.SpentUSD, &r.ReservedUSD, &r.Tainted, (*[]byte)(&r.Facts), &r.DryRun,
		(*[]byte)(&r.Overrides), &r.ForkedFrom, &r.StartKey, &r.ParentRunID, &r.ChainRoot, &r.ContinuedAs,
		&r.FirstClaimedAt, &r.FinishedAt, &r.CreatedAt, &r.UpdatedAt, &r.RunMonth)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

type Event struct {
	RunID     string          `json:"run_id"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	StepID    string          `json:"step_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Epoch     int64           `json:"epoch"`
	CreatedAt time.Time       `json:"created_at"`
}

type NewEvent struct {
	Type    string
	StepID  string
	Payload json.RawMessage
}

// Wait is one condition a waiting run can be woken by.
type Wait struct {
	Kind     string `json:"kind"` // timer | signal | approval | child
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	FireAtMs int64  `json:"fire_at_ms,omitempty"`
}

// Event types only the server may write. Workers appending them is rejected.
var serverOnlyEvents = map[string]bool{
	"RunStarted": true, "RunResumed": true, "TimerFired": true, "SignalReceived": true,
	"ApprovalResolved": true, "ChildRunCompleted": true, "RunCompleted": true, "RunFailed": true,
	"RunCancelled": true, "RunForked": true, "RunContinuedAsNew": true,
}

// ---------------------------------------------------------------- helpers

func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01" // serialization failure, deadlock
	}
	return false
}

// tx runs fn in a transaction, retrying serialization failures and deadlocks.
func (s *Store) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for attempt := 0; attempt < 6; attempt++ {
		err = pgx.BeginFunc(ctx, s.Pool, fn)
		if err == nil || !isRetryable(err) {
			return err
		}
		time.Sleep(time.Duration(5+rand.IntN(20*(attempt+1))) * time.Millisecond)
	}
	return err
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// appendTx inserts events at startSeq.. for a run whose row the caller holds
// locked and whose next_seq the caller has already advanced.
func (s *Store) appendTx(ctx context.Context, tx pgx.Tx, runID string, month time.Time, startSeq, epoch int64, evs []NewEvent) error {
	if len(evs) == 0 {
		return nil
	}
	seqs := make([]int64, len(evs))
	types := make([]string, len(evs))
	steps := make([]string, len(evs))
	payloads := make([]string, len(evs))
	for i, e := range evs {
		p := e.Payload
		if len(p) == 0 {
			p = json.RawMessage(`{}`)
		}
		p, err := blob.Offload(s.Blobs, p)
		if err != nil {
			return fmt.Errorf("blob offload: %w", err)
		}
		seqs[i] = startSeq + int64(i)
		types[i] = e.Type
		steps[i] = e.StepID
		payloads[i] = string(p)
		metrics.EventsAppended.WithLabelValues(e.Type).Inc()
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO events (run_id, seq, type, step_id, payload, epoch, run_month)
		SELECT $1::uuid, u.s, u.t, nullif(u.st, ''), u.p::jsonb, $2, $3::date
		  FROM unnest($4::bigint[], $5::text[], $6::text[], $7::text[]) AS u(s, t, st, p)`,
		runID, epoch, month, seqs, types, steps, payloads)
	return err
}

// appendServerTx appends events to a run that no worker is writing to
// (waiting or pending), advancing next_seq under the row lock.
func (s *Store) appendServerTx(ctx context.Context, tx pgx.Tx, r *Run, evs []NewEvent) error {
	if len(evs) == 0 {
		return nil
	}
	var start int64
	err := tx.QueryRow(ctx, `UPDATE runs SET next_seq = next_seq + $2, updated_at = now()
		WHERE run_id = $1 RETURNING next_seq - $2`, r.RunID, len(evs)).Scan(&start)
	if err != nil {
		return err
	}
	if err := s.appendTx(ctx, tx, r.RunID, r.RunMonth, start, r.LeaseEpoch, evs); err != nil {
		return err
	}
	r.NextSeq = start + int64(len(evs))
	return nil
}

func lockRun(ctx context.Context, tx pgx.Tx, runID, tenant string) (*Run, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, ErrNotFound
	}
	q := `SELECT ` + runCols + ` FROM runs WHERE run_id = $1::uuid`
	args := []any{runID}
	if tenant != "" {
		q += ` AND tenant_id = $2`
		args = append(args, tenant)
	}
	return scanRun(tx.QueryRow(ctx, q+` FOR UPDATE`, args...))
}

// ready records that a run became runnable. NOTIFY inside a transaction makes
// PostgreSQL serialize commits on a global lock (the first benchmark showed
// it), so notifications are coalesced and sent outside transactions by
// RunNotifier; pollers also re-check every second.
func (s *Store) ready() {
	if s.readyCh == nil {
		return
	}
	select {
	case s.readyCh <- struct{}{}:
	default:
	}
}

// RunNotifier sends at most one NOTIFY runs_ready per 10 ms.
func (s *Store) RunNotifier(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.readyCh:
		}
		time.Sleep(10 * time.Millisecond) // let the transaction that signalled commit; coalesce bursts
		_, _ = s.Pool.Exec(ctx, `SELECT pg_notify('runs_ready', '')`)
	}
}

func (s *Store) loadHistory(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, runID string, month time.Time, afterSeq int64, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 1 << 30
	}
	rows, err := q.Query(ctx, `SELECT seq, type, coalesce(step_id,''), payload, epoch, created_at
		FROM events WHERE run_id = $1::uuid AND run_month = $2::date AND seq > $3 ORDER BY seq LIMIT $4`,
		runID, month, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		e := Event{RunID: runID}
		if err := rows.Scan(&e.Seq, &e.Type, &e.StepID, (*[]byte)(&e.Payload), &e.Epoch, &e.CreatedAt); err != nil {
			return nil, err
		}
		if p, err := blob.Resolve(s.Blobs, e.Payload); err == nil {
			e.Payload = p
		} else {
			return nil, fmt.Errorf("resolve blob seq %d: %w", e.Seq, err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
