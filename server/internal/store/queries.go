package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Every query that serves a client filters by tenant; tenant == "" is only
// passed for admin principals.

func (s *Store) GetRun(ctx context.Context, tenant, runID string) (*Run, error) {
	if _, err := uuid.Parse(runID); err != nil {
		return nil, ErrNotFound
	}
	q := `SELECT ` + runCols + ` FROM runs WHERE run_id = $1::uuid`
	args := []any{runID}
	if tenant != "" {
		q += ` AND tenant_id = $2`
		args = append(args, tenant)
	}
	return scanRun(s.Pool.QueryRow(ctx, q, args...))
}

type RunFilter struct {
	Tenant        string
	Workflow      string
	Version       string
	Status        string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Cursor        string // "<unix_nanos>:<run_id>" of the last row of the previous page
	Limit         int
}

func (s *Store) ListRuns(ctx context.Context, f RunFilter) ([]*Run, string, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 50
	}
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if f.Tenant != "" {
		add("tenant_id = ?", f.Tenant)
	}
	if f.Workflow != "" {
		add("workflow_name = ?", f.Workflow)
	}
	if f.Version != "" {
		add("workflow_version = ?", f.Version)
	}
	if f.Status != "" {
		add("status = ?", f.Status)
	}
	if f.CreatedAfter != nil {
		add("created_at >= ?", *f.CreatedAfter)
	}
	if f.CreatedBefore != nil {
		add("created_at < ?", *f.CreatedBefore)
	}
	if f.Cursor != "" {
		parts := strings.SplitN(f.Cursor, ":", 2)
		if len(parts) == 2 {
			if ns, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
				args = append(args, time.Unix(0, ns).UTC(), parts[1])
				where = append(where, fmt.Sprintf("(created_at, run_id) < ($%d, $%d::uuid)", len(args)-1, len(args)))
			}
		}
	}
	q := `SELECT ` + runCols + ` FROM runs`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1)
	q += fmt.Sprintf(` ORDER BY created_at DESC, run_id DESC LIMIT $%d`, len(args))
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	next := ""
	if len(out) > f.Limit {
		out = out[:f.Limit]
		last := out[len(out)-1]
		next = fmt.Sprintf("%d:%s", last.CreatedAt.UnixNano(), last.RunID)
	}
	return out, next, rows.Err()
}

func (s *Store) ListEvents(ctx context.Context, tenant, runID string, afterSeq int64, limit int) ([]Event, *Run, error) {
	r, err := s.GetRun(ctx, tenant, runID)
	if err != nil {
		return nil, nil, err
	}
	evs, err := s.loadHistory(ctx, s.Pool, runID, r.RunMonth, afterSeq, limit)
	return evs, r, err
}

func (s *Store) ListApprovals(ctx context.Context, tenant, status string, limit int) ([]*Approval, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT ` + approvalCols + ` FROM approvals WHERE ($1 = '' OR tenant_id = $1) AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC LIMIT $3`
	rows, err := s.Pool.Query(ctx, q, tenant, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Approval
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) GetApproval(ctx context.Context, tenant, id string) (*Approval, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, ErrNotFound
	}
	return scanApproval(s.Pool.QueryRow(ctx, `SELECT `+approvalCols+` FROM approvals
		WHERE approval_id = $1::uuid AND ($2 = '' OR tenant_id = $2)`, id, tenant))
}

// ---------------------------------------------------------------- workers and workflows

type WorkflowDescriptor struct {
	Name        string          `json:"name"`
	Version     string          `json:"version"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

func (s *Store) UpsertWorker(ctx context.Context, workerID string, workflows []string, descs []WorkflowDescriptor) error {
	if _, err := s.Pool.Exec(ctx, `INSERT INTO workers (worker_id, workflows, last_seen) VALUES ($1, $2, now())
		ON CONFLICT (worker_id) DO UPDATE SET workflows = EXCLUDED.workflows, last_seen = now()`, workerID, workflows); err != nil {
		return err
	}
	for _, d := range descs {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO workflows (workflow_name, workflow_version, input_schema, last_seen)
			VALUES ($1, $2, $3::jsonb, now()) ON CONFLICT (workflow_name, workflow_version)
			DO UPDATE SET input_schema = coalesce(EXCLUDED.input_schema, workflows.input_schema), last_seen = now()`,
			d.Name, d.Version, nullJSON(d.InputSchema)); err != nil {
			return err
		}
	}
	return nil
}

type ServedWorkflow struct {
	Name     string   `json:"name"`
	Versions []string `json:"versions"`
	Workers  int      `json:"workers"`
}

// ListWorkflows returns workflows and versions served by workers seen in the
// last minute.
func (s *Store) ListWorkflows(ctx context.Context) ([]ServedWorkflow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT unnest(workflows) AS wf, count(*) FROM workers
		WHERE last_seen > now() - interval '60 seconds' GROUP BY wf`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byName := map[string]*ServedWorkflow{}
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			return nil, err
		}
		name, version, _ := strings.Cut(key, "@")
		w := byName[name]
		if w == nil {
			w = &ServedWorkflow{Name: name}
			byName[name] = w
		}
		w.Versions = append(w.Versions, version)
		if n > w.Workers {
			w.Workers = n
		}
	}
	out := make([]ServedWorkflow, 0, len(byName))
	for _, w := range byName {
		sort.Slice(w.Versions, func(i, j int) bool { return versionLess(w.Versions[i], w.Versions[j]) })
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, rows.Err()
}

func versionLess(a, b string) bool {
	ai, errA := strconv.Atoi(a)
	bi, errB := strconv.Atoi(b)
	if errA == nil && errB == nil {
		return ai < bi
	}
	return a < b
}

func (s *Store) latestVersionTx(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, workflow string) (string, error) {
	rows, err := q.Query(ctx, `SELECT workflow_version FROM workflows WHERE workflow_name = $1
		ORDER BY last_seen DESC LIMIT 50`, workflow)
	if err != nil {
		return "", err
	}
	vs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return "", err
	}
	if len(vs) == 0 {
		return "", fmt.Errorf("%w: unknown workflow %q", ErrInvalid, workflow)
	}
	sort.Slice(vs, func(i, j int) bool { return versionLess(vs[i], vs[j]) })
	return vs[len(vs)-1], nil
}

// LatestVersion picks the newest version any worker has announced.
func (s *Store) LatestVersion(ctx context.Context, workflow string) (string, error) {
	return s.latestVersionTx(ctx, s.Pool, workflow)
}

func (s *Store) InputSchema(ctx context.Context, workflow, version string) (json.RawMessage, error) {
	var b []byte
	err := s.Pool.QueryRow(ctx, `SELECT input_schema FROM workflows WHERE workflow_name = $1 AND workflow_version = $2`,
		workflow, version).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return b, err
}

// ---------------------------------------------------------------- policy facts

type RunFacts struct {
	TenantID   string
	Status     string
	Epoch      int64
	Tainted    bool
	SpentUSD   float64
	BudgetUSD  *float64
	Input      json.RawMessage
	Facts      json.RawMessage
	ToolCounts map[string]int64
	DryRun     bool
}

// Facts loads what the policy engine knows about a run: taint, spend, input,
// workflow-declared facts and how many times each tool already ran (other
// than the step being decided).
func (s *Store) Facts(ctx context.Context, runID, stepID string) (*RunFacts, error) {
	f := RunFacts{ToolCounts: map[string]int64{}}
	var month time.Time
	err := s.Pool.QueryRow(ctx, `SELECT tenant_id, status, lease_epoch, tainted, spent_usd::float8, budget_usd::float8,
		input, facts, dry_run, date_trunc('month', created_at)::date FROM runs WHERE run_id = $1::uuid`, runID).
		Scan(&f.TenantID, &f.Status, &f.Epoch, &f.Tainted, &f.SpentUSD, &f.BudgetUSD, (*[]byte)(&f.Input),
			(*[]byte)(&f.Facts), &f.DryRun, &month)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT payload->>'tool', count(DISTINCT step_id) FROM events
		WHERE run_id = $1::uuid AND run_month = $2::date AND type = 'StepStarted' AND payload->>'kind' = 'tool'
		  AND step_id IS DISTINCT FROM $3 GROUP BY 1`, runID, month, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var tool *string
		var n int64
		if err := rows.Scan(&tool, &n); err != nil {
			return nil, err
		}
		if tool != nil {
			f.ToolCounts[*tool] = n
		}
	}
	return &f, rows.Err()
}

// ---------------------------------------------------------------- tenants and keys

type Tenant struct {
	TenantID       string   `json:"tenant_id"`
	Weight         int      `json:"weight"`
	MaxRunning     *int     `json:"max_running,omitempty"`
	MaxPending     int      `json:"max_pending"`
	DailyBudgetUSD *float64 `json:"daily_budget_usd,omitempty"`
	AllowedModels  []string `json:"allowed_models,omitempty"`
	Running        int      `json:"running"`
	Pending        int      `json:"pending"`
}

func (s *Store) UpsertTenant(ctx context.Context, t Tenant) error {
	if t.Weight <= 0 {
		t.Weight = 1
	}
	if t.MaxPending <= 0 {
		t.MaxPending = 50000
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO tenants (tenant_id, weight, max_running, max_pending, daily_budget_usd, allowed_models)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tenant_id) DO UPDATE SET weight = EXCLUDED.weight,
		max_running = EXCLUDED.max_running, max_pending = EXCLUDED.max_pending,
		daily_budget_usd = EXCLUDED.daily_budget_usd, allowed_models = EXCLUDED.allowed_models`,
		t.TenantID, t.Weight, t.MaxRunning, t.MaxPending, t.DailyBudgetUSD, t.AllowedModels)
	return err
}

func (s *Store) ListTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.Pool.Query(ctx, `SELECT t.tenant_id, weight, max_running, max_pending, daily_budget_usd::float8,
		allowed_models, coalesce(l.running, 0), coalesce(l.pending, 0)
		FROM tenants t LEFT JOIN tenant_load l USING (tenant_id) ORDER BY t.tenant_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.TenantID, &t.Weight, &t.MaxRunning, &t.MaxPending, &t.DailyBudgetUSD,
			&t.AllowedModels, &t.Running, &t.Pending); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

type APIKey struct {
	TenantID  string
	Principal string
	Scopes    []string
}

func (s *Store) LookupAPIKey(ctx context.Context, keyHash string) (*APIKey, error) {
	var k APIKey
	err := s.Pool.QueryRow(ctx, `SELECT tenant_id, principal, scopes FROM api_keys
		WHERE key_hash = $1 AND revoked_at IS NULL`, keyHash).Scan(&k.TenantID, &k.Principal, &k.Scopes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &k, err
}

func (s *Store) PutAPIKey(ctx context.Context, keyHash string, k APIKey) error {
	if _, err := s.Pool.Exec(ctx, `INSERT INTO tenants (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`, k.TenantID); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO api_keys (key_hash, tenant_id, principal, scopes) VALUES ($1, $2, $3, $4)
		ON CONFLICT (key_hash) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, principal = EXCLUDED.principal,
		scopes = EXCLUDED.scopes, revoked_at = NULL`, keyHash, k.TenantID, k.Principal, k.Scopes)
	return err
}

// ---------------------------------------------------------------- outbox

type OutboxMessage struct {
	ID          int64
	RunID       string
	StepID      string
	Destination string
	Payload     json.RawMessage
	DedupeKey   string
	Attempts    int
}

// ClaimOutbox leases due messages for 30 s so one dispatcher delivers each.
func (s *Store) ClaimOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	var out []OutboxMessage
	err := s.tx(ctx, func(tx pgx.Tx) error {
		out = nil
		rows, err := tx.Query(ctx, `
			WITH due AS (SELECT id FROM outbox WHERE status = 'pending' AND next_attempt_at <= now()
			              ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED)
			UPDATE outbox o SET next_attempt_at = now() + interval '30 seconds', attempts = o.attempts + 1
			  FROM due WHERE o.id = due.id
			RETURNING o.id, coalesce(o.run_id::text,''), coalesce(o.step_id,''), o.destination, o.payload, o.dedupe_key, o.attempts`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var m OutboxMessage
			if err := rows.Scan(&m.ID, &m.RunID, &m.StepID, &m.Destination, (*[]byte)(&m.Payload), &m.DedupeKey, &m.Attempts); err != nil {
				return err
			}
			out = append(out, m)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) MarkOutbox(ctx context.Context, id int64, delivered bool, errMsg string, retryIn time.Duration, dead bool) error {
	switch {
	case delivered:
		_, err := s.Pool.Exec(ctx, `UPDATE outbox SET status = 'delivered', delivered_at = now(), last_error = NULL WHERE id = $1`, id)
		return err
	case dead:
		_, err := s.Pool.Exec(ctx, `UPDATE outbox SET status = 'dead', last_error = $2 WHERE id = $1`, id, errMsg)
		return err
	default:
		_, err := s.Pool.Exec(ctx, `UPDATE outbox SET next_attempt_at = now() + $2 * interval '1 millisecond',
			last_error = $3 WHERE id = $1`, id, retryIn.Milliseconds(), errMsg)
		return err
	}
}

// ---------------------------------------------------------------- gauges and maintenance

type Counts struct {
	Pending, Running, Waiting, ApprovalsPending int64
	TenantSpend                                 map[string]float64
}

func (s *Store) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	if err := s.Pool.QueryRow(ctx, `SELECT coalesce(sum(pending),0), coalesce(sum(running),0) FROM tenant_counters`).
		Scan(&c.Pending, &c.Running); err != nil {
		return c, err
	}
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE status = 'waiting'`).Scan(&c.Waiting); err != nil {
		return c, err
	}
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM approvals WHERE status = 'pending'`).Scan(&c.ApprovalsPending); err != nil {
		return c, err
	}
	c.TenantSpend = map[string]float64{}
	rows, err := s.Pool.Query(ctx, `SELECT tenant_id, spent_usd::float8 FROM tenant_spend WHERE day = current_date`)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		var v float64
		if err := rows.Scan(&t, &v); err != nil {
			return c, err
		}
		c.TenantSpend[t] = v
	}
	return c, rows.Err()
}

// EnsurePartitions creates monthly event partitions for this month and the
// next two. Creating a partition fails if the default partition already holds
// rows for that range; those runs then simply stay in the default partition.
func (s *Store) EnsurePartitions(ctx context.Context, now time.Time) error {
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var errs []string
	for i := 0; i < 3; i++ {
		from := start.AddDate(0, i, 0)
		to := from.AddDate(0, 1, 0)
		name := fmt.Sprintf("events_%04d_%02d", from.Year(), int(from.Month()))
		var exists bool
		if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		_, err := s.Pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')`,
			name, from.Format("2006-01-02"), to.Format("2006-01-02")))
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// DropFinishedPartition drops the event partition of month `month` (and its
// runs) when every run created that month is final. Returns false otherwise.
func (s *Store) DropFinishedPartition(ctx context.Context, month time.Time) (bool, error) {
	from := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 1, 0)
	name := fmt.Sprintf("events_%04d_%02d", from.Year(), int(from.Month()))
	dropped := false
	err := s.tx(ctx, func(tx pgx.Tx) error {
		var open int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE created_at >= $1 AND created_at < $2
			AND status NOT IN ('completed','failed','cancelled')`, from, to).Scan(&open); err != nil {
			return err
		}
		if open > 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name)); err != nil {
			return err
		}
		for _, q := range []string{
			`DELETE FROM signals WHERE run_id IN (SELECT run_id FROM runs WHERE created_at >= $1 AND created_at < $2)`,
			`DELETE FROM approvals WHERE run_id IN (SELECT run_id FROM runs WHERE created_at >= $1 AND created_at < $2)`,
			`UPDATE runs SET parent_run_id = NULL WHERE parent_run_id IN (SELECT run_id FROM runs WHERE created_at >= $1 AND created_at < $2)`,
			`DELETE FROM runs WHERE created_at >= $1 AND created_at < $2`,
		} {
			if _, err := tx.Exec(ctx, q, from, to); err != nil {
				return err
			}
		}
		dropped = true
		return nil
	})
	return dropped, err
}
