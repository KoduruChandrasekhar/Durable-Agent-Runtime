// Package workerapi is the gRPC service workers use to claim runs, append
// events, heartbeat, suspend, finish and ask for policy decisions. Every call
// after PollRun carries the lease epoch; a "lease lost" answer tells the
// worker to cancel that run at once.
package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	keelv1 "github.com/keel-runtime/keel/gen/keelv1"
	"github.com/keel-runtime/keel/server/internal/notify"
	"github.com/keel-runtime/keel/server/internal/policy"
	"github.com/keel-runtime/keel/server/internal/store"
)

type Server struct {
	keelv1.UnimplementedWorkerServiceServer
	Store    *store.Store
	Notifier *notify.Notifier
	Policy   *policy.Watcher
	Logger   *slog.Logger

	seen sync.Map // worker id -> last registration time
}

func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrInvalid) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, store.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		// Connection-level failures (database restarting): the worker retries.
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) register(ctx context.Context, req *keelv1.PollRequest) {
	if v, ok := s.seen.Load(req.WorkerId); ok && time.Since(v.(time.Time)) < 5*time.Second {
		return
	}
	descs := make([]store.WorkflowDescriptor, 0, len(req.Descriptors))
	for _, d := range req.Descriptors {
		descs = append(descs, store.WorkflowDescriptor{Name: d.Name, Version: d.Version, InputSchema: d.InputSchema})
	}
	if err := s.Store.UpsertWorker(ctx, req.WorkerId, req.Workflows, descs); err != nil {
		s.Logger.Warn("register worker", "worker", req.WorkerId, "err", err)
		return
	}
	s.seen.Store(req.WorkerId, time.Now())
}

// PollRun long-polls (up to 30 s) for a run this worker can execute.
func (s *Server) PollRun(ctx context.Context, req *keelv1.PollRequest) (*keelv1.RunTask, error) {
	if req.WorkerId == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id required")
	}
	s.register(ctx, req)
	wait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if wait <= 0 || wait > 30*time.Second {
		wait = 30 * time.Second
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		woken := s.Notifier.Wait()
		c, err := s.Store.ClaimRun(ctx, req.WorkerId, req.Workflows)
		if err != nil {
			if ctx.Err() != nil {
				return &keelv1.RunTask{}, nil
			}
			return nil, grpcErr(err)
		}
		if c != nil {
			return toTask(c), nil
		}
		select {
		case <-woken:
		case <-tick.C:
		case <-deadline.C:
			return &keelv1.RunTask{}, nil
		case <-ctx.Done():
			return &keelv1.RunTask{}, nil
		}
	}
}

func toTask(c *store.ClaimedRun) *keelv1.RunTask {
	r := c.Run
	t := &keelv1.RunTask{
		HasTask: true, RunId: r.RunID, Epoch: r.LeaseEpoch, Workflow: r.WorkflowName, Version: r.WorkflowVersion,
		Input: r.Input, TenantId: r.TenantID, CancelRequested: r.CancelRequested,
		ServerNowUnixMs: c.ServerNow.UnixMilli(), LeaseTtlMs: 30000, DryRun: r.DryRun,
		ParentRunId: r.ParentRunID, NextSeq: r.NextSeq, CreatedAtUnixMs: r.CreatedAt.UnixMilli(),
		Overrides: r.Overrides,
	}
	for _, e := range c.History {
		t.History = append(t.History, &keelv1.Event{Seq: e.Seq, Type: e.Type, StepId: e.StepID,
			Payload: e.Payload, Epoch: e.Epoch, CreatedAtUnixMs: e.CreatedAt.UnixMilli()})
	}
	return t
}

func (s *Server) AppendEvents(ctx context.Context, req *keelv1.AppendRequest) (*keelv1.AppendResponse, error) {
	evs := make([]store.NewEvent, len(req.Events))
	for i, e := range req.Events {
		evs[i] = store.NewEvent{Type: e.Type, StepID: e.StepId, Payload: e.Payload}
	}
	res, err := s.Store.Append(ctx, req.RunId, req.Epoch, req.ExpectedSeq, evs)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &keelv1.AppendResponse{Ok: res.OK, NextSeq: res.NextSeq, ErrorCode: res.ErrorCode,
		CancelRequested: res.CancelRequested, ServerNowUnixMs: res.ServerNow.UnixMilli()}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *keelv1.HeartbeatRequest) (*keelv1.HeartbeatResponse, error) {
	ok, cancel, now, err := s.Store.Heartbeat(ctx, req.RunId, req.Epoch)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &keelv1.HeartbeatResponse{Ok: ok, LeaseLost: !ok, CancelRequested: cancel, ServerNowUnixMs: now.UnixMilli()}, nil
}

func (s *Server) Suspend(ctx context.Context, req *keelv1.SuspendRequest) (*keelv1.SuspendResponse, error) {
	p := store.SuspendParams{RunID: req.RunId, Epoch: req.Epoch, Reason: req.Reason}
	if req.AvailableAtUnixMs > 0 {
		p.AvailableAt = time.UnixMilli(req.AvailableAtUnixMs)
	}
	for _, w := range req.Waits {
		p.Waits = append(p.Waits, store.Wait{Kind: w.Kind, ID: w.Id, Name: w.Name, FireAtMs: w.FireAtUnixMs})
	}
	res, err := s.Store.Suspend(ctx, p)
	if err != nil {
		return nil, grpcErr(err)
	}
	return &keelv1.SuspendResponse{Ok: res.OK, LeaseLost: res.LeaseLost, Woken: res.Woken}, nil
}

func (s *Server) Finish(ctx context.Context, req *keelv1.FinishRequest) (*keelv1.Ack, error) {
	res, err := s.Store.Finish(ctx, store.FinishParams{RunID: req.RunId, Epoch: req.Epoch, ExpectedSeq: req.ExpectedSeq,
		Status: req.Status, Result: req.Result, Error: req.Error, ContinueInput: req.ContinueInput})
	if err != nil {
		return nil, grpcErr(err)
	}
	return &keelv1.Ack{Ok: res.OK, LeaseLost: res.LeaseLost, ErrorCode: res.ErrorCode, NewRunId: res.NewRunID}, nil
}

// EvaluatePolicy decides allow / deny / require_approval for a tool call from
// facts the server owns (taint, spend, tool counts, declared facts).
func (s *Server) EvaluatePolicy(ctx context.Context, req *keelv1.PolicyRequest) (*keelv1.PolicyResponse, error) {
	f, err := s.Store.Facts(ctx, req.RunId, req.StepId)
	if errors.Is(err, store.ErrNotFound) {
		return &keelv1.PolicyResponse{LeaseLost: true}, nil
	}
	if err != nil {
		return nil, grpcErr(err)
	}
	if f.Epoch != req.Epoch || f.Status != store.StatusRunning {
		return &keelv1.PolicyResponse{LeaseLost: true}, nil
	}
	var args map[string]any
	if len(req.Args) > 0 {
		if err := json.Unmarshal(req.Args, &args); err != nil {
			return nil, status.Error(codes.InvalidArgument, "args must be a JSON object")
		}
	}
	d := s.Policy.Engine().Evaluate(policy.Input{
		Tool: req.Tool, ToolRisk: req.ToolRisk, Tenant: f.TenantID, Args: args,
		Run: FactsMap(f), ApprovedRules: req.ApprovedRules,
	})
	return &keelv1.PolicyResponse{Decision: d.Decision, Reason: d.Reason, RuleIds: d.RuleIDs,
		Approvers: d.Approvers, ExpiresInMs: d.ExpiresIn.Milliseconds(), OnExpiry: d.OnExpiry,
		PolicyVersion: d.PolicyVersion}, nil
}

// FactsMap is the `run` variable the policy rules see.
func FactsMap(f *store.RunFacts) map[string]any {
	var input, facts any
	_ = json.Unmarshal(f.Input, &input)
	_ = json.Unmarshal(f.Facts, &facts)
	if facts == nil {
		facts = map[string]any{}
	}
	counts := map[string]any{}
	for k, v := range f.ToolCounts {
		counts[k] = v
	}
	m := map[string]any{
		"tainted": f.Tainted, "spent_usd": f.SpentUSD, "input": input, "facts": facts,
		"tool_counts": counts, "dry_run": f.DryRun, "tenant": f.TenantID,
	}
	if f.BudgetUSD != nil {
		m["budget_usd"] = *f.BudgetUSD
	}
	return m
}
