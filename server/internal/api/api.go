// Package api is the REST API for clients: start and inspect runs, stream
// their events, signal, cancel and fork them, and decide approvals.
// Errors are {"error": {"code", "message"}}.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/keel-runtime/keel/server/internal/auth"
	"github.com/keel-runtime/keel/server/internal/notify"
	"github.com/keel-runtime/keel/server/internal/pii"
	"github.com/keel-runtime/keel/server/internal/policy"
	"github.com/keel-runtime/keel/server/internal/store"
)

type API struct {
	Store    *store.Store
	Auth     *auth.Authenticator
	Notifier *notify.Notifier
	Policy   *policy.Watcher
	Logger   *slog.Logger
	IsLeader func() bool

	schemas sync.Map // "wf@ver" -> *jsonschema.Schema (nil if none)
}

const maxBody = 4 << 20

func (a *API) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, cors)
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok"})
	})
	r.Get("/ready", a.ready)
	r.Handle("/metrics", promhttp.Handler())
	r.Route("/v1", func(r chi.Router) {
		r.Use(a.authenticate)
		r.With(need(auth.ScopeRunsWrite)).Post("/runs", a.startRun)
		r.With(need(auth.ScopeRunsRead)).Get("/runs", a.listRuns)
		r.With(need(auth.ScopeRunsRead)).Get("/runs/{id}", a.getRun)
		r.With(need(auth.ScopeRunsRead)).Get("/runs/{id}/events", a.events)
		r.With(need(auth.ScopeRunsWrite)).Post("/runs/{id}/signals/{name}", a.signal)
		r.With(need(auth.ScopeRunsWrite)).Post("/runs/{id}/cancel", a.cancel)
		r.With(need(auth.ScopeRunsWrite)).Post("/runs/{id}/fork", a.fork)
		r.With(need(auth.ScopeAdmin)).Post("/runs/{id}/unpark", a.unpark)
		r.With(need(auth.ScopeRunsRead)).Get("/approvals", a.listApprovals)
		r.With(need(auth.ScopeRunsRead)).Get("/approvals/{id}", a.getApproval)
		r.With(need(auth.ScopeApprove)).Post("/approvals/{id}", a.decide)
		r.With(need(auth.ScopeRunsRead)).Get("/workflows", a.workflows)
		r.With(need(auth.ScopeRunsRead)).Get("/policy", a.getPolicy)
		r.With(need(auth.ScopeAdmin)).Get("/tenants", a.listTenants)
		r.With(need(auth.ScopeAdmin)).Put("/tenants/{id}", a.putTenant)
		r.With(need(auth.ScopeAdmin)).Post("/api-keys", a.createKey)
	})
	return otelhttp.NewHandler(r, "keel-api", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		return r.Method + " " + routePattern(r)
	}))
}

func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
		return rc.RoutePattern()
	}
	return r.URL.Path
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, X-Keel-Tenant")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------- plumbing

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func (a *API) storeErr(w http.ResponseWriter, err error) {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, 404, "not_found", "no such resource")
	case errors.Is(err, store.ErrConflict):
		writeErr(w, 409, "conflict", "the resource is in a state that does not allow this")
	case errors.Is(err, store.ErrTooManyPending):
		w.Header().Set("Retry-After", "5")
		writeErr(w, 429, "too_many_pending", err.Error())
	case errors.Is(err, store.ErrInvalid):
		writeErr(w, 400, "invalid_request", err.Error())
	case errors.As(err, &pgErr):
		a.Logger.Error("database error", "err", err)
		writeErr(w, 500, "internal", "database error")
	default:
		a.Logger.Error("database unavailable", "err", err)
		writeErr(w, 503, "unavailable", "database unavailable")
	}
}

func decode(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	return dec.Decode(v)
}

func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := a.Auth.Authenticate(r.Context(), auth.KeyFromRequest(r))
		if errors.Is(err, auth.ErrUnauthorized) {
			writeErr(w, 401, "unauthorized", "missing or invalid API key")
			return
		}
		if err != nil {
			a.storeErr(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
}

func need(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !auth.FromContext(r.Context()).Has(scope) {
				writeErr(w, 403, "forbidden", "API key lacks scope "+scope)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.Store.Pool.Ping(ctx); err != nil {
		writeErr(w, 503, "unavailable", "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ready", "leader": a.IsLeader != nil && a.IsLeader()})
}

func revealPII(r *http.Request) bool {
	return r.URL.Query().Get("reveal_pii") == "true" && auth.FromContext(r.Context()).Has(auth.ScopePII)
}

// ---------------------------------------------------------------- runs

type startReq struct {
	Workflow  string          `json:"workflow"`
	Version   string          `json:"version"`
	Input     json.RawMessage `json:"input"`
	BudgetUSD *float64        `json:"budget_usd"`
	Priority  int             `json:"priority"`
	StartKey  string          `json:"start_key"`
	DryRun    bool            `json:"dry_run"`
	Tenant    string          `json:"tenant"`
}

func (a *API) startRun(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	var req startReq
	if err := decode(r, &req); err != nil {
		writeErr(w, 400, "invalid_json", err.Error())
		return
	}
	if req.Workflow == "" {
		writeErr(w, 400, "invalid_request", "workflow is required")
		return
	}
	if req.Priority < -100 || req.Priority > 100 {
		writeErr(w, 400, "invalid_request", "priority must be in [-100, 100]")
		return
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage(`{}`)
	}
	tenant := p.TenantID
	if p.Admin() && req.Tenant != "" {
		tenant = req.Tenant
	}
	if req.Version == "" {
		v, err := a.Store.LatestVersion(r.Context(), req.Workflow)
		if err != nil {
			writeErr(w, 400, "unknown_workflow", fmt.Sprintf("no worker has announced workflow %q", req.Workflow))
			return
		}
		req.Version = v
	}
	if err := a.validateInput(r.Context(), req.Workflow, req.Version, req.Input); err != nil {
		writeErr(w, 400, "invalid_input", err.Error())
		return
	}
	runID, created, err := a.Store.StartRun(r.Context(), store.StartRunParams{
		TenantID: tenant, Workflow: req.Workflow, Version: req.Version, Input: req.Input,
		BudgetUSD: req.BudgetUSD, Priority: req.Priority, StartKey: req.StartKey, DryRun: req.DryRun,
	})
	if err != nil {
		a.storeErr(w, err)
		return
	}
	code := 201
	if !created {
		code = 200
	}
	writeJSON(w, code, map[string]any{"run_id": runID, "created": created, "workflow": req.Workflow, "version": req.Version})
}

func (a *API) validateInput(ctx context.Context, wf, ver string, input json.RawMessage) error {
	key := wf + "@" + ver
	var sch *jsonschema.Schema
	if v, ok := a.schemas.Load(key); ok {
		sch, _ = v.(*jsonschema.Schema)
	} else {
		raw, err := a.Store.InputSchema(ctx, wf, ver)
		if err != nil {
			return nil // schema lookup failing must not block starts
		}
		if len(raw) > 0 && string(raw) != "null" {
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
			if err == nil {
				c := jsonschema.NewCompiler()
				if c.AddResource(key+".json", doc) == nil {
					sch, _ = c.Compile(key + ".json")
				}
			}
		}
		a.schemas.Store(key, sch)
	}
	if sch == nil {
		return nil
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(input))
	if err != nil {
		return err
	}
	return sch.Validate(inst)
}

type runView struct {
	*store.Run
	DurationMs *int64 `json:"duration_ms,omitempty"`
}

func (a *API) view(r *http.Request, run *store.Run) runView {
	if !revealPII(r) {
		run.Input = pii.MaskJSON(run.Input)
		run.Result = pii.MaskJSON(run.Result)
		run.Error = pii.MaskJSON(run.Error)
	}
	v := runView{Run: run}
	if run.FinishedAt != nil {
		d := run.FinishedAt.Sub(run.CreatedAt).Milliseconds()
		v.DurationMs = &d
	}
	return v
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	run, err := a.Store.GetRun(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id"))
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, a.view(r, run))
}

func parseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return &t
	}
	return nil
}

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	tenant := p.TenantFilter(r)
	if p.Admin() && q.Get("tenant") != "" {
		tenant = q.Get("tenant")
	}
	runs, next, err := a.Store.ListRuns(r.Context(), store.RunFilter{
		Tenant: tenant, Workflow: q.Get("workflow"), Version: q.Get("version"), Status: q.Get("status"),
		CreatedAfter: parseTime(q.Get("from")), CreatedBefore: parseTime(q.Get("to")),
		Cursor: q.Get("cursor"), Limit: limit,
	})
	if err != nil {
		a.storeErr(w, err)
		return
	}
	out := make([]runView, len(runs))
	for i, run := range runs {
		out[i] = a.view(r, run)
	}
	writeJSON(w, 200, map[string]any{"runs": out, "next_cursor": next})
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	runID := chi.URLParam(r, "id")
	after, _ := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	reveal := revealPII(r)
	mask := func(evs []store.Event) []store.Event {
		if !reveal {
			for i := range evs {
				evs[i].Payload = pii.MaskJSON(evs[i].Payload)
			}
		}
		return evs
	}
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		evs, run, err := a.Store.ListEvents(r.Context(), p.TenantFilter(r), runID, after, limit)
		if err != nil {
			a.storeErr(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"run_id": runID, "status": run.Status, "events": mask(evs)})
		return
	}
	// Live stream: events as they are appended, plus LLM tokens while live.
	if _, err := a.Store.GetRun(r.Context(), p.TenantFilter(r), runID); err != nil {
		a.storeErr(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, 500, "internal", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)
	tokens, unsub := a.Notifier.Subscribe(runID)
	defer unsub()
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	send := func(event string, id int64, v any) {
		b, _ := json.Marshal(v)
		if id > 0 {
			fmt.Fprintf(w, "id: %d\n", id)
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	}
	for {
		evs, run, err := a.Store.ListEvents(r.Context(), p.TenantFilter(r), runID, after, 500)
		if err != nil {
			return
		}
		for _, e := range mask(evs) {
			send("event", e.Seq, e)
			after = e.Seq
		}
		if len(evs) == 0 && store.IsFinal(run.Status) {
			send("end", 0, map[string]string{"status": run.Status})
			flusher.Flush()
			return
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case t := <-tokens:
			if !reveal {
				t.Delta = pii.MaskString(t.Delta)
			}
			send("token", 0, t)
			flusher.Flush()
		case <-tick.C:
		}
	}
}

type signalReq struct {
	Payload  json.RawMessage `json:"payload"`
	SignalID string          `json:"signal_id"`
}

func (a *API) signal(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	var req signalReq
	if err := decode(r, &req); err != nil {
		writeErr(w, 400, "invalid_json", err.Error())
		return
	}
	if req.SignalID == "" {
		req.SignalID = randomID()
	}
	dup, err := a.Store.Signal(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id"), chi.URLParam(r, "name"), req.Payload, req.SignalID)
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"accepted": true, "duplicate": dup, "signal_id": req.SignalID})
}

func randomID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	if err := a.Store.Cancel(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id")); err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"cancel_requested": true})
}

func (a *API) unpark(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	if err := a.Store.Unpark(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id")); err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{"requeued": true})
}

type forkReq struct {
	FromSeq   int64           `json:"from_seq"`
	Overrides json.RawMessage `json:"overrides"`
	DryRun    *bool           `json:"dry_run"`
}

func (a *API) fork(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	var req forkReq
	if err := decode(r, &req); err != nil {
		writeErr(w, 400, "invalid_json", err.Error())
		return
	}
	dry := true // forks never touch real systems unless asked explicitly
	if req.DryRun != nil {
		dry = *req.DryRun
	}
	if len(req.Overrides) > 0 && !bytes.HasPrefix(bytes.TrimSpace(req.Overrides), []byte("{")) {
		writeErr(w, 400, "invalid_request", "overrides must be an object")
		return
	}
	id, err := a.Store.Fork(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id"), req.FromSeq, req.Overrides, dry)
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"run_id": id, "dry_run": dry, "forked_from": chi.URLParam(r, "id")})
}

// ---------------------------------------------------------------- approvals

func (a *API) listApprovals(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := a.Store.ListApprovals(r.Context(), p.TenantFilter(r), r.URL.Query().Get("status"), limit)
	if err != nil {
		a.storeErr(w, err)
		return
	}
	if !revealPII(r) {
		for _, ap := range list {
			ap.Action = pii.MaskJSON(ap.Action)
			ap.Summary = pii.MaskJSON(ap.Summary)
		}
	}
	if list == nil {
		list = []*store.Approval{}
	}
	writeJSON(w, 200, map[string]any{"approvals": list})
}

func (a *API) getApproval(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	ap, err := a.Store.GetApproval(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id"))
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, ap)
}

type decideReq struct {
	Decision   string          `json:"decision"`
	EditedArgs json.RawMessage `json:"edited_args"`
	Comment    string          `json:"comment"`
}

func (a *API) decide(w http.ResponseWriter, r *http.Request) {
	p := auth.FromContext(r.Context())
	var req decideReq
	if err := decode(r, &req); err != nil {
		writeErr(w, 400, "invalid_json", err.Error())
		return
	}
	switch req.Decision {
	case "approve", "approved":
		req.Decision = "approved"
	case "reject", "rejected":
		req.Decision = "rejected"
	default:
		writeErr(w, 400, "invalid_request", "decision must be approve or reject")
		return
	}
	if len(req.EditedArgs) > 0 && string(req.EditedArgs) != "null" &&
		!bytes.HasPrefix(bytes.TrimSpace(req.EditedArgs), []byte("{")) {
		writeErr(w, 400, "invalid_request", "edited_args must be an object")
		return
	}
	if string(req.EditedArgs) == "null" {
		req.EditedArgs = nil
	}
	if _, err := uuid.Parse(chi.URLParam(r, "id")); err != nil {
		writeErr(w, 404, "not_found", "no such resource")
		return
	}
	ap, changed, err := a.Store.DecideApproval(r.Context(), p.TenantFilter(r), chi.URLParam(r, "id"),
		req.Decision, req.EditedArgs, p.TenantID+"/"+p.Name, req.Comment)
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"approval": ap, "changed": changed})
}

// ---------------------------------------------------------------- catalog and admin

func (a *API) workflows(w http.ResponseWriter, r *http.Request) {
	wfs, err := a.Store.ListWorkflows(r.Context())
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"workflows": wfs})
}

func (a *API) getPolicy(w http.ResponseWriter, _ *http.Request) {
	e := a.Policy.Engine()
	writeJSON(w, 200, map[string]any{"version": e.Version, "rules": e.Rules})
}

func (a *API) listTenants(w http.ResponseWriter, r *http.Request) {
	ts, err := a.Store.ListTenants(r.Context())
	if err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"tenants": ts})
}

func (a *API) putTenant(w http.ResponseWriter, r *http.Request) {
	var t store.Tenant
	if err := decode(r, &t); err != nil {
		writeErr(w, 400, "invalid_json", err.Error())
		return
	}
	t.TenantID = chi.URLParam(r, "id")
	if err := a.Store.UpsertTenant(r.Context(), t); err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 200, t)
}

type keyReq struct {
	Tenant    string   `json:"tenant"`
	Principal string   `json:"principal"`
	Scopes    []string `json:"scopes"`
}

func (a *API) createKey(w http.ResponseWriter, r *http.Request) {
	var req keyReq
	if err := decode(r, &req); err != nil || req.Tenant == "" || req.Principal == "" || len(req.Scopes) == 0 {
		writeErr(w, 400, "invalid_request", "tenant, principal and scopes are required")
		return
	}
	key := "keel_" + randomID() + randomID()
	if err := a.Store.PutAPIKey(r.Context(), auth.HashKey(key), store.APIKey{TenantID: req.Tenant,
		Principal: req.Principal, Scopes: req.Scopes}); err != nil {
		a.storeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"api_key": key, "tenant": req.Tenant, "principal": req.Principal, "scopes": req.Scopes})
}
