// Package gateway is keel's model gateway: one service every LLM call goes
// through, so retries, fallbacks, rate limits, budgets, caching and routing
// are handled once for all workers. Provider API keys live only here.
package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/singleflight"

	"github.com/keel-runtime/keel/gateway/breaker"
	"github.com/keel-runtime/keel/gateway/budget"
	"github.com/keel-runtime/keel/gateway/cache"
	"github.com/keel-runtime/keel/gateway/providers"
	"github.com/keel-runtime/keel/gateway/ratelimit"
	"github.com/keel-runtime/keel/gateway/router"
)

var (
	mRequests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_gateway_requests_total",
		Help: "Gateway calls by outcome."}, []string{"model", "outcome"})
	mTokens = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_llm_tokens_total",
		Help: "LLM tokens."}, []string{"model", "direction"})
	mCost = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_llm_cost_usd_total",
		Help: "LLM spend."}, []string{"tenant", "model"})
	mCache = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_cache_hits_total",
		Help: "Cache hits by type."}, []string{"type"})
	mRetries = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_gateway_retries_total",
		Help: "Upstream retries."}, []string{"model", "code"})
	mFallbacks = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_gateway_fallbacks_total",
		Help: "Calls served by a fallback model."}, []string{"route", "model"})
	mBreaker = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "keel_circuit_open",
		Help: "1 while the circuit for a model is open."}, []string{"model"})
	mLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "keel_gateway_latency_seconds",
		Help: "End-to-end gateway latency.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 18)}, []string{"model"})
	mOverhead = promauto.NewHistogram(prometheus.HistogramOpts{Name: "keel_gateway_overhead_seconds",
		Help: "Gateway latency minus upstream latency.", Buckets: prometheus.ExponentialBuckets(0.00005, 2, 18)})
	mBudget = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_budget_rejections_total",
		Help: "Calls rejected because the reservation did not fit."}, []string{"tenant"})
	mEscalations = promauto.NewCounter(prometheus.CounterOpts{Name: "keel_gateway_escalations_total",
		Help: "Retries on a larger model after invalid output."})
)

type ChatRequest struct {
	RunID          string              `json:"run_id"`
	TenantID       string              `json:"tenant_id"`
	StepKey        string              `json:"step_key"`
	Model          string              `json:"model"`
	Messages       []providers.Message `json:"messages"`
	Tools          []providers.Tool    `json:"tools"`
	Temperature    float64             `json:"temperature"`
	MaxTokens      int                 `json:"max_tokens"`
	ResponseFormat map[string]any      `json:"response_format"`
	Stream         bool                `json:"stream"`
	Cacheable      *bool               `json:"cacheable"`
	Metadata       map[string]any      `json:"metadata"`
	Priority       int                 `json:"priority"`
}

type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type ChatResponse struct {
	Text           *string              `json:"text"`
	ToolCalls      []providers.ToolCall `json:"tool_calls"`
	Usage          Usage                `json:"usage"`
	CostUSD        float64              `json:"cost_usd"`
	Provider       string               `json:"provider"`
	Model          string               `json:"model"`
	CacheHit       *string              `json:"cache_hit"`
	LatencyMs      float64              `json:"latency_ms"`
	Retries        int                  `json:"retries"`
	FallbackUsed   bool                 `json:"fallback_used"`
	Escalated      bool                 `json:"escalated"`
	FinishReason   string               `json:"finish_reason"`
	PricingVersion string               `json:"pricing_version"`
}

type apiError struct {
	Status     int
	Code       string
	Msg        string
	RetryAfter time.Duration
}

func (e *apiError) Error() string { return e.Code + ": " + e.Msg }

type Gateway struct {
	Config    *router.Config
	Providers map[string]providers.Provider
	Limits    map[string]*ratelimit.Pair
	Budget    budget.Ledger
	DB        *pgxpool.Pool
	Logger    *slog.Logger
	APIKey    string

	Exact    *cache.LRU
	Semantic *cache.Semantic
	Idem     *cache.LRU

	MaxRetries    int
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
	Deadline      time.Duration
	AttemptTimout time.Duration

	retryBudget *breaker.RetryBudget
	breakers    sync.Map // model -> *breaker.Breaker
	flight      singleflight.Group
	allowed     sync.Map // tenant -> allowedEntry
	tokens      *tokenPublisher
}

type allowedEntry struct {
	models map[string]bool // nil = any
	exp    time.Time
}

func (g *Gateway) Init(ctx context.Context) {
	if g.MaxRetries == 0 {
		g.MaxRetries = 3
	}
	if g.BaseBackoff == 0 {
		g.BaseBackoff = 200 * time.Millisecond
	}
	if g.MaxBackoff == 0 {
		g.MaxBackoff = 5 * time.Second
	}
	if g.Deadline == 0 {
		g.Deadline = 90 * time.Second
	}
	if g.AttemptTimout == 0 {
		g.AttemptTimout = 60 * time.Second
	}
	if g.Exact == nil {
		g.Exact = cache.NewLRU(20000, time.Hour)
	}
	if g.Idem == nil {
		g.Idem = cache.NewLRU(50000, 10*time.Minute)
	}
	if g.Semantic == nil && g.Config.SemanticCache.Enabled {
		g.Semantic = cache.NewSemantic(g.Config.SemanticCache.Threshold, time.Hour)
	}
	if g.Budget == nil {
		g.Budget = budget.Noop{}
	}
	g.retryBudget = breaker.NewRetryBudget(0.1, 5)
	if g.DB != nil {
		g.tokens = newTokenPublisher(ctx, g.DB, g.Logger)
	}
}

func (g *Gateway) breaker(model string) *breaker.Breaker {
	if b, ok := g.breakers.Load(model); ok {
		return b.(*breaker.Breaker)
	}
	b := breaker.New(5, 30*time.Second, 5*time.Second)
	b.OnChange = func(s breaker.State) {
		v := 0.0
		if s != breaker.Closed {
			v = 1
		}
		mBreaker.WithLabelValues(model).Set(v)
		g.Logger.Info("circuit state", "model", model, "state", s.String())
	}
	actual, _ := g.breakers.LoadOrStore(model, b)
	return actual.(*breaker.Breaker)
}

func (g *Gateway) Router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat", g.handleChat)
	mux.HandleFunc("GET /v1/models", g.handleModels)
	mux.HandleFunc("POST /v1/admin/breakers/reset", func(w http.ResponseWriter, _ *http.Request) {
		g.breakers.Range(func(k, v any) bool { v.(*breaker.Breaker).Success(); return true })
		w.WriteHeader(204)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.Handle("GET /metrics", promhttp.Handler())
	return otelhttp.NewHandler(mux, "keel-gateway")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, e *apiError) {
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%.1f", e.RetryAfter.Seconds()))
	}
	writeJSON(w, e.Status, map[string]any{"error": map[string]string{"code": e.Code, "message": e.Msg}})
}

func (g *Gateway) handleModels(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{}
	for name, m := range g.Config.Models {
		out[name] = map[string]any{"provider": m.Provider, "upstream": m.Upstream, "input_per_mtok": m.InputPerMTok,
			"output_per_mtok": m.OutputPerMTok, "circuit": g.breaker(name).State().String()}
	}
	writeJSON(w, 200, map[string]any{"models": out, "routes": g.Config.Routes, "pricing_version": g.Config.PricingVersion})
}

func (g *Gateway) handleChat(w http.ResponseWriter, r *http.Request) {
	if g.APIKey != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(g.APIKey)) != 1 {
			writeErr(w, &apiError{Status: 401, Code: "unauthorized", Msg: "bad gateway key"})
			return
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeErr(w, &apiError{Status: 400, Code: "invalid_request", Msg: err.Error()})
		return
	}
	var req ChatRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Model == "" || len(req.Messages) == 0 {
		writeErr(w, &apiError{Status: 400, Code: "invalid_request", Msg: "model and messages are required"})
		return
	}
	if req.TenantID == "" {
		req.TenantID = "default"
	}
	if req.MaxTokens <= 0 {
		req.MaxTokens = 1024
	}
	// A step re-sent after a worker crash (same run:step key, same body)
	// joins the in-flight call or gets its recent result instead of paying
	// for the model again.
	idem := r.Header.Get("Idempotency-Key")
	sum := sha256.Sum256(body)
	idemKey := ""
	if idem != "" {
		idemKey = idem + ":" + hex.EncodeToString(sum[:8])
		if cached, ok := g.Idem.Get(idemKey); ok {
			var resp ChatResponse
			if json.Unmarshal(cached, &resp) == nil {
				hit := "idempotent"
				resp.CacheHit, resp.CostUSD = &hit, 0
				mCache.WithLabelValues("idempotent").Inc()
				writeJSON(w, 200, resp)
				return
			}
		}
	}
	// Detached from the caller's connection: if the worker dies mid-call the
	// answer still lands in the idempotency cache for the worker that resumes
	// the run, instead of being paid for twice. Chat applies its own deadline.
	run := func() (any, error) { return g.Chat(context.WithoutCancel(r.Context()), &req) }
	var v any
	if idemKey != "" {
		v, err, _ = g.flight.Do(idemKey, run)
	} else {
		v, err = run()
	}
	if err != nil {
		var ae *apiError
		if errors.As(err, &ae) {
			writeErr(w, ae)
			return
		}
		writeErr(w, &apiError{Status: 500, Code: "internal", Msg: err.Error()})
		return
	}
	resp := v.(*ChatResponse)
	if idemKey != "" && resp.CacheHit == nil {
		b, _ := json.Marshal(resp)
		g.Idem.Put(idemKey, b)
	}
	writeJSON(w, 200, resp)
}

func estimateTokens(req *ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		if m.Content != nil {
			n += len(*m.Content)/4 + 4
		}
		for _, tc := range m.ToolCalls {
			b, _ := json.Marshal(tc.Arguments)
			n += len(b)/4 + 8
		}
	}
	for _, t := range req.Tools {
		b, _ := json.Marshal(t)
		n += len(b) / 4
	}
	return int(float64(n)*1.1) + 16 // safety margin when no tokenizer is available
}

func (g *Gateway) tenantAllows(ctx context.Context, tenant, model string) bool {
	if g.DB == nil {
		return true
	}
	if e, ok := g.allowed.Load(tenant); ok && time.Now().Before(e.(allowedEntry).exp) {
		ms := e.(allowedEntry).models
		return ms == nil || ms[model]
	}
	var list []string
	_ = g.DB.QueryRow(ctx, `SELECT coalesce(allowed_models, '{}') FROM tenants WHERE tenant_id = $1`, tenant).Scan(&list)
	var ms map[string]bool
	if len(list) > 0 {
		ms = map[string]bool{}
		for _, m := range list {
			ms[m] = true
		}
	}
	g.allowed.Store(tenant, allowedEntry{models: ms, exp: time.Now().Add(30 * time.Second)})
	return ms == nil || ms[model]
}

// prefixMessages is the conversation without its final user message.
func prefixMessages(msgs []providers.Message) []providers.Message {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return append(append([]providers.Message{}, msgs[:i]...), msgs[i+1:]...)
		}
	}
	return msgs
}

func lastUser(msgs []providers.Message) (string, string) {
	sys, last := "", ""
	for _, m := range msgs {
		if m.Content == nil {
			continue
		}
		switch m.Role {
		case "system":
			sys += *m.Content
		case "user":
			last = *m.Content
		}
	}
	return sys, last
}

// Chat runs one normalized call: caches, budget reservation, the fallback
// chain with retries and circuit breakers, escalation, then settlement.
func (g *Gateway) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	t0 := time.Now()
	ctx, span := otel.Tracer("keel-gateway").Start(ctx, "chat "+req.Model)
	defer span.End()
	span.SetAttributes(attribute.String("gen_ai.operation.name", "chat"),
		attribute.String("gen_ai.request.model", req.Model), attribute.String("keel.run_id", req.RunID),
		attribute.String("keel.tenant", req.TenantID), attribute.String("keel.step_id", req.StepKey),
		attribute.Int("gen_ai.request.max_tokens", req.MaxTokens),
		attribute.Float64("gen_ai.request.temperature", req.Temperature))
	ctx, cancel := context.WithTimeout(ctx, g.Deadline)
	defer cancel()

	if !g.tenantAllows(ctx, req.TenantID, req.Model) {
		return nil, &apiError{Status: 403, Code: "model_not_allowed", Msg: "tenant may not use " + req.Model}
	}
	cacheable := req.Temperature == 0
	if req.Cacheable != nil {
		cacheable = *req.Cacheable
	}
	exactKey := ""
	if cacheable {
		exactKey = cache.Key(req.TenantID, req.Model, req.Messages, req.Tools, req.Temperature, req.MaxTokens, req.ResponseFormat)
		if b, ok := g.Exact.Get(exactKey); ok {
			return g.cached(b, "exact", t0, span)
		}
	}
	_, last := lastUser(req.Messages)
	semNS := ""
	// Semantic hits only for tool-free calls, scoped to the whole conversation
	// before the last user message (system prompt, facts, earlier turns).
	if cacheable && g.Semantic != nil && last != "" && len(req.Tools) == 0 {
		semNS = cache.Key(req.TenantID, req.Model, prefixMessages(req.Messages), req.ResponseFormat)
		if b, score, ok := g.Semantic.Lookup(semNS, last); ok {
			span.SetAttributes(attribute.Float64("keel.cache_similarity", score))
			return g.cached(b, "semantic", t0, span)
		}
	}
	inTok := estimateTokens(req)
	quarantined, _ := req.Metadata["quarantined"].(bool)
	plan, err := g.Config.Resolve(req.Model, inTok, len(req.Tools) > 0, quarantined)
	if err != nil {
		return nil, &apiError{Status: 400, Code: "invalid_request", Msg: err.Error()}
	}
	est := plan.MaxCost(inTok, req.MaxTokens)
	if plan.EscalateTo != nil {
		est += plan.EscalateTo.Cost(inTok, req.MaxTokens)
	}
	if err := g.Budget.Reserve(ctx, req.RunID, req.TenantID, est); err != nil {
		if errors.Is(err, budget.ErrExceeded) {
			mBudget.WithLabelValues(req.TenantID).Inc()
			span.SetStatus(codes.Error, "budget exceeded")
			return nil, &apiError{Status: 402, Code: "budget_exceeded",
				Msg: fmt.Sprintf("reserving $%.6f would exceed the run or tenant budget", est)}
		}
		return nil, &apiError{Status: 503, Code: "unavailable", Msg: "budget ledger unavailable: " + err.Error()}
	}
	resp, upstream, callErr := g.callChain(ctx, req, plan, inTok)
	actual := 0.0
	if resp != nil {
		actual = resp.CostUSD
	}
	settleCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
	if err := g.Budget.Settle(settleCtx, req.RunID, req.TenantID, est, actual); err != nil {
		g.Logger.Warn("budget settle failed", "run", req.RunID, "err", err)
	}
	sc()
	if callErr != nil {
		span.RecordError(callErr)
		span.SetStatus(codes.Error, callErr.Error())
		return nil, callErr
	}
	resp.LatencyMs = float64(time.Since(t0).Microseconds()) / 1000
	resp.PricingVersion = g.Config.PricingVersion
	mLatency.WithLabelValues(resp.Model).Observe(time.Since(t0).Seconds())
	mOverhead.Observe(max(0, time.Since(t0).Seconds()-upstream.Seconds()))
	mTokens.WithLabelValues(resp.Model, "input").Add(float64(resp.Usage.InputTokens))
	mTokens.WithLabelValues(resp.Model, "output").Add(float64(resp.Usage.OutputTokens))
	mCost.WithLabelValues(req.TenantID, resp.Model).Add(resp.CostUSD)
	span.SetAttributes(attribute.String("gen_ai.response.model", resp.Model),
		attribute.String("gen_ai.system", resp.Provider),
		attribute.Int("gen_ai.usage.input_tokens", resp.Usage.InputTokens),
		attribute.Int("gen_ai.usage.output_tokens", resp.Usage.OutputTokens),
		attribute.Float64("keel.cost_usd", resp.CostUSD), attribute.Int("keel.retries", resp.Retries),
		attribute.Bool("keel.fallback_used", resp.FallbackUsed), attribute.Bool("keel.escalated", resp.Escalated))
	if b, err := json.Marshal(resp); err == nil {
		if exactKey != "" {
			g.Exact.Put(exactKey, b)
		}
		if semNS != "" && len(resp.ToolCalls) == 0 {
			g.Semantic.Store(semNS, last, b)
		}
	}
	return resp, nil
}

func (g *Gateway) cached(b []byte, kind string, t0 time.Time, span interface {
	SetAttributes(...attribute.KeyValue)
}) (*ChatResponse, error) {
	var resp ChatResponse
	if err := json.Unmarshal(b, &resp); err != nil {
		return nil, err
	}
	resp.CacheHit = &kind
	resp.CostUSD = 0
	resp.Retries = 0
	resp.LatencyMs = float64(time.Since(t0).Microseconds()) / 1000
	mCache.WithLabelValues(kind).Inc()
	span.SetAttributes(attribute.String("keel.cache_hit", kind))
	return &resp, nil
}

func (g *Gateway) backoff(attempt int, retryAfter time.Duration) time.Duration {
	capd := float64(g.BaseBackoff) * float64(int(1)<<min(attempt, 10))
	if capd > float64(g.MaxBackoff) {
		capd = float64(g.MaxBackoff)
	}
	d := time.Duration(rand.Float64() * capd) // full jitter
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

// callChain tries each model of the plan in order, skipping open circuits.
func (g *Gateway) callChain(ctx context.Context, req *ChatRequest, plan *router.Plan, inTok int) (*ChatResponse, time.Duration, error) {
	var lastErr *providers.Error
	retries := 0
	var upstream time.Duration
	g.retryBudget.Deposit()
	for i, m := range plan.Chain {
		br := g.breaker(m.Name)
		if !br.Allow() {
			lastErr = &providers.Error{Code: providers.Overloaded, Message: "circuit open for " + m.Name}
			continue
		}
		for attempt := 0; ; attempt++ {
			res, dur, err := g.callModel(ctx, req, m, inTok)
			upstream += dur
			if err == nil {
				br.Success()
				out := g.respond(res, m, retries, i > 0)
				if i > 0 {
					mFallbacks.WithLabelValues(plan.Route, m.Name).Inc()
				}
				mRequests.WithLabelValues(m.Name, "ok").Inc()
				if bad := validateOutput(req, res); bad != "" && plan.EscalateTo != nil && plan.EscalateTo.Name != m.Name {
					// Small model produced invalid output: retry once on the larger one.
					mEscalations.Inc()
					if res2, d2, err2 := g.callModel(ctx, req, plan.EscalateTo, inTok); err2 == nil {
						upstream += d2
						out2 := g.respond(res2, plan.EscalateTo, retries, i > 0)
						out2.Escalated = true
						out2.CostUSD += out.CostUSD
						out2.Usage.InputTokens += out.Usage.InputTokens
						out2.Usage.OutputTokens += out.Usage.OutputTokens
						return out2, upstream, nil
					}
				}
				return out, upstream, nil
			}
			pe, ok := err.(*providers.Error)
			if !ok {
				pe = &providers.Error{Code: providers.Unavailable, Message: err.Error()}
			}
			lastErr = pe
			mRequests.WithLabelValues(m.Name, pe.Code).Inc()
			if pe.BreakerFailure() {
				br.Failure()
			}
			if !pe.Retryable() {
				return nil, upstream, apiFromProvider(pe)
			}
			if attempt >= g.MaxRetries || br.State() != breaker.Closed || !g.retryBudget.Withdraw() {
				break // next model in the chain
			}
			d := g.backoff(attempt, pe.RetryAfter)
			if dl, ok := ctx.Deadline(); ok && time.Until(dl) < d {
				break
			}
			retries++
			mRetries.WithLabelValues(m.Name, pe.Code).Inc()
			select {
			case <-ctx.Done():
				return nil, upstream, &apiError{Status: 504, Code: "timeout", Msg: "gateway deadline exceeded"}
			case <-time.After(d):
			}
		}
	}
	if lastErr == nil {
		lastErr = &providers.Error{Code: providers.Unavailable, Message: "no model available"}
	}
	return nil, upstream, apiFromProvider(lastErr)
}

func apiFromProvider(e *providers.Error) *apiError {
	status := map[string]int{
		providers.RateLimited: 429, providers.Overloaded: 503, providers.Timeout: 504, providers.Unavailable: 503,
		providers.InvalidRequest: 400, providers.ContextTooLong: 400, providers.ContentFiltered: 422,
		providers.AuthFailed: 502,
	}[e.Code]
	if status == 0 {
		status = 502
	}
	return &apiError{Status: status, Code: e.Code, Msg: e.Message, RetryAfter: e.RetryAfter}
}

func (g *Gateway) callModel(ctx context.Context, req *ChatRequest, m *router.Model, inTok int) (*providers.Response, time.Duration, error) {
	p := g.Providers[m.Provider]
	if p == nil {
		return nil, 0, &providers.Error{Code: providers.Unavailable, Message: "provider not configured: " + m.Provider}
	}
	if lim := g.Limits[m.Provider]; lim != nil {
		if err := lim.Take(ctx, inTok+req.MaxTokens, req.Priority); err != nil {
			return nil, 0, &providers.Error{Code: providers.RateLimited, Message: "queued past the request deadline"}
		}
	}
	actx, cancel := context.WithTimeout(ctx, g.AttemptTimout)
	defer cancel()
	preq := &providers.Request{Model: m.Upstream, Messages: req.Messages, Tools: req.Tools, Temperature: req.Temperature,
		MaxTokens: req.MaxTokens, ResponseFormat: req.ResponseFormat, Stream: req.Stream && g.tokens != nil}
	if preq.Stream && req.RunID != "" {
		preq.OnDelta = func(s string) { g.tokens.publish(req.RunID, req.StepKey, s) }
	}
	t := time.Now()
	res, err := p.Chat(actx, preq)
	return res, time.Since(t), err
}

func (g *Gateway) respond(res *providers.Response, m *router.Model, retries int, fallback bool) *ChatResponse {
	out := &ChatResponse{Text: res.Text, ToolCalls: res.ToolCalls, Provider: m.Provider, Model: m.Name,
		Usage: Usage{InputTokens: res.InputTokens, OutputTokens: res.OutputTokens}, Retries: retries,
		FallbackUsed: fallback, FinishReason: res.FinishReason}
	if out.ToolCalls == nil {
		out.ToolCalls = []providers.ToolCall{}
	}
	out.CostUSD = m.Cost(res.InputTokens, res.OutputTokens)
	return out
}

// validateOutput checks what the caller asked for: JSON when a response
// format was requested, and tool calls naming known tools with parseable
// arguments containing the required fields.
func validateOutput(req *ChatRequest, res *providers.Response) string {
	if req.ResponseFormat != nil && len(res.ToolCalls) == 0 {
		text := ""
		if res.Text != nil {
			text = strings.TrimSpace(*res.Text)
		}
		text = strings.TrimSuffix(strings.TrimPrefix(text, "```json"), "```")
		var v any
		if json.Unmarshal([]byte(strings.TrimSpace(text)), &v) != nil {
			return "response is not valid JSON"
		}
	}
	tools := map[string]providers.Tool{}
	for _, t := range req.Tools {
		tools[t.Function.Name] = t
	}
	for _, tc := range res.ToolCalls {
		t, ok := tools[tc.Name]
		if !ok {
			return "unknown tool " + tc.Name
		}
		if _, bad := tc.Arguments["__invalid_json__"]; bad {
			return "invalid JSON arguments for " + tc.Name
		}
		if req, ok := t.Function.Parameters["required"].([]any); ok {
			for _, r := range req {
				if _, ok := tc.Arguments[fmt.Sprint(r)]; !ok {
					return fmt.Sprintf("%s: missing argument %v", tc.Name, r)
				}
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------- token streaming

var secretRe = regexp.MustCompile(`\b(sk-[A-Za-z0-9_\-]{16,}|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{30,})`)

// Redact removes secrets matching known patterns before anything is logged
// or streamed.
func Redact(s string) string { return secretRe.ReplaceAllString(s, "[REDACTED]") }

type tokenMsg struct {
	RunID  string `json:"run_id"`
	StepID string `json:"step_id"`
	Delta  string `json:"delta"`
}

// tokenPublisher batches streamed deltas every 100 ms into NOTIFY llm_tokens,
// which keel-server forwards to SSE clients watching the run.
type tokenPublisher struct {
	ch chan tokenMsg
}

func newTokenPublisher(ctx context.Context, db *pgxpool.Pool, logger *slog.Logger) *tokenPublisher {
	p := &tokenPublisher{ch: make(chan tokenMsg, 4096)}
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		var batch []tokenMsg
		size := 0
		flush := func() {
			if len(batch) == 0 {
				return
			}
			b, _ := json.Marshal(batch)
			if _, err := db.Exec(ctx, `SELECT pg_notify('llm_tokens', $1)`, string(b)); err != nil {
				logger.Debug("token notify", "err", err)
			}
			batch, size = batch[:0], 0
		}
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-p.ch:
				batch = append(batch, m)
				size += len(m.Delta) + 80
				if size > 6000 {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()
	return p
}

func (p *tokenPublisher) publish(runID, step, delta string) {
	select {
	case p.ch <- tokenMsg{RunID: runID, StepID: step, Delta: Redact(delta)}:
	default:
	}
}

// BuildProviders constructs adapters from config; keys come from the
// environment and never leave the gateway.
func BuildProviders(cfg *router.Config) (map[string]providers.Provider, map[string]*ratelimit.Pair) {
	ps := map[string]providers.Provider{}
	ls := map[string]*ratelimit.Pair{}
	httpc := providers.DefaultHTTP()
	for name, pc := range cfg.Providers {
		key := ""
		if pc.APIKeyEnv != "" {
			key = os.Getenv(pc.APIKeyEnv)
		}
		switch pc.Kind {
		case "openai_compat":
			ps[name] = &providers.OpenAICompat{ProviderName: name, BaseURL: pc.BaseURL, APIKey: key, HTTP: httpc}
		case "anthropic":
			ps[name] = &providers.Anthropic{BaseURL: pc.BaseURL, APIKey: key, HTTP: httpc}
		case "mock":
			ps[name] = &providers.Mock{ProviderName: name}
		}
		pair := &ratelimit.Pair{}
		if pc.RPM > 0 {
			pair.Requests = ratelimit.NewBucket(pc.RPM, 0)
		}
		if pc.TPM > 0 {
			pair.Tokens = ratelimit.NewBucket(pc.TPM, 0)
		}
		ls[name] = pair
	}
	return ps, ls
}
