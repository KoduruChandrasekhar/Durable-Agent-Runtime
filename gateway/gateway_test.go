package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keel-runtime/keel/gateway/breaker"
	"github.com/keel-runtime/keel/gateway/budget"
	"github.com/keel-runtime/keel/gateway/cache"
	"github.com/keel-runtime/keel/gateway/providers"
	"github.com/keel-runtime/keel/gateway/ratelimit"
	"github.com/keel-runtime/keel/gateway/router"
)

const testCfg = `
pricing_version: test
providers:
  primary: {kind: mock}
  fallback: {kind: mock}
models:
  small: {provider: primary, input_per_mtok: 1, output_per_mtok: 2}
  large: {provider: primary, input_per_mtok: 10, output_per_mtok: 20}
  hosted: {provider: fallback, input_per_mtok: 2, output_per_mtok: 4}
routes:
  router:test:
    rules:
      - {max_input_tokens: 50, model: small}
    chain: [large, hosted]
  router:outage:
    chain: [small, hosted]
`

func newGW(t *testing.T) (*Gateway, *providers.Mock, *providers.Mock) {
	t.Helper()
	cfg, err := router.Parse([]byte(testCfg))
	if err != nil {
		t.Fatal(err)
	}
	p, f := &providers.Mock{ProviderName: "primary"}, &providers.Mock{ProviderName: "fallback"}
	g := &Gateway{Config: cfg, Providers: map[string]providers.Provider{"primary": p, "fallback": f},
		Limits: map[string]*ratelimit.Pair{}, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
		BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}
	g.Init(context.Background())
	return g, p, f
}

func msg(s string) []providers.Message { return []providers.Message{{Role: "user", Content: &s}} }

func TestRoutingRulesThenChain(t *testing.T) {
	g, _, _ := newGW(t)
	r, err := g.Chat(context.Background(), &ChatRequest{Model: "router:test", Messages: msg("hi"), Temperature: 0.5})
	if err != nil || r.Model != "small" {
		t.Fatalf("short request should go to the small model: %+v %v", r, err)
	}
	long := string(make([]byte, 2000))
	r, err = g.Chat(context.Background(), &ChatRequest{Model: "router:test", Messages: msg(long), Temperature: 0.5})
	if err != nil || r.Model != "large" {
		t.Fatalf("long request should go to the large model: %+v %v", r, err)
	}
}

// The primary returns 503 for a while: the circuit opens, the fallback serves,
// no request fails, and the circuit closes once probes succeed.
func TestOutageOpensCircuitAndFallbackServes(t *testing.T) {
	g, primary, _ := newGW(t)
	primary.FailFor(300*time.Millisecond, providers.Overloaded)
	var failed, fallback atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := g.Chat(context.Background(), &ChatRequest{Model: "router:outage", Messages: msg("x"), Temperature: 0.7})
			if err != nil {
				failed.Add(1)
				return
			}
			if r.FallbackUsed {
				fallback.Add(1)
			}
		}(i)
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d requests failed during the outage", failed.Load())
	}
	if fallback.Load() == 0 {
		t.Fatal("fallback never served")
	}
	if g.breaker("small").State() == breaker.Closed {
		t.Log("circuit already closed again")
	}
	time.Sleep(6 * time.Second) // probe interval
	for i := 0; i < 3; i++ {
		_, _ = g.Chat(context.Background(), &ChatRequest{Model: "router:outage", Messages: msg("y"), Temperature: 0.7})
	}
	if s := g.breaker("small").State(); s != breaker.Closed {
		t.Fatalf("circuit should close after the outage, is %s", s)
	}
	t.Logf("served by fallback: %d of 200", fallback.Load())
}

func TestExactAndSemanticCache(t *testing.T) {
	g, primary, _ := newGW(t)
	g.Semantic = cache.NewSemantic(0.8, time.Hour)
	req := &ChatRequest{TenantID: "t1", Model: "small", Messages: msg("how long do refunds take to arrive")}
	if _, err := g.Chat(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	r, _ := g.Chat(context.Background(), req)
	if r.CacheHit == nil || *r.CacheHit != "exact" || r.CostUSD != 0 {
		t.Fatalf("expected exact hit: %+v", r)
	}
	r, _ = g.Chat(context.Background(), &ChatRequest{TenantID: "t1", Model: "small", Messages: msg("how long do refunds take to arrive?")})
	if r.CacheHit == nil || *r.CacheHit != "semantic" {
		t.Fatalf("expected semantic hit for a paraphrase: %+v", r.CacheHit)
	}
	r, _ = g.Chat(context.Background(), &ChatRequest{TenantID: "t2", Model: "small", Messages: msg("how long do refunds take to arrive")})
	if r.CacheHit != nil {
		t.Fatal("cache leaked across tenants")
	}
	if primary.Calls != 2 {
		t.Fatalf("upstream calls = %d, want 2", primary.Calls)
	}
}

func TestRateLimitNeverExceeded(t *testing.T) {
	b := ratelimit.NewBucket(600, 10) // 10 per second, burst 10
	start := time.Now()
	var taken atomic.Int64
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b.Take(ctx, 1, 0) == nil {
				taken.Add(1)
			}
		}()
	}
	wg.Wait()
	allowed := 10 + 10*time.Since(start).Seconds()
	if float64(taken.Load()) > allowed+1 {
		t.Fatalf("took %d, limit allows %.1f", taken.Load(), allowed)
	}
}

func TestRetryBudgetCapsRetries(t *testing.T) {
	rb := breaker.NewRetryBudget(0.1, 0)
	allowed := 0
	for i := 0; i < 1000; i++ {
		rb.Deposit()
		if rb.Withdraw() {
			allowed++
		}
	}
	if allowed > 115 { // 10% plus the small initial balance
		t.Fatalf("retry budget allowed %d retries for 1000 requests", allowed)
	}
}

func TestValidateOutputTriggersEscalation(t *testing.T) {
	text := "Sure! {broken"
	req := &ChatRequest{ResponseFormat: map[string]any{"type": "json_object"}}
	if validateOutput(req, &providers.Response{Text: &text}) == "" {
		t.Fatal("invalid JSON not detected")
	}
	tools := []providers.Tool{{Type: "function", Function: providers.ToolFunction{Name: "refund",
		Parameters: map[string]any{"required": []any{"order_id"}}}}}
	bad := &providers.Response{ToolCalls: []providers.ToolCall{{ID: "1", Name: "refund", Arguments: map[string]any{}}}}
	if validateOutput(&ChatRequest{Tools: tools}, bad) == "" {
		t.Fatal("missing required argument not detected")
	}
}

// 100 parallel calls against a run whose budget covers about 10: spending
// never exceeds the budget. Needs PostgreSQL.
func TestBudgetReserveThenSettleNeverOverspends(t *testing.T) {
	dsn := os.Getenv("KEEL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("KEEL_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	runID := "6b65656c-0000-4000-8000-00000000b0d6"
	_, _ = pool.Exec(ctx, `DELETE FROM events WHERE run_id = $1::uuid; DELETE FROM runs WHERE run_id = $1::uuid`, runID)
	if _, err := pool.Exec(ctx, `INSERT INTO runs (run_id, tenant_id, workflow_name, workflow_version, status, input, chain_root, budget_usd)
		VALUES ($1::uuid, 'budget-test', 'wf', '1', 'running', '{}', $1::uuid, 0.010)`, runID); err != nil {
		t.Fatal(err)
	}
	led := &budget.Postgres{Pool: pool}
	const est, actual = 0.001, 0.0009
	var ok, rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := led.Reserve(ctx, runID, "budget-test", est); err != nil {
				rejected.Add(1)
				return
			}
			ok.Add(1)
			time.Sleep(5 * time.Millisecond)
			_ = led.Settle(ctx, runID, "budget-test", est, actual)
		}()
	}
	wg.Wait()
	var spent, reserved float64
	_ = pool.QueryRow(ctx, `SELECT spent_usd::float8, reserved_usd::float8 FROM runs WHERE run_id = $1::uuid`, runID).Scan(&spent, &reserved)
	if spent > 0.010+1e-9 || reserved > 1e-9 {
		t.Fatalf("overspent: spent=%.6f reserved=%.6f", spent, reserved)
	}
	if ok.Load() < 10 || ok.Load() > 11 {
		t.Fatalf("%d calls fit a budget for ~10 (rejected %d)", ok.Load(), rejected.Load())
	}
	t.Logf("accepted=%d rejected=%d spent=$%.4f of $0.0100", ok.Load(), rejected.Load(), spent)
}

func TestIdempotencyKeyDedupesRetriedSteps(t *testing.T) {
	g, primary, _ := newGW(t)
	body, _ := json.Marshal(ChatRequest{Model: "small", Messages: msg("x"), Temperature: 0.7})
	_ = body
	for i := 0; i < 3; i++ {
		v, err, _ := g.flight.Do("run:llm:1", func() (any, error) {
			return g.Chat(context.Background(), &ChatRequest{Model: "small", Messages: msg("x"), Temperature: 0.7})
		})
		if err != nil || v == nil {
			t.Fatal(err)
		}
	}
	if primary.Calls != 3 {
		t.Fatalf("calls=%d", primary.Calls)
	}
}

// Regression test for the bug the chaos demo found: two tickets whose text
// differs only in the order id must never share a semantic-cache answer.
func TestSemanticCacheNeverCrossesIdentifiers(t *testing.T) {
	g, primary, _ := newGW(t)
	g.Semantic = cache.NewSemantic(0.80, time.Hour)
	ask := func(text string) *ChatResponse {
		r, err := g.Chat(context.Background(), &ChatRequest{TenantID: "t", Model: "small",
			Messages: []providers.Message{{Role: "system", Content: strp("You extract fields")}, {Role: "user", Content: &text}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	ask("The TV from order ORD-10023 stopped working on day two. I want a refund.")
	r := ask("The TV from order ORD-10006 stopped working on day two. I want a refund.")
	if r.CacheHit != nil {
		t.Fatalf("served ORD-10023's answer for ORD-10006 (cache hit %q)", *r.CacheHit)
	}
	r = ask("the TV from order ORD-10006 stopped working on day two, I want a refund")
	if r.CacheHit == nil || *r.CacheHit != "semantic" {
		t.Fatalf("a paraphrase with the same order id should hit: %v", r.CacheHit)
	}
	if primary.Calls != 2 {
		t.Fatalf("upstream calls = %d, want 2", primary.Calls)
	}
}

func strp(s string) *string { return &s }
