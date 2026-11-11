package policy

import (
	"os"
	"testing"
	"time"
)

func load(t *testing.T) *Engine {
	t.Helper()
	data, err := os.ReadFile("../../../deploy/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	e, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func run(extra map[string]any) map[string]any {
	r := map[string]any{"tainted": false, "tool_counts": map[string]any{}, "facts": map[string]any{},
		"input": map[string]any{"customer_email": "a@x.com"}}
	for k, v := range extra {
		r[k] = v
	}
	return r
}

func TestPlanRules(t *testing.T) {
	e := load(t)
	cases := []struct {
		name, tool, risk string
		args             map[string]any
		run              map[string]any
		approved         []string
		want             string
	}{
		{"big refund", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 6000.0}, run(nil), nil, RequireApproval},
		{"small refund", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 600.0}, run(nil), nil, Allow},
		{"second refund", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 600.0},
			run(map[string]any{"tool_counts": map[string]any{"issue_refund": 1}}), nil, Deny},
		{"tainted", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 10.0}, run(map[string]any{"tainted": true}), nil, RequireApproval},
		{"above order total", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 900.0},
			run(map[string]any{"facts": map[string]any{"order_total_inr": 800.0}}), nil, Deny},
		{"ineligible", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 10.0},
			run(map[string]any{"facts": map[string]any{"eligible": false}}), nil, Deny},
		{"email attacker", "send_email", "medium", map[string]any{"to": "evil@x.com"}, run(nil), nil, Deny},
		{"email customer", "send_email", "medium", map[string]any{"to": "a@x.com"}, run(nil), nil, Allow},
		{"approved rule lifted", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 6000.0}, run(nil),
			[]string{"big-refund-needs-human"}, Allow},
		{"approval does not lift deny", "issue_refund", "high", map[string]any{"order_id": "O", "amount_inr": 6000.0},
			run(map[string]any{"tool_counts": map[string]any{"issue_refund": 2}}), []string{"big-refund-needs-human"}, Deny},
	}
	for _, c := range cases {
		d := e.Evaluate(Input{Tool: c.tool, ToolRisk: c.risk, Tenant: "t", Args: c.args, Run: c.run, ApprovedRules: c.approved})
		if d.Decision != c.want {
			t.Errorf("%s: got %s (%s), want %s", c.name, d.Decision, d.Reason, c.want)
		}
	}
	d := e.Evaluate(Input{Tool: "issue_refund", ToolRisk: "high", Args: map[string]any{"order_id": "O", "amount_inr": 6000.0}, Run: run(nil)})
	if d.ExpiresIn != 48*time.Hour || d.OnExpiry != "reject" || len(d.Approvers) != 1 {
		t.Fatalf("approval details: %+v", d)
	}
}

func TestFailsClosed(t *testing.T) {
	e, err := Parse([]byte(`- {id: r, tool: t, when: "args.missing > 5", action: deny}`))
	if err != nil {
		t.Fatal(err)
	}
	if d := e.Evaluate(Input{Tool: "t", Args: map[string]any{}}); d.Decision != Deny {
		t.Fatalf("expected fail-closed deny, got %+v", d)
	}
}

func TestRejectsBadRules(t *testing.T) {
	for _, bad := range []string{
		`- {id: a, action: maybe}`,
		`- {action: deny}`,
		`- {id: a, action: deny, when: "1 +"}`,
		`- {id: a, action: deny, when: "1 + 1"}`,
		`- {id: a, action: deny}` + "\n" + `- {id: a, action: allow}`,
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("accepted bad policy %q", bad)
		}
	}
}

// Policy expressions and inputs must never crash the engine.
func FuzzEvaluate(f *testing.F) {
	f.Add("args.amount_inr > 5000", 6000.0, "a@x.com")
	f.Add("run.count('issue_refund') >= 1 && run.tainted", 1.0, "")
	f.Add("has(run.facts.x) && args.to != run.input.customer_email", 0.0, "b@y.com")
	f.Fuzz(func(t *testing.T, expr string, amount float64, to string) {
		e, err := Parse([]byte("- id: r\n  action: deny\n  when: " + quote(expr)))
		if err != nil {
			return
		}
		_ = e.Evaluate(Input{Tool: "x", Args: map[string]any{"amount_inr": amount, "to": to}, Run: run(nil)})
	})
}

func quote(s string) string {
	out := []byte{'"'}
	for _, r := range []byte(s) {
		if r == '"' || r == '\\' {
			out = append(out, '\\')
		}
		if r == '\n' || r == '\r' {
			continue
		}
		out = append(out, r)
	}
	return string(append(out, '"'))
}
