// Package policy evaluates guardrail rules before every tool call. Rule
// conditions are CEL expressions: safe to evaluate, no loops, no side effects.
//
// A decision is allow, deny (with a reason) or require_approval (with
// approvers and an expiry). Deny wins over require_approval; a rule that
// errors while evaluating fails closed (it is treated as matched).
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/common/types/traits"
	"gopkg.in/yaml.v3"
)

const (
	Allow           = "allow"
	Deny            = "deny"
	RequireApproval = "require_approval"
)

type Rule struct {
	ID        string   `yaml:"id" json:"id"`
	Tool      string   `yaml:"tool,omitempty" json:"tool,omitempty"`
	ToolRisk  string   `yaml:"tool_risk,omitempty" json:"tool_risk,omitempty"`
	Tenants   []string `yaml:"tenants,omitempty" json:"tenants,omitempty"`
	When      string   `yaml:"when,omitempty" json:"when,omitempty"`
	Action    string   `yaml:"action" json:"action"`
	Reason    string   `yaml:"reason,omitempty" json:"reason,omitempty"`
	Approvers []string `yaml:"approvers,omitempty" json:"approvers,omitempty"`
	ExpiresIn string   `yaml:"expires_in,omitempty" json:"expires_in,omitempty"`
	OnExpiry  string   `yaml:"on_expiry,omitempty" json:"on_expiry,omitempty"`

	prg     cel.Program
	expires time.Duration
}

type Input struct {
	Tool          string
	ToolRisk      string
	Tenant        string
	Args          map[string]any
	Run           map[string]any // tainted, spent_usd, budget_usd, input, facts, tool_counts, dry_run
	ApprovedRules []string
}

type Decision struct {
	Decision      string        `json:"decision"`
	Reason        string        `json:"reason"`
	RuleIDs       []string      `json:"rule_ids"`
	Approvers     []string      `json:"approvers,omitempty"`
	ExpiresIn     time.Duration `json:"-"`
	OnExpiry      string        `json:"on_expiry,omitempty"`
	PolicyVersion string        `json:"policy_version"`
}

type Engine struct {
	Rules   []*Rule
	Version string
}

func newEnv() (*cel.Env, error) {
	mapType := cel.MapType(cel.StringType, cel.DynType)
	return cel.NewEnv(
		cel.Variable("args", mapType),
		cel.Variable("run", mapType),
		cel.Variable("tool", mapType),
		cel.Variable("tenant", cel.StringType),
		// run.count('issue_refund'): how many times that tool already ran in this run.
		cel.Function("count",
			cel.MemberOverload("map_count_string", []*cel.Type{mapType, cel.StringType}, cel.IntType,
				cel.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					m, ok := lhs.(traits.Mapper)
					if !ok {
						return types.IntZero
					}
					counts, found := m.Find(types.String("tool_counts"))
					if !found {
						return types.IntZero
					}
					cm, ok := counts.(traits.Mapper)
					if !ok {
						return types.IntZero
					}
					v, found := cm.Find(rhs)
					if !found {
						return types.IntZero
					}
					switch n := v.(type) {
					case types.Int:
						return n
					case types.Double:
						return types.Int(int64(n))
					case types.Uint:
						return types.Int(int64(n))
					}
					return types.IntZero
				}))),
	)
}

// ParseDuration accepts Go durations plus a "d" (days) suffix.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if d, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(d, 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}
	return time.ParseDuration(s)
}

// Parse compiles a YAML policy: either a list of rules or {rules: [...]}.
func Parse(data []byte) (*Engine, error) {
	var rules []*Rule
	if err := yaml.Unmarshal(data, &rules); err != nil {
		var doc struct {
			Rules []*Rule `yaml:"rules"`
		}
		if err2 := yaml.Unmarshal(data, &doc); err2 != nil {
			return nil, fmt.Errorf("parse policy: %w", err)
		}
		rules = doc.Rules
	}
	env, err := newEnv()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i, r := range rules {
		if r.ID == "" {
			return nil, fmt.Errorf("rule %d: missing id", i)
		}
		if seen[r.ID] {
			return nil, fmt.Errorf("rule %s: duplicate id", r.ID)
		}
		seen[r.ID] = true
		switch r.Action {
		case Allow, Deny, RequireApproval:
		default:
			return nil, fmt.Errorf("rule %s: bad action %q", r.ID, r.Action)
		}
		if r.OnExpiry == "" {
			r.OnExpiry = "reject"
		}
		if r.OnExpiry != "reject" && r.OnExpiry != "escalate" && r.OnExpiry != "approve" {
			return nil, fmt.Errorf("rule %s: bad on_expiry %q", r.ID, r.OnExpiry)
		}
		d, err := ParseDuration(r.ExpiresIn)
		if err != nil {
			return nil, fmt.Errorf("rule %s: expires_in: %w", r.ID, err)
		}
		r.expires = d
		expr := r.When
		if strings.TrimSpace(expr) == "" {
			expr = "true"
		}
		ast, iss := env.Compile(expr)
		if iss != nil && iss.Err() != nil {
			return nil, fmt.Errorf("rule %s: %w", r.ID, iss.Err())
		}
		if ot := ast.OutputType().String(); ot != "bool" && ot != "dyn" {
			return nil, fmt.Errorf("rule %s: condition must be boolean, got %s", r.ID, ast.OutputType())
		}
		r.prg, err = env.Program(ast, cel.EvalOptions(cel.OptOptimize), cel.CostLimit(100000))
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", r.ID, err)
		}
	}
	sum := sha256.Sum256(data)
	return &Engine{Rules: rules, Version: hex.EncodeToString(sum[:])[:12]}, nil
}

func (r *Rule) applies(in Input) bool {
	if r.Tool != "" && r.Tool != "*" && r.Tool != in.Tool {
		return false
	}
	if r.ToolRisk != "" && r.ToolRisk != in.ToolRisk {
		return false
	}
	if len(r.Tenants) > 0 {
		ok := false
		for _, t := range r.Tenants {
			if t == in.Tenant {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// normalize turns JSON-decoded values into types CEL handles predictably
// (whole-number floats become ints so `args.amount_inr > 5000` compares ints).
func normalize(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = normalize(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = normalize(x)
		}
		return out
	case float64:
		if t == float64(int64(t)) && t < 1e15 && t > -1e15 {
			return int64(t)
		}
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	}
	return v
}

func (e *Engine) Evaluate(in Input) Decision {
	if in.Args == nil {
		in.Args = map[string]any{}
	}
	if in.Run == nil {
		in.Run = map[string]any{}
	}
	act := map[string]any{
		"args":   normalize(in.Args),
		"run":    normalize(in.Run),
		"tool":   map[string]any{"name": in.Tool, "risk": in.ToolRisk},
		"tenant": in.Tenant,
	}
	approved := map[string]bool{}
	for _, id := range in.ApprovedRules {
		approved[id] = true
	}
	var denies, approvals, allows []*Rule
	var reasons []string
	for _, r := range e.Rules {
		if !r.applies(in) {
			continue
		}
		matched := true
		errNote := ""
		out, _, err := r.prg.Eval(act)
		if err != nil {
			errNote = " (condition error: " + err.Error() + "; failing closed)"
		} else if b, ok := out.Value().(bool); ok {
			matched = b
		} else {
			errNote = " (condition not boolean; failing closed)"
		}
		if !matched {
			continue
		}
		switch r.Action {
		case Deny:
			denies = append(denies, r)
			reasons = append(reasons, ruleReason(r)+errNote)
		case RequireApproval:
			if approved[r.ID] && errNote == "" {
				continue // a human already approved this rule for this call
			}
			approvals = append(approvals, r)
		case Allow:
			allows = append(allows, r)
		}
	}
	d := Decision{PolicyVersion: e.Version}
	switch {
	case len(denies) > 0:
		d.Decision = Deny
		d.RuleIDs = ids(denies)
		d.Reason = strings.Join(reasons, "; ")
	case len(approvals) > 0:
		d.Decision = RequireApproval
		d.RuleIDs = ids(approvals)
		set := map[string]bool{}
		var rs []string
		for _, r := range approvals {
			rs = append(rs, ruleReason(r))
			for _, a := range r.Approvers {
				if !set[a] {
					set[a] = true
					d.Approvers = append(d.Approvers, a)
				}
			}
			if r.expires > 0 && (d.ExpiresIn == 0 || r.expires < d.ExpiresIn) {
				d.ExpiresIn = r.expires
			}
			if d.OnExpiry == "" {
				d.OnExpiry = r.OnExpiry
			}
		}
		sort.Strings(d.Approvers)
		d.Reason = strings.Join(rs, "; ")
	default:
		d.Decision = Allow
		d.RuleIDs = ids(allows)
		d.Reason = "no rule blocked the call"
	}
	return d
}

func ruleReason(r *Rule) string {
	if r.Reason != "" {
		return r.ID + ": " + r.Reason
	}
	if r.When != "" {
		return r.ID + ": " + r.When
	}
	return r.ID
}

func ids(rs []*Rule) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

// Watcher reloads the policy file when it changes. Decisions are journaled by
// workers, so a reload never changes how an existing run replays.
type Watcher struct {
	path    string
	mu      sync.RWMutex
	engine  *Engine
	modTime time.Time
}

func NewWatcher(path string) (*Watcher, error) {
	w := &Watcher{path: path}
	if path == "" {
		e, _ := Parse([]byte("[]"))
		w.engine = e
		return w, nil
	}
	if err := w.reload(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Watcher) reload() error {
	st, err := os.Stat(w.path)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(w.path)
	if err != nil {
		return err
	}
	e, err := Parse(data)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.engine, w.modTime = e, st.ModTime()
	w.mu.Unlock()
	return nil
}

// Poll reloads when the file's modification time changed. A file that fails
// to parse keeps the previous rules in force.
func (w *Watcher) Poll() error {
	if w.path == "" {
		return nil
	}
	st, err := os.Stat(w.path)
	if err != nil {
		return err
	}
	w.mu.RLock()
	same := st.ModTime().Equal(w.modTime)
	w.mu.RUnlock()
	if same {
		return nil
	}
	return w.reload()
}

func (w *Watcher) Engine() *Engine {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.engine
}
