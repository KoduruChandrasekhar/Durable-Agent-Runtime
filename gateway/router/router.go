// Package router loads the gateway configuration (providers, models with
// prices, routes) and resolves a model spec such as "router:support" into a
// fallback chain: rules first (short or simple requests go to a small model),
// then the route's ordered list of models.
package router

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type ProviderConfig struct {
	Kind      string  `yaml:"kind"` // openai_compat | anthropic | mock
	BaseURL   string  `yaml:"base_url"`
	APIKeyEnv string  `yaml:"api_key_env"`
	RPM       float64 `yaml:"rpm"`
	TPM       float64 `yaml:"tpm"`
	TimeoutS  float64 `yaml:"timeout_s"`
}

type Model struct {
	Name          string  `yaml:"-"`
	Provider      string  `yaml:"provider"`
	Upstream      string  `yaml:"upstream"`
	InputPerMTok  float64 `yaml:"input_per_mtok"`
	OutputPerMTok float64 `yaml:"output_per_mtok"`
	Context       int     `yaml:"context"`
	Tier          string  `yaml:"tier"` // small | large (for reports)
}

// Cost in USD for the given token counts.
func (m *Model) Cost(in, out int) float64 {
	return float64(in)*m.InputPerMTok/1e6 + float64(out)*m.OutputPerMTok/1e6
}

type Rule struct {
	MaxInputTokens int    `yaml:"max_input_tokens"`
	NoTools        bool   `yaml:"no_tools"`
	Quarantined    *bool  `yaml:"quarantined"`
	Model          string `yaml:"model"`
}

type Route struct {
	Chain      []string `yaml:"chain"`
	Rules      []Rule   `yaml:"rules"`
	EscalateTo string   `yaml:"escalate_to"`
}

type Config struct {
	PricingVersion string                    `yaml:"pricing_version"`
	Providers      map[string]ProviderConfig `yaml:"providers"`
	Models         map[string]*Model         `yaml:"models"`
	Routes         map[string]*Route         `yaml:"routes"`
	SemanticCache  struct {
		Enabled   bool    `yaml:"enabled"`
		Threshold float64 `yaml:"threshold"`
	} `yaml:"semantic_cache"`
}

var envRe = regexp.MustCompile(`\$\{([A-Z0-9_]+)(:-([^}]*))?\}`)

func expandEnv(s string) string {
	return envRe.ReplaceAllStringFunc(s, func(m string) string {
		g := envRe.FindStringSubmatch(m)
		if v := os.Getenv(g[1]); v != "" {
			return v
		}
		return g[3]
	})
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse([]byte(expandEnv(string(data))))
}

func Parse(data []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	for name, m := range c.Models {
		m.Name = name
		if m.Upstream == "" {
			m.Upstream = name
		}
		if _, ok := c.Providers[m.Provider]; !ok {
			return nil, fmt.Errorf("model %s: unknown provider %q", name, m.Provider)
		}
	}
	for name, r := range c.Routes {
		if !strings.HasPrefix(name, "router:") {
			return nil, fmt.Errorf("route %q must start with %q", name, "router:")
		}
		for _, m := range append(append([]string{}, r.Chain...), r.EscalateTo) {
			if m != "" && c.Models[m] == nil {
				return nil, fmt.Errorf("route %s: unknown model %q", name, m)
			}
		}
		for _, rule := range r.Rules {
			if c.Models[rule.Model] == nil {
				return nil, fmt.Errorf("route %s: rule uses unknown model %q", name, rule.Model)
			}
		}
	}
	if c.SemanticCache.Threshold == 0 {
		c.SemanticCache.Threshold = 0.92
	}
	return &c, nil
}

type Plan struct {
	Chain      []*Model
	EscalateTo *Model
	Route      string
}

// Resolve turns a model spec into an ordered fallback chain.
func (c *Config) Resolve(spec string, inputTokens int, hasTools, quarantined bool) (*Plan, error) {
	if m := c.Models[spec]; m != nil {
		return &Plan{Chain: []*Model{m}}, nil
	}
	r := c.Routes[spec]
	if r == nil {
		return nil, fmt.Errorf("unknown model or route %q", spec)
	}
	p := &Plan{Route: spec}
	seen := map[string]bool{}
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			p.Chain = append(p.Chain, c.Models[name])
		}
	}
	for _, rule := range r.Rules {
		if rule.MaxInputTokens > 0 && inputTokens > rule.MaxInputTokens {
			continue
		}
		if rule.NoTools && hasTools {
			continue
		}
		if rule.Quarantined != nil && *rule.Quarantined != quarantined {
			continue
		}
		add(rule.Model)
		break
	}
	for _, m := range r.Chain {
		add(m)
	}
	if r.EscalateTo != "" {
		p.EscalateTo = c.Models[r.EscalateTo]
	}
	return p, nil
}

// MaxPrice is the most expensive model in the plan (used for reservations so
// a fallback can never overspend the reserved amount).
func (p *Plan) MaxCost(in, out int) float64 {
	best := 0.0
	ms := append([]*Model{}, p.Chain...)
	if p.EscalateTo != nil {
		ms = append(ms, p.EscalateTo)
	}
	for _, m := range ms {
		if c := m.Cost(in, out); c > best {
			best = c
		}
	}
	return best
}
