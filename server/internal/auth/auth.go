// Package auth authenticates API keys (stored only as SHA-256 hashes) and
// worker tokens. Each key belongs to one tenant and carries scopes:
// runs:write, runs:read, approvals:decide, pii:read, admin.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/keel-runtime/keel/server/internal/store"
)

const (
	ScopeRunsWrite = "runs:write"
	ScopeRunsRead  = "runs:read"
	ScopeApprove   = "approvals:decide"
	ScopePII       = "pii:read"
	ScopeAdmin     = "admin"
)

type Principal struct {
	TenantID string
	Name     string
	Scopes   map[string]bool
}

func (p *Principal) Admin() bool { return p.Scopes[ScopeAdmin] }

func (p *Principal) Has(scope string) bool { return p.Scopes[ScopeAdmin] || p.Scopes[scope] }

// TenantFilter is the tenant every store query must be scoped to. Admins may
// act for one tenant via the X-Keel-Tenant header, or for all with "".
func (p *Principal) TenantFilter(r *http.Request) string {
	if p.Admin() {
		return r.Header.Get("X-Keel-Tenant")
	}
	return p.TenantID
}

type ctxKey struct{}

func FromContext(ctx context.Context) *Principal {
	p, _ := ctx.Value(ctxKey{}).(*Principal)
	return p
}

func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

type cached struct {
	p   *Principal
	exp time.Time
}

type Authenticator struct {
	Store    *store.Store
	Disabled bool
	cache    sync.Map // key hash -> cached
}

var ErrUnauthorized = errors.New("unauthorized")

func (a *Authenticator) Authenticate(ctx context.Context, key string) (*Principal, error) {
	if a.Disabled {
		return &Principal{TenantID: "default", Name: "dev", Scopes: map[string]bool{ScopeAdmin: true}}, nil
	}
	if key == "" {
		return nil, ErrUnauthorized
	}
	h := HashKey(key)
	if c, ok := a.cache.Load(h); ok && time.Now().Before(c.(cached).exp) {
		return c.(cached).p, nil
	}
	k, err := a.Store.LookupAPIKey(ctx, h)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrUnauthorized
	}
	if err != nil {
		return nil, err
	}
	p := &Principal{TenantID: k.TenantID, Name: k.Principal, Scopes: map[string]bool{}}
	for _, s := range k.Scopes {
		p.Scopes[s] = true
	}
	a.cache.Store(h, cached{p: p, exp: time.Now().Add(30 * time.Second)})
	return p, nil
}

func KeyFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	return r.URL.Query().Get("api_key") // EventSource cannot set headers
}

// ParseBootstrapKeys parses "key=tenant:principal:scope1,scope2;key2=...".
func ParseBootstrapKeys(spec string) (map[string]store.APIKey, error) {
	out := map[string]store.APIKey{}
	for _, part := range strings.Split(spec, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, rest, ok := strings.Cut(part, "=")
		fields := strings.SplitN(rest, ":", 3)
		if !ok || len(fields) != 3 || key == "" {
			return nil, fmt.Errorf("bad api key spec %q (want key=tenant:principal:scopes)", part)
		}
		out[key] = store.APIKey{TenantID: fields[0], Principal: fields[1], Scopes: strings.Split(fields[2], ",")}
	}
	return out, nil
}

// WorkerAuth checks the worker token sent as gRPC metadata.
func WorkerAuth(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if token != "" {
			md, _ := metadata.FromIncomingContext(ctx)
			got := ""
			if v := md.Get("authorization"); len(v) > 0 {
				got = strings.TrimPrefix(v[0], "Bearer ")
			}
			if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				return nil, status.Error(codes.Unauthenticated, "bad worker token")
			}
		}
		return h(ctx, req)
	}
}
