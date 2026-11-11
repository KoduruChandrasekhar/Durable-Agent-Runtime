// keel-gateway: the model gateway. POST /v1/chat with run id, tenant, step
// key, a model or route ("router:support"), messages, tools and parameters.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/keel-runtime/keel/gateway"
	"github.com/keel-runtime/keel/gateway/budget"
	"github.com/keel-runtime/keel/gateway/cache"
	"github.com/keel-runtime/keel/gateway/ratelimit"
	"github.com/keel-runtime/keel/gateway/router"
	"github.com/keel-runtime/keel/internal/otelx"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := router.Load(env("KEEL_GATEWAY_CONFIG", "deploy/gateway.yaml"))
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	shutdown, _ := otelx.Setup(ctx, "keel-gateway")
	defer func() { _ = shutdown(context.Background()) }()

	ps, limits := gateway.BuildProviders(cfg)
	if url := os.Getenv("KEEL_REDIS_URL"); url != "" {
		opt, err := redis.ParseURL(url)
		if err != nil {
			logger.Error("redis url", "err", err)
			os.Exit(1)
		}
		rc := redis.NewClient(opt)
		for name, pc := range cfg.Providers {
			pair := &ratelimit.Pair{}
			if pc.RPM > 0 {
				pair.Requests = &ratelimit.RedisBucket{Client: rc, Key: name + ":rpm", Capacity: pc.RPM / 6, PerSec: pc.RPM / 60}
			}
			if pc.TPM > 0 {
				pair.Tokens = &ratelimit.RedisBucket{Client: rc, Key: name + ":tpm", Capacity: pc.TPM / 6, PerSec: pc.TPM / 60}
			}
			limits[name] = pair
		}
		logger.Info("rate limits shared through redis")
	}
	g := &gateway.Gateway{Config: cfg, Providers: ps, Limits: limits, Logger: logger, APIKey: os.Getenv("KEEL_GATEWAY_KEY")}
	if dsn := os.Getenv("KEEL_DATABASE_URL"); dsn != "" {
		pc, err := pgxpool.ParseConfig(dsn)
		if err == nil {
			pc.MaxConns = 40
			if pool, err := pgxpool.NewWithConfig(ctx, pc); err == nil {
				g.DB = pool
				g.Budget = &budget.Postgres{Pool: pool}
				defer pool.Close()
			}
		}
		if g.DB == nil {
			logger.Error("database unavailable; budgets disabled", "err", err)
		}
	} else {
		logger.Warn("KEEL_DATABASE_URL unset: budgets are not enforced")
	}
	if os.Getenv("KEEL_CACHE") == "off" {
		g.Exact = cache.NewLRU(1, time.Nanosecond)
		cfg.SemanticCache.Enabled = false
	}
	if th := os.Getenv("KEEL_SEMANTIC_THRESHOLD"); th != "" {
		if v, err := strconv.ParseFloat(th, 64); err == nil {
			cfg.SemanticCache.Threshold = v
		}
	}
	g.Init(ctx)
	srv := &http.Server{Addr: env("KEEL_GATEWAY_ADDR", ":8090"), Handler: g.Router(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "err", err)
			stop()
		}
	}()
	logger.Info("keel-gateway started", "addr", srv.Addr, "models", len(cfg.Models), "routes", len(cfg.Routes))
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
}
