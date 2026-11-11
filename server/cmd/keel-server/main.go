// keel-server is the control plane: REST API for clients, gRPC for workers,
// and (on the elected leader) the sweeper, timers, approval expiry and outbox.
//
//	keel-server            serve (migrates first)
//	keel-server migrate    apply migrations and exit
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	keelv1 "github.com/keel-runtime/keel/gen/keelv1"
	"github.com/keel-runtime/keel/internal/otelx"
	"github.com/keel-runtime/keel/server/internal/api"
	"github.com/keel-runtime/keel/server/internal/auth"
	"github.com/keel-runtime/keel/server/internal/blob"
	"github.com/keel-runtime/keel/server/internal/notify"
	"github.com/keel-runtime/keel/server/internal/policy"
	"github.com/keel-runtime/keel/server/internal/scheduler"
	"github.com/keel-runtime/keel/server/internal/store"
	"github.com/keel-runtime/keel/server/internal/workerapi"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("keel-server exited", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := env("KEEL_DATABASE_URL", "postgres://keel:keel@localhost:55432/keel?sslmode=disable")
	blobs, err := blob.NewFS(env("KEEL_BLOB_DIR", "./data/blobs"))
	if err != nil {
		return err
	}
	maxConns, _ := strconv.Atoi(env("KEEL_DB_MAX_CONNS", "60"))
	st, err := openWithRetry(ctx, logger, dsn, int32(maxConns), blobs)
	if err != nil {
		return err
	}
	defer st.Close()
	if d, err := time.ParseDuration(env("KEEL_LEASE_TTL", "30s")); err == nil {
		st.LeaseTTL = d
	}
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		logger.Info("migrations applied")
		return nil
	}
	shutdownTracing, err := otelx.Setup(ctx, "keel-server")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	pol, err := policy.NewWatcher(env("KEEL_POLICY_FILE", "deploy/policy.yaml"))
	if err != nil {
		return err
	}
	authn := &auth.Authenticator{Store: st}
	if spec := os.Getenv("KEEL_API_KEYS"); spec != "" {
		keys, err := auth.ParseBootstrapKeys(spec)
		if err != nil {
			return err
		}
		for k, v := range keys {
			if err := st.PutAPIKey(ctx, auth.HashKey(k), v); err != nil {
				return err
			}
		}
	}
	authn.Disabled = env("KEEL_AUTH", "enabled") == "disabled"
	if authn.Disabled {
		logger.Warn("API authentication is DISABLED (KEEL_AUTH=disabled); every caller is an admin of tenant 'default'")
	}

	replica, _ := os.Hostname()
	replica = env("KEEL_REPLICA_ID", replica)
	notifier := notify.New(logger)
	go notifier.Run(ctx, dsn)
	go st.RunNotifier(ctx)

	sched := &scheduler.Scheduler{Store: st, Policy: pol, Logger: logger, ReplicaID: replica,
		Destinations: scheduler.ParseDestinations(os.Getenv("KEEL_OUTBOX_DESTINATIONS"))}
	if u := os.Getenv("KEEL_APPROVAL_WEBHOOK_URL"); u != "" {
		sched.Destinations["approval_webhook"] = u
	}
	if env("KEEL_SCHEDULER", "on") == "on" {
		go sched.Run(ctx)
	}

	// gRPC for workers.
	gs := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(auth.WorkerAuth(os.Getenv("KEEL_WORKER_TOKEN"))),
		grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	keelv1.RegisterWorkerServiceServer(gs, &workerapi.Server{Store: st, Notifier: notifier, Policy: pol, Logger: logger})
	glis, err := net.Listen("tcp", env("KEEL_GRPC_ADDR", ":7233"))
	if err != nil {
		return err
	}
	go func() {
		if err := gs.Serve(glis); err != nil {
			logger.Error("grpc serve", "err", err)
		}
	}()

	// REST for clients.
	a := &api.API{Store: st, Auth: authn, Notifier: notifier, Policy: pol, Logger: logger, IsLeader: sched.IsLeader}
	hs := &http.Server{Addr: env("KEEL_HTTP_ADDR", ":8080"), Handler: a.Router(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http serve", "err", err)
			stop()
		}
	}()
	logger.Info("keel-server started", "http", hs.Addr, "grpc", glis.Addr().String(), "replica", replica,
		"policy_version", pol.Engine().Version)

	<-ctx.Done()
	logger.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-sctx.Done():
		gs.Stop()
	}
	return nil
}

func openWithRetry(ctx context.Context, logger *slog.Logger, dsn string, maxConns int32, blobs blob.Store) (*store.Store, error) {
	var lastErr error
	for i := 0; i < 60; i++ {
		st, err := store.Open(ctx, dsn, maxConns, blobs)
		if err == nil {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = st.Pool.Ping(pctx)
			cancel()
			if err == nil {
				return st, nil
			}
			st.Close()
		}
		lastErr = err
		logger.Warn("waiting for database", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}
