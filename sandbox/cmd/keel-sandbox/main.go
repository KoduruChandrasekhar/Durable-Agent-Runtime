// keel-sandbox: POST /execute {language, code, files, timeout} -> stdout,
// stderr, exit code and output files (anything written to /tmp/out).
//
// The service drives Docker; access to the Docker socket is effectively root
// on the host, so run it against rootless Docker or on a separate VM.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/keel-runtime/keel/internal/otelx"
	"github.com/keel-runtime/keel/sandbox"
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
	shutdown, _ := otelx.Setup(ctx, "keel-sandbox")
	defer func() { _ = shutdown(context.Background()) }()
	pool, _ := strconv.Atoi(env("KEEL_SANDBOX_POOL", "3"))
	svc := sandbox.New(sandbox.Config{Image: env("KEEL_SANDBOX_IMAGE", "python:3.12-alpine"),
		Runtime: os.Getenv("KEEL_SANDBOX_RUNTIME"), PoolSize: pool}, logger)
	go svc.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /execute", func(w http.ResponseWriter, r *http.Request) {
		var req sandbox.Request
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil || req.Code == "" {
			http.Error(w, `{"error":{"code":"invalid_request","message":"code is required"}}`, 400)
			return
		}
		res, err := svc.Execute(r.Context(), req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(503)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "sandbox_unavailable", "message": err.Error()}})
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{Addr: env("KEEL_SANDBOX_ADDR", ":8095"), Handler: otelhttp.NewHandler(mux, "keel-sandbox"),
		ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "err", err)
			stop()
		}
	}()
	logger.Info("keel-sandbox started", "addr", srv.Addr, "pool", pool)
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	time.Sleep(time.Second) // let the pool goroutine remove warm containers
}
