// Package scheduler runs the server's background loops: the lease sweeper,
// durable timers, approval expiry, the outbox dispatcher and partition
// maintenance. Only one replica runs them at a time, elected with a
// PostgreSQL advisory lock held on a dedicated session; if the leader dies its
// session ends, the lock is released and another replica takes over.
package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/keel-runtime/keel/server/internal/metrics"
	"github.com/keel-runtime/keel/server/internal/policy"
	"github.com/keel-runtime/keel/server/internal/store"
)

const leaderLockKey = 0x6b65656c // "keel"

type Scheduler struct {
	Store        *store.Store
	Policy       *policy.Watcher
	Logger       *slog.Logger
	ReplicaID    string
	Destinations map[string]string // outbox destination name -> URL
	HTTP         *http.Client

	SweepEvery  time.Duration
	TimersEvery time.Duration

	mu       sync.Mutex
	isLeader bool
}

func (s *Scheduler) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isLeader
}

func (s *Scheduler) setLeader(v bool) {
	s.mu.Lock()
	s.isLeader = v
	s.mu.Unlock()
	if v {
		metrics.IsLeader.Set(1)
	} else {
		metrics.IsLeader.Set(0)
	}
}

// Run blocks until ctx ends. Loops that every replica runs (gauges, policy
// reload) start here; leader-only loops start once the lock is won.
func (s *Scheduler) Run(ctx context.Context) {
	if s.SweepEvery == 0 {
		s.SweepEvery = time.Second
	}
	if s.TimersEvery == 0 {
		s.TimersEvery = 500 * time.Millisecond
	}
	if s.HTTP == nil {
		s.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	go s.every(ctx, 5*time.Second, "gauges", s.gauges)
	go s.every(ctx, 5*time.Second, "policy-reload", func(context.Context) error { return s.Policy.Poll() })
	for ctx.Err() == nil {
		if err := s.lead(ctx); err != nil && ctx.Err() == nil {
			s.Logger.Warn("leader election", "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Scheduler) lead(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.Store.DSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&got); err != nil {
		return err
	}
	if !got {
		return nil
	}
	s.Logger.Info("became leader", "replica", s.ReplicaID)
	s.setLeader(true)
	defer s.setLeader(false)
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	start := func(name string, d time.Duration, fn func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); s.every(lctx, d, name, fn) }()
	}
	if err := s.Store.EnsurePartitions(lctx, time.Now()); err != nil {
		s.Logger.Warn("partitions", "err", err)
	}
	start("sweeper", s.SweepEvery, func(c context.Context) error { _, err := s.Store.SweepExpiredLeases(c); return err })
	start("timers", s.TimersEvery, func(c context.Context) error { _, err := s.Store.FireTimers(c); return err })
	start("approval-expiry", time.Second, func(c context.Context) error { _, err := s.Store.ExpireApprovals(c); return err })
	start("outbox", 500*time.Millisecond, s.dispatchOutbox)
	start("partitions", time.Hour, func(c context.Context) error { return s.Store.EnsurePartitions(c, time.Now()) })
	// Hold the lock while the session is alive; losing it ends leadership.
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			return nil
		case <-t.C:
			pctx, pc := context.WithTimeout(ctx, 3*time.Second)
			err := conn.Ping(pctx)
			pc()
			if err != nil {
				s.Logger.Warn("lost leader session", "err", err)
				cancel()
				wg.Wait()
				return err
			}
		}
	}
}

func (s *Scheduler) every(ctx context.Context, d time.Duration, name string, fn func(context.Context) error) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			s.Logger.Warn("loop failed", "loop", name, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Scheduler) gauges(ctx context.Context) error {
	c, err := s.Store.Counts(ctx)
	if err != nil {
		return err
	}
	metrics.PendingRuns.Set(float64(c.Pending))
	metrics.RunningRuns.Set(float64(c.Running))
	metrics.WaitingRuns.Set(float64(c.Waiting))
	metrics.ApprovalsPending.Set(float64(c.ApprovalsPending))
	for t, v := range c.TenantSpend {
		metrics.TenantSpend.WithLabelValues(t).Set(v)
	}
	return nil
}

// dispatchOutbox delivers outbox messages with retries and exponential
// backoff. Receivers deduplicate on the Idempotency-Key header.
func (s *Scheduler) dispatchOutbox(ctx context.Context) error {
	msgs, err := s.Store.ClaimOutbox(ctx, 50)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		url, ok := s.Destinations[m.Destination]
		if !ok {
			if m.Destination == "approval_webhook" {
				// No webhook configured: the UI inbox is the only channel.
				_ = s.Store.MarkOutbox(ctx, m.ID, true, "", 0, false)
				metrics.OutboxDelivered.WithLabelValues("skipped").Inc()
				continue
			}
			_ = s.Store.MarkOutbox(ctx, m.ID, false, "unknown destination "+m.Destination, 0, true)
			metrics.OutboxDelivered.WithLabelValues("dead").Inc()
			continue
		}
		err := s.post(ctx, url, m)
		if err == nil {
			_ = s.Store.MarkOutbox(ctx, m.ID, true, "", 0, false)
			metrics.OutboxDelivered.WithLabelValues("delivered").Inc()
			continue
		}
		dead := m.Attempts >= 12
		backoff := time.Duration(math.Min(float64(time.Second)*math.Pow(2, float64(m.Attempts)), float64(10*time.Minute)))
		_ = s.Store.MarkOutbox(ctx, m.ID, false, err.Error(), backoff, dead)
		if dead {
			metrics.OutboxDelivered.WithLabelValues("dead").Inc()
		} else {
			metrics.OutboxDelivered.WithLabelValues("retry").Inc()
		}
	}
	return nil
}

func (s *Scheduler) post(ctx context.Context, url string, m store.OutboxMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(m.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.DedupeKey)
	req.Header.Set("X-Keel-Run-Id", m.RunID)
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// ParseDestinations parses "name=url,name2=url2".
func ParseDestinations(spec string) map[string]string {
	out := map[string]string{}
	for _, p := range strings.Split(spec, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(p), "="); ok && k != "" {
			out[k] = v
		}
	}
	return out
}
