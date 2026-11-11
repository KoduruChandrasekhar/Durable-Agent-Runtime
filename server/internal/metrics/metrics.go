// Package metrics holds the Prometheus metrics exported by keel-server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RunsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_runs_started_total", Help: "Runs started.",
	}, []string{"workflow", "tenant"})

	RunsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_runs_finished_total", Help: "Runs that reached a final state.",
	}, []string{"status"})

	RunDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "keel_run_duration_seconds", Help: "Wall time from start to finish.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 18),
	})

	StepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "keel_step_duration_seconds", Help: "Step durations reported by workers.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 20),
	}, []string{"kind"})

	PendingRuns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "keel_pending_runs", Help: "Runs waiting in the queue (status pending).",
	})

	RunningRuns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "keel_running_runs", Help: "Runs currently leased by a worker.",
	})

	WaitingRuns = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "keel_waiting_runs", Help: "Runs waiting on a timer, signal, approval or child.",
	})

	QueueWait = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "keel_queue_wait_seconds", Help: "Time from pending (available) to claimed.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	})

	StepRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keel_step_retries_total", Help: "Retryable step failures.",
	})

	LeaseExpirations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keel_lease_expirations_total", Help: "Leases reclaimed by the sweeper.",
	})

	FencingRejections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keel_fencing_rejections_total", Help: "Writes rejected because the epoch was stale.",
	})

	SeqConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keel_seq_conflicts_total", Help: "Appends rejected because expected_seq did not match.",
	})

	EventsAppended = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_events_appended_total", Help: "Events appended to run logs.",
	}, []string{"type"})

	PolicyDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_policy_decisions_total", Help: "Guardrail decisions.",
	}, []string{"rule", "decision"})

	ApprovalsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "keel_approvals_pending", Help: "Approvals waiting for a human.",
	})

	TimersFired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "keel_timers_fired_total", Help: "Durable timers fired.",
	})

	Claims = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_claims_total", Help: "Runs claimed by workers.",
	}, []string{"reason"})

	IsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "keel_leader", Help: "1 when this replica runs the background loops.",
	})

	OutboxDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "keel_outbox_messages_total", Help: "Outbox deliveries by outcome.",
	}, []string{"outcome"})

	TenantSpend = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "keel_tenant_spend_usd", Help: "Today's LLM spend per tenant (from the gateway ledger).",
	}, []string{"tenant"})
)
