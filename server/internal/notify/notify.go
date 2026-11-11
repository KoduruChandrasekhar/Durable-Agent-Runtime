// Package notify wakes long-polling workers on NOTIFY runs_ready and fans out
// streamed LLM tokens (NOTIFY llm_tokens from the gateway) to SSE clients.
// Notifications can be missed during reconnects, so pollers also re-check
// every second.
package notify

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Token struct {
	RunID  string `json:"run_id"`
	StepID string `json:"step_id"`
	Delta  string `json:"delta"`
}

type Notifier struct {
	mu     sync.Mutex
	ready  chan struct{}
	subs   map[string]map[chan Token]struct{}
	logger *slog.Logger
}

func New(logger *slog.Logger) *Notifier {
	return &Notifier{ready: make(chan struct{}), subs: map[string]map[chan Token]struct{}{}, logger: logger}
}

// Wait returns a channel closed at the next runs_ready notification. Take it
// before trying to claim so a notification in between is not missed.
func (n *Notifier) Wait() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ready
}

func (n *Notifier) Broadcast() {
	n.mu.Lock()
	close(n.ready)
	n.ready = make(chan struct{})
	n.mu.Unlock()
}

func (n *Notifier) Subscribe(runID string) (<-chan Token, func()) {
	ch := make(chan Token, 256)
	n.mu.Lock()
	if n.subs[runID] == nil {
		n.subs[runID] = map[chan Token]struct{}{}
	}
	n.subs[runID][ch] = struct{}{}
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		delete(n.subs[runID], ch)
		if len(n.subs[runID]) == 0 {
			delete(n.subs, runID)
		}
		n.mu.Unlock()
	}
}

func (n *Notifier) publish(t Token) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[t.RunID] {
		select {
		case ch <- t:
		default: // slow consumer: drop tokens rather than block
		}
	}
}

// Run keeps a dedicated LISTEN connection open, reconnecting on failure.
func (n *Notifier) Run(ctx context.Context, dsn string) {
	for ctx.Err() == nil {
		if err := n.listen(ctx, dsn); err != nil && ctx.Err() == nil {
			n.logger.Warn("notify listener reconnecting", "err", err)
			n.Broadcast() // let pollers re-check while we are deaf
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (n *Notifier) listen(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for _, ch := range []string{"runs_ready", "llm_tokens"} {
		if _, err := conn.Exec(ctx, "LISTEN "+ch); err != nil {
			return err
		}
	}
	for {
		note, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		switch note.Channel {
		case "runs_ready":
			n.Broadcast()
		case "llm_tokens":
			var batch []Token
			if json.Unmarshal([]byte(note.Payload), &batch) == nil {
				for _, t := range batch {
					n.publish(t)
				}
			}
		}
	}
}
