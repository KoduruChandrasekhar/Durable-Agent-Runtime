package providers

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Mock is an in-process scripted provider for unit tests: it echoes the last
// user message, with configurable latency and error injection.
type Mock struct {
	ProviderName string
	Latency      time.Duration
	ErrorRate    float64
	ErrorCode    string

	mu        sync.Mutex
	failUntil time.Time
	Calls     int
}

func (m *Mock) Name() string {
	if m.ProviderName == "" {
		return "mock"
	}
	return m.ProviderName
}

// FailFor makes every call fail with code until d has passed (outage tests).
func (m *Mock) FailFor(d time.Duration, code string) {
	m.mu.Lock()
	m.failUntil = time.Now().Add(d)
	m.ErrorCode = code
	m.mu.Unlock()
}

func (m *Mock) Chat(ctx context.Context, req *Request) (*Response, error) {
	m.mu.Lock()
	m.Calls++
	failing := time.Now().Before(m.failUntil)
	code := m.ErrorCode
	m.mu.Unlock()
	if m.Latency > 0 {
		select {
		case <-time.After(m.Latency):
		case <-ctx.Done():
			return nil, &Error{Code: Timeout, Message: "deadline"}
		}
	}
	if code == "" {
		code = Overloaded
	}
	if failing || (m.ErrorRate > 0 && rand.Float64() < m.ErrorRate) {
		return nil, &Error{Code: code, Message: "mock failure", Status: 503}
	}
	last := ""
	in := 0
	for _, msg := range req.Messages {
		if msg.Content != nil {
			in += len(*msg.Content)/4 + 4
			if msg.Role == "user" {
				last = *msg.Content
			}
		}
	}
	text := fmt.Sprintf("[%s] %s", req.Model, strings.TrimSpace(last))
	if req.OnDelta != nil {
		req.OnDelta(text)
	}
	return &Response{Text: &text, InputTokens: in, OutputTokens: len(text)/4 + 1, FinishReason: "stop", Model: req.Model}, nil
}
