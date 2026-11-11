// Package providers adapts LLM APIs to one normalized request/response and
// one set of errors. Providers format tool definitions and tool calls
// differently; the adapters translate both ways.
package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type ToolCall struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type Message struct {
	Role       string     `json:"role"`
	Content    *string    `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	Name       string     `json:"name,omitempty"`
}

type ToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type Tool struct {
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

type Request struct {
	Model          string
	Messages       []Message
	Tools          []Tool
	Temperature    float64
	MaxTokens      int
	ResponseFormat map[string]any
	Stream         bool
	OnDelta        func(string) // called with streamed text while live
}

type Response struct {
	Text         *string
	ToolCalls    []ToolCall
	InputTokens  int
	OutputTokens int
	FinishReason string
	Model        string
}

// Normalized error codes.
const (
	RateLimited     = "rate_limited"
	Overloaded      = "overloaded"
	Timeout         = "timeout"
	Unavailable     = "unavailable"
	InvalidRequest  = "invalid_request"
	ContextTooLong  = "context_too_long"
	ContentFiltered = "content_filtered"
	AuthFailed      = "auth_failed"
)

type Error struct {
	Code       string
	Message    string
	Status     int
	RetryAfter time.Duration
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Retryable errors may succeed on another attempt or another model.
func (e *Error) Retryable() bool {
	switch e.Code {
	case RateLimited, Overloaded, Timeout, Unavailable:
		return true
	}
	return false
}

// BreakerFailure reports whether the error says the backend is unhealthy.
func (e *Error) BreakerFailure() bool {
	switch e.Code {
	case Overloaded, Timeout, Unavailable:
		return true
	}
	return false
}

type Provider interface {
	Name() string
	Chat(ctx context.Context, req *Request) (*Response, error)
}

// MapHTTPError turns an upstream HTTP status and body into a normalized error.
func MapHTTPError(status int, body []byte, header http.Header) *Error {
	msg := string(body)
	if len(msg) > 500 {
		msg = msg[:500]
	}
	e := &Error{Status: status, Message: msg}
	if ra := header.Get("Retry-After"); ra != "" {
		if s, err := strconv.ParseFloat(ra, 64); err == nil {
			e.RetryAfter = time.Duration(s * float64(time.Second))
		}
	}
	lower := []byte(msg)
	switch {
	case status == 429:
		e.Code = RateLimited
	case status == 529 || status == 503 || status == 502 || status == 504:
		e.Code = Overloaded
	case status >= 500:
		e.Code = Overloaded
	case status == 401 || status == 403:
		e.Code = AuthFailed
	case status == 400 && (contains(lower, "context") && (contains(lower, "length") || contains(lower, "too long") || contains(lower, "maximum"))):
		e.Code = ContextTooLong
	case status == 400 && contains(lower, "content") && (contains(lower, "filter") || contains(lower, "policy")):
		e.Code = ContentFiltered
	default:
		e.Code = InvalidRequest
	}
	return e
}

func contains(b []byte, s string) bool {
	return len(b) >= len(s) && indexFold(string(b), s) >= 0
}

func indexFold(s, sub string) int {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		match := true
		for j := 0; j < n; j++ {
			a, b := s[i+j], sub[j]
			if 'A' <= a && a <= 'Z' {
				a += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// ParseArgs decodes a tool-call argument string; models sometimes emit
// invalid JSON, which the gateway reports for escalation.
func ParseArgs(s string) (map[string]any, error) {
	if s == "" {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strPtr(s string) *string { return &s }
