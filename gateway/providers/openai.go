package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// OpenAICompat speaks the OpenAI chat-completions API: hosted APIs, vLLM and
// your own inference server, and keel's mock LLM server.
type OpenAICompat struct {
	ProviderName string
	BaseURL      string
	APIKey       string
	HTTP         *http.Client
}

func (p *OpenAICompat) Name() string { return p.ProviderName }

type oaiToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaiMessage struct {
	Role       string        `json:"role"`
	Content    *string       `json:"content"`
	ToolCalls  []oaiToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	Name       string        `json:"name,omitempty"`
}

func toOAI(msgs []Message) []oaiMessage {
	out := make([]oaiMessage, len(msgs))
	for i, m := range msgs {
		om := oaiMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID, Name: m.Name}
		for _, tc := range m.ToolCalls {
			var o oaiToolCall
			o.ID, o.Type = tc.ID, "function"
			o.Function.Name = tc.Name
			b, _ := json.Marshal(tc.Arguments)
			o.Function.Arguments = string(b)
			om.ToolCalls = append(om.ToolCalls, o)
		}
		if m.Role == "tool" && om.Name == "" {
			om.Name = ""
		}
		out[i] = om
	}
	return out
}

func (p *OpenAICompat) Chat(ctx context.Context, req *Request) (*Response, error) {
	body := map[string]any{
		"model":       req.Model,
		"messages":    toOAI(req.Messages),
		"temperature": req.Temperature,
		"max_tokens":  req.MaxTokens,
	}
	if len(req.Tools) > 0 {
		body["tools"] = req.Tools
	}
	if rf := req.ResponseFormat; rf != nil {
		if s, ok := rf["schema"]; ok {
			body["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{
				"name": orDefault(rf["name"], "output"), "schema": s}}
		} else {
			body["response_format"] = rf
		}
	}
	if req.Stream {
		body["stream"] = true
		body["stream_options"] = map[string]any{"include_usage": true}
	}
	b, _ := json.Marshal(body)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.BaseURL, "/")+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, &Error{Code: InvalidRequest, Message: err.Error()}
	}
	hr.Header.Set("Content-Type", "application/json")
	if p.APIKey != "" {
		hr.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := p.HTTP.Do(hr)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return nil, MapHTTPError(resp.StatusCode, data, resp.Header)
	}
	if req.Stream && strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return p.readStream(ctx, resp.Body, req)
	}
	var out struct {
		Model   string `json:"model"`
		Choices []struct {
			Message      oaiMessage `json:"message"`
			FinishReason string     `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, transportError(ctx, err)
	}
	if len(out.Choices) == 0 {
		return nil, &Error{Code: Overloaded, Message: "empty choices"}
	}
	r := &Response{Text: out.Choices[0].Message.Content, FinishReason: out.Choices[0].FinishReason,
		InputTokens: out.Usage.PromptTokens, OutputTokens: out.Usage.CompletionTokens, Model: out.Model}
	for _, tc := range out.Choices[0].Message.ToolCalls {
		args, err := ParseArgs(tc.Function.Arguments)
		if err != nil {
			args = map[string]any{"__invalid_json__": tc.Function.Arguments}
		}
		r.ToolCalls = append(r.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: args})
	}
	if out.Choices[0].FinishReason == "content_filter" {
		return nil, &Error{Code: ContentFiltered, Message: "provider filtered the output"}
	}
	return r, nil
}

func (p *OpenAICompat) readStream(ctx context.Context, body io.Reader, req *Request) (*Response, error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	var text strings.Builder
	type partial struct {
		id, name string
		args     strings.Builder
	}
	calls := map[int]*partial{}
	r := &Response{}
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var chunk struct {
			Model   string `json:"model"`
			Choices []struct {
				Delta struct {
					Content   *string `json:"content"`
					ToolCalls []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if chunk.Model != "" {
			r.Model = chunk.Model
		}
		if chunk.Usage != nil {
			r.InputTokens, r.OutputTokens = chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens
		}
		for _, c := range chunk.Choices {
			if c.Delta.Content != nil && *c.Delta.Content != "" {
				text.WriteString(*c.Delta.Content)
				if req.OnDelta != nil {
					req.OnDelta(*c.Delta.Content)
				}
			}
			for _, tc := range c.Delta.ToolCalls {
				pc := calls[tc.Index]
				if pc == nil {
					pc = &partial{}
					calls[tc.Index] = pc
				}
				if tc.ID != "" {
					pc.id = tc.ID
				}
				if tc.Function.Name != "" {
					pc.name = tc.Function.Name
				}
				pc.args.WriteString(tc.Function.Arguments)
			}
			if c.FinishReason != nil {
				r.FinishReason = *c.FinishReason
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, transportError(ctx, err)
	}
	if text.Len() > 0 {
		r.Text = strPtr(text.String())
	}
	for i := 0; i < len(calls); i++ {
		pc := calls[i]
		if pc == nil {
			continue
		}
		args, err := ParseArgs(pc.args.String())
		if err != nil {
			args = map[string]any{"__invalid_json__": pc.args.String()}
		}
		r.ToolCalls = append(r.ToolCalls, ToolCall{ID: pc.id, Name: pc.name, Arguments: args})
	}
	return r, nil
}

func transportError(ctx context.Context, err error) *Error {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil || (errors.As(err, &ne) && ne.Timeout()) {
		return &Error{Code: Timeout, Message: err.Error()}
	}
	return &Error{Code: Unavailable, Message: err.Error()}
}

func orDefault(v any, d string) any {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return d
}

// DefaultHTTP is a client tuned for many concurrent upstream calls.
func DefaultHTTP() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 1024
	tr.MaxIdleConnsPerHost = 512
	tr.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: tr}
}
