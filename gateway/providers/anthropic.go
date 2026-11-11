package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// Anthropic adapts the Messages API: system prompts move to the top-level
// `system` field, tool calls are `tool_use` content blocks and tool results
// are `tool_result` blocks in a user turn.
type Anthropic struct {
	BaseURL string
	APIKey  string
	Version string
	HTTP    *http.Client
}

func (p *Anthropic) Name() string { return "anthropic" }

func (p *Anthropic) Chat(ctx context.Context, req *Request) (*Response, error) {
	var system []string
	var msgs []map[string]any
	appendBlock := func(role string, block map[string]any) {
		if n := len(msgs); n > 0 && msgs[n-1]["role"] == role {
			msgs[n-1]["content"] = append(msgs[n-1]["content"].([]map[string]any), block)
			return
		}
		msgs = append(msgs, map[string]any{"role": role, "content": []map[string]any{block}})
	}
	for _, m := range req.Messages {
		content := ""
		if m.Content != nil {
			content = *m.Content
		}
		switch m.Role {
		case "system":
			system = append(system, content)
		case "user":
			appendBlock("user", map[string]any{"type": "text", "text": content})
		case "assistant":
			if content != "" {
				appendBlock("assistant", map[string]any{"type": "text", "text": content})
			}
			for _, tc := range m.ToolCalls {
				appendBlock("assistant", map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": tc.Arguments})
			}
		case "tool":
			appendBlock("user", map[string]any{"type": "tool_result", "tool_use_id": m.ToolCallID, "content": content})
		}
	}
	maxTok := req.MaxTokens
	if maxTok <= 0 {
		maxTok = 1024
	}
	body := map[string]any{"model": req.Model, "messages": msgs, "max_tokens": maxTok, "temperature": req.Temperature}
	if len(system) > 0 {
		body["system"] = strings.Join(system, "\n\n")
	}
	if len(req.Tools) > 0 {
		var tools []map[string]any
		for _, t := range req.Tools {
			params := t.Function.Parameters
			if params == nil {
				params = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			tools = append(tools, map[string]any{"name": t.Function.Name, "description": t.Function.Description,
				"input_schema": params})
		}
		body["tools"] = tools
	}
	b, _ := json.Marshal(body)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.BaseURL, "/")+"/v1/messages", bytes.NewReader(b))
	if err != nil {
		return nil, &Error{Code: InvalidRequest, Message: err.Error()}
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("x-api-key", p.APIKey)
	v := p.Version
	if v == "" {
		v = "2023-06-01"
	}
	hr.Header.Set("anthropic-version", v)
	resp, err := p.HTTP.Do(hr)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != 200 {
		return nil, MapHTTPError(resp.StatusCode, data, resp.Header)
	}
	var out struct {
		Model   string `json:"model"`
		Content []struct {
			Type  string         `json:"type"`
			Text  string         `json:"text"`
			ID    string         `json:"id"`
			Name  string         `json:"name"`
			Input map[string]any `json:"input"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, &Error{Code: Overloaded, Message: "bad response: " + err.Error()}
	}
	r := &Response{Model: out.Model, FinishReason: out.StopReason, InputTokens: out.Usage.InputTokens,
		OutputTokens: out.Usage.OutputTokens}
	var text strings.Builder
	for _, c := range out.Content {
		switch c.Type {
		case "text":
			text.WriteString(c.Text)
		case "tool_use":
			r.ToolCalls = append(r.ToolCalls, ToolCall{ID: c.ID, Name: c.Name, Arguments: c.Input})
		}
	}
	if text.Len() > 0 {
		r.Text = strPtr(text.String())
		if req.OnDelta != nil {
			req.OnDelta(text.String())
		}
	}
	if out.StopReason == "refusal" {
		return nil, &Error{Code: ContentFiltered, Message: "model refused"}
	}
	return r, nil
}
