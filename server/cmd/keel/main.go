// keel is the command-line client for keel-server's REST API.
//
//	keel events <run_id> [--follow]   print a run's event log
//	keel runs [--status s] [--workflow w]
//	keel run <run_id>
//	keel start <workflow> '<input-json>' [--version v] [--start-key k] [--budget usd]
//	keel signal <run_id> <name> '<payload-json>'
//	keel cancel <run_id>
//	keel fork <run_id> <from_seq> ['<overrides-json>'] [--live]
//	keel approvals [--status pending]
//	keel approve <approval_id> ['<edited-args-json>'] | keel reject <approval_id>
//	keel workflows | keel tenants
//
// KEEL_URL (default http://localhost:8080) and KEEL_API_KEY configure it.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	baseURL = envOr("KEEL_URL", "http://localhost:8080")
	apiKey  = os.Getenv("KEEL_API_KEY")
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "events":
		err = events(args)
	case "runs":
		fs := flag.NewFlagSet("runs", flag.ExitOnError)
		status := fs.String("status", "", "")
		wf := fs.String("workflow", "", "")
		limit := fs.Int("limit", 50, "")
		_ = fs.Parse(args)
		var out struct {
			Runs []map[string]any `json:"runs"`
		}
		if err = call("GET", fmt.Sprintf("/v1/runs?status=%s&workflow=%s&limit=%d", *status, *wf, *limit), nil, &out); err == nil {
			for _, r := range out.Runs {
				fmt.Printf("%s  %-10s %-20s v%-4v spent=$%.4f  %v\n", r["run_id"], r["status"], r["workflow"], r["version"], r["spent_usd"], r["created_at"])
			}
		}
	case "run":
		need(args, 1)
		err = printCall("GET", "/v1/runs/"+args[0], nil)
	case "start":
		fs := flag.NewFlagSet("start", flag.ExitOnError)
		version := fs.String("version", "", "")
		key := fs.String("start-key", "", "")
		budget := fs.Float64("budget", 0, "")
		need(args, 2)
		_ = fs.Parse(args[2:])
		body := map[string]any{"workflow": args[0], "input": json.RawMessage(args[1])}
		if *version != "" {
			body["version"] = *version
		}
		if *key != "" {
			body["start_key"] = *key
		}
		if *budget > 0 {
			body["budget_usd"] = *budget
		}
		err = printCall("POST", "/v1/runs", body)
	case "signal":
		need(args, 3)
		err = printCall("POST", fmt.Sprintf("/v1/runs/%s/signals/%s", args[0], args[1]),
			map[string]any{"payload": json.RawMessage(args[2])})
	case "cancel":
		need(args, 1)
		err = printCall("POST", "/v1/runs/"+args[0]+"/cancel", nil)
	case "fork":
		need(args, 2)
		body := map[string]any{"from_seq": json.Number(args[1])}
		rest := args[2:]
		live := false
		for _, a := range rest {
			if a == "--live" {
				live = true
			} else {
				body["overrides"] = json.RawMessage(a)
			}
		}
		body["dry_run"] = !live
		err = printCall("POST", "/v1/runs/"+args[0]+"/fork", body)
	case "approvals":
		fs := flag.NewFlagSet("approvals", flag.ExitOnError)
		status := fs.String("status", "pending", "")
		_ = fs.Parse(args)
		err = printCall("GET", "/v1/approvals?status="+*status, nil)
	case "approve", "reject":
		need(args, 1)
		body := map[string]any{"decision": cmd}
		if len(args) > 1 {
			body["edited_args"] = json.RawMessage(args[1])
		}
		err = printCall("POST", "/v1/approvals/"+args[0], body)
	case "workflows":
		err = printCall("GET", "/v1/workflows", nil)
	case "tenants":
		err = printCall("GET", "/v1/tenants", nil)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: keel events|runs|run|start|signal|cancel|fork|approvals|approve|reject|workflows|tenants ...")
	os.Exit(2)
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func request(method, path string, body any) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(baseURL, "/")+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	return req, nil
}

func call(method, path string, body, out any) error {
	req, err := request(method, path, body)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func printCall(method, path string, body any) error {
	var out any
	if err := call(method, path, body, &out); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	return nil
}

type event struct {
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	StepID    string          `json:"step_id"`
	Payload   json.RawMessage `json:"payload"`
	Epoch     int64           `json:"epoch"`
	CreatedAt time.Time       `json:"created_at"`
}

func printEvent(e event) {
	p := string(e.Payload)
	if len(p) > 160 {
		p = p[:157] + "..."
	}
	fmt.Printf("%4d  %s  e%-3d %-22s %-28s %s\n", e.Seq, e.CreatedAt.Format("15:04:05.000"), e.Epoch, e.Type, e.StepID, p)
}

func events(args []string) error {
	need(args, 1)
	follow := len(args) > 1 && (args[1] == "--follow" || args[1] == "-f")
	if !follow {
		var out struct {
			Status string  `json:"status"`
			Events []event `json:"events"`
		}
		if err := call("GET", "/v1/runs/"+args[0]+"/events", nil, &out); err != nil {
			return err
		}
		for _, e := range out.Events {
			printEvent(e)
		}
		fmt.Printf("-- status: %s, %d events\n", out.Status, len(out.Events))
		return nil
	}
	req, err := request("GET", "/v1/runs/"+args[0]+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	kind := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data := strings.TrimPrefix(line, "data: ")
			switch kind {
			case "event":
				var e event
				if json.Unmarshal([]byte(data), &e) == nil {
					printEvent(e)
				}
			case "token":
				var t struct{ Delta string }
				_ = json.Unmarshal([]byte(data), &t)
				fmt.Print(t.Delta)
			case "end":
				fmt.Println("-- end", data)
				return nil
			}
		}
	}
	return sc.Err()
}
