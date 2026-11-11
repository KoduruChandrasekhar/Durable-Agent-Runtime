// Package sandbox runs untrusted code (written by an agent) in locked-down,
// single-use Docker containers: no network, read-only root filesystem, a
// small tmpfs for /tmp, non-root user, all capabilities dropped, pid, memory
// and CPU limits, and a hard wall-clock timeout after which the container is
// killed. A warm pool hides container start-up; a reaper removes leftovers.
//
// Threat model: plain containers share the host kernel. This stops the
// agent's code from reaching the network, the host filesystem, secrets or
// other runs, and bounds its resources; it is not a boundary against kernel
// exploits. Set KEEL_SANDBOX_RUNTIME=runsc (gVisor) for a stronger one.
package sandbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	mExec = promauto.NewCounterVec(prometheus.CounterOpts{Name: "keel_sandbox_executions_total",
		Help: "Sandbox executions by outcome."}, []string{"outcome"})
	mStart = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "keel_sandbox_start_seconds",
		Help: "Time until the code starts running.", Buckets: prometheus.ExponentialBuckets(0.01, 2, 12)}, []string{"mode"})
	mDur = promauto.NewHistogram(prometheus.HistogramOpts{Name: "keel_sandbox_duration_seconds",
		Help: "Total execution time.", Buckets: prometheus.ExponentialBuckets(0.05, 2, 12)})
	mPool = promauto.NewGauge(prometheus.GaugeOpts{Name: "keel_sandbox_warm_pool", Help: "Warm containers ready."})
)

const label = "keel.sandbox=1"

type Config struct {
	Image       string
	Runtime     string // "" or "runsc"
	PoolSize    int
	MemoryMB    int
	CPUs        string
	PidsLimit   int
	TmpfsMB     int
	MaxOutput   int // bytes of stdout/stderr returned
	MaxFileSize int
	MaxFiles    int
	Docker      string
}

func (c *Config) defaults() {
	if c.Image == "" {
		c.Image = "python:3.12-alpine"
	}
	if c.MemoryMB == 0 {
		c.MemoryMB = 512
	}
	if c.CPUs == "" {
		c.CPUs = "1"
	}
	if c.PidsLimit == 0 {
		c.PidsLimit = 128
	}
	if c.TmpfsMB == 0 {
		c.TmpfsMB = 64
	}
	if c.MaxOutput == 0 {
		c.MaxOutput = 64 << 10
	}
	if c.MaxFileSize == 0 {
		c.MaxFileSize = 256 << 10
	}
	if c.MaxFiles == 0 {
		c.MaxFiles = 10
	}
	if c.Docker == "" {
		c.Docker = "docker"
	}
}

type Request struct {
	Language string            `json:"language"`
	Code     string            `json:"code"`
	Files    map[string]string `json:"files"` // name -> content (text)
	TimeoutS float64           `json:"timeout"`
	Warm     *bool             `json:"warm"`
}

type Result struct {
	Stdout     string            `json:"stdout"`
	Stderr     string            `json:"stderr"`
	ExitCode   int               `json:"exit_code"`
	TimedOut   bool              `json:"timed_out"`
	OOMKilled  bool              `json:"oom_killed"`
	Truncated  bool              `json:"truncated"`
	Files      map[string]string `json:"files"`
	DurationMs float64           `json:"duration_ms"`
	StartMs    float64           `json:"start_ms"`
	Mode       string            `json:"mode"`
}

type Service struct {
	Cfg    Config
	Logger *slog.Logger

	mu   sync.Mutex
	pool []string
	fill chan struct{}
}

func New(cfg Config, logger *slog.Logger) *Service {
	cfg.defaults()
	return &Service{Cfg: cfg, Logger: logger, fill: make(chan struct{}, 1)}
}

// runArgs are the isolation flags every sandbox container gets.
func (s *Service) runArgs(name string) []string {
	c := s.Cfg
	args := []string{"run", "-d", "--name", name, "--label", label,
		"--network", "none", "--read-only",
		"--tmpfs", fmt.Sprintf("/tmp:rw,size=%dm,mode=1777,exec", c.TmpfsMB),
		"--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--pids-limit", fmt.Sprint(c.PidsLimit),
		"--memory", fmt.Sprintf("%dm", c.MemoryMB), "--memory-swap", fmt.Sprintf("%dm", c.MemoryMB),
		"--cpus", c.CPUs, "--env", "HOME=/tmp", "--env", "PYTHONDONTWRITEBYTECODE=1",
		"--workdir", "/tmp"}
	if c.Runtime != "" {
		args = append(args, "--runtime", c.Runtime)
	}
	return append(args, c.Image, "sleep", "3600")
}

func (s *Service) docker(ctx context.Context, stdin []byte, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, s.Cfg.Docker, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code, err = ee.ExitCode(), nil
	}
	return out.String(), errb.String(), code, err
}

func (s *Service) start(ctx context.Context) (string, error) {
	name := fmt.Sprintf("keel-sbx-%d", time.Now().UnixNano())
	out, stderr, code, err := s.docker(ctx, nil, s.runArgs(name)...)
	if err != nil || code != 0 {
		return "", fmt.Errorf("docker run failed (%d): %v %s", code, err, strings.TrimSpace(stderr))
	}
	return strings.TrimSpace(out)[:12], nil
}

func (s *Service) remove(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, _, _, _ = s.docker(ctx, nil, "rm", "-f", id)
}

// Run keeps the warm pool full and reaps leftovers until ctx ends.
func (s *Service) Run(ctx context.Context) {
	s.reap(ctx, 0) // containers left by a previous crash
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.reap(ctx, 10*time.Minute)
			}
		}
	}()
	s.kick()
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			ids := s.pool
			s.pool = nil
			s.mu.Unlock()
			for _, id := range ids {
				s.remove(id)
			}
			return
		case <-s.fill:
			for {
				s.mu.Lock()
				n := len(s.pool)
				s.mu.Unlock()
				if n >= s.Cfg.PoolSize {
					break
				}
				id, err := s.start(ctx)
				if err != nil {
					s.Logger.Warn("warm pool", "err", err)
					time.Sleep(2 * time.Second)
					break
				}
				s.mu.Lock()
				s.pool = append(s.pool, id)
				mPool.Set(float64(len(s.pool)))
				s.mu.Unlock()
			}
		}
	}
}

func (s *Service) kick() {
	select {
	case s.fill <- struct{}{}:
	default:
	}
}

// reap removes sandbox containers older than maxAge that are not in the pool.
func (s *Service) reap(ctx context.Context, maxAge time.Duration) {
	out, _, _, err := s.docker(ctx, nil, "ps", "-a", "--filter", "label="+label, "--format", "{{.ID}} {{.CreatedAt}}")
	if err != nil {
		return
	}
	s.mu.Lock()
	inPool := map[string]bool{}
	for _, id := range s.pool {
		inPool[id] = true
	}
	s.mu.Unlock()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 4 || inPool[f[0]] {
			continue
		}
		created, err := time.Parse("2006-01-02 15:04:05 -0700", f[1]+" "+f[2]+" "+f[3])
		if maxAge > 0 && err == nil && time.Since(created) < maxAge {
			continue
		}
		s.remove(f[0])
		n++
	}
	if n > 0 {
		s.Logger.Info("reaped leftover sandbox containers", "count", n)
	}
}

func (s *Service) take(ctx context.Context, warm bool) (string, string, error) {
	if warm {
		s.mu.Lock()
		if len(s.pool) > 0 {
			id := s.pool[0]
			s.pool = s.pool[1:]
			mPool.Set(float64(len(s.pool)))
			s.mu.Unlock()
			s.kick()
			return id, "warm", nil
		}
		s.mu.Unlock()
		s.kick()
	}
	id, err := s.start(ctx)
	return id, "cold", err
}

// runnerPy writes the input files, runs the agent's code and appends the
// files it wrote to /tmp/out to stderr after a marker, all in one exec.
const runnerPy = `import sys, json, os, base64
d = json.loads(sys.stdin.read())
os.makedirs('/tmp/out', exist_ok=True)
for n, c in (d.get('files') or {}).items():
    with open(os.path.join('/tmp', os.path.basename(n)), 'w') as f:
        f.write(c)
sys.stdin = open(os.devnull)
code = 0
try:
    exec(compile(d['code'], '<agent>', 'exec'), {'__name__': '__main__'})
except SystemExit as e:
    code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
except BaseException:
    import traceback
    traceback.print_exc()
    code = 1
finally:
    sys.stdout.flush()
    out = {}
    try:
        for n in sorted(os.listdir('/tmp/out'))[:d['max_files']]:
            p = os.path.join('/tmp/out', n)
            if os.path.isfile(p):
                with open(p, 'rb') as f:
                    out[n] = base64.b64encode(f.read(d['max_size'])).decode()
    except Exception:
        pass
    sys.stderr.write(chr(10) + '__KEEL_FILES__' + json.dumps(out) + chr(10))
    sys.stderr.flush()
os._exit(code)
`

const filesMarker = "\n__KEEL_FILES__"

// Execute runs one program in a fresh (or warm) container, which is
// destroyed afterwards whatever happens.
func (s *Service) Execute(ctx context.Context, req Request) (*Result, error) {
	if req.Language != "" && req.Language != "python" {
		return nil, fmt.Errorf("unsupported language %q", req.Language)
	}
	timeout := time.Duration(req.TimeoutS * float64(time.Second))
	if timeout <= 0 || timeout > 120*time.Second {
		timeout = 30 * time.Second
	}
	t0 := time.Now()
	warm := req.Warm == nil || *req.Warm
	id, mode, err := s.take(ctx, warm)
	if err != nil {
		mExec.WithLabelValues("error").Inc()
		return nil, err
	}
	defer func() { go s.remove(id) }() // single use, destroyed in the background
	payload, _ := json.Marshal(map[string]any{"files": req.Files, "code": req.Code,
		"max_files": s.Cfg.MaxFiles, "max_size": s.Cfg.MaxFileSize})
	startDur := time.Since(t0)
	mStart.WithLabelValues(mode).Observe(startDur.Seconds())
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// env -i: the agent's code sees no environment variables at all.
	stdout, stderr, code, err := s.docker(rctx, payload, "exec", "-i", "-w", "/tmp", id,
		"env", "-i", "HOME=/tmp", "PATH=/usr/local/bin:/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE=1",
		"python", "-c", runnerPy)
	res := &Result{ExitCode: code, Mode: mode, StartMs: float64(startDur.Microseconds()) / 1000}
	if rctx.Err() != nil {
		res.TimedOut = true
		res.ExitCode = -1
		kctx, kc := context.WithTimeout(context.Background(), 10*time.Second)
		_, _, _, _ = s.docker(kctx, nil, "kill", id)
		kc()
	} else if err != nil {
		mExec.WithLabelValues("error").Inc()
		return nil, err
	}
	if i := strings.LastIndex(stderr, filesMarker); i >= 0 {
		var fs map[string]string
		if json.Unmarshal([]byte(strings.TrimSpace(stderr[i+len(filesMarker):])), &fs) == nil {
			res.Files = map[string]string{}
			for k, v := range fs {
				if b, err := base64.StdEncoding.DecodeString(v); err == nil {
					res.Files[k] = string(b)
				}
			}
		}
		stderr = stderr[:i]
	}
	if code == 137 {
		res.OOMKilled = s.oomKilled(id)
	}
	res.Stdout, res.Truncated = truncate(stdout, s.Cfg.MaxOutput)
	var t2 bool
	res.Stderr, t2 = truncate(stderr, s.Cfg.MaxOutput)
	res.Truncated = res.Truncated || t2
	res.DurationMs = float64(time.Since(t0).Microseconds()) / 1000
	mDur.Observe(time.Since(t0).Seconds())
	outcome := "ok"
	switch {
	case res.TimedOut:
		outcome = "timeout"
	case res.OOMKilled:
		outcome = "oom"
	case res.ExitCode != 0:
		outcome = "error"
	}
	mExec.WithLabelValues(outcome).Inc()
	return res, nil
}

func (s *Service) oomKilled(id string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, _, _, _ := s.docker(ctx, nil, "inspect", "-f", "{{.State.OOMKilled}}", id)
	return strings.TrimSpace(out) == "true"
}

func truncate(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	return s[:n] + "\n...[truncated]", true
}
