// Package breaker is a per provider-and-model circuit breaker: it opens
// after N failures within a window, lets an occasional probe through while
// open, and closes once a probe succeeds.
package breaker

import (
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

type Breaker struct {
	Threshold  int           // failures within Window that open the circuit
	Window     time.Duration // failure counting window
	ProbeEvery time.Duration // while open, allow one probe this often

	mu        sync.Mutex
	state     State
	failures  []time.Time
	openedAt  time.Time
	lastProbe time.Time
	probing   bool
	now       func() time.Time
	OnChange  func(State)
}

func New(threshold int, window, probe time.Duration) *Breaker {
	return &Breaker{Threshold: threshold, Window: window, ProbeEvery: probe, now: time.Now}
}

func (b *Breaker) setState(s State) {
	if b.state != s {
		b.state = s
		if b.OnChange != nil {
			b.OnChange(s)
		}
	}
}

// Allow reports whether a request may go to this backend now. While open,
// one probe per ProbeEvery is allowed (the half-open state).
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Sub(b.lastProbe) >= b.ProbeEvery && !b.probing {
			b.lastProbe = b.now()
			b.probing = true
			b.setState(HalfOpen)
			return true
		}
		return false
	default: // half-open: only the probe in flight
		return false
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures = b.failures[:0]
	b.setState(Closed)
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.state == HalfOpen {
		b.probing = false
		b.openedAt = now
		b.setState(Open)
		return
	}
	cut := now.Add(-b.Window)
	kept := b.failures[:0]
	for _, t := range b.failures {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	b.failures = append(kept, now)
	if len(b.failures) >= b.Threshold && b.state == Closed {
		b.openedAt = now
		b.lastProbe = now
		b.setState(Open)
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// RetryBudget caps retries at a fraction of requests (plus a small floor) so
// an outage does not turn into a retry storm.
type RetryBudget struct {
	Ratio    float64 // e.g. 0.1: retries may add at most ~10% load
	MinPerS  float64 // floor so low traffic can still retry
	mu       sync.Mutex
	balance  float64
	last     time.Time
	capacity float64
}

func NewRetryBudget(ratio, minPerSecond float64) *RetryBudget {
	return &RetryBudget{Ratio: ratio, MinPerS: minPerSecond, capacity: 100, balance: 10, last: time.Now()}
}

// Deposit is called once per original request.
func (r *RetryBudget) Deposit() {
	r.mu.Lock()
	r.refill()
	r.balance = min(r.capacity, r.balance+r.Ratio)
	r.mu.Unlock()
}

// Withdraw returns true if a retry is allowed.
func (r *RetryBudget) Withdraw() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refill()
	if r.balance >= 1 {
		r.balance--
		return true
	}
	return false
}

func (r *RetryBudget) refill() {
	now := time.Now()
	r.balance = min(r.capacity, r.balance+now.Sub(r.last).Seconds()*r.MinPerS)
	r.last = now
}
