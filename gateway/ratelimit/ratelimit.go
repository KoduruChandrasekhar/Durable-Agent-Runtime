// Package ratelimit keeps one token bucket per provider API key for
// requests/minute and tokens/minute, shared by every caller of the gateway.
// When a bucket is empty the request queues until its deadline instead of
// failing; higher-priority waiters are served first. With several gateway
// replicas the buckets move to Redis (see redis.go).
package ratelimit

import (
	"context"
	"sync"
	"time"
)

type Limiter interface {
	// Take blocks until n units are available (or ctx ends).
	Take(ctx context.Context, n float64, priority int) error
}

// Bucket is an in-memory token bucket with priority waiters.
type Bucket struct {
	Capacity float64
	PerSec   float64

	mu      sync.Mutex
	tokens  float64
	last    time.Time
	waiting map[int]int // priority -> waiters
}

func NewBucket(perMinute float64, burst float64) *Bucket {
	if burst <= 0 {
		burst = perMinute / 6 // 10 s worth
	}
	return &Bucket{Capacity: burst, PerSec: perMinute / 60, tokens: burst, last: time.Now(), waiting: map[int]int{}}
}

func (b *Bucket) refill(now time.Time) {
	b.tokens = min(b.Capacity, b.tokens+now.Sub(b.last).Seconds()*b.PerSec)
	b.last = now
}

func (b *Bucket) higherWaiting(p int) bool {
	for prio, n := range b.waiting {
		if prio > p && n > 0 {
			return true
		}
	}
	return false
}

func (b *Bucket) Take(ctx context.Context, n float64, priority int) error {
	if b == nil || b.PerSec <= 0 {
		return nil
	}
	if n > b.Capacity {
		n = b.Capacity // a request larger than the burst still gets through eventually
	}
	registered := false
	defer func() {
		if registered {
			b.mu.Lock()
			b.waiting[priority]--
			b.mu.Unlock()
		}
	}()
	for {
		b.mu.Lock()
		b.refill(time.Now())
		if b.tokens >= n && !b.higherWaiting(priority) {
			b.tokens -= n
			b.mu.Unlock()
			return nil
		}
		if !registered {
			b.waiting[priority]++
			registered = true
		}
		wait := time.Duration((n - b.tokens) / b.PerSec * float64(time.Second))
		b.mu.Unlock()
		if wait < 5*time.Millisecond {
			wait = 5 * time.Millisecond
		}
		if wait > 100*time.Millisecond {
			wait = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// Pair limits requests and tokens per minute for one API key.
type Pair struct {
	Requests Limiter
	Tokens   Limiter
}

func (p *Pair) Take(ctx context.Context, tokens int, priority int) error {
	if p == nil {
		return nil
	}
	if p.Requests != nil {
		if err := p.Requests.Take(ctx, 1, priority); err != nil {
			return err
		}
	}
	if p.Tokens != nil && tokens > 0 {
		return p.Tokens.Take(ctx, float64(tokens), priority)
	}
	return nil
}
