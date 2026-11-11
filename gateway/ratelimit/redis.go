package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// takeScript atomically refills and takes from a bucket stored in Redis, so
// every gateway replica shares one limit per provider key. It returns the
// milliseconds to wait (0 = taken).
var takeScript = redis.NewScript(`
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local per_ms = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local b = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(b[1]) or cap
local ts = tonumber(b[2]) or now
tokens = math.min(cap, tokens + (now - ts) * per_ms)
local wait = 0
if tokens >= n then
  tokens = tokens - n
else
  wait = math.ceil((n - tokens) / per_ms)
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, 120000)
return wait
`)

type RedisBucket struct {
	Client   *redis.Client
	Key      string
	Capacity float64
	PerSec   float64
}

func (r *RedisBucket) Take(ctx context.Context, n float64, priority int) error {
	if n > r.Capacity {
		n = r.Capacity
	}
	for {
		now := time.Now().UnixMilli()
		wait, err := takeScript.Run(ctx, r.Client, []string{"keel:rl:" + r.Key},
			r.Capacity, r.PerSec/1000, n, now).Int64()
		if err != nil {
			return err
		}
		if wait == 0 {
			return nil
		}
		// Lower priorities back off a little longer so higher ones win races.
		d := time.Duration(wait)*time.Millisecond + time.Duration(max(0, 5-priority))*time.Millisecond
		if d > 200*time.Millisecond {
			d = 200 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
}
