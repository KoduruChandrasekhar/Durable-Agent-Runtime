// Package cache holds the gateway's exact and semantic response caches.
//
// Exact: key = hash of tenant, model spec, messages, tools and parameters;
// only for temperature-0 or explicitly cacheable calls; per tenant; with TTL.
//
// Semantic: embed the last user message and search the tenant's namespace,
// scoped to the same model, tools and system prompt; accept only above a
// similarity threshold chosen from the measured hit-rate / wrong-hit-rate
// curve (see bench/cache_curve.py).
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

type entry struct {
	key     string
	value   []byte
	expires time.Time
}

// LRU is a size-bounded cache with per-entry TTL.
type LRU struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	ll    *list.List
	items map[string]*list.Element
}

func NewLRU(max int, ttl time.Duration) *LRU {
	return &LRU{max: max, ttl: ttl, ll: list.New(), items: map[string]*list.Element{}}
}

func (c *LRU) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if time.Now().After(e.expires) {
		c.ll.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e.value, true
}

func (c *LRU) Put(key string, value []byte) {
	c.PutTTL(key, value, c.ttl)
}

func (c *LRU) PutTTL(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value.(*entry).value = value
		el.Value.(*entry).expires = time.Now().Add(ttl)
		c.ll.MoveToFront(el)
		return
	}
	c.items[key] = c.ll.PushFront(&entry{key: key, value: value, expires: time.Now().Add(ttl)})
	for c.ll.Len() > c.max {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.items, last.Value.(*entry).key)
	}
}

func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Key hashes any JSON-serializable parts into a cache key.
func Key(parts ...any) string {
	h := sha256.New()
	for _, p := range parts {
		b, _ := json.Marshal(p)
		h.Write(b)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---------------------------------------------------------------- semantic cache

const Dim = 512

// Embed is a small local embedding: hashed word unigrams and bigrams plus
// character trigrams, L2-normalized. It needs no model server, and the
// measured threshold curve keeps it honest; swap in a real embedding model by
// implementing Embedder.
func Embed(text string) []float32 {
	v := make([]float32, Dim)
	words := tokenize(text)
	add := func(s string, w float32) {
		h := fnv.New32a()
		h.Write([]byte(s))
		x := h.Sum32()
		sign := float32(1)
		if x&1 == 1 {
			sign = -1
		}
		v[(x>>1)%Dim] += sign * w
	}
	for i, w := range words {
		add("w:"+w, 1.0)
		if i > 0 {
			add("b:"+words[i-1]+" "+w, 0.7)
		}
		padded := " " + w + " "
		for j := 0; j+3 <= len(padded); j++ {
			add("c:"+padded[j:j+3], 0.3)
		}
	}
	var norm float64
	for _, x := range v {
		norm += float64(x * x)
	}
	if norm > 0 {
		inv := float32(1 / math.Sqrt(norm))
		for i := range v {
			v[i] *= inv
		}
	}
	return v
}

var stop = map[string]bool{"the": true, "a": true, "an": true, "is": true, "are": true, "to": true, "of": true,
	"and": true, "or": true, "for": true, "in": true, "on": true, "my": true, "i": true, "me": true, "you": true,
	"please": true, "can": true, "could": true, "would": true, "it": true, "this": true, "that": true, "what": true}

func tokenize(s string) []string {
	f := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
	out := f[:0]
	for _, w := range f {
		if !stop[w] {
			out = append(out, w)
		}
	}
	return out
}

func Cosine(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i] * b[i])
	}
	return dot
}

type Embedder interface {
	Embed(text string) []float32
}

type LocalEmbedder struct{}

func (LocalEmbedder) Embed(text string) []float32 { return Embed(text) }

type semEntry struct {
	vec      []float32
	value    []byte
	expires  time.Time
	text     string
	entities string
}

var entityRe = regexp.MustCompile(`[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}|[^\s,.;:!?()"']*\d[^\s,.;:!?()"']*`)

// Entities returns the identifiers in a text (anything containing a digit,
// email addresses), sorted. Two texts that differ only in an order id or an
// amount embed almost identically, so a semantic hit is accepted only when
// these match exactly. Found by the chaos demo: without this guard the cache
// served one customer's extraction and reply to another.
func Entities(text string) string {
	m := entityRe.FindAllString(strings.ToLower(text), -1)
	sort.Strings(m)
	out := m[:0]
	for i, s := range m {
		if i == 0 || s != m[i-1] {
			out = append(out, s)
		}
	}
	return strings.Join(out, " ")
}

// Semantic is an in-memory vector index partitioned by namespace (tenant +
// model + tools + system prompt). A real deployment points this at a vector
// database; the interface and the threshold logic stay the same.
type Semantic struct {
	Threshold     float64
	TTL           time.Duration
	MaxPerNS      int
	Embedder      Embedder
	NoEntityGuard bool // only for measuring what the guard buys

	mu sync.RWMutex
	ns map[string][]semEntry
}

func NewSemantic(threshold float64, ttl time.Duration) *Semantic {
	return &Semantic{Threshold: threshold, TTL: ttl, MaxPerNS: 5000, Embedder: LocalEmbedder{}, ns: map[string][]semEntry{}}
}

// Lookup returns the best cached value above the threshold and its score.
func (s *Semantic) Lookup(namespace, text string) ([]byte, float64, bool) {
	vec := s.Embedder.Embed(text)
	ents := Entities(text)
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	best, bestScore := -1, -1.0
	entries := s.ns[namespace]
	for i, e := range entries {
		if now.After(e.expires) || (!s.NoEntityGuard && e.entities != ents) {
			continue
		}
		if sc := Cosine(vec, e.vec); sc > bestScore {
			best, bestScore = i, sc
		}
	}
	if best >= 0 && bestScore >= s.Threshold {
		return entries[best].value, bestScore, true
	}
	return nil, bestScore, false
}

func (s *Semantic) Store(namespace, text string, value []byte) {
	vec := s.Embedder.Embed(text)
	s.mu.Lock()
	defer s.mu.Unlock()
	es := append(s.ns[namespace], semEntry{vec: vec, value: value, expires: time.Now().Add(s.TTL), text: text,
		entities: Entities(text)})
	if len(es) > s.MaxPerNS {
		es = es[len(es)-s.MaxPerNS:]
	}
	s.ns[namespace] = es
}
