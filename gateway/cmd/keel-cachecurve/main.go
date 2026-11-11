// keel-cachecurve measures the semantic cache's hit rate (paraphrases served
// from cache) and wrong-hit rate (different questions served the wrong
// answer) across similarity thresholds, using the gateway's own embedding.
//
//	go run ./gateway/cmd/keel-cachecurve bench/cache_pairs.json > bench/results/cache_curve.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/keel-runtime/keel/gateway/cache"
)

type pair struct {
	A    string `json:"a"`
	B    string `json:"b"`
	Same bool   `json:"same"`
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var pairs []pair
	if err := json.Unmarshal(data, &pairs); err != nil {
		panic(err)
	}
	var same, diff []float64
	var sameGuard, diffGuard []bool // does the entity guard allow the hit?
	for _, p := range pairs {
		s := cache.Cosine(cache.Embed(p.A), cache.Embed(p.B))
		ok := cache.Entities(p.A) == cache.Entities(p.B)
		if p.Same {
			same, sameGuard = append(same, s), append(sameGuard, ok)
		} else {
			diff, diffGuard = append(diff, s), append(diffGuard, ok)
		}
	}
	type point struct {
		Threshold           float64 `json:"threshold"`
		HitRate             float64 `json:"hit_rate"`
		WrongHitRate        float64 `json:"wrong_hit_rate"`
		HitRateGuarded      float64 `json:"hit_rate_guarded"`
		WrongHitRateGuarded float64 `json:"wrong_hit_rate_guarded"`
	}
	var curve []point
	for t := 0.50; t <= 0.991; t += 0.01 {
		h, w, hg, wg := 0, 0, 0, 0
		for i, s := range same {
			if s >= t {
				h++
				if sameGuard[i] {
					hg++
				}
			}
		}
		for i, s := range diff {
			if s >= t {
				w++
				if diffGuard[i] {
					wg++
				}
			}
		}
		n, d := float64(len(same)), float64(len(diff))
		curve = append(curve, point{Threshold: float64(int(t*100+0.5)) / 100, HitRate: float64(h) / n,
			WrongHitRate: float64(w) / d, HitRateGuarded: float64(hg) / n, WrongHitRateGuarded: float64(wg) / d})
	}
	chosen := 0.0
	for _, p := range curve { // the lowest threshold with zero wrong hits (entity guard on)
		if p.WrongHitRateGuarded == 0 {
			chosen = p.Threshold
			break
		}
	}
	out := map[string]any{"pairs": len(pairs), "paraphrase_similarity": same, "different_similarity": diff,
		"curve": curve, "chosen_threshold": chosen}
	b, _ := json.MarshalIndent(out, "", " ")
	fmt.Println(string(b))
}
