package blob

import (
	"bytes"
	"strings"
	"testing"
)

func TestOffloadResolve(t *testing.T) {
	s, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	small := []byte(`{"a":1}`)
	if out, _ := Offload(s, small); !bytes.Equal(out, small) {
		t.Fatal("small payload should stay inline")
	}
	big := []byte(`{"x":"` + strings.Repeat("y", Threshold) + `"}`)
	ref, err := Offload(s, big)
	if err != nil || !bytes.Contains(ref, []byte("sha256:")) {
		t.Fatalf("offload: %s %v", ref, err)
	}
	back, err := Resolve(s, ref)
	if err != nil || !bytes.Equal(back, big) {
		t.Fatal("round trip failed")
	}
	again, _ := Offload(s, big)
	if !bytes.Equal(again, ref) {
		t.Fatal("content addressing should be stable")
	}
}

// Event payload decoding must never crash on arbitrary bytes.
func FuzzResolve(f *testing.F) {
	f.Add([]byte(`{"blob":"sha256:00"}`))
	f.Add([]byte(`{"a":1}`))
	s, _ := NewFS(f.TempDir())
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Resolve(s, b)
	})
}
