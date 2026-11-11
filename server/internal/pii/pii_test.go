package pii

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMask(t *testing.T) {
	in := "Mail asha.k@example.com, call +91 98765 43210, card 4111 1111 1111 1111, aadhaar 2345 6789 0123, order ORD-12345"
	out := MaskString(in)
	for _, leak := range []string{"asha.k@example.com", "98765 43210", "4111 1111 1111 1111", "2345 6789 0123"} {
		if strings.Contains(out, leak) {
			t.Errorf("leaked %q in %q", leak, out)
		}
	}
	if !strings.Contains(out, "ORD-12345") {
		t.Errorf("masked an order id: %q", out)
	}
	if !Luhn("4111111111111111") || Luhn("4111111111111112") {
		t.Fatal("luhn")
	}
}

func TestMaskJSON(t *testing.T) {
	out := MaskJSON(json.RawMessage(`{"a":["x@y.com", {"b":"call 9876543210"}], "n": 5}`))
	if strings.Contains(string(out), "x@y.com") || strings.Contains(string(out), "9876543210") {
		t.Fatalf("not masked: %s", out)
	}
}

func FuzzMaskJSON(f *testing.F) {
	f.Add([]byte(`{"a":"b@c.de"}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		_ = MaskJSON(b)
	})
}
