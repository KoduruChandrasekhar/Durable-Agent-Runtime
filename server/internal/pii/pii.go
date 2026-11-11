// Package pii masks personal data (emails, phone numbers, card numbers that
// pass the Luhn check, Aadhaar numbers) in JSON returned by the API, unless the
// caller holds pii:read and asks to reveal it.
package pii

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	emailRe   = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	cardRe    = regexp.MustCompile(`\b(?:\d[ \-]?){12,18}\d\b`)
	aadhaarRe = regexp.MustCompile(`\b[2-9]\d{3}[ \-]?\d{4}[ \-]?\d{4}\b`)
	phoneRe   = regexp.MustCompile(`\+?\(?\d(?:[ \-()]{0,2}\d){9,12}\b`)
)

func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Luhn reports whether a digit string passes the Luhn checksum.
func Luhn(d string) bool {
	if len(d) < 13 {
		return false
	}
	sum, alt := 0, false
	for i := len(d) - 1; i >= 0; i-- {
		n := int(d[i] - '0')
		if alt {
			n *= 2
			if n > 9 {
				n -= 9
			}
		}
		sum += n
		alt = !alt
	}
	return sum%10 == 0
}

func maskEmail(m string) string {
	local, domain, _ := strings.Cut(m, "@")
	if len(local) <= 1 {
		return "*@" + domain
	}
	return local[:1] + strings.Repeat("*", len(local)-1) + "@" + domain
}

// MaskString masks every PII pattern in s.
func MaskString(s string) string {
	s = emailRe.ReplaceAllStringFunc(s, maskEmail)
	s = cardRe.ReplaceAllStringFunc(s, func(m string) string {
		d := digits(m)
		if !Luhn(d) {
			return m
		}
		return "[card ****" + d[len(d)-4:] + "]"
	})
	s = aadhaarRe.ReplaceAllStringFunc(s, func(m string) string {
		d := digits(m)
		if len(d) != 12 {
			return m
		}
		return "[aadhaar ****" + d[8:] + "]"
	})
	s = phoneRe.ReplaceAllStringFunc(s, func(m string) string {
		d := digits(m)
		if len(d) < 10 || len(d) > 13 {
			return m
		}
		return "[phone ****" + d[len(d)-2:] + "]"
	})
	return s
}

func maskValue(v any) any {
	switch t := v.(type) {
	case string:
		return MaskString(t)
	case map[string]any:
		for k, x := range t {
			t[k] = maskValue(x)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = maskValue(x)
		}
		return t
	}
	return v
}

// MaskJSON masks every string inside a JSON document.
func MaskJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	out, err := json.Marshal(maskValue(v))
	if err != nil {
		return raw
	}
	return out
}
