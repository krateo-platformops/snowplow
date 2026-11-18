package credhttp

// retry_after_1533_test.go — #1533: one Retry-After parser, two conversions. retryAfter (the apiserver send path) is
// plumbing's as transcribed, unchanged: absent or garbage 2 s, seconds as they are (a negative value stays
// negative), a date's time.Until. RetryAfter (the RUM forwarder) is never a default, never negative, never an
// overflow.

import (
	"math"
	"net/http"
	"testing"
	"time"
)

func respWithRetryAfter(v string) *http.Response {
	h := http.Header{}
	if v != "" {
		h.Set("Retry-After", v)
	}
	return &http.Response{Header: h}
}

func TestRetryAfter1533_TheSendPathsConversionIsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
	}{
		{"", 2 * time.Second},
		{"soon", 2 * time.Second},
		{"7", 7 * time.Second},
		{"-5", -5 * time.Second},
		{"0", 0},
	} {
		if got := retryAfter(respWithRetryAfter(tc.header)); got != tc.want {
			t.Fatalf("retryAfter(%q) = %v, want %v (plumbing's transcribed conversion)", tc.header, got, tc.want)
		}
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got := retryAfter(respWithRetryAfter(future)); got < 85*time.Second || got > 90*time.Second {
		t.Fatalf("retryAfter(a date 90 s ahead) = %v", got)
	}
	if got := retryAfter(respWithRetryAfter("Wed, 21 Oct 2015 07:28:00 GMT")); got >= 0 {
		t.Fatalf("retryAfter(a past date) = %v: the transcribed conversion keeps time.Until's negative value", got)
	}
}

func TestRetryAfter1533_TheExportedWaitIsNeverADefaultNegativeOrOverflowed(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
		ok     bool
	}{
		{"", 0, false},
		{"soon", 0, false},
		{"7", 7 * time.Second, true},
		{"-5", 0, true},
		{"0", 0, true},
		{"1000000000000", time.Duration(math.MaxInt64), true},
		{"9223372036854775807", time.Duration(math.MaxInt64), true},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0, true},
	} {
		got, ok := RetryAfter(respWithRetryAfter(tc.header))
		if got != tc.want || ok != tc.ok {
			t.Fatalf("RetryAfter(%q) = (%v, %v), want (%v, %v)", tc.header, got, ok, tc.want, tc.ok)
		}
	}
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	if got, ok := RetryAfter(respWithRetryAfter(future)); !ok || got < 85*time.Second || got > 90*time.Second {
		t.Fatalf("RetryAfter(a date 90 s ahead) = (%v, %v)", got, ok)
	}
}
