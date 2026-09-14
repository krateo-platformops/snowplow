// retired_flags_c9_test.go — 1.12.6 C9: PARTIAL_RESULT_TTL_SECONDS is a
// RETIRED flag. The bounded-partial Put layer (partial_result_ttl.go) is gone;
// the only surviving reader of the env name is the audit below, so an
// operator who still sets it learns at boot that it does nothing.
//
// The Warn trigger for this flag is NOT "false" (the #57 split for boolean
// gates): the flag was numeric and default "0" (off), so the silent change is
// a NON-zero value — the operator asked for a bounded-stale partial cache and
// now gets a bare decline.
package cache_test

import (
	"log/slog"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestAuditRetiredFlags_C9_PartialResultTTL(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want slog.Level
	}{
		{"30", slog.LevelWarn},   // relied on the removed layer → silent change → WARN
		{"0", slog.LevelInfo},    // explicitly off, exactly the retired default → no-op
		{"", slog.LevelInfo},     // set-empty → no-op
		{" 60 ", slog.LevelWarn}, // trimmed like every other value
	} {
		t.Run("value="+tc.val, func(t *testing.T) {
			cache.ResetRetiredFlagAuditForTest()
			t.Cleanup(cache.ResetRetiredFlagAuditForTest)
			t.Setenv("PARTIAL_RESULT_TTL_SECONDS", tc.val)
			log, recs := newCaptureLogger()
			cache.AuditRetiredFlags(log)
			got := findAuditRecords(*recs, "PARTIAL_RESULT_TTL_SECONDS")
			if len(got) != 1 {
				t.Fatalf("PARTIAL_RESULT_TTL_SECONDS=%q: want exactly 1 audit line; got %d (%+v)", tc.val, len(got), *recs)
			}
			if got[0].level != tc.want {
				t.Fatalf("PARTIAL_RESULT_TTL_SECONDS=%q: want %v; got %v", tc.val, tc.want, got[0].level)
			}
			if got[0].attrs["status"] != "ignored" {
				t.Fatalf("status = %q, want ignored", got[0].attrs["status"])
			}
		})
	}
	// Absent → nothing (the #57 invariant holds for the new entry too).
	cache.ResetRetiredFlagAuditForTest()
	log, recs := newCaptureLogger()
	cache.AuditRetiredFlags(log)
	if got := findAuditRecords(*recs, "PARTIAL_RESULT_TTL_SECONDS"); len(got) != 0 {
		t.Fatalf("absent flag emitted %d line(s)", len(got))
	}
}
