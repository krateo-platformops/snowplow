// retired_flags_966_test.go — #966: PREWARM_PAGE_LIMIT is a RETIRED flag.
// Prewarm resolves each widget with exactly the pagination its cell key
// encodes (the ref's declared slice, else unpaginated), so no prewarm page size
// is left to set. ANY value is the silent change: the operator asked for a
// bounded prewarm window and no longer gets one, so it WARNs; set-empty is a
// no-op (Info) and absent emits nothing. Without the registry entry this arm
// is RED (#967 review F3).
package cache_test

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestAuditRetiredFlags_966_PrewarmPageLimit(t *testing.T) {
	const flag = "PREWARM_PAGE_LIMIT"
	for _, tc := range []struct {
		val  string
		want slog.Level
	}{
		{"5", slog.LevelWarn},   // the retired default, still a bound the operator asked for
		{"25", slog.LevelWarn},  // an override the walk no longer honours
		{" 0 ", slog.LevelWarn}, // any non-empty value: there is no "off" value to be a no-op
		{"", slog.LevelInfo},    // set-empty → no-op
	} {
		t.Run("value="+tc.val, func(t *testing.T) {
			cache.ResetRetiredFlagAuditForTest()
			t.Cleanup(cache.ResetRetiredFlagAuditForTest)
			t.Setenv(flag, tc.val)
			log, recs := newCaptureLogger()
			cache.AuditRetiredFlags(log)
			got := findAuditRecords(*recs, flag)
			if len(got) != 1 {
				t.Fatalf("%s=%q: want exactly 1 audit line; got %d (%+v)", flag, tc.val, len(got), *recs)
			}
			if got[0].level != tc.want {
				t.Fatalf("%s=%q: want %v; got %v", flag, tc.val, tc.want, got[0].level)
			}
			if got[0].attrs["status"] != "ignored" {
				t.Fatalf("status = %q, want ignored", got[0].attrs["status"])
			}
			if tc.want == slog.LevelWarn && !strings.Contains(got[0].attrs["effective_behavior"], "declared slice") {
				t.Fatalf("%s WARN must state the replacement behaviour (the ref's declared slice); attrs=%v", flag, got[0].attrs)
			}
		})
	}
	cache.ResetRetiredFlagAuditForTest()
	log, recs := newCaptureLogger()
	cache.AuditRetiredFlags(log)
	if got := findAuditRecords(*recs, flag); len(got) != 0 {
		t.Fatalf("absent %s emitted %d line(s)", flag, len(got))
	}
}
