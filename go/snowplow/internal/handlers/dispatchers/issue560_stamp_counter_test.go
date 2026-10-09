//go:build unit || integration

package dispatchers

import (
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestIssue560_StampCounterCountsExactlyTheArmedResponses pins the arm counter
// to the SAME condition that actually arms a browser.
//
// The browser subscribes on seeing X-Snowplow-Refresh-Key. So the counter is
// only a useful denominator for #560 if it moves exactly when that header is
// stamped — not more (which would invent arming that never happened) and not
// less (which would hide it). The two gated variants exist precisely to SUPPRESS
// arming in cases where a publisher could never announce the key, so they must
// not count either.
func TestIssue560_StampCounterCountsExactlyTheArmedResponses(t *testing.T) {
	cases := []struct {
		name string
		call func(w *httptest.ResponseRecorder)
		want uint64
	}{
		{
			name: "plain stamp counts",
			call: func(w *httptest.ResponseRecorder) { setRefreshKeyHeader(w, "k1", "widgets") },
			want: 1,
		},
		{
			name: "an empty key is not arming and must not count",
			call: func(w *httptest.ResponseRecorder) { setRefreshKeyHeader(w, "", "widgets") },
			want: 0,
		},
		{
			name: "armable=true counts",
			call: func(w *httptest.ResponseRecorder) {
				setRefreshKeyHeaderIfArmable(w, "k2", "widgets", true)
			},
			want: 1,
		},
		{
			name: "armable=false stamps nothing, so counts nothing",
			call: func(w *httptest.ResponseRecorder) {
				setRefreshKeyHeaderIfArmable(w, "k3", "widgets", false)
			},
			want: 0,
		},
		{
			name: "external-TTL suppression stamps nothing, so counts nothing",
			call: func(w *httptest.ResponseRecorder) {
				setRefreshKeyHeaderUnlessExternal(w, "k4", "widgets", true)
			},
			want: 0,
		},
		{
			name: "non-external counts",
			call: func(w *httptest.ResponseRecorder) {
				setRefreshKeyHeaderUnlessExternal(w, "k5", "widgets", false)
			},
			want: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache.ResetLiveRefreshArmingStatsForTest()
			rec := httptest.NewRecorder()
			tc.call(rec)

			got := cache.LiveRefreshArmingStatsSnapshot().ArmStamped
			if got != tc.want {
				t.Fatalf("ArmStamped=%d want %d", got, tc.want)
			}
			// The counter must agree with the HEADER, which is what the browser
			// actually reads. Counting without stamping (or the reverse) makes
			// the #560 denominator a fiction.
			hdr := rec.Header().Get(refreshKeyHeader)
			if (hdr != "") != (tc.want == 1) {
				t.Fatalf("counter and header disagree: header=%q count=%d — "+
					"the arm counter must track the header exactly", hdr, got)
			}
		})
	}
}
