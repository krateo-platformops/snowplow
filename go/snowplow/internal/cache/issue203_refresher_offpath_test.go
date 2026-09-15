// issue203_refresher_offpath_test.go — #203 (1.12.6 C7): a telemetry read
// under CACHE_ENABLED=false must not construct the refresher.
//
// The defect: refresherStatsSnapshot() / RefresherSelfNotFoundEvictTotal()
// called refresherSingleton(), whose sync.Once builds two client-go
// rate-limiting workqueues; each delaying queue spawns a waitingLoop
// goroutine that never exits. So a /debug/vars scrape or an OTLP collection
// on a cache-off pod created the thing it was measuring, and C1's off-path
// arm had to be narrowed to the bridge read to stay green
// (issue1126_c1_state_derived_test.go, "flagged to the TL, not fixed here").
//
// The arm is a RAW goroutine delta, not a stack-frame count: the process
// count before the reads, the count after, with a bounded settle so
// unrelated goroutines still winding down from a sibling test cannot fail
// it. A goroutine THIS read spawned never exits, so it survives the settle.
// RED on 73808ad: +2 (two waitingLoop goroutines).

package cache

import (
	"expvar"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

// goroutinesQuiesce waits (bounded) until runtime.NumGoroutine() reads the
// same value three samples in a row, then returns it — the baseline for a
// delta arm, taken after sibling tests' goroutines have wound down.
func goroutinesQuiesce(bound time.Duration) int {
	deadline := time.Now().Add(bound)
	last, stable := runtime.NumGoroutine(), 0
	for stable < 2 && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			stable++
		} else {
			last, stable = n, 0
		}
	}
	return last
}

// goroutinesSettleTo waits up to bound for runtime.NumGoroutine() to come
// back down to at most target and returns the last count observed.
func goroutinesSettleTo(target int, bound time.Duration) int {
	deadline := time.Now().Add(bound)
	n := runtime.NumGoroutine()
	for n > target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n
}

func TestIssue203_CacheOff_TelemetryReadConstructsNoRefresher(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	if !Disabled() {
		t.Fatalf("Disabled() false with CACHE_ENABLED=false")
	}
	// A cache-off process never built the singleton; start from that state.
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)
	resetDepWatchForTest()

	before := goroutinesQuiesce(2 * time.Second) // whatever a sibling left behind has wound down

	// Every accessor the two telemetry surfaces read.
	_ = RefresherStatsByStat()
	_ = RefresherSelfNotFoundEvictTotal()
	_, _ = ClusterListRefresherStats()
	_ = DepsStatsByStat()
	_, _, _, _, _, _, _, _, _, _, _, _ = RefresherSnapshot()
	_, _, _, _, _ = RefresherTerminalSnapshot()
	rec := httptest.NewRecorder()
	expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))

	if refresherPeek() != nil {
		t.Fatalf("#203 RED: a telemetry read under CACHE_ENABLED=false constructed the refresher singleton")
	}
	after := goroutinesSettleTo(before, 2*time.Second)
	if after > before {
		t.Fatalf("#203 RED: telemetry reads under CACHE_ENABLED=false left %d extra goroutine(s) running (%d → %d) — a scrape built the workqueues it was reporting on",
			after-before, before, after)
	}
}
