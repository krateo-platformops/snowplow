// objects_summary_quiesce_348_test.go — #348 cross-package coverage arm:
// quiesceDefaultSummaryForTest (and therefore captureDefaultSlogForTest) must
// join the objects-get summary emitter, not only the dispatch one. Without the
// objects.ResetObjectsGetSummaryForTest wiring this arm REDs (the goroutine
// stays alive past the quiesce), which is precisely the leaked-goroutine-vs-
// unguarded-default-slog-capture race #348 fixes for the objects sibling.
//
// #368 — order/timing independence. The liveness and join assertions read the
// objects package's OWN emitter lifecycle state
// (objects.ObjectsGetSummaryRunningForTest, backed by the stop channel the
// sync.Once installs), NOT a process-global runtime.NumGoroutine() delta. The
// global goroutine count is contaminated by unrelated background-goroutine
// churn from the rest of the dispatchers package (e.g. internal/cache
// CRD-discovery goroutines winding down): a +1 from THIS emitter can be masked
// by an unrelated goroutine exiting inside the measurement window, so the old
// `NumGoroutine() > base` check failed flakily depending on scheduling — a
// false failure that #218's goroutine-timing shift happened to surface. The
// package-owned accessor targets exactly this emitter and is immune to that
// churn; there is no reset→measure window to race.

package dispatchers

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/objects"
)

// waitSummaryRunning polls the objects-get emitter's OWN lifecycle state until
// it reaches want or the timeout elapses. Order-independent: it reflects only
// this emitter, never the process-global goroutine count.
func waitSummaryRunning(want bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if objects.ObjectsGetSummaryRunningForTest() == want {
			return true
		}
		if time.Now().After(deadline) {
			return objects.ObjectsGetSummaryRunningForTest() == want
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestQuiesceDefaultSummary_JoinsObjectsGetEmitter proves the capture-quiesce
// seam enumerates the objects-get emitter: start that goroutine (fast ticker),
// confirm it is alive via the emitter's own state, then call
// quiesceDefaultSummaryForTest and assert it is joined (the emitter reports
// stopped). A helper that quiesced only the dispatch emitter would leave this
// goroutine running → the emitter stays "running" → RED.
func TestQuiesceDefaultSummary_JoinsObjectsGetEmitter(t *testing.T) {
	// Fast interval so the goroutine is unambiguously running; the stop path is
	// signal-driven, so the value does not affect the join.
	t.Setenv("OBJECTS_GET_SUMMARY_EVERY_SECONDS", "1")

	// Clean slate in case an earlier test in this binary left it running, then
	// start. Asserted via the objects package's OWN state — no reset→measure
	// window, no global NumGoroutine sample (#368).
	objects.ResetObjectsGetSummaryForTest()
	objects.StartObjectsGetSummaryForTest()
	if !objects.ObjectsGetSummaryRunningForTest() {
		t.Fatalf("objects-get summary emitter did not start: ObjectsGetSummaryRunningForTest()=false after StartObjectsGetSummaryForTest()")
	}

	// The seam under test: must stop + join the objects-get emitter. A helper
	// that quiesced only the dispatch emitter would leave this one running → RED.
	quiesceDefaultSummaryForTest(t)

	if !waitSummaryRunning(false, 2*time.Second) {
		t.Fatalf("quiesceDefaultSummaryForTest did not join the objects-get emitter: ObjectsGetSummaryRunningForTest()=true — the leaked ticker can still race a default-slog capture (#348)")
	}
}
