// informer_serve_summary_stop_348_test.go — #348 goroutine-leak arm for
// startObjectsGetSummary, the objects-get sibling of the dispatch-summary
// #221/#329 fix.
//
// #368 — the start/stop assertions read the emitter's OWN lifecycle state
// (ObjectsGetSummaryRunningForTest, backed by the stop channel the sync.Once
// installs) instead of a process-global runtime.NumGoroutine() delta. The
// absolute count is contaminated by neighbour-test goroutine churn in the
// shared package binary (a +1 from THIS emitter can be masked by an unrelated
// goroutine exiting in the sample window), which made the old `n > base` /
// `n <= base` checks flaky under -race. The join itself is still proven by
// stopObjectsGetSummaryForTest's WaitGroup.Wait(): an unreachable stop blocks it
// forever → test timeout (the real RED for the leak property). Only the flaky
// liveness SIGNAL is replaced; the reachable-stop contract is unchanged.

package objects

import (
	"testing"
)

// TestObjectsGetSummary_GoroutineStops is the #348 goroutine-leak arm: the
// summary goroutine must have a REACHABLE stop. It starts the loop, proves the
// emitter reports running, then stops it via the test seam and asserts the
// emitter reports stopped — stopObjectsGetSummaryForTest joins the WaitGroup, so
// a stopped report guarantees the goroutine frame is gone (no leak). On pre-#348
// code the loop is `for range t.C` with an unreachable `defer t.Stop()` and NO
// stop seam, so this arm cannot even compile against it (stop API absent) — that
// is the RED. If the select-on-stop is not wired, stopObjectsGetSummaryForTest's
// join blocks forever and the test times out.
func TestObjectsGetSummary_GoroutineStops(t *testing.T) {
	// Short interval so the goroutine is unambiguously running; the value is
	// irrelevant to the stop path, which is signal-driven, not tick-driven.
	t.Setenv(envObjectsGetSummaryEvery, "1")

	// Clean slate in case an earlier test in this binary already started it.
	stopObjectsGetSummaryForTest()
	if ObjectsGetSummaryRunningForTest() {
		t.Fatalf("precondition: emitter still reports running after stop")
	}

	startObjectsGetSummary()
	if !ObjectsGetSummaryRunningForTest() {
		t.Fatalf("summary goroutine did not start: ObjectsGetSummaryRunningForTest()=false")
	}

	// The stop must join the goroutine; if the stop is unreachable this call
	// blocks forever and the test times out.
	stopObjectsGetSummaryForTest()
	if ObjectsGetSummaryRunningForTest() {
		t.Fatalf("summary goroutine did not exit after stop: ObjectsGetSummaryRunningForTest()=true")
	}

	// The Once must have been reset so a subsequent production start relaunches
	// the goroutine (lifecycle contract preserved, restart-capable).
	startObjectsGetSummary()
	if !ObjectsGetSummaryRunningForTest() {
		t.Fatalf("summary goroutine did not restart after reset: ObjectsGetSummaryRunningForTest()=false")
	}
	stopObjectsGetSummaryForTest()
	if ObjectsGetSummaryRunningForTest() {
		t.Fatalf("summary goroutine did not exit after second stop: ObjectsGetSummaryRunningForTest()=true")
	}
}
