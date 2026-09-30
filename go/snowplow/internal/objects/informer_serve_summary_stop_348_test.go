// informer_serve_summary_stop_348_test.go — #348 goroutine-leak arm for
// startObjectsGetSummary, the objects-get sibling of the dispatch-summary
// #221/#329 fix. Kept in its own file because informer_serve_test.go aliases
// k8s.io/apimachinery/pkg/runtime as `runtime`, which would shadow the stdlib
// runtime this arm needs for NumGoroutine.

package objects

import (
	"runtime"
	"testing"
	"time"
)

// waitGoroutineCount polls runtime.NumGoroutine() until pred(n) holds or the
// deadline elapses; returns whether pred was finally satisfied. Mirrors the
// dispatch-summary #221 arm's helper, tolerating scheduler lag on Done().
func waitGoroutineCount(pred func(n int) bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if pred(runtime.NumGoroutine()) {
			return true
		}
		if time.Now().After(deadline) {
			return pred(runtime.NumGoroutine())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestObjectsGetSummary_GoroutineStops is the #348 goroutine-leak arm: the
// summary goroutine must have a REACHABLE stop. It starts the loop, proves the
// goroutine is alive, then stops it via the test seam and asserts the goroutine
// actually exits — NumGoroutine returns to baseline and the joined WaitGroup
// guarantees the frame is gone (no leak). On pre-#348 code the loop is
// `for range t.C` with an unreachable `defer t.Stop()` and NO stop seam, so
// this arm cannot even compile against it (stop API absent) — that is the RED.
// If the select-on-stop is not wired, stopObjectsGetSummaryForTest's join
// blocks forever and the test times out.
func TestObjectsGetSummary_GoroutineStops(t *testing.T) {
	// Short interval so the goroutine is unambiguously running; the value is
	// irrelevant to the stop path, which is signal-driven, not tick-driven.
	t.Setenv(envObjectsGetSummaryEvery, "1")

	// Clean slate in case an earlier test in this binary already started it.
	stopObjectsGetSummaryForTest()

	base := runtime.NumGoroutine()
	startObjectsGetSummary()

	if !waitGoroutineCount(func(n int) bool { return n > base }, time.Second) {
		t.Fatalf("summary goroutine did not start: NumGoroutine stayed at baseline %d", base)
	}
	afterStart := runtime.NumGoroutine()
	t.Logf("goroutines: baseline=%d after-start=%d (delta=%+d)", base, afterStart, afterStart-base)

	// The stop must join the goroutine; if the stop is unreachable this call
	// blocks forever and the test times out.
	stopObjectsGetSummaryForTest()

	if !waitGoroutineCount(func(n int) bool { return n <= base }, 2*time.Second) {
		t.Fatalf("summary goroutine did not exit after stop: NumGoroutine=%d baseline=%d", runtime.NumGoroutine(), base)
	}
	t.Logf("goroutines: after-stop=%d baseline=%d (back to baseline)", runtime.NumGoroutine(), base)

	// The Once must have been reset so a subsequent production start relaunches
	// the goroutine (lifecycle contract preserved, restart-capable).
	startObjectsGetSummary()
	if !waitGoroutineCount(func(n int) bool { return n > base }, time.Second) {
		t.Fatalf("summary goroutine did not restart after reset: NumGoroutine stayed at baseline %d", base)
	}
	stopObjectsGetSummaryForTest()
	if !waitGoroutineCount(func(n int) bool { return n <= base }, 2*time.Second) {
		t.Fatalf("summary goroutine did not exit after second stop: NumGoroutine=%d baseline=%d", runtime.NumGoroutine(), base)
	}
}
