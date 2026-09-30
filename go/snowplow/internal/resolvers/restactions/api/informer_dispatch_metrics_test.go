// informer_dispatch_metrics_test.go — Tag 0.30.96: tests for the stable
// serve-rate counters added to the 0.30.95 `dispatchViaInformer` pivot.
//
// The 0.30.95 pivot emitted per-call debug lines only; the Phase 6 bench
// could not measure serve-rate at 50K volume. 0.30.96 adds atomic
// counters + an `informer_dispatch.summary` line. These tests assert the
// counters increment on the served / fallthrough paths and that the
// summary snapshot exposes the stable shape.

package api

import (
	"net/http"
	"runtime"
	"testing"
	"time"
)

// waitGoroutineCount polls runtime.NumGoroutine() until pred(n) holds or
// the deadline elapses; it returns whether pred was satisfied. Used by the
// summary-goroutine lifecycle test to observe the goroutine start and then
// return to baseline after a stop, tolerating scheduler lag on Done().
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

// TestDispatchSummary_GoroutineStops is the #221 goroutine-leak arm: the
// summary goroutine must have a REACHABLE stop. It starts the loop, proves
// the goroutine is alive, then stops it via the test seam and asserts the
// goroutine actually exits — NumGoroutine returns to baseline and the
// joined WaitGroup guarantees the frame is gone (no leak). On pre-#221 code
// the loop is `for range t.C` with an unreachable `defer t.Stop()` and NO
// stop seam, so this arm cannot even be written against it (stop API
// absent) — that is the RED.
func TestDispatchSummary_GoroutineStops(t *testing.T) {
	// Short interval so the goroutine is unambiguously running; the value
	// is irrelevant to the stop path, which is signal-driven, not tick-driven.
	t.Setenv(envDispatchSummaryEvery, "1")

	// Clean slate in case an earlier test in this binary already started it.
	stopDispatchSummaryForTest()

	base := runtime.NumGoroutine()
	startDispatchSummary()

	if !waitGoroutineCount(func(n int) bool { return n > base }, time.Second) {
		t.Fatalf("summary goroutine did not start: NumGoroutine stayed at baseline %d", base)
	}
	afterStart := runtime.NumGoroutine()
	t.Logf("goroutines: baseline=%d after-start=%d (delta=%+d)", base, afterStart, afterStart-base)

	// The stop must join the goroutine; if the stop is unreachable this
	// call blocks forever and the test times out.
	stopDispatchSummaryForTest()

	if !waitGoroutineCount(func(n int) bool { return n <= base }, 2*time.Second) {
		t.Fatalf("summary goroutine did not exit after stop: NumGoroutine=%d baseline=%d", runtime.NumGoroutine(), base)
	}
	t.Logf("goroutines: after-stop=%d baseline=%d (back to baseline)", runtime.NumGoroutine(), base)

	// The Once must have been reset so a subsequent production start
	// relaunches the goroutine (lifecycle contract preserved).
	startDispatchSummary()
	if !waitGoroutineCount(func(n int) bool { return n > base }, time.Second) {
		t.Fatalf("summary goroutine did not restart after reset: NumGoroutine stayed at baseline %d", base)
	}
	stopDispatchSummaryForTest()
	if !waitGoroutineCount(func(n int) bool { return n <= base }, 2*time.Second) {
		t.Fatalf("summary goroutine did not exit after second stop: NumGoroutine=%d baseline=%d", runtime.NumGoroutine(), base)
	}
}

// resetDispatchCounters zeroes the package-level pivot counters so each
// test asserts deltas from a clean slate.
func resetDispatchCounters() {
	dispatchInformerListServed.Store(0)
	dispatchInformerGetServed.Store(0)
	dispatchInformerFallthrough.Store(0)
}

// TestDispatchCounters_GetServed — a synced informer GET-by-name hit
// increments dispatchInformerGetServed and nothing else.
func TestDispatchCounters_GetServed(t *testing.T) {
	resetDispatchCounters()
	newDispatchWatcher(t, newTestRestActionRuntimeObject("default", "alpha", "m"))

	call := buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/default/restactions/alpha")
	if _, served := dispatchViaInformer(dispatchCtx(), call); !served {
		t.Fatalf("GET: expected served=true")
	}
	s := DispatchInformerStatsSnapshot()
	if s.GetServed != 1 {
		t.Fatalf("GetServed want 1; got %d", s.GetServed)
	}
	if s.ListServed != 0 || s.Fallthrough != 0 {
		t.Fatalf("GET: ListServed/Fallthrough must stay 0; got list=%d fallthrough=%d", s.ListServed, s.Fallthrough)
	}
}

// TestDispatchCounters_ListServed — a synced informer LIST hit
// increments dispatchInformerListServed.
func TestDispatchCounters_ListServed(t *testing.T) {
	resetDispatchCounters()
	newDispatchWatcher(t,
		newTestRestActionRuntimeObject("default", "a", "alpha"),
		newTestRestActionRuntimeObject("default", "b", "bravo"),
	)

	call := buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/default/restactions")
	if _, served := dispatchViaInformer(dispatchCtx(), call); !served {
		t.Fatalf("LIST: expected served=true")
	}
	s := DispatchInformerStatsSnapshot()
	if s.ListServed != 1 {
		t.Fatalf("ListServed want 1; got %d", s.ListServed)
	}
	if s.GetServed != 0 || s.Fallthrough != 0 {
		t.Fatalf("LIST: GetServed/Fallthrough must stay 0; got get=%d fallthrough=%d", s.GetServed, s.Fallthrough)
	}
}

// TestDispatchCounters_Fallthrough — a write verb takes the apiserver
// fallthrough branch; the fallthrough counter increments and no served
// counter moves.
func TestDispatchCounters_Fallthrough(t *testing.T) {
	resetDispatchCounters()
	newDispatchWatcher(t, newTestRestActionRuntimeObject("default", "alpha", "m"))

	// Write verb → Gate 1 fallthrough.
	call := buildCall(http.MethodPost, "/apis/templates.krateo.io/v1/namespaces/default/restactions")
	if _, served := dispatchViaInformer(dispatchCtx(), call); served {
		t.Fatalf("POST: expected served=false (fallthrough)")
	}
	// Subresource path → Gate 2 fallthrough.
	call = buildCall(http.MethodGet, "/apis/apps/v1/namespaces/default/deployments/foo/status")
	if _, served := dispatchViaInformer(dispatchCtx(), call); served {
		t.Fatalf("subresource: expected served=false (fallthrough)")
	}
	// GET-miss → Gate-after-sync fallthrough.
	call = buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/default/restactions/missing")
	if _, served := dispatchViaInformer(dispatchCtx(), call); served {
		t.Fatalf("GET-miss: expected served=false (fallthrough)")
	}

	s := DispatchInformerStatsSnapshot()
	if s.Fallthrough != 3 {
		t.Fatalf("Fallthrough want 3 (verb + subresource + get-miss); got %d", s.Fallthrough)
	}
	if s.ListServed != 0 || s.GetServed != 0 {
		t.Fatalf("fallthrough-only run: served counters must stay 0; got list=%d get=%d", s.ListServed, s.GetServed)
	}
}

// TestDispatchCounters_MixedTotals — a representative mix of served +
// fallthrough calls; the counters sum exactly to the call count, so the
// bench can compute serve-rate = (list+get)/(list+get+fallthrough).
func TestDispatchCounters_MixedTotals(t *testing.T) {
	resetDispatchCounters()
	newDispatchWatcher(t,
		newTestRestActionRuntimeObject("default", "a", "alpha"),
		newTestRestActionRuntimeObject("default", "b", "bravo"),
	)

	served := []string{
		"/apis/templates.krateo.io/v1/namespaces/default/restactions",   // LIST
		"/apis/templates.krateo.io/v1/namespaces/default/restactions/a", // GET
		"/apis/templates.krateo.io/v1/namespaces/default/restactions/b", // GET
	}
	for _, p := range served {
		if _, ok := dispatchViaInformer(dispatchCtx(), buildCall(http.MethodGet, p)); !ok {
			t.Fatalf("expected served=true for %s", p)
		}
	}
	fallthroughs := []string{
		"/apis/templates.krateo.io/v1/namespaces/default/restactions/nope", // GET-miss
		"https://external.invalid/x",                                       // external
	}
	for _, p := range fallthroughs {
		if _, ok := dispatchViaInformer(dispatchCtx(), buildCall(http.MethodGet, p)); ok {
			t.Fatalf("expected served=false for %s", p)
		}
	}

	s := DispatchInformerStatsSnapshot()
	total := s.ListServed + s.GetServed + s.Fallthrough
	if total != uint64(len(served)+len(fallthroughs)) {
		t.Fatalf("counter total want %d; got %d (list=%d get=%d fallthrough=%d)",
			len(served)+len(fallthroughs), total, s.ListServed, s.GetServed, s.Fallthrough)
	}
	if s.ListServed != 1 || s.GetServed != 2 || s.Fallthrough != 2 {
		t.Fatalf("mixed totals: want list=1 get=2 fallthrough=2; got list=%d get=%d fallthrough=%d",
			s.ListServed, s.GetServed, s.Fallthrough)
	}
}

// TestDispatchSummaryEverySeconds covers the summary-interval env knob:
// unset / non-int / non-positive all fall back to the default.
func TestDispatchSummaryEverySeconds(t *testing.T) {
	cases := []struct {
		env  string
		want int
	}{
		{"", defaultDispatchSummarySeconds},
		{"not-a-number", defaultDispatchSummarySeconds},
		{"0", defaultDispatchSummarySeconds},
		{"-5", defaultDispatchSummarySeconds},
		{"30", 30},
		{"120", 120},
	}
	for _, tc := range cases {
		t.Run("env="+tc.env, func(t *testing.T) {
			t.Setenv(envDispatchSummaryEvery, tc.env)
			if got := dispatchSummaryEverySeconds(); got != tc.want {
				t.Fatalf("dispatchSummaryEverySeconds(): want %d; got %d", tc.want, got)
			}
		})
	}
}

// TestDispatchInformerStatsSnapshot_Independence confirms the snapshot
// reads each counter independently — bumping one field must not perturb
// the others (guards against a copy-paste atomic mixup).
func TestDispatchInformerStatsSnapshot_Independence(t *testing.T) {
	resetDispatchCounters()
	dispatchInformerListServed.Store(11)
	dispatchInformerGetServed.Store(22)
	dispatchInformerFallthrough.Store(33)
	defer resetDispatchCounters()

	s := DispatchInformerStatsSnapshot()
	if s.ListServed != 11 || s.GetServed != 22 || s.Fallthrough != 33 {
		t.Fatalf("snapshot mixup: want list=11 get=22 fallthrough=33; got list=%d get=%d fallthrough=%d",
			s.ListServed, s.GetServed, s.Fallthrough)
	}
}
