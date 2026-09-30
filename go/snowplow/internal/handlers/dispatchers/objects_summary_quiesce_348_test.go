// objects_summary_quiesce_348_test.go — #348 cross-package coverage arm:
// quiesceDefaultSummaryForTest (and therefore captureDefaultSlogForTest) must
// join the objects-get summary emitter, not only the dispatch one. Without the
// objects.ResetObjectsGetSummaryForTest wiring this arm REDs (the goroutine
// stays alive past the quiesce), which is precisely the leaked-goroutine-vs-
// unguarded-default-slog-capture race #348 fixes for the objects sibling.

package dispatchers

import (
	"runtime"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/objects"
)

func waitNumGoroutine(pred func(n int) bool, timeout time.Duration) bool {
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

// TestQuiesceDefaultSummary_JoinsObjectsGetEmitter proves the capture-quiesce
// seam enumerates the objects-get emitter: start that goroutine (fast ticker),
// confirm it is alive, then call quiesceDefaultSummaryForTest and assert it is
// joined immediately (back to baseline). A helper that quiesced only the
// dispatch emitter would leave this goroutine running → RED.
func TestQuiesceDefaultSummary_JoinsObjectsGetEmitter(t *testing.T) {
	// Fast interval so the goroutine is unambiguously running; the stop path is
	// signal-driven, so the value does not affect the join.
	t.Setenv("OBJECTS_GET_SUMMARY_EVERY_SECONDS", "1")

	// Clean slate in case an earlier test in this binary left it running.
	objects.ResetObjectsGetSummaryForTest()

	base := runtime.NumGoroutine()
	objects.StartObjectsGetSummaryForTest()
	if !waitNumGoroutine(func(n int) bool { return n > base }, time.Second) {
		t.Fatalf("objects-get summary goroutine did not start: NumGoroutine stayed at baseline %d", base)
	}

	// The seam under test: must stop + join the objects-get emitter synchronously.
	quiesceDefaultSummaryForTest(t)

	if !waitNumGoroutine(func(n int) bool { return n <= base }, 2*time.Second) {
		t.Fatalf("quiesceDefaultSummaryForTest did not join the objects-get emitter: NumGoroutine=%d baseline=%d — the leaked ticker can still race a default-slog capture (#348)", runtime.NumGoroutine(), base)
	}
}
