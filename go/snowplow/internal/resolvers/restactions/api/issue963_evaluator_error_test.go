package api

// issue963_evaluator_error_test.go — #963 review F1: when the RBAC evaluator
// errors, the served-LIST gate decides NOTHING in process. It falls through to
// the caller's own apiserver, whose answer (here a 403) is the authoritative
// one, and it serves no item from snowplow's copy.
//
// The arm drives the REAL api.ResolveWithFailure on the content and inline
// paths with the RBAC snapshot unpublished, so every EvaluateRBAC errors, for a
// caller with NO grant listing one namespace. Under the mutation "evaluator
// error = allowed" (review R3), the gate served team-a's items with no dial.
// With the per-item memo no longer seeded from the scope verdict (#963 F1),
// that mutation no longer leaks items (the per-item step, failing closed on
// the same error, drops them), and this arm still reads RED on the missing
// fall-through.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestR963EvaluatorErrorFallsThrough(t *testing.T) {
	rw := watcher961(t)
	restore := cache.DisableRBACSnapshotForTest()
	t.Cleanup(restore)
	for _, arm := range []arm961{armContent, armInline} {
		got := resolve961(t, rw, arm, u961None, p961TeamA, true, apiAnswer961{forbidden: true})
		if got.value != "" && got.value != "null" {
			if names := itemNames961(t, got.value); len(names) > 0 {
				t.Errorf("%s: LEAK — a caller with no grant was served %v while the evaluator errored", arm, names)
			}
		}
		if got.dials < 1 {
			t.Errorf("%s: evaluator error was decided in process (dials=%d), want a fall-through", arm, got.dials)
		}
	}
}
