//go:build falsifier_268_267

// informer_serve_nonreg_268_test.go — NON-REGRESSION arm for the SA-serve fix
// bundle: the informer-served (content-cache) path is INDEPENDENT of the SA
// endpoint attach and must stay byte-identical after Part 1 removes it.
//
// A per-user MISS whose stage is an informer-servable LIST is served from the
// in-memory informer store (dispatchViaInformer / apistage content) and RBAC-
// narrowed per requester — it never dials call.Endpoint. The f1 harness proves
// this by pointing the (unused) endpoint at http://test.invalid: if the resolver
// ever fell through to an external dial, the LIST would error, not narrow. So a
// correct narrowed result IS the proof the informer served it — a property Part 1
// (which only removes the SA endpoint from the live ctx) must preserve.
//
// GREEN now, MUST STAY GREEN post-fix (feedback_phase6_validates_l1_always_hit,
// feedback_cache_must_not_constrain_jq).
package api

import (
	"testing"
)

// TestInformerServe_NonReg_ContentServedAndNarrowed_268 — the broad user sees
// widgets in every namespace; the narrow user sees only their authorized subset —
// both served from the informer, neither dialing the SA (or any) endpoint.
func TestInformerServe_NonReg_ContentServedAndNarrowed_268(t *testing.T) {
	newF1Watcher(t)

	broad := f1WidgetNamesIn(f1ResolveAs(t, f1BroadUser, f1ListStage("wids"))["wids"])
	narrow := f1WidgetNamesIn(f1ResolveAs(t, f1NarrowUser, f1ListStage("wids"))["wids"])

	t.Logf("NON-REG informer-serve: broad=%v narrow=%v", f1SortedKeys(broad), f1SortedKeys(narrow))

	// The broad user is granted every seeded namespace → the informer serves all.
	if len(broad) != len(f1AllNamespaces) {
		t.Fatalf("NON-REG: broad user must be served every namespace's widget from the informer; got %v want %d",
			f1SortedKeys(broad), len(f1AllNamespaces))
	}
	// The narrow user is granted only f1NarrowNamespaces → the informer narrows.
	if len(narrow) != len(f1NarrowNamespaces) {
		t.Fatalf("NON-REG: narrow user must be RBAC-narrowed to %d namespaces by the informer serve; got %v",
			len(f1NarrowNamespaces), f1SortedKeys(narrow))
	}
	// Narrowing is a strict subset (the informer path is per-user, not SA-maximal).
	for n := range narrow {
		if !broad[n] {
			t.Fatalf("NON-REG: narrow result %q not in the broad result — narrowing must be a subset of the served set", n)
		}
	}
	if len(narrow) >= len(broad) {
		t.Fatalf("NON-REG: the narrow user must see FEWER widgets than the broad user (per-user narrowing); broad=%v narrow=%v",
			f1SortedKeys(broad), f1SortedKeys(narrow))
	}
	t.Logf("NON-REG GREEN: the informer content-serve is per-user narrowed and never dialed an endpoint (Part 1 preserves it).")
}
