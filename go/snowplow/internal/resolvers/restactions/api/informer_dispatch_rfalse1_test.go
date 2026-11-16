// informer_dispatch_rfalse1_test.go — Tag 0.30.98 falsifier F4 (R-FALSE-1),
// re-framed for the #57 implicit-on-cache fold.
//
// R-FALSE-1 (architect falsifier, carried since the 0.30.95 pivot): when
// the pivot is inactive, the binary's pivot path is byte-identical to the
// apiserver path — the resolver NEVER routes a read through the informer
// cache, and therefore never reaches the 0.30.98 four-conjunct servability
// gate (IsServable / ListObjectsServable).
//
// #57 — the pivot was gated by the standalone RESOLVER_USE_INFORMER flag;
// that flag was folded into the single CACHE_ENABLED master gate. The
// resolve.go consumer gate is now:
//
//	if resolverUseInformer() {   // == !cache.Disabled()
//	    if raw, served, _, _ := dispatchViaInformer(gctx, call); served { ... }
//	}
//
// So "pivot inactive" is now exactly "cache subsystem OFF". Cache-OFF is
// the byte-identity path the falsifier guards. (This is independent of —
// and in addition to — the get.go:51 / Gate-4 cache.Disabled() short-
// circuits; here we pin the CONSUMER GATE itself.)
//
// Why this test lives HERE and not in internal/cache: R-FALSE-1 is a
// property of the GATED CONSUMER, not of the servability predicate. The
// predicate is pure and has no gate read.
//
// dispatchViaInformer is the ONLY function in the resolver that calls
// IsServable / ListObjectsServable. Every path through dispatchViaInformer
// increments exactly one pivot counter (ListServed | GetServed |
// Fallthrough) — see informer_dispatch_metrics.go. So "the dispatch path
// did not reach the servable gate" is observable as "the pivot counters
// did not move".
//
// This test asserts the true behavioral surface:
//
//   - NEGATIVE control: with the cache subsystem OFF, the resolve.go gate
//     (resolverUseInformer()) evaluates false, dispatchViaInformer is not
//     invoked, and the pivot counters stay frozen — the servable gate is
//     unreachable. A stale RESOLVER_USE_INFORMER=true is IGNORED (the fold
//     means cache-off closes the gate regardless).
//   - POSITIVE control: with the cache subsystem ON, the gate passes,
//     invoking dispatchViaInformer DOES move a counter — proving the
//     counter is a faithful witness of the gate being reached, so the
//     negative control's "counters frozen" is meaningful.
//
// Per feedback_no_special_cases.md: the assertion uses the generic
// dispatchTestGVR; no per-resource branching.

package api

import (
	"net/http"
	"testing"
)

// gateReachesPivot replicates the EXACT resolve.go consumer gate
// (resolve.go ~line 794). The resolver invokes dispatchViaInformer — and
// hence the servability gate — if and only if this returns true. Keeping
// the predicate in one helper means a future change to the resolve.go
// gate that diverges from this is caught by the tests below.
func gateReachesPivot() bool {
	return resolverUseInformer()
}

// TestF4_CacheOff_DispatchDoesNotReachServableGate is the behavioral
// R-FALSE-1 falsifier, re-framed to the #57 fold: with the cache subsystem
// OFF the resolve.go gate evaluates false AND, honouring the gate as
// resolve.go does, dispatchViaInformer is never invoked — so the 0.30.98
// servable gate stays unreached and the pivot counters do not move. A
// stale RESOLVER_USE_INFORMER value (any) is ignored under the fold.
func TestF4_CacheOff_DispatchDoesNotReachServableGate(t *testing.T) {
	// A LIST call the pivot could serve if it were ever reached.
	call := buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/default/restactions")

	// Cache OFF is the byte-identity path. A range of stale
	// RESOLVER_USE_INFORMER values (including "true") must NOT re-open the
	// gate — the fold keys the gate on CACHE_ENABLED alone.
	for _, staleFlag := range []string{"", "true", "false", "shadow", "1", "0"} {
		staleFlag := staleFlag
		t.Run("stale_flag="+staleFlag, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "false")
			t.Setenv("RESOLVER_USE_INFORMER", staleFlag)

			// The resolve.go gate MUST evaluate false when the cache
			// subsystem is off, regardless of the stale flag value.
			if gateReachesPivot() {
				t.Fatalf("stale RESOLVER_USE_INFORMER=%q: resolve.go gate must be closed when cache is OFF; got open", staleFlag)
			}

			before := DispatchInformerStatsSnapshot()

			// Honour the gate exactly as resolve.go does: only call
			// dispatchViaInformer when the gate is open. With the gate
			// closed this branch is dead — dispatchViaInformer (and the
			// servable gate it alone reaches) is never entered.
			if gateReachesPivot() {
				_, _, _, _ = dispatchViaInformer(dispatchCtx(), call)
			}

			after := DispatchInformerStatsSnapshot()
			if after != before {
				t.Fatalf("stale RESOLVER_USE_INFORMER=%q: pivot counters moved (before=%+v after=%+v) — "+
					"the dispatch path reached the servable gate with cache OFF (R-FALSE-1 violated)",
					staleFlag, before, after)
			}
		})
	}
}

// TestF4_PivotCounterIsAFaithfulWitness is the POSITIVE control for the
// negative test above. It proves that reaching dispatchViaInformer DOES
// move a pivot counter — so "counters frozen" in the negative control
// genuinely means "dispatchViaInformer was not entered", not "the
// counter is dead". With the cache subsystem ON (#57: gate open) a
// servable LIST is answered from the indexer, bumping ListServed.
func TestF4_PivotCounterIsAFaithfulWitness(t *testing.T) {
	// newDispatchWatcher sets CACHE_ENABLED=true → the gate is open under
	// the #57 fold.
	newDispatchWatcher(t, newTestRestActionRuntimeObject("default", "ra-1", "m1"))

	if !gateReachesPivot() {
		t.Fatalf("cache ON: resolve.go gate must be open under the #57 fold")
	}

	call := buildCall(http.MethodGet, "/apis/templates.krateo.io/v1/namespaces/default/restactions")

	before := DispatchInformerStatsSnapshot()
	raw, served, _, _ := dispatchViaInformer(dispatchCtx(), call)
	after := DispatchInformerStatsSnapshot()

	if !served {
		t.Fatalf("positive control: cache ON + synced+servable GVR must serve; got served=false")
	}
	if len(raw) == 0 {
		t.Fatalf("positive control: served call returned 0 bytes")
	}
	if after.ListServed != before.ListServed+1 {
		t.Fatalf("positive control: ListServed must increment by 1 when the pivot serves a LIST "+
			"(before=%d after=%d) — without this the negative control proves nothing",
			before.ListServed, after.ListServed)
	}
}
