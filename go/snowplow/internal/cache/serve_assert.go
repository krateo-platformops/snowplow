// serve_assert.go — Convergence hardening #1 (solidity map row #2,
// docs/architecture-solidity-hardening-2026-06-26.md §2/§4).
//
// THE INVARIANT (the single most load-bearing one in the cache, and
// until now trusted-by-construction rather than asserted):
//
//	serve(gvr) ⇒ registered ∧ HasSynced ∧ watchHealthy ∧ typeConfirmed
//	            ( == servableLocked(gvr) )
//
// i.e. an authoritative cache HIT is NEVER returned from an informer that
// is not servable. A not-synced / watch-broken / unconfirmed informer must
// fall through to the apiserver, not serve stale/partial/empty data as if
// authoritative.
//
// WHY ASSERT IT NOW: Fix 1b (the bounded sync barrier) deliberately flips
// /readyz to 200 with a LARGER population of registered-but-not-synced
// informers at readiness (the "good enough" set deferred to lazy-register
// fallthrough + refresher). That enlarges the surface where a future serve
// path could read a not-synced informer's indexer and serve it. The serve
// sites (GetObject / GetTypedObject / ListObjectsServable) are the place to
// machine-check the invariant — and doing it UNDER the same rw.mu hold as
// the indexer read also closes the documented GET check-then-act gap
// (informer_dispatch.go:521 — IsServable and GetObject were two separate
// lock acquisitions; the informer's state could flip between them).
//
// APPARATUS: reuses the AssertReadPathsScoped test/prod asymmetry
// (fallthrough_assert.go) and the snowplow_assertion_violations_total
// expvar map (fallthrough_meter_expvar.go), adding the
// check="serve_requires_servable" label:
//
//   - env.TestMode()==true  → panic. A test that wires a serve from a
//     not-servable informer fails loud.
//   - env.TestMode()==false → ERROR log + serveRequiresServableViolations
//     .Add(1) + the caller FALLS THROUGH (returns a miss) so unsynced data
//     is never actually served. Pod stays up; the violation is alertable.
//
// Cost-proportional: the check runs only on a confirmed indexer HIT, AFTER
// the cache-hit lookup, under a lock the serve already holds — it adds no
// new unbounded path (feedback_bounding_mechanism_discipline). On the
// common (servable) path it is a single map+bool predicate already computed
// by servableLocked.
package cache

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/env"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// serveRequiresServableViolations is the production-mode counter bumped
// when an authoritative serve was attempted against a not-servable
// informer. Exposed via the snowplow_assertion_violations_total expvar
// under check="serve_requires_servable" (fallthrough_meter_expvar.go).
var serveRequiresServableViolations atomic.Uint64

// ServeRequiresServableViolations returns the cumulative count of
// serve-requires-servable violations observed in production mode. Exported
// for the falsifier test gate.
func ServeRequiresServableViolations() uint64 {
	return serveRequiresServableViolations.Load()
}

// ResetServeRequiresServableViolationsForTest zeroes the counter. Test-only
// — production never resets it (operators threshold on the cumulative
// expvar).
func ResetServeRequiresServableViolationsForTest() {
	serveRequiresServableViolations.Store(0)
}

// AssertServeRequiresServableForTest drives the serve-requires-servable
// assertion directly (acquiring rw.mu) so the falsifier can exercise the
// violation path WITHOUT a serve site that bypasses servableLocked — which
// is impossible to construct through the real GetObject/ListObjectsServable
// flow (they gate on servableLocked first, by design). This is the only
// way to prove the guard fires; production code never calls it.
func (rw *ResourceWatcher) AssertServeRequiresServableForTest(gvr schema.GroupVersionResource, servePath string) bool {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.assertServeRequiresServableLocked(context.Background(), gvr, servePath)
}

// assertServeRequiresServableLocked asserts the serve(gvr)⇒servable
// invariant at an authoritative serve site. Callers MUST hold rw.mu (read
// or write) — it re-reads servableLocked under that hold so the predicate
// reflects the EXACT instant the indexer is about to be read (no
// check-then-act gap).
//
// Returns true when the serve may proceed (gvr is servable). Returns false
// when the invariant is violated and the caller MUST fall through instead
// of serving:
//
//   - test mode → panic (loud failure; this should be impossible by
//     construction, so a test reaching here is a real regression).
//   - prod mode → count + ERROR log + return false (the caller falls
//     through to the apiserver; unsynced data is never served).
//
// servePath is a short, closed-enum label of the serve site for the log
// (e.g. "GetObject", "GetTypedObject", "ListObjectsServable") — never
// per-GVR/per-name, so no special-casing (feedback_no_special_cases).
func (rw *ResourceWatcher) assertServeRequiresServableLocked(ctx context.Context, gvr schema.GroupVersionResource, servePath string) bool {
	if _, ok := rw.servableLocked(gvr); ok {
		return true // invariant holds — the common path.
	}

	if env.TestMode() {
		panic("cache.assertServeRequiresServable: authoritative serve attempted from a NOT-servable informer for gvr=" +
			gvr.String() + " at " + servePath +
			" — serve(gvr) MUST imply registered∧HasSynced∧watchHealthy∧typeConfirmed; " +
			"a not-servable informer MUST fall through to the apiserver, never serve as authoritative")
	}

	serveRequiresServableViolations.Add(1)
	slog.ErrorContext(ctx, "cache.serve_requires_servable.violation",
		slog.String("subsystem", "cache"),
		slog.String("check", "serve_requires_servable"),
		slog.String("gvr", gvr.String()),
		slog.String("serve_path", servePath),
		slog.String("hint", "an authoritative cache HIT was about to be served from a not-servable "+
			"informer (not-synced / watch-broken / unconfirmed) — falling through to the apiserver "+
			"instead; this is the never-serve-from-not-synced invariant firing"),
	)
	return false
}
