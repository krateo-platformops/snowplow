// servable_s4_test.go — Tag 0.30.98 falsifier + acceptance tests for the
// S4 fix ("post-startup-CRD served as empty").
//
// S4 root cause (regression journal 2026-05-15): the 0.30.95 resolver
// pivot's servability gate was `registered(gvr) AND HasSynced(gvr)`.
// HasSynced() is a one-shot latch — when a GVR's resource *type* did not
// exist in the apiserver's served set at the moment the dynamic informer
// ran its initial LIST (e.g. a CompositionDefinition CRD created minutes
// after pod startup), the informer completes its initial LIST over an
// empty result and HasSynced() latches true forever. The pivot then
// served `[]` as `servable=true`, the freshly-born compositions never
// appeared, and the Compositions feature showed 0.
//
// 0.30.98 reworks `servable(gvr)` into four conjuncts:
//
//	servable(gvr) := registered(gvr)
//	  AND HasSynced(gvr)
//	  AND watchHealthy(gvr)
//	  AND resourceTypeConfirmed(gvr)
//
// Conjunct 3 (watchHealthy) closes F2 — a dropped WATCH connection makes
// the informer store stale; the pivot must fall through to apiserver
// until the reflector reconnects.
//
// Conjunct 4 (resourceTypeConfirmed) closes F1 (S4) — a GVR is servable
// only once its resource *type* is verified present in the apiserver's
// currently-served API surface via the discovery-refresh ticker.
//
// Falsifier protocol (feedback_falsifier_first_before_ship.md):
//
//   - BEHAVIORAL negative control (TestF1_S4_PriorGate_ServesEmptyAsServable):
//     replicates 0.30.97's actual servability predicate inline — the
//     two-line `registered AND HasSynced` check — and asserts it returns
//     servable=TRUE for the exact F1 scenario (a synced-over-empty
//     post-startup CRD) where the new four-conjunct gate returns FALSE.
//     That true-vs-false delta is the proof the new conjunct catches the
//     S4 bug. This is the negative control of record.
//
//   - SUPPORTING evidence: run against 0.30.97 code this file also fails
//     to compile (SetDiscoveryClient / RefreshDiscovery / FireWatchError
//     do not exist) — proof the four-conjunct API is genuinely new, but
//     NOT by itself a behavioral falsifier.
//
// Per feedback_no_special_cases.md: every assertion uses a generic
// customer GVR — the four-conjunct predicate is uniform, no per-GVR
// carve-out, no hardcoded business-GVR list.

package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// s4TestGVR is a customer-style CRD-backed GVR used across the S4 tests.
// It reuses the secrets List-kind registration from servableListKinds so
// the fake dynamic client can serve the informer's initial LIST without
// panicking — the test only cares about the servability *gate*, not the
// object payload, so any registered GVR works.
var s4TestGVR = servableTestGVR

// fakeDiscovery is a minimal discovery double. It satisfies
// cache.ResourceTypeDiscovery — the narrow interface 0.30.98's
// resourceTypeConfirmed check depends on. `served` is the set of
// group/version strings the fake apiserver currently serves a non-empty
// APIResourceList for; toggling it simulates a CRD being installed
// post-startup.
//
// 1.12.6: guarded by a mutex. The map is read by the watcher's async confirm
// prime (primeConfirmAsyncLocked → ConfirmResourceType → resourceTypeServed)
// on its own goroutine while tests toggle it from the test goroutine; the
// unguarded version was a latent fixture data race that -race surfaced once
// the C1 dep-event worker shifted scheduling (same class as the fixture
// races fixed in 1.12.5). Toggle through setServed.
type fakeDiscovery struct {
	mu     sync.RWMutex
	served map[string]bool // groupVersion -> served
}

// setServed flips whether the fake apiserver serves groupVersion.
func (f *fakeDiscovery) setServed(groupVersion string, served bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.served == nil {
		f.served = map[string]bool{}
	}
	f.served[groupVersion] = served
}

func (f *fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	f.mu.RLock()
	served := f.served[groupVersion]
	f.mu.RUnlock()
	if served {
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{{Name: "secrets", Namespaced: true, Kind: "Secret"}},
		}, nil
	}
	// Resource type not (yet) served — empty list, no error. This mirrors
	// a real apiserver returning a 404 / empty set for an un-installed
	// group/version.
	return &metav1.APIResourceList{GroupVersion: groupVersion}, nil
}

// gvString renders a GVR's group/version the way discovery keys it.
func gvString(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Version
	}
	return gvr.Group + "/" + gvr.Version
}

// --- F1 — post-startup-CRD (the S4 fix) -----------------------------------

// priorGateServable009730 replicates the EXACT 0.30.97 servability
// predicate as it stood before the four-conjunct rework. The 0.30.97
// IsServable / ListObjectsServable bodies reduced to:
//
//	gi, ok := rw.informers[gvr]
//	return ok && gi.Informer().HasSynced()
//
// From outside the package we cannot read rw.informers directly, but the
// two-conjunct predicate is observably equivalent to "the GVR was
// registered (EnsureResourceType handed back a sync channel) AND that
// channel has closed (the informer's initial LIST reconciled —
// HasSynced latched true)". `registered` and `syncClosed` are exactly
// those two observable facts. This helper is the inline 0.30.97 gate —
// it deliberately has NO notion of resource-type confirmation, because
// 0.30.97 had none.
func priorGateServable009730(registered, syncClosed bool) bool {
	return registered && syncClosed
}

// TestF1_S4_PriorGate_ServesEmptyAsServable is the BEHAVIORAL negative
// control. It builds the EXACT F1 scenario — a GVR registered, its
// informer synced over an empty fake apiserver (the resource type does
// not yet exist) — and asserts a true-vs-false DELTA:
//
//	0.30.97 inline gate (priorGateServable009730) → servable = TRUE
//	0.30.98 four-conjunct gate (rw.IsServable)    → servable = FALSE
//
// The TRUE from the prior gate IS the S4 bug reproduced behaviorally:
// 0.30.97 would have vouched for this synced-over-empty post-startup CRD
// and the pivot would have served [] as servable=true, zeroing the
// Compositions feature. The FALSE from the new gate is the fix. The
// delta — not a compile failure — is the negative control of record.
func TestF1_S4_PriorGate_ServesEmptyAsServable(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// A discovery client that does NOT serve the test GVR's resource
	// type — the post-startup-CRD-not-yet-installed state.
	disco := &fakeDiscovery{served: map[string]bool{}}
	rw.SetDiscoveryClient(disco)

	// Register the GVR; the informer syncs over the empty fake apiserver.
	registered, syncCh := rw.EnsureResourceType(s4TestGVR)
	if !registered {
		t.Fatalf("EnsureResourceType: want registered=true")
	}
	syncClosed := false
	select {
	case <-syncCh:
		syncClosed = true
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}

	// Drive one discovery refresh against the un-installed-type state so
	// the new gate's conjunct 4 has run.
	rw.RefreshDiscovery(context.Background())

	// PRIOR GATE: registered=true AND syncClosed=true → servable=TRUE.
	// This is the S4 bug — 0.30.97 vouched for the empty post-startup CRD.
	prior := priorGateServable009730(registered, syncClosed)
	if !prior {
		t.Fatalf("negative control invalid: prior 0.30.97 gate must return servable=true "+
			"for a registered+synced GVR (registered=%v syncClosed=%v)", registered, syncClosed)
	}

	// NEW GATE: same scenario → servable=FALSE (resource type unconfirmed).
	got := rw.IsServable(s4TestGVR)
	if got {
		t.Fatalf("S4 fix regressed: four-conjunct gate must return servable=false "+
			"for the synced-over-empty post-startup CRD; got true")
	}

	// The delta — prior=true, new=false — is the captured negative
	// control. Assert it explicitly so a future regression that weakens
	// conjunct 4 back toward HasSynced-only is caught here.
	if prior == got {
		t.Fatalf("negative control: expected a true-vs-false delta "+
			"(prior 0.30.97 gate=%v, new 0.30.98 gate=%v); the new conjunct "+
			"is not catching the S4 bug", prior, got)
	}
	t.Logf("S4 behavioral negative control: prior 0.30.97 gate=%v, "+
		"new 0.30.98 four-conjunct gate=%v — conjunct 4 catches the bug", prior, got)
}

// TestF1_PostStartupCRD_UnconfirmedUntilDiscovered is the S4 falsifier +
// acceptance test. A GVR is registered and its informer syncs over an
// empty apiserver result (the resource type does not yet exist). The
// four-conjunct gate must report servable=false because the resource
// type is unconfirmed — even though registered+synced both hold. After
// the discovery refresh observes the type, servable flips to true.
//
// Against 0.30.97 (HasSynced-only) this scenario is inexpressible: there
// is no SetDiscoveryClient, no RefreshDiscovery, and IsServable would
// return true the instant HasSynced latched — exactly the S4 bug.
func TestF1_PostStartupCRD_UnconfirmedUntilDiscovered(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// Discovery client that does NOT yet serve the test GVR's type.
	disco := &fakeDiscovery{served: map[string]bool{}}
	rw.SetDiscoveryClient(disco)

	// Register + sync the informer. The initial LIST completes over an
	// empty fake apiserver — HasSynced latches true.
	added, syncCh := rw.EnsureResourceType(s4TestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}

	// Run one discovery refresh against the un-installed-type state.
	rw.RefreshDiscovery(context.Background())

	// S4 ASSERTION: registered + synced both hold, but the resource type
	// is unconfirmed → servable MUST be false. Pre-0.30.98 this returned
	// true (HasSynced latched) and the pivot served [] — the S4 bug.
	if rw.IsServable(s4TestGVR) {
		t.Fatalf("S4: unconfirmed resource type must NOT be servable (IsServable)")
	}
	if _, servable := rw.ListObjectsServable(s4TestGVR, ""); servable {
		t.Fatalf("S4: unconfirmed resource type must NOT be servable (ListObjectsServable)")
	}

	// Now the CRD is installed: the apiserver starts serving the type.
	disco.setServed(gvString(s4TestGVR), true)
	rw.RefreshDiscovery(context.Background())

	// FIX ASSERTION: confirmed + synced + watch-healthy → servable=true.
	if !rw.IsServable(s4TestGVR) {
		t.Fatalf("S4 fix: confirmed + synced GVR must be servable after discovery refresh")
	}
	items, servable := rw.ListObjectsServable(s4TestGVR, "")
	if !servable {
		t.Fatalf("S4 fix: confirmed + synced GVR must be servable (ListObjectsServable)")
	}
	if items == nil {
		t.Fatalf("S4 fix: confirmed + synced GVR must return non-nil (empty) slice")
	}
}

// TestF1_NoDiscoveryClient_DefaultsConfirmed asserts the conservative
// degradation: when no discovery client is wired, resourceTypeConfirmed
// defaults to true so the pivot keeps its pre-0.30.98 behaviour
// (HasSynced-only) rather than dying entirely. This is a uniform policy
// ("discovery unavailable ⇒ assume confirmed"), NOT a per-GVR carve-out.
func TestF1_NoDiscoveryClient_DefaultsConfirmed(t *testing.T) {
	rw := newSyncedServableWatcher(t)
	// No SetDiscoveryClient call. The four-conjunct gate must still
	// report servable=true for a registered+synced informer.
	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("no discovery client: registered+synced GVR must default to servable")
	}
}

// --- F2 — WATCH-disconnected ----------------------------------------------

// TestF2_WatchDisconnected_FallsThroughThenRecovers asserts conjunct 3.
// A registered + synced + confirmed GVR is servable; firing the
// watch-error handler marks the informer's WATCH broken and servable
// flips to false; a successful resync clears the flag and servable
// recovers.
//
// Against 0.30.97 this is inexpressible — there is no watchBroken map,
// no SetWatchErrorHandler wiring, and no FireWatchError test hook.
func TestF2_WatchDisconnected_FallsThroughThenRecovers(t *testing.T) {
	rw := newSyncedServableWatcher(t)

	// Confirmed via the no-discovery-client default (assume-confirmed).
	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("setup: registered+synced GVR must be servable before watch error")
	}

	// Simulate the reflector dropping the WATCH connection.
	rw.FireWatchError(servableTestGVR)
	if rw.IsServable(servableTestGVR) {
		t.Fatalf("F2: GVR with broken WATCH must NOT be servable (IsServable)")
	}
	if _, servable := rw.ListObjectsServable(servableTestGVR, ""); servable {
		t.Fatalf("F2: GVR with broken WATCH must NOT be servable (ListObjectsServable)")
	}

	// Simulate the reflector successfully reconnecting / relisting.
	rw.MarkWatchRecovered(servableTestGVR)
	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("F2 recovery: GVR must be servable again after successful resync")
	}
}

// --- F4 — R-FALSE-1: cache-off byte-identical pivot path ------------------
//
// F4 is NOT in this file. R-FALSE-1 is a property of the GATED CONSUMER,
// not of the servability predicate — the predicate is pure and has no
// gate read. Testing the predicate's side-effect-freedom would not prove
// that the dispatch path leaves the servable gate untouched when the
// pivot is inactive. #57: the pivot gate was the standalone
// RESOLVER_USE_INFORMER flag, now folded into CACHE_ENABLED (pivot
// inactive == cache OFF). The behavioral F4 therefore lives next to the
// consumer:
//
//	internal/resolvers/restactions/api/informer_dispatch_rfalse1_test.go
//	  → TestF4_CacheOff_DispatchDoesNotReachServableGate
//
// That test asserts that with the cache subsystem OFF the resolver
// dispatch branch never invokes dispatchViaInformer — hence never reaches
// IsServable / ListObjectsServable — by observing the pivot's served /
// fallthrough counters stay at zero.
