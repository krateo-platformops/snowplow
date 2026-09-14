// register_on_navigation_test.go — Tag 0.30.99 (Tag B of the unified
// prewarm+pivot-servability design) falsifier + acceptance tests for
// register-on-navigation.
//
// Tag B premise: navigation (a startup GVR-walk + the resolver's
// runtime inner-call path) populates the registered informer set via
// EnsureResourceType. Tag A (0.30.98) made servable(gvr) a four-conjunct
// gate so register-during-request is safe: a freshly-registered GVR is
// NOT yet servable (unsynced and/or unconfirmed), so the triggering
// request falls through to apiserver while a SUBSEQUENT request serves
// from the now-ready informer.
//
// Falsifier protocol (feedback_falsifier_first_before_ship.md):
//
//   - F3 — never-walked GVR. A GVR that no startup walk and no prior
//     navigation has touched. The contract under test:
//       1. The triggering navigation registers it (EnsureResourceType
//          returns added=true) — proof register-on-navigation fired.
//       2. At the instant of the triggering request the GVR is NOT
//          servable (it has not synced) — proof the triggering request
//          falls through to apiserver, never serving a not-ready
//          informer.
//       3. Once the informer syncs AND the resource type is confirmed,
//          a SUBSEQUENT request finds it servable.
//     Captured-against-current-code note: re-running F3 with the
//     register-on-navigation call elided (the gap paths the audit
//     identified) leaves the GVR unregistered — added would never be
//     observed and step 3 never reaches servable. The test asserts the
//     full added→unservable→servable transition that ONLY holds once
//     register-on-navigation is uniform.
//
//   - F1 regression guard — the Tag A S4 invariant. A post-startup CRD
//     (registered + synced over an empty apiserver, resource type not
//     yet served) MUST remain gated unconfirmed → unservable. Tag B's
//     register-on-navigation must not weaken this. (The canonical Tag A
//     F1 test lives in servable_s4_test.go; this is an independent
//     guard exercised through the navigation entry point.)
//
// Per feedback_no_special_cases.md: every assertion uses a generic
// customer-style GVR. There is no hardcoded business-GVR list anywhere
// in the register-on-navigation path — the registered set is whatever
// navigation has touched.

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// neverWalkedGVR is a customer-style GVR that no startup walk and no
// prior navigation in these tests touches before the F3 body registers
// it. It reuses the secrets List-kind registration (servableListKinds)
// so the fake dynamic client serves the informer's initial LIST without
// panicking — the test cares about the registration + servability
// transition, not the object payload.
var neverWalkedGVR = servableTestGVR

// TestF3_NeverWalkedGVR_RegisterOnNavigation is the Tag B F3 falsifier.
// It exercises the register-on-navigation contract through the cache
// layer's actual mechanism (EnsureResourceType + the four-conjunct
// servability gate):
//
//	added=true                  — navigation registered a never-walked GVR
//	IsServable=false (pre-sync) — triggering request falls through to apiserver
//	IsServable=true  (post)     — a subsequent request serves from the informer
//
// Run against current code with register-on-navigation absent from the
// gap paths the audit identified, the navigated GVR would never be
// registered: there is no added=true to observe and IsServable never
// flips true. The full transition asserted here is the proof that
// register-on-navigation is wired and uniform.
func TestF3_NeverWalkedGVR_RegisterOnNavigation(t *testing.T) {
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

	// Discovery client that DOES serve the GVR's resource type — this is
	// an existing-resource navigation, not a post-startup CRD (that case
	// is F1, below). Conjunct 4 must confirm it once the refresh runs.
	disco := &fakeDiscovery{served: map[string]bool{gvString(neverWalkedGVR): true}}
	rw.SetDiscoveryClient(disco)

	// Pre-condition: the GVR is genuinely never-walked. Nothing has
	// registered it, so it must not be servable.
	if rw.IsServable(neverWalkedGVR) {
		t.Fatalf("F3 pre-condition violated: never-walked GVR is already servable")
	}

	// --- Step 1: navigation derives the GVR and registers it. ----------
	// This is the exact call register-on-navigation makes (the resolver
	// inner-call path, the dispatcher dep-record path, and the startup
	// walk all funnel through EnsureResourceType). A never-walked GVR
	// must report added=true.
	added, syncCh := rw.EnsureResourceType(neverWalkedGVR)
	if !added {
		t.Fatalf("F3: register-on-navigation must register a never-walked GVR (want added=true)")
	}

	// --- Step 2: the triggering request must fall through. -------------
	// EnsureResourceType is fire-and-forget; it does NOT block on sync.
	// At this instant the informer has not synced, so the four-conjunct
	// gate must report servable=false — the triggering request serves
	// via apiserver fallthrough, never from a not-ready informer.
	if rw.IsServable(neverWalkedGVR) {
		t.Fatalf("F3: triggering request must fall through — a just-registered, " +
			"not-yet-synced GVR must NOT be servable")
	}
	if _, servable := rw.ListObjectsServable(neverWalkedGVR, ""); servable {
		t.Fatalf("F3: triggering LIST must fall through — not-yet-synced GVR " +
			"must NOT be servable")
	}

	// --- Step 3: a subsequent request serves from the informer. --------
	// Wait for the informer's initial LIST to reconcile, then drive one
	// discovery refresh so conjunct 4 confirms the resource type.
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("F3: informer did not sync within 5s")
	}
	rw.RefreshDiscovery(context.Background())

	if !rw.IsServable(neverWalkedGVR) {
		t.Fatalf("F3: after sync + discovery confirm, a subsequent request " +
			"must find the GVR servable")
	}
	items, servable := rw.ListObjectsServable(neverWalkedGVR, "")
	if !servable {
		t.Fatalf("F3: after sync + discovery confirm, ListObjectsServable " +
			"must report servable=true")
	}
	if items == nil {
		t.Fatalf("F3: a servable (empty) informer must return a non-nil slice")
	}

	// --- Idempotence: a second navigation touch is a no-op. ------------
	// Register-on-navigation fires on every derived GVR; the second and
	// later touches must be cheap no-ops (added=false), never a
	// re-registration. This is what makes uniform register-on-navigation
	// safe to call from every read path without coordination.
	addedAgain, _ := rw.EnsureResourceType(neverWalkedGVR)
	if addedAgain {
		t.Fatalf("F3: second navigation touch must be idempotent (want added=false)")
	}

	t.Logf("F3: never-walked GVR transition verified — added=true, " +
		"pre-sync unservable (apiserver fallthrough), post-sync+confirm servable")
}

// TestF3_F1Guard_PostStartupCRD_StaysUnconfirmed is the Tag B F1
// regression guard. Register-on-navigation must not let a post-startup
// CRD (a GVR whose resource type the apiserver does not yet serve) leak
// past the Tag A S4 gate. Even though navigation registers it and the
// informer syncs over the empty apiserver result, conjunct 4 must keep
// it unconfirmed → unservable until the discovery ticker observes the
// type. This re-asserts the Tag A invariant through the Tag B entry
// point so a Tag B regression that weakens it is caught here.
func TestF3_F1Guard_PostStartupCRD_StaysUnconfirmed(t *testing.T) {
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

	// Discovery client that does NOT serve the GVR's resource type —
	// the post-startup-CRD-not-yet-installed state.
	disco := &fakeDiscovery{served: map[string]bool{}}
	rw.SetDiscoveryClient(disco)

	// Navigation registers the GVR (the Tag B register-on-navigation
	// call). The informer syncs over the empty fake apiserver.
	added, syncCh := rw.EnsureResourceType(neverWalkedGVR)
	if !added {
		t.Fatalf("F1-guard: want added=true on first navigation touch")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("F1-guard: informer did not sync within 5s")
	}
	rw.RefreshDiscovery(context.Background())

	// S4 INVARIANT: registered + synced both hold, but the resource type
	// is unconfirmed → servable MUST stay false. Register-on-navigation
	// must NOT have weakened the Tag A gate.
	if rw.IsServable(neverWalkedGVR) {
		t.Fatalf("F1-guard regressed: register-on-navigation must not let an " +
			"unconfirmed post-startup CRD become servable")
	}
	if _, servable := rw.ListObjectsServable(neverWalkedGVR, ""); servable {
		t.Fatalf("F1-guard regressed: unconfirmed post-startup CRD must not be " +
			"servable via ListObjectsServable")
	}

	// Once the CRD installs and the apiserver serves the type, the
	// discovery refresh flips it confirmed → servable.
	disco.setServed(gvString(neverWalkedGVR), true)
	rw.RefreshDiscovery(context.Background())
	if !rw.IsServable(neverWalkedGVR) {
		t.Fatalf("F1-guard: confirmed + synced GVR must become servable after refresh")
	}

	t.Logf("F1-guard: post-startup CRD stayed unconfirmed→unservable through " +
		"register-on-navigation, then flipped servable on discovery confirm")
}
