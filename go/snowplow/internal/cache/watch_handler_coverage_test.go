// watch_handler_coverage_test.go — Tag 0.30.99 (Tag B) tests for the
// watch-handler coverage guard (Part 3 — the architect's Tag B note).
//
// The invariant: EVERY GVR in rw.informers has had its conjunct-3
// WATCH-error handler installed. Tag A wired SetWatchErrorHandler on the
// three informer-creation paths, but its constructor install-loop only
// covered informers present at construction (the RBAC bootstrap set). A
// future pre-Start lazy-register path bypassing the constructor loop
// could silently drop coverage.
//
// 0.30.99 closes the gap structurally: addResourceTypeLocked installs
// the handler unconditionally at registration time (not only in its
// post-Start branch). HasWatchHandlerForTest is the test surface that
// verifies the structural fix held — a fake dynamic client never drops a
// real WATCH, so install-time coverage cannot be observed via
// FireWatchError (which writes watchBroken directly, bypassing the
// installed handler).
//
// Per feedback_no_special_cases.md: the invariant is uniform — it holds
// for the RBAC bootstrap GVRs and for every lazily-registered customer
// GVR alike, with no per-resource carve-out.

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// TestWatchHandlerCoverage_RBACBootstrapGVRs asserts the four RBAC GVRs
// the constructor registers pre-Start all have a WATCH-error handler.
// This is the constructor-path half of the coverage invariant.
func TestWatchHandlerCoverage_RBACBootstrapGVRs(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), rbacListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	for _, gvr := range cache.RBACResourceTypes {
		if !rw.HasWatchHandlerForTest(gvr) {
			t.Fatalf("watch-handler coverage gap: RBAC bootstrap GVR %s has no "+
				"conjunct-3 WATCH-error handler", gvr)
		}
	}
}

// TestWatchHandlerCoverage_LazyRegisteredGVR asserts a GVR registered
// post-Start via EnsureResourceType (the lazy register-on-navigation
// path) also gets a WATCH-error handler. This is the lazy-path half of
// the coverage invariant — the path the architect's note is concerned
// about.
func TestWatchHandlerCoverage_LazyRegisteredGVR(t *testing.T) {
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

	// #398: v1/secrets is never informed; any core GVR serves this arm.
	lazyGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	// Pre-condition: never registered → no handler.
	if rw.HasWatchHandlerForTest(lazyGVR) {
		t.Fatalf("pre-condition: %s should have no handler before registration", lazyGVR)
	}

	// Lazy register (post-Start — the constructor's install-loop already
	// ran). The structural fix means addResourceTypeLocked installs the
	// handler at registration time regardless of the rw.started branch.
	added, syncCh := rw.EnsureResourceType(lazyGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true for never-registered GVR")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer for %s did not sync within 5s", lazyGVR)
	}

	if !rw.HasWatchHandlerForTest(lazyGVR) {
		t.Fatalf("watch-handler coverage gap: lazily-registered GVR %s has no "+
			"conjunct-3 WATCH-error handler — addResourceTypeLocked did not "+
			"install it", lazyGVR)
	}

	// Belt-and-braces: conjunct 3 must actually GATE this lazily-
	// registered GVR. FireWatchError drives the same state transition
	// the installed handler performs; servable must flip false.
	if !rw.IsServable(lazyGVR) {
		t.Fatalf("setup: lazily-registered synced GVR must be servable")
	}
	rw.FireWatchError(lazyGVR)
	if rw.IsServable(lazyGVR) {
		t.Fatalf("conjunct 3: lazily-registered GVR with broken WATCH must NOT be servable")
	}
	rw.MarkWatchRecovered(lazyGVR)
	if !rw.IsServable(lazyGVR) {
		t.Fatalf("conjunct 3 recovery: GVR must be servable again after resync")
	}
}

// TestWatchHandlerCoverage_PrewarmWalkRegisteredGVRs asserts the Part 2
// startup navigation walk's GVRs also gain WATCH-error handlers — the
// walk routes through EnsureResourceType, so it inherits the structural
// install. This is the Part-2 ∩ Part-3 intersection: every GVR the
// startup walk registers is conjunct-3-covered.
func TestWatchHandlerCoverage_PrewarmWalkRegisteredGVRs(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		buildSchemeWithRestActions(),
		inventoryListKinds(),
		makeRestAction("ra-cov", "demo",
			"/api/v1/namespaces",
			"/api/v1/pods",
			"/apis/apps/v1/deployments",
		),
	)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	if _, _, walkErr := rw.PrewarmRegisterFromNavigation(context.Background(), dyn); walkErr != nil {
		t.Fatalf("PrewarmRegisterFromNavigation: %v", walkErr)
	}

	for _, gvr := range []schema.GroupVersionResource{
		{Group: "", Version: "v1", Resource: "namespaces"},
		{Group: "", Version: "v1", Resource: "pods"},
		{Group: "apps", Version: "v1", Resource: "deployments"},
	} {
		if !rw.HasWatchHandlerForTest(gvr) {
			t.Fatalf("watch-handler coverage gap: startup-walk-registered GVR %s "+
				"has no conjunct-3 WATCH-error handler", gvr)
		}
	}
}
