// serve_assert_falsifier_test.go — Convergence hardening #1 falsifiers for
// the serve-requires-servable invariant (solidity map row #2).
//
// The invariant: an authoritative cache HIT is NEVER returned from a
// not-servable informer. The discriminating controls:
//
//   - GATE: a SYNCED+servable informer serves the hit; a NOT-synced
//     informer (erroring LIST) returns a silent miss (the caller falls
//     through to apiserver) — NOT a panic, NOT a served stale object, and
//     the violation counter stays 0 (a not-servable MISS is the correct
//     fallback, not a violation).
//   - GUARD FIRES (prod): driving the assert directly against a
//     not-servable GVR (the only way to reach the by-construction-
//     unreachable violation branch) counts exactly one
//     serve_requires_servable violation and returns false.
//   - GUARD FIRES (test): the same in env.TestMode()==true PANICS (loud
//     failure — a real serve-from-not-synced regression aborts the test).
//   - -race: concurrent GET/LIST while an informer flips servable.

package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/cache"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// TestServeRequiresServable_SyncedServesNotSyncedFallsThrough is the GATE
// control: a servable informer serves the hit; a not-servable one (the
// stuck erroring-LIST informer) returns a silent miss with ZERO violations
// (the correct fallback, not a violation).
func TestServeRequiresServable_SyncedServesNotSyncedFallsThrough(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetServeRequiresServableViolationsForTest()
	t.Cleanup(cache.ResetServeRequiresServableViolationsForTest)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())
	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// Register the healthy seeds (sync normally) + the stuck informer.
	rw.RegisterMetaQuerySeeds()
	rw.EnsureResourceType(stuckGVR())

	// Wait for the healthy seeds to become servable (the stuck one never
	// will). restactions is a seed that syncs in the fake.
	servableGVR := cache.MetaQuerySeeds()[0]
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsServable(servableGVR) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !rw.IsServable(servableGVR) {
		t.Fatalf("setup: healthy seed %v never became servable", servableGVR)
	}

	// The stuck informer must be registered but NOT servable.
	if !rw.IsRegistered(stuckGVR()) {
		t.Fatalf("setup: stuck GVR must be registered")
	}
	if rw.IsServable(stuckGVR()) {
		t.Fatalf("setup: stuck GVR must NOT be servable (its LIST errors forever)")
	}

	// LIST a not-servable GVR → silent miss (servable=false), NO violation.
	if _, servable := rw.ListObjectsServable(context.Background(), stuckGVR(), ""); servable {
		t.Fatalf("not-servable LIST must return servable=false (fall through), not serve")
	}
	// GET a not-servable GVR → silent miss, NO violation.
	if _, hit := rw.GetObject(context.Background(), stuckGVR(), "", "anything"); hit {
		t.Fatalf("not-servable GET must return hit=false (fall through), not serve")
	}

	// A not-servable MISS is the CORRECT fallback — it is NOT a violation.
	if v := cache.ServeRequiresServableViolations(); v != 0 {
		t.Fatalf("a not-servable fallthrough must NOT count a violation; got %d", v)
	}

	// LIST a servable GVR → servable=true (a real served answer, possibly
	// empty — the fake has no objects, but servable means "vouched for").
	if _, servable := rw.ListObjectsServable(context.Background(), servableGVR, ""); !servable {
		t.Fatalf("servable LIST must return servable=true")
	}
	// Still zero violations: serving from a servable informer is the
	// invariant holding, not firing.
	if v := cache.ServeRequiresServableViolations(); v != 0 {
		t.Fatalf("serving from a servable informer must NOT count a violation; got %d", v)
	}
}

// TestServeRequiresServable_GuardCountsInProd is the GUARD-FIRES control in
// production mode: driving the assert against a not-servable GVR counts
// exactly one violation and returns false (caller would fall through).
func TestServeRequiresServable_GuardCountsInProd(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetServeRequiresServableViolationsForTest()
	t.Cleanup(cache.ResetServeRequiresServableViolationsForTest)

	// Production posture: count + log + return false (do NOT panic).
	env.SetTestMode(false)
	t.Cleanup(func() { env.SetTestMode(true) })

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())
	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.EnsureResourceType(stuckGVR()) // registered, never servable

	ok := rw.AssertServeRequiresServableForTest(stuckGVR(), "test")
	if ok {
		t.Fatalf("guard must return false for a not-servable GVR in prod mode")
	}
	if v := cache.ServeRequiresServableViolations(); v != 1 {
		t.Fatalf("guard must count exactly one violation in prod mode; got %d", v)
	}
}

// TestServeRequiresServable_GuardPanicsInTestMode is the GUARD-FIRES
// control in test mode: the same not-servable assert PANICS (loud failure).
func TestServeRequiresServable_GuardPanicsInTestMode(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetServeRequiresServableViolationsForTest()
	t.Cleanup(cache.ResetServeRequiresServableViolationsForTest)

	// env.TestMode() is true by default in the test binary; assert it.
	if !env.TestMode() {
		env.SetTestMode(true)
		t.Cleanup(func() { env.SetTestMode(true) })
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())
	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.EnsureResourceType(stuckGVR())

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("guard must PANIC for a not-servable GVR in test mode")
		}
	}()
	_ = rw.AssertServeRequiresServableForTest(stuckGVR(), "test")
	t.Fatalf("unreachable: the assert must have panicked")
}

// TestServeRequiresServable_RaceGetListWhileFlipping is the -race control:
// concurrent GET/LIST + IsServable while the stuck informer stays not-
// servable and the healthy seeds are servable. No data race on the serve
// path's rw.mu hold + the violation counter; no spurious panic in prod mode.
func TestServeRequiresServable_RaceGetListWhileFlipping(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetServeRequiresServableViolationsForTest()
	t.Cleanup(cache.ResetServeRequiresServableViolationsForTest)
	env.SetTestMode(false) // prod posture so a transient not-servable read counts, never panics
	t.Cleanup(func() { env.SetTestMode(true) })

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())
	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.RegisterMetaQuerySeeds()
	rw.EnsureResourceType(stuckGVR())
	servableGVR := cache.MetaQuerySeeds()[0]

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = rw.ListObjectsServable(context.Background(), servableGVR, "")
				_, _ = rw.GetObject(context.Background(), servableGVR, "", "x")
				_, _ = rw.ListObjectsServable(context.Background(), stuckGVR(), "")
				_, _ = rw.GetObject(context.Background(), stuckGVR(), "", "x")
				_ = rw.IsServable(servableGVR)
			}
		}()
	}
	wg.Wait()

	// The serve path only ever serves from servableLocked-vouched informers,
	// so the by-construction guard never fires through GET/LIST — the
	// not-servable reads all returned silent misses.
	if v := cache.ServeRequiresServableViolations(); v != 0 {
		t.Fatalf("concurrent GET/LIST must serve only from servable informers (0 violations); got %d", v)
	}
}
