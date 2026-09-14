// servability_snapshot_test.go — Task #130 F1.
//
// Diagnostic-only accessor ServabilitySnapshotFor: returns the four
// servableLocked conjuncts (registered, HasSynced, watchHealthy,
// typeConfirmed) for a GVR under ONE rw.mu.RLock() so the tuple is
// internally consistent (no check-then-act gap across four separate
// public accessors). It carries NO behaviour — it is consumed only by the
// internal_dispatch.list.serve_miss slog to discriminate WHY the
// informer-serve branch was not taken on a given boot.
//
// This file asserts the accessor returns a consistent 4-tuple that AGREES
// with the servability predicate (IsServable) in both directions:
//   - synced + type-confirmed GVR → all four true → IsServable true.
//   - synced-over-empty post-startup CRD (the S4 shape) → typeConfirmed
//     false while registered+HasSynced+watchHealthy hold → IsServable
//     false. This is the exact tuple the serve_miss log must surface so a
//     boot operator can read typeConfirmed=false directly.
//
// Per feedback_no_special_cases.md: a generic customer GVR; the accessor
// is uniform over every GVR.

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// TestServabilitySnapshotFor_ConsistentTupleAgreesWithIsServable drives a
// GVR through its lifecycle and asserts the snapshot's 4-tuple is both
// internally consistent and in lockstep with IsServable.
func TestServabilitySnapshotFor_ConsistentTupleAgreesWithIsServable(t *testing.T) {
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

	// A discovery client that does NOT (yet) serve the test GVR's type —
	// the post-startup-CRD-not-yet-installed state (the S4 shape).
	disco := &fakeDiscovery{served: map[string]bool{}}
	rw.SetDiscoveryClient(disco)

	// Before registration: nothing holds.
	pre := rw.ServabilitySnapshotFor(servableTestGVR)
	if pre.Registered || pre.HasSynced || pre.WatchHealthy || pre.TypeConfirmed {
		t.Fatalf("pre-registration snapshot must be all-false; got %+v", pre)
	}
	if rw.IsServable(servableTestGVR) {
		t.Fatalf("pre-registration IsServable must be false")
	}

	// Register + sync the informer over the empty fake apiserver.
	added, syncCh := rw.EnsureResourceType(servableTestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}
	rw.RefreshDiscovery(context.Background()) // conjunct 4 runs; type still unserved.

	// S4 shape: registered + synced + watch-healthy hold, but the resource
	// type is UNCONFIRMED. This is the tuple the serve_miss log must surface.
	miss := rw.ServabilitySnapshotFor(servableTestGVR)
	if !miss.Registered || !miss.HasSynced || !miss.WatchHealthy {
		t.Fatalf("post-sync snapshot: registered+hasSynced+watchHealthy must all hold; got %+v", miss)
	}
	if miss.TypeConfirmed {
		t.Fatalf("post-sync snapshot: typeConfirmed must be FALSE for the un-served type; got %+v", miss)
	}
	// Tuple must AGREE with the predicate: type-unconfirmed → not servable.
	if rw.IsServable(servableTestGVR) {
		t.Fatalf("consistency: IsServable must be false when snapshot.TypeConfirmed=false; snapshot=%+v", miss)
	}

	// Install the type; refresh discovery → conjunct 4 flips true.
	disco.setServed(gvString(servableTestGVR), true)
	rw.RefreshDiscovery(context.Background())

	hit := rw.ServabilitySnapshotFor(servableTestGVR)
	if !hit.Registered || !hit.HasSynced || !hit.WatchHealthy || !hit.TypeConfirmed {
		t.Fatalf("post-confirm snapshot: all four conjuncts must hold; got %+v", hit)
	}
	// Tuple must AGREE with the predicate: all four true → servable.
	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("consistency: IsServable must be true when all four snapshot conjuncts hold; snapshot=%+v", hit)
	}
}
