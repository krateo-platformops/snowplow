// store_verify_ownsinformer_registration_test.go — snowplow#246.
//
// THE GAP (found by the architect during the #237-B / PR #245 delta review):
// no arm asserts the `ownsInformer` VALUE produced by a REAL registration. The
// field gates whether a GVR's store-divergence repair is attempted or refused
// (#237 B, bound 6): a GVR whose informer it does NOT own must never be torn
// down and rebuilt by a repair, because a shared-factory informer cannot be
// evicted and the factory hands the stopped one back — the repair would kill
// the cache for that GVR.
//
// Before this file, `ownsInformer` reached the registry ONLY through the drift
// arm's direct rememberStoreVerification(...) calls — i.e. the repair path
// tested GIVEN a value. Nothing pinned that REGISTRATION derives the right
// value. Flipping a construction branch to `ownsInformer = false` would
// silently widen the unrepairable class to every streaming GVR, with every
// existing arm still green — the exact failure the refused-set arm exists to
// prevent, reached by a path that arm does not cover
// (feedback_arm_that_cannot_fail_is_not_coverage; feedback_consulted≠correct-key
// in spirit — a value covered by grep is covered only at the moment you looked).
//
// These two arms drive the REAL construction path (EnsureResourceType /
// EnsureResourceTypeMetadataOnly → addResourceTypeLocked /
// addResourceTypeMetadataOnlyLocked), NOT a direct rememberStoreVerification,
// and assert the recorded field read back via verificationRowFor.
//
// RED PROVEN BY MUTATION (recorded in the #246 deliverable report):
//   - streaming arm:      flip watcher.go:1395 `ownsInformer = true` → false → FAIL
//   - metadata-only arm:  flip watcher.go:928  `ownsInformer = true` → false → FAIL
//   - both reverted       → PASS
//
// Hermetic: fake dynamic/metadata clients, informers are CONSTRUCTED (the
// recorded value is written synchronously inside the *Locked registration
// method) and never Run — no cluster is touched.
package cache

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	metadatafake "k8s.io/client-go/metadata/fake"
)

// ownsInformerStreamGVR is a plain, non-typed-RBAC GVR. Its group is NOT in
// typedResourceOverrides, so isStreamingException(gvr) is false and — with the
// streaming toggle on and a *rest.Config wired — addResourceTypeLocked takes the
// streaming construction branch (watcher.go:1390-1395), which owns its informer.
var ownsInformerStreamGVR = schema.GroupVersionResource{
	Group:    "synthetic-246-stream.krateo.io",
	Version:  "v1",
	Resource: "things",
}

// ownsInformerMetaGVR is exercised through the explicit metadata-only entry
// point (EnsureResourceTypeMetadataOnly). Its group is marked
// navigation-discovered so addResourceTypeMetadataOnlyLocked takes the
// STANDALONE branch (watcher.go:928), which owns its informer outright.
var ownsInformerMetaGVR = schema.GroupVersionResource{
	Group:    "synthetic-246-meta.krateo.io",
	Version:  "v1",
	Resource: "things",
}

// TestOwnsInformerRecordedByStreamingRegistration pins the ownsInformer VALUE
// derived by the real streaming construction branch of addResourceTypeLocked.
//
// It is the missing arm from #246: the streaming constructor builds a fresh
// informer every call and therefore OWNS it, so the recorded ownsInformer must
// be true — the repair verb is permitted to tear this informer down and rebuild
// it. Flipping watcher.go:1395 to false makes this arm RED.
func TestOwnsInformerRecordedByStreamingRegistration(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	resetStoreVerificationStateForTest()
	t.Cleanup(resetStoreVerificationStateForTest)

	// A wired *rest.Config so newStreamingDynamicInformer can build its REST
	// client; a stub host is enough because the informer is constructed, never
	// Run (streamingRESTClient does not connect until a LIST/WATCH is issued).
	rw := newRouteRaceWatcher(t, true, ownsInformerStreamGVR)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	rw.EnsureResourceType(ownsInformerStreamGVR)

	// Precondition — prove the REAL construction path was the streaming branch.
	// Without this an arm that silently fell to the shared factory would assert
	// ownsInformer against the wrong branch and could never fail for the right
	// reason (feedback_arm_that_cannot_fail_is_not_coverage).
	if !isStreamingInformer(rw, ownsInformerStreamGVR) {
		t.Fatalf("precondition: GVR %v did not route to the streaming informer — "+
			"the streaming construction branch was not exercised", ownsInformerStreamGVR)
	}

	row, ok := verificationRowFor(ownsInformerStreamGVR)
	if !ok {
		t.Fatalf("no verification row recorded for %v — registration did not call "+
			"rememberStoreVerification", ownsInformerStreamGVR)
	}
	if !row.ownsInformer {
		t.Fatalf("ownsInformer recorded false for a STREAMING-constructed informer %v; "+
			"want true — the streaming constructor builds a fresh informer and owns it, "+
			"so this GVR's store-divergence repair must be PERMITTED. A false here silently "+
			"widens the unrepairable class to every streaming GVR (#246, #237 B bound 6).",
			ownsInformerStreamGVR)
	}
}

// TestOwnsInformerRecordedByMetadataOnlyRegistration mirrors the streaming arm
// for addResourceTypeMetadataOnlyLocked.
//
// That path is inert in production today (shouldUseMetadataOnly is
// constant-false post-H5), but it wires ownsInformer identically and must not
// silently lose the gate when it goes live — the same reason both sites were
// wired in the first place (#246). The explicit EnsureResourceTypeMetadataOnly
// entry point registers through the real *Locked method regardless of the
// predicate; marking the group navigation-discovered drives the STANDALONE
// branch (watcher.go:928), which owns its informer. Flipping watcher.go:928 to
// false makes this arm RED.
func TestOwnsInformerRecordedByMetadataOnlyRegistration(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	resetStoreVerificationStateForTest()
	t.Cleanup(resetStoreVerificationStateForTest)

	rw := newSyntheticRemoveWatcher(t)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	// Metadata-only registration requires a metadata client. A fake one is
	// sufficient: the informer is constructed, never Run.
	rw.SetMetadataClient(metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme()))

	// Mark the group navigation-discovered so addResourceTypeMetadataOnlyLocked
	// takes the standalone (owned) branch rather than the shared metadata factory.
	AddNavigationDiscoveredGroup(ownsInformerMetaGVR.Group)
	if !IsNavigationDiscoveredGroup(ownsInformerMetaGVR.Group) {
		t.Fatalf("precondition: group %q was not marked navigation-discovered",
			ownsInformerMetaGVR.Group)
	}

	added, _ := rw.EnsureResourceTypeMetadataOnly(ownsInformerMetaGVR)
	if !added {
		t.Fatalf("EnsureResourceTypeMetadataOnly(%v) returned added=false — the "+
			"metadata-only construction path did not run", ownsInformerMetaGVR)
	}

	row, ok := verificationRowFor(ownsInformerMetaGVR)
	if !ok {
		t.Fatalf("no verification row recorded for %v — metadata-only registration "+
			"did not call rememberStoreVerification", ownsInformerMetaGVR)
	}
	if !row.ownsInformer {
		t.Fatalf("ownsInformer recorded false for a STANDALONE metadata-only informer %v; "+
			"want true. This path is inert today but must not silently lose its "+
			"ownership gate when it goes live (#246).", ownsInformerMetaGVR)
	}
}
