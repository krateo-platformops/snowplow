// crd_post_boot_register_serve_test.go — #118 defect-2 → #116 standing
// regression arm (discriminating).
//
// #118 defect-2 ("a post-boot-published CRD is unwatched until restart") was
// closed as a DUP of #116 (Ship 0.30.233 self-heal): on a CRD ADD/UPDATE,
// triggerCRDDiscovery → DiscoverGroupResourcesFresh → discoverGroupResources
// (forceFresh) walks the new group's discovery surface and, for each CRD-backed
// GVR, calls rw.EnsureResourceType(gvr) (discovery_lookup.go:478 — REGISTERS the
// data informer) + Deps().OnResourceTypeAvailable(gvr) (:487 — dirty-marks
// stale-negative LIST deps). This arm is the standing guard that #116 stays wired.
//
// THE DISCRIMINATION REQUIREMENT (docs/118-defect2-eager-crd-informer-design-
// 2026-07-22.md §falsifiers arm 1): a naive "first /call returns 200" arm does
// NOT discriminate — Path 3 (not-synced → live apiserver dispatch,
// informer_dispatch.go:411-422) returns 200 for a post-boot CRD REGARDLESS of
// #116. #116's SPECIFIC contribution is the informer REGISTRATION (IsServable
// becomes true → the CACHED/consistent serve path, not a bare live dispatch) AND
// the stale-negative-LIST dirty-mark. So this arm asserts EXACTLY those two
// outcomes, and the RED (neuter DiscoverGroupResourcesFresh / EnsureResourceType)
// makes them disappear.

package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// postBootGVR is the CRD-backed GVR published AFTER boot (not known at watcher
// construction). Group "widgets.krateo.io" is CRD-shaped (not a built-in), so
// discoverGroupResources's isBuiltInKind filter keeps it.
var postBootGVR = schema.GroupVersionResource{
	Group: "acme.example.io", Version: "v1", Resource: "gadgets",
}

// TestCRDPostBoot_RegistersAndServesCached_116Discriminating is the #116
// regression guard. It drives the REAL DiscoverGroupResourcesFresh chain (NOT a
// stubbed bridge like TestCRDAdd_TriggersGroupDiscovery) and asserts the two
// #116-SPECIFIC outcomes that a Path-3 live-serve does NOT provide:
//
//	(a) rw.IsServable(postBootGVR) becomes true — the GVR is REGISTERED and its
//	    informer SYNCED (the cached/consistent serve path), without a restart or
//	    a priming /call; and
//	(b) a pre-seeded stale-negative LIST dep for postBootGVR is DIRTY-MARKED (the
//	    Deps().OnResourceTypeAvailable(gvr) FD1 effect at discovery_lookup.go:487).
//
// GREEN on current main (defect-2 is already fixed — that is the verdict).
// RED against a binary where #116's registration branch is neutered (the
// DiscoverGroupResourcesFresh @crd_discovery_side_effect.go:433 /
// EnsureResourceType @discovery_lookup.go:478 call removed): IsServable stays
// false AND the stale-negative dep is never dirty-marked → this test FAILS. That
// mutation is the discriminator — Path 2/3 keep the first /call at 200 but
// provide NEITHER IsServable-true-synchronously NOR the dirty-mark, so a naive
// 200-arm would stay green under the neuter while THIS arm goes red.
func TestCRDPostBoot_RegistersAndServesCached_116Discriminating(t *testing.T) {
	// CACHE_ENABLED=true → NewResourceWatcher builds a modeInformer watcher (not
	// modePassthrough). Without it discoverGroupResources early-returns at the
	// `rw.mode == modePassthrough` guard and NEVER reaches EnsureResourceType —
	// which would make the arm falsely RED for a harness reason, not a #116 one.
	t.Setenv("CACHE_ENABLED", "true")
	withCleanCRDDiscovery(t)

	// A dynamicfake that SERVES the post-boot GVR (so once EnsureResourceType
	// registers it, the informer can actually sync and IsServable can flip true).
	// An empty object set is fine — servability is registration + HasSynced, not
	// item count.
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		postBootGVR: "GadgetList",
		// The modeInformer watcher (CACHE_ENABLED=true) eagerly registers the 4
		// RBAC GVRs and LISTs them; the dynamicfake panics on a LIST whose
		// list-kind isn't registered. Register them (same set as the
		// informer_serve harness's serveTestListKinds).
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	SetGlobal(rw)

	// #116's discovery path needs a non-nil process SA rest.Config (else
	// triggerCRDDiscovery degrades to schema-relist-only, crd_discovery_side_
	// effect.go:398). Wire a placeholder — the fake discovery client below is
	// what actually answers, via the discoveryClientBuilder seam.
	SetProcessSARestConfig(&rest.Config{Host: "https://fake.test"})

	// Inject a fake discovery surface that reports the post-boot group + its one
	// CRD-backed GVR — this is what DiscoverGroupResourcesFresh walks.
	installFakeDiscoveryForCRD(t, &fakeDiscoveryForCRD{
		group:   postBootGVR.Group,
		version: postBootGVR.Version,
		res: []metav1.APIResource{
			{Name: postBootGVR.Resource, Kind: "Gadget", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
		},
	})

	// PRE-CONDITION: the GVR is NOT servable before the CRD publish (nothing has
	// registered it). If it were already servable the arm would be vacuous.
	if rw.IsServable(postBootGVR) {
		t.Fatalf("pre-condition: postBootGVR must NOT be servable before the CRD publish")
	}

	// Seed a STALE-NEGATIVE LIST dep for the post-boot GVR: a cached LIST result
	// that returned empty because the type was unknown at LIST time. When the
	// type becomes available, #116's OnResourceTypeAvailable must dirty-mark it.
	// Observe the dirty-mark via the refresh hook.
	Deps().RecordList(context.Background(), "L1_stale_negative_list", postBootGVR, "some-ns")
	var mu sync.Mutex
	marked := map[string]bool{}
	Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		mu.Lock()
		marked[k] = true
		mu.Unlock()
	})
	t.Cleanup(func() { Deps().SetRefreshHook(nil) })

	// FIRE THE REAL #116 REGISTRATION PATH — the exact function the CRD-ADD event
	// bridge (triggerCRDDiscovery:433) calls. Not a stub.
	if _, derr := DiscoverGroupResourcesFresh(context.Background(), ProcessSARestConfig(), postBootGVR.Group); derr != nil {
		t.Fatalf("DiscoverGroupResourcesFresh(%s): %v", postBootGVR.Group, derr)
	}

	// OUTCOME (a) — the GVR is now REGISTERED + SYNCED (IsServable), the informer/
	// cached path. This is #116's contribution that Path 3's live-serve does NOT
	// provide. Poll briefly for HasSynced (EnsureResourceType's informer sync is
	// async).
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsServable(postBootGVR) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !rw.IsServable(postBootGVR) {
		t.Fatalf("#116 DISCRIMINATOR (a) FAILED: postBootGVR is NOT servable after the CRD-publish discovery path — #116's EnsureResourceType registration did not fire (a bare live-serve/Path-3 would still 200, so this is the outcome that discriminates #116)")
	}

	// OUTCOME (b) — the pre-seeded stale-negative LIST dep was DIRTY-MARKED by
	// Deps().OnResourceTypeAvailable(gvr) (discovery_lookup.go:487). Path 2/3 do
	// NOT dirty-mark; only #116's registration path does.
	mu.Lock()
	gotMark := marked["L1_stale_negative_list"]
	mu.Unlock()
	if !gotMark {
		t.Fatalf("#116 DISCRIMINATOR (b) FAILED: the stale-negative LIST dep for postBootGVR was NOT dirty-marked — Deps().OnResourceTypeAvailable did not fire from the registration path (the FD1 self-heal that makes a cached-empty LIST re-resolve once the type appears)")
	}
}
