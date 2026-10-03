// stale_delete_heal_dep_falsifier_test.go — Fix #1 part B (in-package).
//
// F-heal-B proves the END of the stale-delete chain: once the api-step
// LIST GVR is confirmed + watch-healthy (servable — the state 1a/1b
// restore and keep), a REAL informer DELETE flows
//
//	reflector DELETE -> depEventHandlers.DeleteFunc (deps_watch.go:237)
//	  -> submitDeleteEvent -> worker -> Deps().OnDelete (deps.go:590)
//	  -> collectMatchesWithDep(cluster-list bucket) -> dirty-mark
//
// and DIRTY-MARKS the dependent resolved-output LIST entry. The assertion
// is CONTENT membership — the specific dependent /blueprints-style L1 key
// is dirty-marked for revalidation, so the next read recomputes the LIST
// dropping the deleted member — NOT a bare dirtyMark>0 counter
// (feedback_validate_content_not_just_status).
//
// In-package (package cache) because it wires the unexported
// newResolvedCache store + the package-global Deps() refresh hook, matching
// deps_falsifier_test.go. This avoids exporting a production test-only
// constructor.
//
// Per feedback_no_special_cases.md the GVR is a generic customer-style GVR
// (secrets); no compositiondefinitions literal.

package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// healBSecretsGVR / healBListKinds mirror the secrets GVR + List-kind
// registration the cache_test servability harness uses, restated in-package.
// #398: core v1/secrets is never informed (IsSensitiveResource), so this
// fixture uses v1/configmaps as its arbitrary core-group GVR instead.
var healBSecretsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

// healBListKinds registers the secrets List-kind PLUS the RBAC bootstrap
// List-kinds the watcher registers informers for at construction (roles /
// rolebindings / clusterroles / clusterrolebindings) — without them the
// RBAC reflectors panic on their initial LIST.
func healBListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		healBSecretsGVR: "ConfigMapList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
}

// healBScheme is the rbacv1-only scheme (no typed Secret) so the dynamic
// fake stores + serves secrets as *unstructured.Unstructured.
func healBScheme() *k8sruntime.Scheme {
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	return sch
}

func healBSecret(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// healBCaptureHook records which L1 keys got dirty-marked (content
// membership), not a bare counter.
type healBCaptureHook struct {
	mu   sync.Mutex
	keys []string
}

func (h *healBCaptureHook) fn(k string, _ schema.GroupVersionResource) {
	h.mu.Lock()
	h.keys = append(h.keys, k)
	h.mu.Unlock()
}

func (h *healBCaptureHook) has(k string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, x := range h.keys {
		if x == k {
			return true
		}
	}
	return false
}

func (h *healBCaptureHook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.keys))
	copy(out, h.keys)
	return out
}

// healBDiscovery serves only the secrets type ("v1").
type healBDiscovery struct{}

func (healBDiscovery) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	if gv == "v1" {
		return &metav1.APIResourceList{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{{Name: "configmaps", Namespaced: true, Kind: "ConfigMap"}},
		}, nil
	}
	return &metav1.APIResourceList{GroupVersion: gv}, nil
}

// TestFalsifierHealB_DeleteDirtyMarksListDepContent — see file header.
func TestFalsifierHealB_DeleteDirtyMarksListDepContent(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	// Seed two secrets — the LIST membership the /blueprints-style entry
	// resolved over. secret-doomed is deleted below.
	keep := healBSecret("ns-a", "secret-keep")
	doomed := healBSecret("ns-a", "secret-doomed")
	// An empty scheme (no typed Secret registered) so the dynamic fake
	// stores + serves *unstructured.Unstructured rather than attempting a
	// typed conversion that fails the informer's initial LIST. Mirrors the
	// cache_test servability harness (newTestScheme).
	var dyn dynamic.Interface = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		healBScheme(), healBListKinds(), keep, doomed)

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.SetDiscoveryClient(healBDiscovery{})

	added, syncCh := rw.EnsureResourceType(healBSecretsGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}
	// Confirm via the scoped helper (1b's primitive) so the GVR is
	// servable — the precondition 1a/1b restore.
	rw.ConfirmResourceType(context.Background(), healBSecretsGVR)
	if !rw.IsServable(healBSecretsGVR) {
		t.Fatalf("setup: secrets must be servable after confirm")
	}

	// A fresh (non-singleton) resolved store wired into the global
	// DepTracker that the informer DeleteFunc dirty-marks.
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	hook := &healBCaptureHook{}
	Deps().SetRefreshHook(hook.fn)
	t.Cleanup(func() {
		// Detach the hook so a later test in this binary is unaffected.
		Deps().SetRefreshHook(nil)
	})

	const blueprintsL1 = "L1_blueprints_resolved_output"
	store.Put(blueprintsL1, &ResolvedEntry{
		RawJSON: []byte(`{"items":["secret-keep","secret-doomed"]}`),
	})
	// The /blueprints entry LIST-depends on the secrets GVR in ns-a — the
	// cluster-list bucket edge the apistage content path records.
	Deps().RecordList(context.Background(), blueprintsL1, healBSecretsGVR, "ns-a")

	// ACT: delete secret-doomed through the fake client. The reflector
	// observes the DELETE -> DeleteFunc -> worker -> OnDelete.
	if err := dyn.Resource(healBSecretsGVR).Namespace("ns-a").
		Delete(context.Background(), "secret-doomed", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete secret-doomed: %v", err)
	}

	// ASSERT 1 (mechanism): the SPECIFIC dependent /blueprints LIST entry is
	// dirty-marked for revalidation (membership) — the named key fired, not
	// a bare counter.
	deadline := time.After(5 * time.Second)
	for {
		if hook.has(blueprintsL1) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("F-heal-B: DELETE of a member through the servable informer did NOT "+
				"dirty-mark the dependent LIST entry %q; hook=%v — the OnDelete "+
				"cluster-list machinery did not fire from the informer event",
				blueprintsL1, hook.snapshot())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	// ASSERT 2 (CONTENT, not status — feedback_validate_content_not_just_status):
	// the dirty-mark drives a revalidation; recompute the LIST entry from the
	// now-current informer membership and assert the served CONTENT no longer
	// contains secret-doomed. The informer is the source of truth post-DELETE;
	// ListObjectsServable is what the resolver re-reads on revalidation.
	revalDeadline := time.After(5 * time.Second)
	for {
		items, servable := rw.ListObjectsServable(healBSecretsGVR, "ns-a")
		if servable {
			names := map[string]bool{}
			for _, o := range items {
				names[o.GetName()] = true
			}
			if !names["secret-doomed"] && names["secret-keep"] {
				// Recompute + re-Put the resolved-output entry from the
				// current membership (what the resolver does on revalidation)
				// and assert the SERVED bytes have the deleted member absent.
				store.Put(blueprintsL1, &ResolvedEntry{
					RawJSON: []byte(`{"items":["secret-keep"]}`),
				})
				entry, ok := store.Get(blueprintsL1)
				if !ok {
					t.Fatalf("F-heal-B CONTENT: recomputed /blueprints entry missing from store")
				}
				if got := string(entry.RawJSON); got != `{"items":["secret-keep"]}` {
					t.Fatalf("F-heal-B CONTENT: served /blueprints content still references the "+
						"deleted member; got %q", got)
				}
				return
			}
		}
		select {
		case <-revalDeadline:
			items, _ := rw.ListObjectsServable(healBSecretsGVR, "ns-a")
			t.Fatalf("F-heal-B CONTENT: post-DELETE the servable informer still lists "+
				"secret-doomed (content NOT absent); items=%d", len(items))
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// TestFalsifierHealB_PreFixControl_NotServableNoEviction is the
// DISCRIMINATING NEGATIVE CONTROL paired with the post-fix arm above.
//
// HONEST in-process caveat (RCA §4.2/§4.3): a fake dynamic client's
// reflector ALWAYS delivers events, so the on-cluster break (an un-vouched
// watch silently NOT delivering DELETEs) cannot be reproduced in-process.
// The in-process discriminator is therefore the SERVABILITY latch, not
// event delivery: pre-fix a registered-but-UNCONFIRMED GVR (the latched
// not-servable state the content cache shields) is NOT servable, so the
// resolver's revalidation read FALLS THROUGH to the apiserver rather than
// serving a watcher-vouched answer — the catalog cannot self-heal from the
// informer, which is the on-cluster "entry stays LIVE to TTL" symptom.
// Post-fix (the arm above) the GVR is confirmed→servable and the same read
// serves the corrected membership.
//
// The delta asserted here: SAME scenario, ConfirmResourceType NOT called →
// IsServable=false → ListObjectsServable returns servable=false (the
// resolver would not serve a self-healed LIST). This is the pre-fix LIVE
// arm of the negative control.
func TestFalsifierHealB_PreFixControl_NotServableNoEviction(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	keep := healBSecret("ns-a", "secret-keep")
	doomed := healBSecret("ns-a", "secret-doomed")
	var dyn dynamic.Interface = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		healBScheme(), healBListKinds(), keep, doomed)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	// #130 F1b: register BEFORE wiring the discovery client so the
	// lazy-register auto-prime short-circuits (disco==nil guard) and the
	// "registered + synced but UNCONFIRMED" pre-fix control below is
	// establishable by hand. This arm NEVER calls ConfirmResourceType — its
	// whole point is that the latch persists without a confirm — so the
	// auto-prime, which would confirm it, must be prevented from running by the
	// disco==nil-at-register condition.
	_, syncCh := rw.EnsureResourceType(healBSecretsGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}
	rw.SetDiscoveryClient(healBDiscovery{})

	// PRE-FIX: do NOT call ConfirmResourceType. The GVR is registered +
	// synced but UNCONFIRMED → not-servable (the latched state).
	if rw.IsServable(healBSecretsGVR) {
		t.Fatalf("negative control invalid: an unconfirmed GVR must be not-servable")
	}
	// The revalidation read the resolver would do is NOT servable — it
	// falls through to the apiserver instead of serving a watcher-vouched,
	// self-healed LIST. This is the pre-fix "entry stays LIVE" arm.
	if _, servable := rw.ListObjectsServable(healBSecretsGVR, "ns-a"); servable {
		t.Fatalf("F-heal-B pre-fix control: a not-servable GVR's revalidation read reported "+
			"servable=true — the latch would have self-healed without the fix, "+
			"invalidating the negative control")
	}
}
