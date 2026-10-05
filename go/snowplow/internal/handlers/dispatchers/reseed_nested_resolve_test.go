package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// reseed_nested_arm_test.go — #258 arch req#3 / #378: the REAL nested-resolve arm.
//
// Drives a genuine restactions resolve (real restactions.Resolve, informer-served,
// hermetic — NOT a stubbed resolver) whose single api stage LISTs configmaps, so
// the resolve NESTS an apistage content Put (verified: 2 cells — the terminal
// restactions cell + the apistage LIST content cell).
//
// #378: the only BornAt-resetting write is the refresher terminal
// (cache.ReplaceIfGenRefresh). TestRefresherReMint_TerminalResets_NestedApistageUntouched
// ages BOTH cells into the lead window by REAL elapse and runs the refresher's
// resolveAndPopulateL1 on the terminal cell: the TERMINAL cell's BornAt RESETS
// while the resident nested apistage cell — consulted by the same real resolve —
// keeps its birth. It replaces the retired seedModeReMint arm
// (TestSeedOneRestaction_ReMint_TerminalResets_NestedApistageUntouched).
//
// NOTE (mechanism, verified): apistage content is INSERT-ON-MISS (apistage.go:599
// miss-branch PutIfGen; a resident cell is HIT at :585 with NO Put), so on the
// refresh the resident nested cell is HIT (untouched). The arm proves the nested
// cell is NOT re-minted; there is no nested-REPLACE path by which the re-mint
// could even be mis-applied.

var nestedRAGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
var nestedCMGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

const nestedRAName = "nested-lister"
const nestedNS = "krateo-system"

// buildNestedApistageWatcher publishes a hermetic watcher with a RESTAction whose
// one api stage LISTs configmaps in tenant-a (informer-served), + RBAC granting the
// cohort get restactions / list configmaps. A real seedOneRestaction resolve then
// nests an apistage content Put (the configmaps LIST cell).
func buildNestedApistageWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		nestedRAGVR: "RESTActionList",
		nestedCMGVR: "ConfigMapList",
		crbGVR:      "ClusterRoleBindingList",
		crGVR:       "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}

	ra := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": nestedRAName, "namespace": nestedNS},
		"spec": map[string]any{
			"api": []any{
				map[string]any{
					"name": "list-cms",
					"path": "/api/v1/namespaces/" + a1TenantA + "/configmaps",
					"verb": "GET",
				},
			},
		},
	}}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: a1TenantA, Name: "cm-1"}, Data: map[string]string{"k": "v"}}

	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "list"},
				APIGroups: []string{nestedRAGVR.Group, ""},
				Resources: []string{nestedRAGVR.Resource, "configmaps"},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "reader-bind", UID: types.UID("uid-reader")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: a1Group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"},
		},
		ra,
		cm,
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(nestedRAGVR)
	rw.EnsureResourceType(nestedCMGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

func nestedCohortCtx() context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: a1Alice, Groups: []string{a1Group}},
		endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"},
		&rest.Config{Host: "http://127.0.0.1:1"})
}

func nestedRARef() templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: nestedRAName, Namespace: nestedNS},
		APIVersion: "templates.krateo.io/v1",
		Resource:   "restactions",
	}
}

// keysByClass returns the single live key of each cache entry class after a resolve.
func keysByClass(t *testing.T, store *cache.ResolvedCacheStore) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range store.KeysForTest() {
		e, ok := store.Get(k)
		if !ok || e == nil || e.Inputs == nil {
			continue
		}
		out[e.Inputs.CacheEntryClass] = k
	}
	return out
}

// TestRefresherReMint_TerminalResets_NestedApistageUntouched — arch req#3 on the
// #378 carrier (replaces the seedModeReMint arm).
func TestRefresherReMint_TerminalResets_NestedApistageUntouched(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "60")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "4") // L = min(60, 2) = 2s → window [2s, 4s)
	buildNestedApistageWatcher(t)
	ctx := nestedCohortCtx()
	ref := nestedRARef()
	store := cache.ResolvedCache()

	// (1) POPULATE via a real resolve: terminal restactions cell + nested apistage
	// content cell.
	if err := seedOneRestaction(ctx, "cohort", ref, nestedNS, seedModeGVRDiscovered); err != nil {
		t.Fatalf("populate seedOneRestaction: %v", err)
	}
	keys := keysByClass(t, store)
	termKey, haveTerm := keys["restactions"]
	apiKey, haveApi := keys["apistage"]
	if !haveTerm || !haveApi {
		t.Fatalf("PRECONDITION: the real resolve must produce BOTH a terminal restactions cell and a nested "+
			"apistage content cell (else the nested arm is vacuous); got classes %v", keys)
	}
	term0, _ := store.GetNoTouch(termKey)
	api0, _ := store.GetNoTouch(apiKey)

	// (2) Age BOTH cells into the lead window by real elapse.
	time.Sleep(2300 * time.Millisecond)

	// (3) The refresher terminal re-resolves the terminal cell for real; the
	// resident nested apistage cell is consulted (hit), never re-minted.
	if err := resolveAndPopulateL1(context.Background(), *term0.Inputs, nil, nil); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	term, ok := store.GetNoTouch(termKey)
	if !ok || term == term0 {
		t.Fatal("the refresher terminal must have re-written the terminal cell")
	}
	api, ok := store.GetNoTouch(apiKey)
	if !ok {
		t.Fatal("nested apistage cell missing after the refresh")
	}
	if !term.BornAt.After(term0.BornAt) {
		t.Fatalf("req#3: the TERMINAL cell must RE-MINT (BornAt reset); got %v want > %v", term.BornAt, term0.BornAt)
	}
	if !api.BornAt.Equal(api0.BornAt) {
		t.Fatalf("req#3 LEAK: the nested apistage cell's BornAt changed under the terminal's re-mint "+
			"(%v != %v) — the re-mint reached a NON-terminal cell", api.BornAt, api0.BornAt)
	}
}

// TestSeedOneRestaction_GVRDiscovered_TerminalInheritsBornAt — the plain-path
// control at the restactions terminal: a non-reseed mode re-Put INHERITS the
// terminal cell's BornAt (never resets): no seed mode re-mints (#378).
func TestSeedOneRestaction_GVRDiscovered_TerminalInheritsBornAt(t *testing.T) {
	buildNestedApistageWatcher(t)
	ctx := nestedCohortCtx()
	ref := nestedRARef()
	store := cache.ResolvedCache()

	if err := seedOneRestaction(ctx, "cohort", ref, nestedNS, seedModeGVRDiscovered); err != nil {
		t.Fatalf("populate: %v", err)
	}
	termKey := keysByClass(t, store)["restactions"]
	if termKey == "" {
		t.Fatal("no terminal cell after populate")
	}
	past := time.Now().Add(-2 * time.Hour)
	if !store.SetBornAtForTest(termKey, past) {
		t.Fatal("SetBornAtForTest failed")
	}
	if err := seedOneRestaction(ctx, "cohort", ref, nestedNS, seedModeGVRDiscovered); err != nil {
		t.Fatalf("re-put: %v", err)
	}
	term, _ := store.Get(termKey)
	if !term.BornAt.Equal(past) {
		t.Fatalf("a plain (gvr-discovered) terminal re-Put must INHERIT BornAt; got %v want %v", term.BornAt, past)
	}
}
