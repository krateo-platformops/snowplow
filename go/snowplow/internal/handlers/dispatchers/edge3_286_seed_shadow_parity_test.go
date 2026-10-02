// edge3_286_seed_shadow_parity_test.go — #286 (v7-cert fast-follow for #254).
//
// THE PROOF pm-1218 required before the #254 cert: turning the dark shadow-parity
// measurement ON during the boot SEED (Step 2E, #275 — installShadowParitySeed*
// on the seed resolve ctx, hook fired in rbac.EvaluateRBAC's post-verdict defer)
// must NOT perturb the edge-3 capture/replay dep graph (#277/#278/#281). F-2E-DARK
// gave byte/key parity + a structural trace; this is the BEHAVIORAL arm:
// dep-graph identity (raKey + recorded deps + sibling convergence) shadow-ON vs
// shadow-OFF. It also checks, with the toggle ON, that the dep-tracker LIST-edge
// record→match substrate the #279 C3 seed re-walk relies on still functions —
// the REAL C3 re-walk repopulation is #285's F-C3, and non-perturbation of the
// real dep-record path under shadow-ON is the main dep-graph-identity assertion
// (both cross-referenced at that sub-arm).
//
// WHY THIS LIVES IN THE dispatchers PACKAGE (not apiref, where TestEdge3_E2E is):
// dispatchers imports apiref (phase1_pip_seed.go), so apiref cannot import back;
// the seed installs (installShadowParitySeed*) and the real hook registration
// (rbac.SetShadowHook(runShadowParityHook), this package's init) are
// dispatchers-private. An apiref-package arm would be the vacuous
// "toggle-on but the hook never installs" shape. So the faithful home is here —
// it drives apiref.Resolve (exported) with the REAL seed shadow install.
//
// FAITHFULNESS: the shadow hook fires only when the toggle is on AND a
// shadowContext was installed AND a snapshot exists (see installShadowParity +
// the EvaluateRBAC defer). This test installs the REAL installShadowParitySeed
// RESTAction on the resolve ctx and asserts populate_seed_installs_total>0 +
// checks_total>0 on the ON run — a silent no-install can't false-green (gate 3).

package dispatchers

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	pmaps "github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// --- fixture constants (unique coordinates — no cross-test dep/sliceability bleed) ---

const e3286NS = "krateo-system"
const e3286RAName = "edge3-286-ra"
const e3286BindingUID = "C:crb-a-286-uid"

func e3286RAGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
}
func e3286BackingGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps.krateo.io", Version: "v1", Resource: "compositions286"}
}

// e3286SliceJQ — a cleanly-sliceable top-level slice filter (the compositions-
// panels shape): sorts an array under one key and returns a strict sub-page.
const e3286SliceJQ = `
{
  compositionspanels: (
    (.compositionspanels // []) as $items
    | ($items | sort_by(.metadata.creationTimestamp // "") | reverse) as $sorted
    | (.slice.offset  // 0)                 as $offset
    | (.slice.perPage // ($sorted | length)) as $perPage
    | [ $sorted | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $sorted[.] ]
  )
}
`

// e3286FullDict builds the UNPAGINATED full list with a mutable marker in each
// item name (flip the marker to model a backing mutation).
func e3286FullDict(marker string) map[string]any {
	const n = 30
	items := make([]any, n)
	for i := 0; i < n; i++ {
		items[i] = map[string]any{"metadata": map[string]any{
			"name":              marker + "-panel",
			"creationTimestamp": "2026-01-01T00:00:00Z",
		}}
	}
	return map[string]any{"compositionspanels": items}
}

func e3286RA() *templatesv1.RESTAction {
	return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{Filter: ptr.To(e3286SliceJQ)}}
}

// e3286BuildWatcher stands up a real watcher over a dynamic fake seeded with the
// RBAC grant (admin get/list on restactions) AND the RESTAction CR objects.Get
// serves, registers the restactions + backing list-kinds, and makes the
// restactions GVR servable so apiref.Resolve serves the RA from the informer.
func e3286BuildWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		e3286RAGVR():      "RESTActionList",
		e3286BackingGVR(): "Composition286List",
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "ra-reader-286"},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{"templates.krateo.io"},
				Resources: []string{"restactions"},
				Verbs:     []string{"get", "list"},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "crb-286-admin", UID: types.UID("crb-a-286-uid")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "admin"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader-286"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "templates.krateo.io/v1",
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": e3286RAName, "namespace": e3286NS},
			"spec":       map[string]any{"filter": e3286SliceJQ},
		}},
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	// Make the restactions GVR servable so objects.Get serves the RA from the
	// informer (offline-degraded-true over the fake client).
	added, syncCh := rw.EnsureResourceType(e3286RAGVR())
	if added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for !rw.IsServable(e3286RAGVR()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !rw.IsServable(e3286RAGVR()) {
		t.Fatalf("restactions GVR never became servable — objects.Get would fall through to the apiserver")
	}
}

// e3286PrewarmRAKey installs the known-sliceable, resident raKey cell + edge-2
// (backing LIST) + the RA-CR self-dep, so raFullListServe takes the fast-path
// HIT and the real restactions.Resolve is never invoked. Returns the raKey.
func e3286PrewarmRAKey(t *testing.T, marker string) string {
	t.Helper()
	g := e3286RAGVR()
	inputs := e3286KeyInputs(g.Group, g.Version, g.Resource, e3286NS, e3286RAName, e3286BindingUID, nil)
	raKey := cache.ComputeKey(inputs)
	// The apiRef caller-class is "apiref" (raFullListServe's raFullListCallerClass).
	shape := cache.SliceShapeHash("apiref", g.Group, g.Version, g.Resource, e3286NS, e3286RAName, e3286SliceJQ)
	cache.ResolvedCache().PutRAFullList(raKey, inputs, e3286FullDict(marker))
	cache.RecordSliceability(raKey, shape, true)
	cache.Deps().RecordList(context.Background(), raKey, e3286BackingGVR(), e3286NS) // edge-2 (backing LIST)
	cache.Deps().Record(context.Background(), raKey, g, e3286NS, e3286RAName)        // RA-CR self-dep
	return raKey
}

func e3286Ctx(username string, groups []string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: username, Groups: groups}))
}

func e3286ApiRef() templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: e3286RAName, Namespace: e3286NS},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}
}

// e3286SortedDepStrings returns EdgesUnder(l1Key) as sorted "g/v/r|ns|name"
// strings — a stable, comparable projection of the recorded dep graph.
func e3286SortedDepStrings(l1Key string) []string {
	edges := cache.Deps().EdgesUnder(l1Key)
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, e.GVR.Group+"/"+e.GVR.Version+"/"+e.GVR.Resource+"|"+e.Namespace+"|"+e.Name)
	}
	sort.Strings(out)
	return out
}

func e3286ContainsBacking(deps []string) bool {
	want := e3286BackingGVR()
	target := want.Group + "/" + want.Version + "/" + want.Resource + "|" + e3286NS + "|*"
	for _, d := range deps {
		if d == target {
			return true
		}
	}
	return false
}

// e3286Run drives ONE producer+sibling pass (w1 memo-miss → B2 capture, w2 memo-
// hit → B3 replay) through apiref.Resolve under a fresh memo, with the seed
// shadow context installed when shadowOn. Returns the sibling (w2) recorded deps.
// wPrefix keeps the two runs' widget keys distinct so they share the raKey but
// not the widget cells.
func e3286Run(t *testing.T, base context.Context, wPrefix string, shadowOn bool) (w1Key, w2Key string, w2Deps []string) {
	t.Helper()
	w1Key = "L1_edge3_286_" + wPrefix + "_w1"
	w2Key = "L1_edge3_286_" + wPrefix + "_w2"
	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	ra := e3286RA()
	opts := apiref.ResolveOptions{ApiRef: e3286ApiRef(), PerPage: 5, Page: 1}

	mkCtx := func(wKey string) context.Context {
		c := cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, wKey), memo)
		if shadowOn {
			// The REAL seed-path install (bumps populate_seed_installs_total when
			// a shadowContext is actually installed; the hook then fires in
			// EvaluateRBAC's post-verdict defer).
			c = installShadowParitySeedRESTAction(c, ra)
		}
		return c
	}

	if _, err := apiref.Resolve(mkCtx(w1Key), opts); err != nil {
		t.Fatalf("%s w1 apiref.Resolve failed: %v", wPrefix, err)
	}
	if _, err := apiref.Resolve(mkCtx(w2Key), opts); err != nil {
		t.Fatalf("%s w2 apiref.Resolve failed: %v", wPrefix, err)
	}
	return w1Key, w2Key, e3286SortedDepStrings(w2Key)
}

// TestEdge3_286_SeedShadowParityDoesNotPerturbCapture is the #286 behavioral
// arm: shadow-ON-during-seed does not perturb the edge-3 capture/replay dep
// graph (identical raKey + recorded deps + sibling convergence, shadow-ON vs
// shadow-OFF, via the real EvaluateRBAC+replay path). It also sanity-checks the
// dep-tracker LIST-edge record→match substrate with the toggle ON (honest scope
// noted at that sub-arm; the real #279 C3 re-walk is #285's F-C3).
func TestEdge3_286_SeedShadowParityDoesNotPerturbCapture(t *testing.T) {
	e3286BuildWatcher(t)
	cache.Deps().SetStore(cache.ResolvedCache())

	g := e3286RAGVR()
	raKey := cache.ComputeKey(e3286KeyInputs(g.Group, g.Version, g.Resource, e3286NS, e3286RAName, e3286BindingUID, nil))
	e3286PrewarmRAKey(t, "OLD")

	base := e3286Ctx("admin", []string{"system:masters"})

	// ── shadow-OFF baseline ──
	rbac.SetShadowParityEnabled(false)
	reset2E()
	rbac.ResetRequesterProfileMemoForTest()
	_, w2Off, depsOff := e3286Run(t, base, "off", false)

	// ── shadow-ON ──
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	reset2E()
	rbac.ResetRequesterProfileMemoForTest()
	_, w2On, depsOn := e3286Run(t, base, "on", true)

	// PROVE-IT-RAN (gate 3): the seed shadow context actually installed and the
	// hook actually fired on the ON run — a silent no-install can't false-green.
	if installs := shadowPopulateSeedInstallsTotal.Load(); installs == 0 {
		t.Fatalf("#286 prove-it-ran RED: populate_seed_installs_total==0 on the shadow-ON run — the seed shadow context never installed, so shadow-ON was vacuous")
	}
	if checks := shadowChecksTotal.Load(); checks == 0 {
		t.Fatalf("#286 prove-it-ran RED: checks_total==0 on the shadow-ON run — the hook never fired in EvaluateRBAC, so shadow-ON exercised nothing")
	}

	// ── DEP-GRAPH IDENTITY (the #286 core): raKey + recorded deps identical ──
	// raKey is deterministic from RBAC; assert both runs derived the SAME cell.
	if _, ok := cache.ResolvedCache().Get(raKey); !ok {
		t.Fatalf("#286 setup: the pre-warmed raKey cell must be resident")
	}
	if !e3286ContainsBacking(depsOff) {
		t.Fatalf("#286 setup: shadow-OFF sibling must carry the backing edge (edge-3): %v", depsOff)
	}
	if !reflect.DeepEqual(depsOff, depsOn) {
		t.Fatalf("#286 RED: shadow-ON perturbed the edge-3 capture/replay dep graph.\n  OFF (%s): %v\n  ON  (%s): %v", w2Off, depsOff, w2On, depsOn)
	}

	// ── SIBLING CONVERGENCE (ON vs OFF both reflect a backing mutation) ──
	// Flip the backing marker to NEW and refresh the shared raKey cell (models
	// edge-2's raKey refresh — not under test), then drive the backing UPDATE
	// through the real dep decision site; a refresher re-serves any dirty-marked
	// widget. Both siblings carry edge-3, so both must converge.
	inputs := e3286KeyInputs(g.Group, g.Version, g.Resource, e3286NS, e3286RAName, e3286BindingUID, nil)
	cache.ResolvedCache().PutRAFullList(raKey, inputs, e3286FullDict("NEW"))

	widgets := map[string]context.Context{
		w2Off: cache.WithL1KeyContext(base, w2Off),
		w2On:  cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, w2On), cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)),
	}
	var mu sync.Mutex
	marked := map[string]bool{}
	cache.Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		mu.Lock()
		defer mu.Unlock()
		marked[k] = true
		wc, ok := widgets[k]
		if !ok {
			return
		}
		got, err := apiref.Resolve(wc, apiref.ResolveOptions{ApiRef: e3286ApiRef(), PerPage: 5, Page: 1})
		if err != nil {
			t.Errorf("refresh re-serve of %q failed: %v", k, err)
			return
		}
		cache.ResolvedCache().Put(k, &cache.ResolvedEntry{RawJSON: e3286MustJSON(t, got), Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets", Namespace: e3286NS, Name: k}})
	})
	cache.Deps().OnUpdate(e3286BackingGVR(), e3286NS, "comp-286-x")

	mu.Lock()
	mOff, mOn := marked[w2Off], marked[w2On]
	mu.Unlock()
	if !mOff || !mOn {
		t.Fatalf("#286 RED: a sibling did not enter the dirty set on the backing UPDATE (off=%v on=%v) — edge-3 missing under shadow-%s", mOff, mOn, map[bool]string{true: "ON", false: "OFF"}[!mOn])
	}
	if !e3286ServedHasMarker(t, w2Off, "NEW") || !e3286ServedHasMarker(t, w2On, "NEW") {
		t.Fatalf("#286 RED: a sibling served body did not converge to the mutated backing content (off/on)")
	}

	// ── dep-tracker LIST-edge record→match mechanics remain intact, toggle ON ──
	// HONEST SCOPE (freshness-audit + pm-1218 nit): this runs while shadow-parity
	// is ON, but it drives cache.Deps().OnAdd/RecordList DIRECTLY — those never
	// call rbac.EvaluateRBAC, where the shadow hook lives (post-verdict defer), so
	// the toggle cannot affect this path. It therefore does NOT itself exercise
	// the #279 C3 re-walk *under the hook*; it asserts only that the dep-tracker
	// substrate the re-walk repopulation relies on (a LIST-edge record makes a
	// later CR ADD match) still functions with the toggle enabled.
	//
	// The REAL iterator-empty seed re-walk repopulation is covered by #285's F-C3
	// (fc3_seed_rewalk_repopulates_edge_test.go — which itself models the
	// re-record over the inert hermetic transport). Non-perturbation of the REAL
	// dep-record path under shadow-ON is covered by the main dep-graph-identity
	// assertion above: the backing LIST edge (edge-3) is re-recorded IDENTICALLY
	// shadow-ON vs shadow-OFF through the real EvaluateRBAC+replay path (where the
	// hook fires), and the spurious-RecordList mutant in runShadowParityHook REDs
	// it — so that arm is discriminating, this substrate check is not claimed to be.
	depGVR := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "githubprovider-286-sub"}
	const depNS, depName, depCell = "ns-286-sub", "app-286-sub", "cell-286-sub"
	if pre := cache.Deps().OnAdd(depGVR, depNS, depName); pre != 0 {
		t.Fatalf("#286 dep-substrate setup: expected 0 matches before the LIST record on a unique GVR; got %d (cross-test bleed)", pre)
	}
	cache.Deps().RecordList(context.Background(), depCell, depGVR, depNS)
	if post := cache.Deps().OnAdd(depGVR, depNS, depName); post < 1 {
		t.Fatalf("#286 dep-substrate: LIST-edge record→match broke with the toggle ON; OnAdd=%d", post)
	}
}

func e3286MustJSON(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func e3286ServedHasMarker(t *testing.T, key, marker string) bool {
	t.Helper()
	entry, ok := cache.ResolvedCache().Get(key)
	if !ok {
		t.Fatalf("no cell stored under %q", key)
	}
	var body map[string]any
	if err := json.Unmarshal(entry.RawJSON, &body); err != nil {
		t.Fatalf("decode cell %q: %v", key, err)
	}
	items, _ := body["compositionspanels"].([]any)
	for _, it := range items {
		m, _ := it.(map[string]any)
		meta, _ := m["metadata"].(map[string]any)
		if name, _ := meta["name"].(string); name == marker+"-panel" {
			return true
		}
	}
	return false
}
