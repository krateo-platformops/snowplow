// phase1_content_prewarm_phantom_dep_falsifier_250_test.go — #250 falsifiers.
//
// #250: prewarmOneRESTAction threaded a restactions-class WithL1KeyContext
// (phase1_content_prewarm.go:432, the ":433" subset key) into
// restactions.Resolve. That key is a dep-edge CONTEXT anchor — it is NEVER
// Put (the pass Puts only per-K8s-call apistage/cluster_list CONTENT cells,
// each self-keyed under its own contentKey). So the ambient key only anchored
// the resolver's top-level dep edges (resolve.go:483/:1610) under a key with
// no entry — a PHANTOM invalidation target (dirty-marking a non-existent
// cell). sole-coverage = NO / double-covered → Option A: drop the install,
// resolve under the bare rctx.
//
// The suite is the differential/redundancy proof that the drop is safe AND a
// STANDING drift guard. Two guard MECHANISMS, catching DIFFERENT defects:
//
//   INV1  ":433 is never itself a Put key" — a Put landing at the subset key.
//   INV2  self-coverage: no dep edge is anchored at a key that was never Put
//         (a DYNAMIC full-index scan, cache.Deps().RangeEdges). This is the
//         outcome-2 detector — a content cell whose edge LEAKS to the ambient
//         :433 (a nested site not re-installing its own key) makes the ambient
//         key an edge-anchor with no entry.
//
// Because 1+3 already hold on main (this is a cleanup — the invariants are
// green before the edit; INV2 is RED on the UNFIXED tree because the phantom
// resolve.go:483/:1610 edges are anchored at the never-Put :433, and GREEN
// after the drop), each mechanism carries a POSITIVE CONTROL proving it CAN
// fire (arm-that-cannot-fail discipline, one control per mechanism):
//
//   Control-A → INV2 fires (edge anchored at a never-Put key); the
//               present-vs-dropped differential is BLIND to it (stale both
//               ways) — documenting that the INVARIANT, not the differential,
//               is the outcome-2 guard.
//   Control-B → INV1 fires (Put AT :433) AND the differential fires (present:
//               a backing change invalidates the :433 cell; dropped: it does
//               not → they DIFFER).
//
// Put-capture + the INV2 scan are DYNAMIC (over whatever the real resolve
// produces), never a hardcoded {apistage, cluster_list} list — so a future
// nested Put site leaking under :433 is caught, not silently passed.

package dispatchers

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// w250GVR is the served/seeded content GVR the RESTAction's api stage LISTs.
var w250GVR = schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1", Resource: "widgets"}

const (
	w250NS      = "team-a"
	w250RAName  = "obs-list"
	w250SAUser  = "system:serviceaccount:krateo-system:snowplow"
	w250BackObj = "widget-1"

	// api-stage paths: a namespaced LIST (name="" content cell, dirty-marks on a
	// backing change) and a GET-by-name (name-keyed content cell — the #216
	// self-evict shape whose owning-object DELETE self-evicts it).
	w250ListPath   = "/apis/widgets.krateo.io/v1/namespaces/" + w250NS + "/widgets"
	w250ByNamePath = w250ListPath + "/" + w250BackObj
)

func w250Widget(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.krateo.io/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// w250RESTAction is a data-source RESTAction whose single api stage LISTs the
// seeded widgets in w250NS — the shape the content prewarm harvests + resolves.
func w250RESTAction(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec": map[string]any{
			"api": []any{map[string]any{
				"name": "widgets",
				"path": "/apis/widgets.krateo.io/v1/namespaces/" + ns + "/widgets",
				"verb": "GET",
			}},
		},
	}}
}

func new250Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	seed := []k8sruntime.Object{
		w250Widget(w250NS, "widget-1"),
		w250Widget(w250NS, "widget-2"),
		w250RESTAction(w250NS, w250RAName),
		// The prewarm SA must be RBAC-authorized to GET the RESTAction CR
		// (objects.Get filterGetByRBAC, get.go:120) — else the informer serve
		// falls through to an apiserver dial. The real content-prewarm SA has
		// this grant; seed it here.
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "prewarm250-reader"},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}},
				{APIGroups: []string{"widgets.krateo.io"}, Resources: []string{"widgets"}, Verbs: []string{"get", "list"}},
			},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "prewarm250-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: w250SAUser}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "prewarm250-reader"},
		},
	}
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	listKinds := map[schema.GroupVersionResource]string{
		w250GVR:       "WidgetList",
		restActionGVR: "RESTActionList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

	for _, gvr := range []schema.GroupVersionResource{w250GVR, restActionGVR} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s informer did not sync within 5s", gvr)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// l250Key is the exact ":433" subset key prewarmOneRESTAction built for the
// w250 RESTAction — recomputed here so the phantom-anchor assertions target
// the SAME key the (dropped) install used.
func l250Key() string {
	return cache.ComputeKey(cache.ResolvedKeyInputs{
		CacheEntryClass: "restactions",
		Group:           restActionGVR.Group,
		Version:         restActionGVR.Version,
		Resource:        restActionGVR.Resource,
		Namespace:       w250NS,
		Name:            w250RAName,
	})
}

// drive250Resolve runs the widget-LIST stage through the REAL restactions
// resolver under the content-prewarm markers. l1Key!="" installs the ambient
// key (== the pre-fix prewarm install); l1Key=="" is the post-fix bare ctx.
func drive250Resolve(t *testing.T, rw *cache.ResourceWatcher, l1Key, path string) {
	t.Helper()
	ctx := cache.WithApistagePrewarm(xcontext.BuildContext(context.Background(),
		// The real prewarm SA context carries a system:serviceaccount username
		// alongside ApistagePrewarm; the resolver needs an identity on ctx even
		// though the per-user content gate is skipped under prewarm.
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: w250SAUser})))
	ctx = cache.WithServeWatcher(ctx, rw)
	// 127.0.0.1:1 fails fast (connection refused, no DNS wait); the K8s call is
	// served from the synced informer anyway — the endpoint is only dialed for the
	// stage's discarded live call.
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"})
	if l1Key != "" {
		ctx = cache.WithL1KeyContext(ctx, l1Key)
	}
	stage := &templates.API{
		Name: "widgets",
		Path: path,
		Verb: ptr.To("GET"),
	}
	_ = restactionsapi.Resolve(ctx, restactionsapi.ResolveOptions{
		RC:      &rest.Config{Host: "http://127.0.0.1:1"},
		Watcher: rw,
		Items:   []*templates.API{stage},
	})
}

// put250Keys captures every L1 key currently in the resolved store — the
// DYNAMIC Put-capture (the store is reset per test, so post-resolve contents
// == exactly what this resolve Put).
func put250Keys(store *cache.ResolvedCacheStore) map[string]bool {
	keys := map[string]bool{}
	store.RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		keys[m.KeyHash] = true
		return true
	})
	return keys
}

// phantomAnchors250 is the DYNAMIC INV2 scan (the mode-a / outcome-2 detector):
// every anchor carrying edges but with NO store entry is a phantom —
// invalidation would dirty-mark a cell that does not exist. It is the EDGE-
// ANCHOR clause: an entry keyed K_E whose edge leaked to :433 reds ONLY here,
// never in the ":433 ∉ Put-keys" entry-key clause (INV1). Returns the violating
// anchors (empty == self-covered).
//
// Scope (arch guardrail 1): RESOLVE-SCOPED by construction — every test resets
// the dep index (ResetDepsForTest) before driving the prewarm, so
// cache.Deps().RangeEdges sees ONLY the anchors THIS resolve produced (:433 +
// the contentKeys). It therefore means "this resolve produced no orphan edge,"
// not a global process scan (which could false-RED on an unrelated never-Put
// anchor). The post-fix real-chain GREEN empirically confirms the harness has
// no stray never-Put anchors.
//
// Boundary (arch guardrail 2): this guards the ORPHAN-:433 drift ONLY; it does
// NOT catch a cross-attribution to a DIFFERENT LIVE key (an edge misfiled to
// another real cell's contentKey leaves no orphan → INV2 green) — a distinct
// failure mode, out of #250's scope, deliberately not chased here.
func phantomAnchors250(store *cache.ResolvedCacheStore) []string {
	var bad []string
	cache.Deps().RangeEdges(func(l1Key string, edges []cache.DepKey) bool {
		if len(edges) == 0 {
			return true
		}
		if _, ok := store.Get(l1Key); !ok {
			bad = append(bad, l1Key)
		}
		return true
	})
	return bad
}

func contentKeys250(puts map[string]bool, l433 string) []string {
	out := make([]string, 0, len(puts))
	for k := range puts {
		if k != l433 {
			out = append(out, k)
		}
	}
	return out
}

func contains250(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// hookCapture250 returns a refresh-hook + snapshot capturing every enqueued
// L1 key (dirty-mark propagation).
func hookCapture250() (func(string, schema.GroupVersionResource), func() map[string]bool) {
	seen := map[string]bool{}
	hook := func(k string, _ schema.GroupVersionResource) { seen[k] = true }
	snap := func() map[string]bool {
		out := map[string]bool{}
		for k := range seen {
			out[k] = true
		}
		return out
	}
	return hook, snap
}

// -----------------------------------------------------------------------------
// PROBE — validates the hermetic mechanism the suite rests on.
// -----------------------------------------------------------------------------

func TestPrewarm250_Probe_HarnessProducesPutAndPhantomEdge(t *testing.T) {
	rw := new250Watcher(t)
	l433 := l250Key()

	drive250Resolve(t, rw, l433, w250ListPath)
	store := cache.ResolvedCache()
	puts := put250Keys(store)
	if len(puts) == 0 {
		t.Fatalf("#250 PROBE: the widget-LIST resolve Put NO content cell — the apistage " +
			"content layer did not fire; harness is inert (nothing to assert self-coverage over)")
	}
	if puts[l433] {
		t.Fatalf("#250 PROBE: the resolve Put an entry AT the restactions subset key l433=%q", l433)
	}
	if got := cache.Deps().EdgesUnder(l433); len(got) == 0 {
		t.Fatalf("#250 PROBE: resolving WITH the ambient l433 recorded NO parent edge under it — the " +
			"phantom-edge mechanism this fix removes is not exercised (differential would be vacuous)")
	}

	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	drive250Resolve(t, rw, "", w250ListPath)
	store = cache.ResolvedCache()
	if len(put250Keys(store)) == 0 {
		t.Fatalf("#250 PROBE: the bare-ctx resolve Put NO content cell — dropping the ambient key " +
			"must NOT change what is warmed")
	}
	if got := cache.Deps().EdgesUnder(l433); len(got) != 0 {
		t.Fatalf("#250 PROBE: the bare-ctx resolve recorded %d edge(s) under l433 — must be 0", len(got))
	}
}

// -----------------------------------------------------------------------------
// REAL-CHAIN — the actual prewarmOneRESTAction (the function whose :433 install
// #250 drops). GREEN post-fix; RED on the UNFIXED tree (phantom l433 anchor).
// -----------------------------------------------------------------------------

func TestPrewarm250_RealChain_PrewarmOneRESTAction_SelfCovered(t *testing.T) {
	rw := new250Watcher(t)
	l433 := l250Key()

	// Build the content-prewarm SA context (identity + ApistagePrewarm +
	// ServeWatcher + internal endpoint/rc), then drive the REAL function.
	rc := &rest.Config{Host: "http://127.0.0.1:1"}
	saEP := endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"}
	rctx := cache.WithApistagePrewarm(xcontext.BuildContext(context.Background(),
		// WithUserConfig sets the per-user *Endpoint objects.Get reads to fetch the
		// RESTAction CR (served from the synced informer here); WithUserInfo is the
		// identity the resolver needs even though the prewarm content gate is skipped.
		xcontext.WithUserConfig(saEP),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: w250SAUser})))
	rctx = cache.WithServeWatcher(rctx, rw)
	rctx = cache.WithInternalEndpoint(rctx, &saEP)
	rctx = cache.WithInternalRESTConfig(rctx, rc)

	ref := templates.ObjectReference{
		Reference:  templates.Reference{Namespace: w250NS, Name: w250RAName},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}
	if _, err := prewarmOneRESTAction(rctx, ref, w250NS); err != nil {
		t.Fatalf("prewarmOneRESTAction returned error: %v", err)
	}

	store := cache.ResolvedCache()
	puts := put250Keys(store)
	if len(puts) == 0 {
		t.Fatalf("#250 REAL-CHAIN: prewarmOneRESTAction warmed NO content cell — nothing to assert over")
	}
	// INV1 — the subset key is never itself Put.
	if puts[l433] {
		t.Fatalf("#250 INV1: prewarmOneRESTAction Put an entry AT l433=%q (the subset key must never be Put)", l433)
	}
	if _, ok := store.Get(l433); ok {
		t.Fatalf("#250: store has an entry at l433=%q — must be absent", l433)
	}
	// INV2 — self-coverage full-index scan. RED on the UNFIXED tree: the phantom
	// resolve.go:483/:1610 edges anchored at the never-Put l433. GREEN post-fix.
	if bad := phantomAnchors250(store); len(bad) != 0 {
		t.Fatalf("#250 INV2 (RED on the UNFIXED tree): %d dep edge-anchor(s) point at a key that was "+
			"NEVER Put — a phantom invalidation target (dirty-marking a non-existent cell). "+
			"prewarmOneRESTAction still threads the :433 WithL1KeyContext. anchors=%v (l433=%q)",
			len(bad), bad, l433)
	}
	// Every warmed content cell is invalidatable via its OWN contentKey edge — a
	// backing DELETE dirty-marks it for refresh (double-covered, independent of
	// any ambient key).
	content := contentKeys250(puts, l433)
	if !dirtyMarks250(false, w250GVR, w250NS, w250BackObj, content) {
		t.Fatalf("#250 REAL-CHAIN: a backing DELETE did NOT dirty-mark every warmed content cell %v — "+
			"its contentKey edge is missing (dropping :433 must not cost real invalidation)", content)
	}
}

// -----------------------------------------------------------------------------
// DIFFERENTIAL — present (ambient l433) vs dropped (bare), both verbs. The ONLY
// difference is the phantom l433 edges; Puts + REAL invalidation are identical.
// -----------------------------------------------------------------------------

type obs250 struct {
	content  []string
	edges433 int
	dirtyUpd bool // content cells dirty-marked on a backing UPDATE
	dirtyDel bool // content cells dirty-marked on a backing DELETE
}

// dirtyMarks250 drives one backing-object event and reports whether EVERY key
// in `want` was dirty-marked (enqueued for refresh). A dependent LIST content
// cell is DIRTY-MARKED (scheduled for re-resolve), never evicted, on a backing
// ADD/UPDATE/DELETE — eviction is reserved for the owning-object-gone path
// (#216). So dirty-mark is the invalidation signal for both verbs.
func dirtyMarks250(update bool, gvr schema.GroupVersionResource, ns, name string, want []string) bool {
	hook, snap := hookCapture250()
	cache.Deps().SetRefreshHook(hook)
	if update {
		cache.Deps().OnUpdate(gvr, ns, name)
	} else {
		cache.Deps().OnDelete(gvr, ns, name)
	}
	enq := snap()
	if len(want) == 0 {
		return false
	}
	for _, k := range want {
		if !enq[k] {
			return false
		}
	}
	return true
}

func run250Observe(t *testing.T, rw *cache.ResourceWatcher, l433, l1Key string) obs250 {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	drive250Resolve(t, rw, l1Key, w250ListPath)
	store := cache.ResolvedCache()
	content := contentKeys250(put250Keys(store), l433)
	edges433 := len(cache.Deps().EdgesUnder(l433))
	dirtyU := dirtyMarks250(true, w250GVR, w250NS, w250BackObj, content)
	dirtyD := dirtyMarks250(false, w250GVR, w250NS, w250BackObj, content)
	return obs250{content: content, edges433: edges433, dirtyUpd: dirtyU, dirtyDel: dirtyD}
}

func TestPrewarm250_Differential_PutAndRealInvalidationIdentical(t *testing.T) {
	rw := new250Watcher(t)
	l433 := l250Key()

	present := run250Observe(t, rw, l433, l433) // ambient key installed (pre-fix)
	dropped := run250Observe(t, rw, l433, "")   // bare ctx (post-fix)

	// Put-identical: the SAME content cells are warmed either way.
	if len(present.content) == 0 || len(dropped.content) == 0 {
		t.Fatalf("#250 DIFFERENTIAL: a run warmed no content cell (present=%d dropped=%d)",
			len(present.content), len(dropped.content))
	}
	pset := map[string]bool{}
	for _, k := range present.content {
		pset[k] = true
	}
	if len(present.content) != len(dropped.content) {
		t.Fatalf("#250 DIFFERENTIAL Put-identical FAILED: present warmed %v, dropped warmed %v",
			present.content, dropped.content)
	}
	for _, k := range dropped.content {
		if !pset[k] {
			t.Fatalf("#250 DIFFERENTIAL Put-identical FAILED: dropped warmed %q not present in %v",
				k, present.content)
		}
	}
	// Real-invalidation-identical: content cells dirty-mark (UPDATE) and evict
	// (DELETE) in BOTH configs — dropping :433 costs no real invalidation.
	if !present.dirtyUpd || !dropped.dirtyUpd {
		t.Fatalf("#250 DIFFERENTIAL invalidation-identical (UPDATE) FAILED: content dirty-marked "+
			"present=%v dropped=%v", present.dirtyUpd, dropped.dirtyUpd)
	}
	if !present.dirtyDel || !dropped.dirtyDel {
		t.Fatalf("#250 DIFFERENTIAL invalidation-identical (DELETE) FAILED: content dirty-marked "+
			"present=%v dropped=%v", present.dirtyDel, dropped.dirtyDel)
	}
	// The ONLY difference: phantom edges under l433 with the install present,
	// none when dropped (proves the differential is non-vacuous AND that the
	// drop removes exactly the phantom, nothing real).
	if present.edges433 == 0 {
		t.Fatalf("#250 DIFFERENTIAL: expected phantom edges under l433 with the install PRESENT " +
			"(else the differential is vacuous)")
	}
	if dropped.edges433 != 0 {
		t.Fatalf("#250 DIFFERENTIAL: expected NO edges under l433 with the install DROPPED; got %d",
			dropped.edges433)
	}
}

// -----------------------------------------------------------------------------
// POSITIVE CONTROLS — prove each guard mechanism CAN fire (this suite is a
// cleanup; without the controls its green is vacuous). One control per mechanism.
// -----------------------------------------------------------------------------

// Control-A: the SELF-COVERAGE invariant (INV2) is the outcome-2 detector. Inject
// a content cell Put at K_E whose dep edge is (wrongly) recorded under the AMBIENT
// l433 — a nested site relying on the ambient key instead of re-installing its
// own. INV2 must fire (edge anchored at the never-Put l433); and the present-vs-
// dropped DIFFERENTIAL is BLIND (K_E is stale either way), documenting that the
// invariant, not the differential, guards outcome-2.
func TestPrewarm250_ControlA_SelfCoverageInvariantFires(t *testing.T) {
	_ = new250Watcher(t)
	l433 := l250Key()
	store := cache.ResolvedCache()

	const kE = "L1_control_A_content_cell"
	store.PutIfGen(context.Background(), kE, &cache.ResolvedEntry{RawJSON: []byte("{}")}, store.CaptureGen(kE))
	cache.Deps().RecordList(context.Background(), l433, w250GVR, w250NS) // edge anchored at l433, which has NO entry

	bad := phantomAnchors250(store)
	if !contains250(bad, l433) {
		t.Fatalf("Control-A: the self-coverage invariant (INV2) did NOT fire on an injected outcome-2 "+
			"leak (edge anchored at the never-Put l433=%q); phantomAnchors=%v — the guard is vacuous "+
			"and a real future leak would pass green", l433, bad)
	}
	// The present-vs-dropped DIFFERENTIAL is GREEN for mode-a — CORRECT BY DESIGN,
	// not a gap: the mis-edged cell is stale in BOTH configs (present → the change
	// dirty-marks l433, the cell@K_E is never hit; dropped → the edge is never
	// recorded) → identical → green. Division of labor (do NOT "fix" the
	// differential to red here — it structurally cannot distinguish mode-a): the
	// DIFFERENTIAL is the REDUNDANCY proof (both-stale ⇒ dropping :433 changes
	// nothing), and the self-coverage INVARIANT (INV2, asserted above) is the
	// outcome-2 DETECTOR. Below we witness the blindness: the backing change does
	// NOT dirty-mark K_E.
	hook, snap := hookCapture250()
	cache.Deps().SetRefreshHook(hook)
	cache.Deps().OnDelete(w250GVR, w250NS, w250BackObj)
	if snap()[kE] {
		t.Fatalf("Control-A doc: K_E must NOT be dirty-marked by the backing change (its edge leaked to " +
			"l433, so it is never scheduled for refresh) — proving the differential is blind to outcome-2 " +
			"and INV2 is the guard")
	}
}

// Control-B: INV1 (":433 never Put") and the DIFFERENTIAL. Inject a cell Put AT
// l433 depending on the widget backing. INV1 must fire; and WITH the edge a
// backing DELETE evicts the l433 cell (the differential can drive+detect a real
// invalidation), WITHOUT the edge it does not (present != dropped).
func TestPrewarm250_ControlB_Put433AndDifferentialFire(t *testing.T) {
	_ = new250Watcher(t)
	l433 := l250Key()
	store := cache.ResolvedCache()

	store.PutIfGen(context.Background(), l433, &cache.ResolvedEntry{RawJSON: []byte("{}")}, store.CaptureGen(l433))
	cache.Deps().RecordList(context.Background(), l433, w250GVR, w250NS)

	if !put250Keys(store)[l433] {
		t.Fatalf("Control-B: the ':433 is never Put' invariant (INV1) did NOT fire on an injected Put "+
			"AT l433=%q — the guard is vacuous", l433)
	}
	// Differential present arm: with the edge, a backing change dirty-marks the l433 cell.
	hookP, snapP := hookCapture250()
	cache.Deps().SetRefreshHook(hookP)
	cache.Deps().OnUpdate(w250GVR, w250NS, w250BackObj)
	if !snapP()[l433] {
		t.Fatalf("Control-B: with an edge under l433 a backing change must dirty-mark the l433 cell " +
			"(proving the differential can drive+detect a real invalidation); it did not")
	}
	// Differential dropped arm: drop the edge (reset the dep index; the l433 store
	// entry persists) → the SAME change must NOT dirty-mark it, so present != dropped
	// (the differential fires).
	cache.ResetDepsForTest()
	hookD, snapD := hookCapture250()
	cache.Deps().SetRefreshHook(hookD)
	cache.Deps().OnUpdate(w250GVR, w250NS, w250BackObj)
	if snapD()[l433] {
		t.Fatalf("Control-B: with NO edge under l433 the backing change must NOT dirty-mark the l433 cell " +
			"— the present-vs-dropped differential must DIFFER (it did not)")
	}
}

// -----------------------------------------------------------------------------
// BY-NAME SELF-EVICT (arch's verb-distinction arm). A GET-by-name apiStage
// produces a NAME-keyed content cell (contentKey includes the name) — a real
// production shape and a distinct apistage Put path from the LIST cell. Its
// owning-object DELETE SELF-EVICTS it (isSelfRepresentation: the cell's Inputs
// == the deleted object, deps.go:1116), unlike the LIST cell which dirty-marks
// — so this makes "both verbs" honest (UPDATE dirty-mark vs DELETE self-evict).
// It is also DISCRIMINATING: if the by-name Put mis-edged its dep to the ambient
// key instead of its own contentKey, the DROPPED (bare-ctx) config would leave
// the cell with NO edge → the owning DELETE would not evict it → this arm reds.
// Self-coverage (INV2 on the dropped config) + evict/dirty must be IDENTICAL
// present-vs-dropped.
// -----------------------------------------------------------------------------

func TestPrewarm250_ByName_SelfEvictBothVerbsIdentical(t *testing.T) {
	rw := new250Watcher(t)
	l433 := l250Key()

	// observe drives a by-name resolve under one config and reports: the content
	// cells Put, any orphan (phantom) anchor, whether an owning-object UPDATE
	// dirty-marks the cell, and whether the owning-object DELETE self-evicts it.
	observe := func(l1Key string) (content, phantom []string, dirty, evicted bool) {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
		drive250Resolve(t, rw, l1Key, w250ByNamePath)
		store := cache.ResolvedCache()
		content = contentKeys250(put250Keys(store), l433)
		phantom = phantomAnchors250(store)
		dirty = dirtyMarks250(true, w250GVR, w250NS, w250BackObj, content)
		cache.Deps().OnDelete(w250GVR, w250NS, w250BackObj)
		evicted = len(content) > 0
		for _, k := range content {
			if _, ok := store.Get(k); ok {
				evicted = false
			}
		}
		return
	}

	pContent, _, pDirty, pEvict := observe(l433)      // present (ambient l433)
	dContent, dPhantom, dDirty, dEvict := observe("") // dropped (bare ctx = post-fix)

	if len(pContent) == 0 || len(dContent) == 0 {
		t.Fatalf("#250 BY-NAME: expected a name-keyed content cell Put; present=%d dropped=%d",
			len(pContent), len(dContent))
	}
	// Self-coverage: the DROPPED (post-fix) resolve leaves NO orphan anchor — the
	// by-name cell's edge is under its OWN contentKey, not the ambient key. (The
	// present config carries the parent phantom @l433 by design — the resolver's
	// top-level parent edge, not the by-name cell — so it is not asserted here.)
	if len(dPhantom) != 0 {
		t.Fatalf("#250 BY-NAME INV2: the bare-ctx by-name resolve left orphan anchor(s) %v — the "+
			"name-keyed content cell must self-cover under its own contentKey", dPhantom)
	}
	// Verb distinction, IDENTICAL present-vs-dropped: UPDATE dirty-marks; the
	// owning-object DELETE SELF-EVICTS. Discriminating — a mis-edged by-name Put
	// would fail to invalidate in the dropped config (no edge).
	if !pDirty || !dDirty {
		t.Fatalf("#250 BY-NAME UPDATE dirty-mark must be identical present-vs-dropped; present=%v dropped=%v",
			pDirty, dDirty)
	}
	if !pEvict || !dEvict {
		t.Fatalf("#250 BY-NAME owning-object DELETE self-evict must be identical present-vs-dropped; "+
			"present=%v dropped=%v (the name-keyed cell's Inputs match the deleted object → isSelfRepresentation)",
			pEvict, dEvict)
	}
}
