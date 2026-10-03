// issue426_rafulllist_sub_emit_parity_test.go — #426: the raFullList SSE
// subscription key must equal the key the raFullList cell is stored and
// refreshed under.
//
// Before #426, DeriveSubscriptionKey minted class raFullList through
// dispatchCacheLookupKey. That builder folds RBACSubGen and the normalized
// request pagination (0 -> -1, or the real page). The cell's own builder
// (apiref seedFullListRAKey -> cache.RAFullListKeyInputs) is page-independent
// (0/0), strips the slice extras, and folds no RBACSubGen. So the armed key never
// matched the emitted key, and no raFullList refresh reached a subscriber.
//
// Two arms, both RED on 248601d1 and both RED again if the two sides diverge:
//
//   - TestIssue426_RAFullListSubscribeKeyEqualsEmitKey (parity golden): for one
//     identity and one RA, across pagination and extras shapes, the subscription's
//     pre-hash inputs equal the producer's field by field (every field ComputeKey
//     folds is diffed, and all diffs are reported), then the digests are compared.
//   - TestIssue426_RAFullListRefreshDeliversToSubscriber (end to end): the cell
//     is warmed under the producer's inputs, and a real paginated apiref.Resolve
//     must be served from it (so a producer that keys another way fails SETUP).
//     The test then finds the cell by scanning L1, arms a subscriber through
//     DeriveSubscriptionKeyWithReason (the call the /refreshes handler makes), runs
//     the refresher's resolveAndPopulateL1 on the stored inputs, and requires the
//     refresh event on the subscriber's channel.
package dispatchers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
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

const (
	i426NS      = "krateo-system"
	i426RAName  = "issue426-ra"
	i426User    = "alice-426"
	i426Group   = "portal-426"
	i426OtherNS = "other-426"
)

func i426RAGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
}

var i426RBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}

// i426Filter is a cleanly sliceable top-level filter, so the first paginated
// resolve verifies sliceability and stores the full-list cell.
const i426Filter = `
{
  items: (
    (.items // []) as $all
    | (.slice.offset  // 0)              as $offset
    | (.slice.perPage // ($all | length)) as $perPage
    | [ $all | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $all[.] ]
  )
}
`

func i426Groups() []string { return []string{i426Group} }

func i426Ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: i426User, Groups: i426Groups()}))
}

// i426BuildWatcher stands up a real RBAC snapshot (group portal-426 may get
// restactions cluster-wide) plus the RESTAction CR, served from the informer.
func i426BuildWatcher(t *testing.T) *dynamicfake.FakeDynamicClient {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(func() {
		cache.ResetRefreshBroadcasterForTest()
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
	})

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}: "RoleList",
		i426RBGVR: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		i426RAGVR(): "RESTActionList",
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "ra-reader-426"},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{"templates.krateo.io"},
				Resources: []string{"restactions"},
				Verbs:     []string{"get", "list"},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "crb-426-portal", UID: types.UID("crb-426-uid")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: i426Group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader-426"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "templates.krateo.io/v1",
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": i426RAName, "namespace": i426NS},
			"spec":       map[string]any{"filter": i426Filter},
		}},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	// Activate the binding deltas that bump the per-subject sub-gens.
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{i426RAGVR()})

	if added, syncCh := rw.EnsureResourceType(i426RAGVR()); added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for !rw.IsServable(i426RAGVR()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !rw.IsServable(i426RAGVR()) {
		t.Fatalf("restactions GVR never became servable")
	}
	return dyn
}

// i426MoveSubGen grants alice an unrelated RoleBinding through the real watcher
// so her effective RBACSubGen is non-zero. The pre-#426 subscription folded that
// counter while the cell's key does not, so a zero counter would hide the
// divergence on that dimension.
func i426MoveSubGen(t *testing.T, dyn *dynamicfake.FakeDynamicClient) {
	t.Helper()
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb-426-alice", Namespace: i426OtherNS, UID: types.UID("rb-426-alice-uid")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: i426User}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader-426"},
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if err != nil {
		t.Fatalf("ToUnstructured: %v", err)
	}
	before := rbac.SubjectBindingSetDigest(i426User, i426Groups())
	if _, err := dyn.Resource(i426RBGVR).Namespace(i426OtherNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create RoleBinding: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cache.RBACSubGenForSubject(i426User, rbac.WithAuthenticatedGroup(i426Groups())) != 0 &&
			rbac.SubjectBindingSetDigest(i426User, i426Groups()) != before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("PRE: the RoleBinding never moved alice's RBACSubGen and binding set (subgen=%d digestMoved=%v allowedOther=%v)",
		cache.RBACSubGenForSubject(i426User, rbac.WithAuthenticatedGroup(i426Groups())),
		rbac.SubjectBindingSetDigest(i426User, i426Groups()) != before, func() bool {
			ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{Username: i426User, Verb: "get",
				Group: "templates.krateo.io", Resource: "restactions", Namespace: i426OtherNS, Name: "z"})
			return ok
		}())
}

// i426PreHashDiff lists every field ComputeKey folds that differs between a
// and b. Empty means the two inputs hash identically.
func i426PreHashDiff(a, b cache.ResolvedKeyInputs) []string {
	var d []string
	add := func(field string, x, y any) {
		if fmt.Sprint(x) != fmt.Sprint(y) {
			d = append(d, fmt.Sprintf("%s: emit=%v sub=%v", field, x, y))
		}
	}
	add("CacheEntryClass", a.CacheEntryClass, b.CacheEntryClass)
	add("Group", a.Group, b.Group)
	add("Version", a.Version, b.Version)
	add("Resource", a.Resource, b.Resource)
	add("Namespace", a.Namespace, b.Namespace)
	add("Name", a.Name, b.Name)
	add("BindingUID", a.BindingUID, b.BindingUID)
	add("SubjectBindingSet", a.SubjectBindingSet, b.SubjectBindingSet)
	add("RBACSubGen", a.RBACSubGen, b.RBACSubGen)
	add("PerPage", a.PerPage, b.PerPage)
	add("Page", a.Page, b.Page)
	add("Stage", a.Stage, b.Stage)
	add("Extras(canonical)", cache.HashExtras(a.Extras), cache.HashExtras(b.Extras))
	return d
}

func i426Coords(perPage, page int, extras map[string]any) SubscriptionCoordinates {
	g := i426RAGVR()
	return SubscriptionCoordinates{
		Class:     cache.CacheEntryClassRAFullList,
		Group:     g.Group,
		Version:   g.Version,
		Resource:  g.Resource,
		Namespace: i426NS,
		Name:      i426RAName,
		PerPage:   perPage,
		Page:      page,
		Extras:    extras,
	}
}

// TestIssue426_RAFullListSubscribeKeyEqualsEmitKey is the parity golden: for the
// same identity and RA, the subscription's pre-hash inputs equal the inputs the
// raFullList producer stores the cell under.
func TestIssue426_RAFullListSubscribeKeyEqualsEmitKey(t *testing.T) {
	dyn := i426BuildWatcher(t)
	i426MoveSubGen(t, dyn)
	ctx := i426Ctx()

	cases := []struct {
		name          string
		perPage, page int
		extras        map[string]any
	}{
		{"unpaginated-no-extras", 0, 0, nil},
		{"page1-request-extras", 5, 1, map[string]any{"q": "x"}},
		{"page3-with-slice-extras", 10, 3, map[string]any{"q": "x", "slice": map[string]any{"offset": 20, "perPage": 10}, "page": 3, "perPage": 10}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// EMIT side: the inputs the producer stores the cell under (and the
			// refresher re-emits ComputeKey of).
			emit, ok := apiref.SeedFullListRAKeyInputsForTest(ctx, i426RAGVR(), i426NS, i426RAName, tc.extras)
			if !ok {
				t.Fatalf("PRE: the producer must derive a raKey for alice")
			}
			if emit.RBACSubGen != 0 {
				t.Fatalf("PRE: the raFullList key folds no RBACSubGen (#424); got %d", emit.RBACSubGen)
			}

			// The arm can fail: the pre-#426 subscription derivation
			// (dispatchCacheLookupKey over the normalized pagination) diverges
			// from the cell's key for this identity.
			pp, pg := normalizePagination(tc.perPage, tc.page)
			oldKey, _, oldIn := dispatchCacheLookupKey(ctx, cache.CacheEntryClassRAFullList,
				i426RAGVR().Group, i426RAGVR().Version, i426RAGVR().Resource, i426NS, i426RAName, pp, pg, tc.extras)
			if oldIn == nil || oldKey == cache.ComputeKey(emit) {
				t.Fatalf("PRE: the pre-#426 derivation must diverge from the emit key, or this arm cannot fail")
			}

			// SUBSCRIBE side: the real /refreshes derivation.
			sub, ok := deriveSubscriptionKeyInputsForTest(ctx, i426Coords(tc.perPage, tc.page, tc.extras))
			if !ok || sub == nil {
				t.Fatalf("#426 RED: the raFullList subscription did not arm for alice")
			}
			if d := i426PreHashDiff(emit, *sub); len(d) > 0 {
				t.Fatalf("#426 RED: subscribe pre-hash inputs differ from emit:\n  %s", strings.Join(d, "\n  "))
			}
			subKey, ok := DeriveSubscriptionKey(ctx, i426Coords(tc.perPage, tc.page, tc.extras))
			if !ok || subKey != cache.ComputeKey(emit) {
				t.Fatalf("#426 RED: subscribe key %q != emit key %q", subKey, cache.ComputeKey(emit))
			}
		})
	}

	// Forgery-proof posture kept: another identity sending alice's coordinates
	// arms a different key.
	bob := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "bob-426", Groups: i426Groups()}))
	aliceKey, _ := DeriveSubscriptionKey(ctx, i426Coords(5, 1, nil))
	if bobKey, ok := DeriveSubscriptionKey(bob, i426Coords(5, 1, nil)); ok && bobKey == aliceKey {
		t.Fatalf("#426: bob (different binding set) armed alice's raFullList key")
	}
}

// TestIssue426_RAFullListRefreshDeliversToSubscriber is the end-to-end arm: a
// refresh of a raFullList cell stored by the real producer delivers an SSE
// event to a client that subscribed with the cell's coordinates.
func TestIssue426_RAFullListRefreshDeliversToSubscriber(t *testing.T) {
	dyn := i426BuildWatcher(t)
	i426MoveSubGen(t, dyn)
	ctx := i426Ctx()
	extras := map[string]any{"q": "x"}

	// Producer cell. A hermetic cold resolve cannot verify sliceability (the
	// RA's `.slice` is injected only by the api stage, which needs an endpoint),
	// so warm the cell the way the boot seed does: the producer's own inputs,
	// a verified-sliceable verdict, PutRAFullList.
	g := i426RAGVR()
	seedIn, ok := apiref.SeedFullListRAKeyInputsForTest(ctx, g, i426NS, i426RAName, extras)
	if !ok {
		t.Fatalf("SETUP: the producer must derive a raKey for alice")
	}
	seedKey := cache.ComputeKey(seedIn)
	full := make([]any, 12)
	for i := range full {
		full[i] = map[string]any{"n": i}
	}
	c := cache.ResolvedCache()
	c.PutRAFullList(seedKey, seedIn, map[string]any{"items": full})
	cache.RecordSliceability(seedKey, cache.SliceShapeHash("apiref", g.Group, g.Version, g.Resource,
		i426NS, i426RAName, i426Filter), true)
	cache.Deps().Record(context.Background(), seedKey, g, i426NS, i426RAName) // RA-CR self-dep

	// The real producer serves a page from that cell: proves the serve path keys
	// the cell where it was stored (a producer minting another way would miss).
	hits0 := cache.RAFullListServeSnapshot().Hit
	page, err := apiref.Resolve(cache.WithL1KeyContext(ctx, "L1_issue426_widget"), apiref.ResolveOptions{
		ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: i426RAName, Namespace: i426NS},
			Resource:   "restactions",
			APIVersion: "templates.krateo.io/v1",
		},
		PerPage: 5, Page: 1, Extras: extras,
	})
	if err != nil {
		t.Fatalf("SETUP: apiref.Resolve: %v", err)
	}
	if got := cache.RAFullListServeSnapshot().Hit - hits0; got != 1 {
		t.Fatalf("SETUP: the paginated resolve must be served from the raFullList cell (hits=%d, out=%v)", got, page)
	}
	if items, _ := page["items"].([]any); len(items) != 5 {
		t.Fatalf("SETUP: expected a 5-item Go-slice page, got %v", page)
	}

	// Find the stored cell by scanning L1 (independent of the key builder).
	var storedKey string
	var stored cache.ResolvedKeyInputs
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && e.Inputs != nil &&
			e.Inputs.CacheEntryClass == cache.CacheEntryClassRAFullList && e.Inputs.Name == i426RAName {
			if storedKey != "" {
				t.Fatalf("SETUP: expected one raFullList cell, found more")
			}
			storedKey, stored = k, *e.Inputs
		}
	}
	if storedKey == "" {
		t.Fatalf("SETUP: no raFullList cell in L1")
	}

	// Subscriber: the derivation the /refreshes handler runs, on its ctx.
	armed, ok, reason := DeriveSubscriptionKeyWithReason(cache.WithInformerOnlyReads(ctx), i426Coords(5, 1, extras))
	if !ok {
		t.Fatalf("#426 RED: the raFullList subscription did not arm (reason=%d)", reason)
	}
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{armed: {}})
	defer unsub()

	// #431: raFullList cells carry no representative yet, so the refresher's
	// #424 guard declines their re-Put (binding_set) and emits nothing, whatever
	// the key. Until #431 lands, stamp the representative the producer will carry.
	// It is not a key field (ComputeKey ignores it), so the emitted key is still
	// the stored one. Once the producer sets it, this is a no-op.
	if stored.RepresentativeUsername == "" {
		stored.RepresentativeUsername = i426User
		stored.RepresentativeGroups = i426Groups()
	}
	restore := setResolveOnceForTest(func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(`{"items":[{"name":"fresh"}]}`), nil
	})
	t.Cleanup(restore)
	if err := resolveAndPopulateL1(context.Background(), stored, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1: %v", err)
	}
	if e, ok := c.GetNoTouch(storedKey); !ok || !strings.Contains(string(e.RawJSON), "fresh") {
		t.Fatalf("SETUP: the refresh did not commit the fresh body under the stored key")
	}

	select {
	case got := <-ch:
		if got != storedKey {
			t.Fatalf("#426: delivered key %q, want the refreshed cell %q", got, storedKey)
		}
	case <-time.After(time.Second):
		t.Fatalf("#426 RED: the raFullList refresh was emitted under %q but the subscriber armed %q; no event delivered",
			storedKey, armed)
	}
}
