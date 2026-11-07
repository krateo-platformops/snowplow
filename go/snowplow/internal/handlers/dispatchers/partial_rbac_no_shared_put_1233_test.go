package dispatchers

// #1233 — while the RBAC snapshot is incomplete, no writer stores a shared cache
// entry. Each writer runs its own production code path twice, on a partial
// snapshot and on the same snapshot complete: the partial run stores nothing and
// is counted under its own name, the complete run stores as before.
//
// The partial snapshot here is the complete one marked not Synced: the bindings
// are all present, so every key is the one the complete snapshot derives and no
// identity-class guard has a reason to decline. What refuses the write is the
// store's admission, and nothing else.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func declined1233(label string) uint64 {
	return cache.PutDeclinedRBACIncompleteSnapshot()[label]
}

// A user's /call (restActionHandler.ServeHTTP) on a partial snapshot serves its
// body and stores nothing.
func TestIssue1233_UserCallStoresNothingOnAPartialSnapshot(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			a := psArm{name: "configmaps", target: psConfigmapsGVR, watchShape: true}
			psBuildWatcher(t, a)
			perUser := map[string]*atomic.Int64{psAlice: {}, psBob: {}}
			srv := psFakeAPIServer(t, a, perUser)
			psSeedClientconfigs(t, srv.URL)
			ctx := psUserCtx(a, psAlice)
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			key, handle, _ := dispatchCacheLookupKey(ctx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			if handle == nil || key == "" {
				t.Fatal("PRE: need a live cache handle and key")
			}
			label := cache.RefDenialOriginServe + "/restactions"
			before := declined1233(label)

			rec := psServe(t, a, srv.URL, ctx)

			if !psHasSentinel(rec.Body.Bytes()) {
				t.Fatalf("PRE: alice's /call must be served her content either way; code=%d body=%s", rec.Code, rec.Body.String())
			}
			_, stored := handle.Get(key)
			got := declined1233(label) - before
			if !partial {
				if !stored || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the /call must store its cell (stored=%v, declines=%d)", stored, got)
				}
				return
			}
			if stored {
				t.Fatal("#1233 RED: a user's /call stored a shared cell on a partial RBAC snapshot")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}

// The refresher's impersonated read (#574, #615), the route the #1230 review
// found: no re-gate covers it, so on a partial snapshot it returned the
// representative's apiserver view unmarked, and the refresh stored it in the
// shared cell. RED on main.
func TestIssue1233_ImpersonatedRefreshStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			const user = "alice-1233"
			k := f608Kinds[0]
			e := f608Setup(t, nil, nil)
			e.srv.addObject(f608Widget(k, "b1233"), k, "b1233")
			key := e.seedCell(t, f608CohortInputs(k, "b1233", user, nil))
			if !e.store.Has(key) {
				t.Fatal("PRE: the shared cell must be resident before the refresh")
			}
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			label := cache.BackgroundOriginRefresher + "/widgets"
			before := declined1233(label)

			done := make(chan error, 4)
			cache.RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
				err := resolveAndPopulateL1(ctx, in, e.saEP, e.saRC)
				done <- err
				return err
			})
			ctx, cancel := context.WithCancel(context.Background())
			cache.StartRefresher(ctx)
			t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })
			cache.EnqueueRefresh(key)
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("the refresh did not run within 20s")
			}

			objPath := fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/b1233", f608Group, f608Version, f608NS, k.resource)
			asCohort := 0
			for _, r := range e.srv.requests() {
				if r.path == objPath && r.user == user {
					asCohort++
				}
			}
			if asCohort == 0 {
				t.Fatal("PRE: the refresh's data read must go out impersonating the representative (the #1233 route)")
			}
			got := declined1233(label) - before
			if !partial {
				if !e.accepted(key) || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the impersonated refresh must store (accepted=%v, declines=%d)", e.accepted(key), got)
				}
				return
			}
			if e.accepted(key) {
				t.Fatal("#1233 RED: on a partial RBAC snapshot the impersonated refresh stored the representative's view in the shared cell")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}

// The seed's terminal write (seedTerminalPut, the one write of the walk's and the
// seeds' primitives): the boot scope's plain write and every other mode's
// guarded one.
func TestIssue1233_SeedStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, mode := range []seedScopeMode{seedModeBoot, seedModeKeepwarm} {
			t.Run(fmt.Sprintf("partial=%v/mode=%v", partial, mode), func(t *testing.T) {
				t.Setenv("CACHE_ENABLED", "true")
				t.Setenv("RESOLVED_CACHE_ENABLED", "true")
				cache.ResetResolvedCacheForTest()
				t.Cleanup(cache.ResetResolvedCacheForTest)
				cache.ResetPhase1DoneForTest()
				t.Cleanup(cache.ResetPhase1DoneForTest)
				cache.CompleteRBACSnapshotForTest(t)

				ctx := seedCohortCtx("cohort-1233")
				key, handle, inputs := dispatchCacheLookupKey(ctx, "restactions",
					h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, "seeded-1233", -1, -1, nil)
				if handle == nil || key == "" {
					t.Fatal("PRE: need a live cache handle and key")
				}
				if partial {
					cache.PartialRBACSnapshotForTest(t)
				}
				label := cache.BackgroundOriginCohortSeed + "/restactions"
				before := declined1233(label)
				g := seedTerminalGuardFor(mode, handle, key)
				wrote := seedTerminalPut(cache.WithL1KeyContext(ctx, key), handle, fixtureStoreVerdict, key,
					&cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs}, g)
				_, stored := handle.Get(key)
				got := declined1233(label) - before
				if !partial {
					if !wrote || !stored || got != 0 {
						t.Fatalf("CONTROL: on a complete snapshot the seed must store (wrote=%v stored=%v declines=%d)", wrote, stored, got)
					}
					return
				}
				if wrote || stored {
					t.Fatalf("#1233 RED: the seed stored a shared cell on a partial RBAC snapshot (wrote=%v stored=%v)", wrote, stored)
				}
				if got != 1 {
					t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
				}
			})
		}
	}
}

// The content prewarm's write (populateWidgetContentL1, the identity-free
// widgetContent cell the SA walk fills).
func TestIssue1233_ContentPrewarmStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			cache.ResetResolvedCacheForTest()
			t.Cleanup(cache.ResetResolvedCacheForTest)
			cache.ResetDepsForTest()
			t.Cleanup(cache.ResetDepsForTest)
			cache.CompleteRBACSnapshotForTest(t)
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			in := h1WidgetUnstructured(map[string]any{})
			res := h1WidgetUnstructured(map[string]any{})
			if err := unstructured.SetNestedField(res.Object, "content", "status", "widgetData", "label"); err != nil {
				t.Fatalf("setup: %v", err)
			}
			key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
			if key == "" {
				t.Fatal("PRE: the widgetContent layer must be live")
			}
			label := cache.RefDenialOriginPrewarmPath + "/" + cache.CacheEntryClassWidgetContent
			before := declined1233(label)
			ctx := cache.WithPrewarmPath(withWidgetContentPutSinks(context.Background()))
			populateWidgetContentL1(ctx, h1WidgetGVR, in, -1, -1, res, 0)
			_, stored := cache.ResolvedCache().Get(key)
			got := declined1233(label) - before
			if !partial {
				if !stored || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the content prewarm must store (stored=%v declines=%d)", stored, got)
				}
				return
			}
			if stored {
				t.Fatal("#1233 RED: the content prewarm stored a shared widgetContent cell on a partial RBAC snapshot")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}

// The #1243 review's straddle: a resolve that began on a partial snapshot, whose
// snapshot completed before its Put. The partial snapshot lacks alice's Role
// target-reader (a Role the initial LIST has not delivered yet): her binding set
// and sub-generation are the same on both snapshots (#682), so no identity-class
// guard can see it, and her namespaced LIST is filtered to nothing. The complete
// snapshot is published at the first configmaps verdict of the resolve. The body
// must not be stored, and the next call, on the complete snapshot, must bring
// the configmap. RED before #1243's resolve-start admission: the under-granted
// body was stored and served from L1.
func TestIssue1233_AResolveBegunOnAPartialSnapshotStoresNothingAfterItCompletes(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	a := psArm{name: "configmaps", target: psConfigmapsGVR, watchShape: true}
	psBuildWatcher(t, a)
	perUser := map[string]*atomic.Int64{psAlice: {}, psBob: {}}
	srv := psFakeAPIServer(t, a, perUser)
	psSeedClientconfigs(t, srv.URL)
	ctx := psUserCtx(a, psAlice)
	list := psRACR(a)
	_ = unstructured.SetNestedSlice(list.Object, []any{map[string]any{
		"name": "target", "path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "continueOnError": true,
	}}, "spec", "api")

	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("PRECONDITION: %v", err)
	}
	full := cache.RBACSnapshotForTest()
	if full == nil || !full.Synced {
		t.Fatal("PRECONDITION: the watcher's snapshot must be complete")
	}
	roles := map[string]*rbacv1.Role{}
	for k, r := range full.RolesByNSName {
		if r.Name != "target-reader" {
			roles[k] = r
		}
	}
	if len(roles) == len(full.RolesByNSName) {
		t.Fatal("PRECONDITION: the complete snapshot must hold alice's Role target-reader")
	}
	partial := &cache.RBACSnapshot{
		Synced:              false,
		PublishSeq:          full.PublishSeq + 1<<32,
		ClusterRoleBindings: full.ClusterRoleBindings,
		RoleBindingsByNS:    full.RoleBindingsByNS,
		ClusterRolesByName:  full.ClusterRolesByName,
		RolesByNSName:       roles,
	}
	cache.RebuildSubjectIndexesForTest(partial)
	cache.PublishRBACSnapshotForTest(partial)
	t.Cleanup(func() { cache.PublishRBACSnapshotForTest(full) })

	key, handle, in := dispatchCacheLookupKey(ctx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
	if handle == nil || key == "" || in == nil {
		t.Fatal("PRECONDITION: need a live cache handle and key")
	}
	fullKey := func() string {
		cache.PublishRBACSnapshotForTest(full)
		k, _, _ := dispatchCacheLookupKey(ctx, "restactions",
			h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
		return k
	}

	var flipped atomic.Bool
	rbac.SetShadowHook(func(_ context.Context, snap *cache.RBACSnapshot, opts rbac.EvaluateOptions, _ bool) {
		if snap == partial && opts.Resource == a.target.Resource && flipped.CompareAndSwap(false, true) {
			cache.PublishRBACSnapshotForTest(full) // the initial LIST completes mid-resolve
		}
	})
	t.Cleanup(func() { rbac.SetShadowHook(nil) })
	prevParity := rbac.ShadowParityEnabled()
	rbac.SetShadowParityEnabled(true) // the hook only runs with the dark parity on
	t.Cleanup(func() { rbac.SetShadowParityEnabled(prevParity) })
	label := cache.RefDenialOriginServe + "/restactions"
	before := declined1233(label)

	first := psServeCR(t, list, srv.URL, ctx)

	if !flipped.Load() {
		t.Fatal("PRECONDITION: the snapshot must complete during the resolve (the shadow hook never saw a configmaps verdict on the partial snapshot)")
	}
	if psHasSentinel(first.Body.Bytes()) {
		t.Fatalf("PRECONDITION: the resolve must read through the partial snapshot, which lacks alice's Role; body=%s", first.Body.String())
	}
	if k := fullKey(); k != key {
		t.Fatalf("PRECONDITION: the key must be the same on both snapshots (a missing Role moves no key dimension)")
	}
	if _, stored := handle.Get(key); stored {
		t.Fatal("#1233 RED: the body read on the partial snapshot was stored after the snapshot completed")
	}
	if got := declined1233(label) - before; got != 1 {
		t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
	}
	second := psServeCR(t, list, srv.URL, ctx)
	if !psHasSentinel(second.Body.Bytes()) {
		t.Fatalf("#1233 RED: after completion alice's call must bring the configmap; body=%s", second.Body.String())
	}
	if _, stored := handle.Get(key); !stored {
		t.Fatal("CONTROL: the call that began on the complete snapshot must store its cell")
	}
}

// The prewarm loses no cell across the gap (#506): with the initial RBAC LIST
// still open, the snapshot is partial and the seed unit waits instead of
// resolving; once the LIST completes and the complete snapshot is published,
// it resolves and stores its cell, and nothing is refused. Before #1233's wait
// the unit ran at once and its write was refused, leaving the cell cold.
func TestIssue1233_ASeedWaitsForTheCompleteSnapshotAndLosesNoCell(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)
	prevSnap := cache.RBACSnapshotForTest()
	cache.PublishRBACSnapshotForTest(nil)
	t.Cleanup(func() { cache.PublishRBACSnapshotForTest(prevSnap) })

	const user = "seed-1233"
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		restActionGVR: "RESTActionList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds,
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "ra-reader"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{restActionGVR.Group}, Resources: []string{restActionGVR.Resource}}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "seed-ra-bind", UID: types.UID("uid-seed-1233")},
			Subjects: []rbacv1.Subject{{Kind: "User", Name: user}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"}},
	)
	// The initial roles LIST is held: the informer cannot sync, so the snapshot
	// published meanwhile is partial and the initial publish is not done.
	var released atomic.Bool
	var heldLists atomic.Int64
	dyn.PrependReactor("list", "roles", func(k8stesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		heldLists.Add(1)
		return true, nil, errors.New("roles LIST held by the test (#1233)")
	})
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(rw.Stop)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	t.Cleanup(func() { released.Store(true) })
	waitUntil1233(t, 10*time.Second, "the roles LIST to be held", func() bool { return heldLists.Load() > 0 })
	if cache.RBACSnapshotComplete() {
		t.Fatal("PRECONDITION: the snapshot must not be complete while the roles LIST is held")
	}

	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "seeded-1233", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	origPut, origGet := seedRestactionResolveAndPutFn, seedObjectsGetFn
	t.Cleanup(func() { seedRestactionResolveAndPutFn, seedObjectsGetFn = origPut, origGet })
	var fetched atomic.Bool
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		fetched.Store(true)
		return a1SeedFetchedRestaction(false)
	}
	var mu sync.Mutex
	var wrote bool
	var putKey string
	seedRestactionResolveAndPutFn = func(
		_, resCtx context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		g := seedTerminalGuardFor(seedModeBoot, handle, key)
		ok := seedTerminalPut(resCtx, handle, fixtureStoreVerdict, key,
			&cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs}, g)
		mu.Lock()
		wrote, putKey = ok, key
		mu.Unlock()
		return nil
	}
	label := cache.BackgroundOriginCohortSeed + "/restactions"
	before := declined1233(label)

	done := make(chan error, 1)
	go func() { done <- seedOneRestaction(seedCohortCtx(user), ref, "krateo-system", seedModeBoot) }()

	// While the LIST is held the unit has not started: no fetch, no write, no refusal.
	select {
	case err := <-done:
		t.Fatalf("#1233 RED: the seed unit ran on the partial snapshot (err=%v, fetched=%v)", err, fetched.Load())
	case <-time.After(300 * time.Millisecond):
	}
	if fetched.Load() || declined1233(label) != before {
		t.Fatalf("#1233 RED: the seed unit started on the partial snapshot (fetched=%v, refusals=%d)",
			fetched.Load(), declined1233(label)-before)
	}

	released.Store(true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("seedOneRestaction: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the seed unit did not run within 30s of the roles LIST completing")
	}
	mu.Lock()
	defer mu.Unlock()
	if !wrote || putKey == "" {
		t.Fatal("#1233 RED: the seed unit's write was refused: the cell is cold after the snapshot completed")
	}
	if _, ok := cache.ResolvedCache().Get(putKey); !ok {
		t.Fatal("#1233 RED: the seeded cell is not resident after the snapshot completed")
	}
	if got := declined1233(label) - before; got != 0 {
		t.Fatalf("no write of the seed may be refused, %d were", got)
	}
}

func waitUntil1233(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var _ = strings.Contains
