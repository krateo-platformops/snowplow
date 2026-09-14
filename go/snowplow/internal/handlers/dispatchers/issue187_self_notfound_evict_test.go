// issue187_self_notfound_evict_test.go — #187 (i) falsifier: a refresh whose
// re-fetch of the entry's OWN object returns a definite apiserver 404 must
// EVICT the entry, not requeue it five times and drop it with the stale body
// still resident.
//
// WHY THIS FILE EXISTS RATHER THAN THE CACHE-PACKAGE ARM. The architect's B2
// (internal/cache/issue187_recreate_falsifier_test.go) drives the real
// refresher loop but installs its own RefreshFunc via RegisterRefreshFunc —
// a seam ABOVE the frame the fix lives in. Its RED transcript on main is a
// valid proof of the DEFECT (the refresher does drop the key and leave the
// body), but no change to resolveAndPopulateL1 can ever turn it GREEN,
// because the stub replaces resolveAndPopulateL1 entirely. Per
// feedback_seamed_dispatch_cannot_falsify_a_deep_frame, a fix-present arm
// that cannot observe the fix is not an arm. So the GREEN driver lives here,
// below that seam, and goes all the way down to a real HTTP 404.
//
// HOW REAL IS REAL. Every frame between the refresher queue and the wire is
// production code:
//
//	cache.EnqueueRefresh -> the real refresher worker pool -> the production
//	refreshFunc body -> resolveAndPopulateL1 -> resolveOnceProd ->
//	objects.Get -> getFromAPIServer -> client-go dynamic + RESTMapper ->
//	an httptest apiserver that answers discovery and then 404s the object.
//
// Nothing is stubbed, nothing is hand-installed. The 404 is produced by an
// HTTP server and travels back through apierrors.IsNotFound exactly as a real
// deletion does. No cluster, no kind, -race clean.
//
// ARMS
//
//	E2E     404 on the entry's own object -> entry EVICTED, dep edges cleared,
//	        selfNotFoundEvict counted, evict_delete_total moved, and the
//	        refresher does NOT burn its requeue budget (no refresh_dropped).
//	B500    the SAME driver with the apiserver returning 500 -> the entry
//	        SURVIVES and the error propagates (requeue preserved). An
//	        apiserver hiccup must never evict a slice of L1.
//	B403    same with 403 (an RBAC blip) -> entry survives.
//	BINF    resolveOnceProd under cache.WithInformerOnlyReads, where
//	        objects.Get SYNTHESISES a 404 instead of asking the apiserver ->
//	        the error must NOT carry the sentinel. That synthesised 404 means
//	        "not in the indexer", which is the CRD-re-registration transient.
//	BSTR    an error whose TEXT says "not found" but which is not the wrapped
//	        sentinel -> no eviction. Proves the discrimination is structural
//	        (errors.Is), not string matching, so an inner call's NotFound
//	        cannot evict the parent (the bucket-2/3 contract).

package dispatchers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	i187Group    = "widgets.templates.krateo.io"
	i187Version  = "v1beta1"
	i187Resource = "flexes"
	i187Kind     = "Flex"
	i187NS       = "krateo-system"
	i187Name     = "alerts-new-cta"
)

func i187GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: i187Group, Version: i187Version, Resource: i187Resource}
}

// i187APIServer is a minimal apiserver: enough discovery for client-go's
// RESTMapper to resolve flexes -> Flex, and a configurable reply for the
// object GET itself.
type i187APIServer struct {
	*httptest.Server
	objectStatus int
	objectGets   atomic.Int64
}

func newI187APIServer(t *testing.T, objectStatus int) *i187APIServer {
	t.Helper()
	s := &i187APIServer{objectStatus: objectStatus}
	objectPath := fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s",
		i187Group, i187Version, i187NS, i187Resource, i187Name)

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}

	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == objectPath:
			s.objectGets.Add(1)
			writeJSON(w, s.objectStatus, &metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   metav1.StatusFailure,
				Code:     int32(s.objectStatus),
				Reason:   i187StatusReason(s.objectStatus),
				Message:  i187StatusMessage(s.objectStatus),
			})

		case r.URL.Path == "/api":
			writeJSON(w, http.StatusOK, &metav1.APIVersions{
				TypeMeta: metav1.TypeMeta{Kind: "APIVersions"},
				Versions: []string{"v1"},
			})

		case r.URL.Path == "/api/v1":
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: "v1",
			})

		case r.URL.Path == "/apis":
			writeJSON(w, http.StatusOK, &metav1.APIGroupList{
				TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
				Groups: []metav1.APIGroup{{
					Name: i187Group,
					Versions: []metav1.GroupVersionForDiscovery{{
						GroupVersion: i187Group + "/" + i187Version,
						Version:      i187Version,
					}},
					PreferredVersion: metav1.GroupVersionForDiscovery{
						GroupVersion: i187Group + "/" + i187Version,
						Version:      i187Version,
					},
				}},
			})

		case r.URL.Path == "/apis/"+i187Group+"/"+i187Version:
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: i187Group + "/" + i187Version,
				APIResources: []metav1.APIResource{{
					Name:         i187Resource,
					SingularName: "flex",
					Namespaced:   true,
					Kind:         i187Kind,
					Verbs:        metav1.Verbs{"get", "list", "watch"},
				}},
			})

		default:
			writeJSON(w, http.StatusNotFound, &metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   metav1.StatusFailure,
				Code:     http.StatusNotFound,
				Reason:   metav1.StatusReasonNotFound,
				Message:  "unexpected path " + r.URL.Path,
			})
		}
	}))
	t.Cleanup(s.Close)
	return s
}

// i187StatusMessage mirrors the apiserver's own wording per code, so the
// 500/403 arms cannot accidentally pass a "not found"-worded body through a
// text-matching implementation.
func i187StatusMessage(code int) string {
	switch code {
	case http.StatusNotFound:
		return fmt.Sprintf("%s.%s %q not found", i187Resource, i187Group, i187Name)
	case http.StatusForbidden:
		return fmt.Sprintf("%s.%s %q is forbidden: user cannot get resource",
			i187Resource, i187Group, i187Name)
	default:
		return "the server encountered an internal error"
	}
}

func i187StatusReason(code int) metav1.StatusReason {
	switch code {
	case http.StatusNotFound:
		return metav1.StatusReasonNotFound
	case http.StatusForbidden:
		return metav1.StatusReasonForbidden
	default:
		return metav1.StatusReasonInternalError
	}
}

// i187Watcher builds a REAL ResourceWatcher over a fake dynamic client and
// publishes it as the process global, so the #187 (i) type-exists conjunct
// (selfObjectTypeStillRegistered → cache.Global().IsRegistered) sees a real
// answer rather than a stub.
//
// registerSelfGVR models the CRD lifecycle:
//
//	true  — the widget CRD exists, an informer is registered for flexes. A 404
//	        on one object means THAT OBJECT is gone.
//	false — the CRD itself is absent (deleted and recreated, which is what a
//	        portal release does). The apiserver then 404s EVERY object of the
//	        GVR, and evicting on that would empty L1 of the whole type.
//
// Registering the informer does not make the arms vacuous: the fake client
// holds no flexes, so objects.Get's informer branch MISSES and falls through
// to the httptest apiserver exactly as production does on a real miss.
func i187Watcher(t *testing.T, registerSelfGVR bool) *cache.ResourceWatcher {
	t.Helper()

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		i187GVR(): "FlexList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("NewResourceWatcher returned nil with CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

	if registerSelfGVR {
		_, syncCh := rw.EnsureResourceType(i187GVR())
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("EnsureResourceType(%s): informer did not sync", i187GVR())
		}
		if !rw.IsRegistered(i187GVR()) {
			t.Fatalf("setup: %s should be registered after EnsureResourceType", i187GVR())
		}
	} else if rw.IsRegistered(i187GVR()) {
		t.Fatalf("setup: %s should NOT be registered — the arm would be vacuous", i187GVR())
	}

	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// i187Fixture wires a clean L1 store + dep tracker with ONE resident
// widgets-class entry whose own object is the CR the apiserver will answer
// for, and returns the SA transport pair resolveAndPopulateL1 expects.
func i187Fixture(t *testing.T, srv *i187APIServer, typeRegistered bool) (store *cache.ResolvedCacheStore, inputs cache.ResolvedKeyInputs, key string, saEP *endpoints.Endpoint, saRC *rest.Config) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)

	store = cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	cache.Deps().SetStore(store)
	// The type-exists conjunct must be TRUE for every arm except the
	// CRD-absent one, otherwise the bound arms below would pass for the
	// wrong reason (no eviction because the type is unknown, rather than
	// because the error class is not a deletion).
	i187Watcher(t, typeRegistered)

	inputs = cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  i187Group,
		Version:                i187Version,
		Resource:               i187Resource,
		Namespace:              i187NS,
		Name:                   i187Name,
		BindingUID:             "uid-187",
		RepresentativeUsername: "admin",
		RepresentativeGroups:   []string{"admins"},
	}
	key = cache.ComputeKey(inputs)
	store.Put(key, &cache.ResolvedEntry{
		RawJSON: []byte(`{"children":["agents-header","agents-list","agents-servers"]}`),
		Inputs:  &inputs,
	})
	// The self dep edge a cold dispatch records (deps_extract.go).
	cache.Deps().Record(key, i187GVR(), i187NS, i187Name)

	saEP = &endpoints.Endpoint{ServerURL: srv.URL}
	saRC = &rest.Config{Host: srv.URL}
	return store, inputs, key, saEP, saRC
}

// i187RunRefreshCycle drives the FULL production refresh loop for one key and
// returns how many times the handler ran. It registers the production refresh
// closure verbatim (dispatchers.go), starts the real refresher pool, enqueues
// the key, and waits until either the entry is gone or the requeue budget has
// been spent.
//
// Every bound arm goes through here rather than calling resolveAndPopulateL1
// directly. Under the drop-point design resolveAndPopulateL1 NEVER evicts — it
// only classifies — so a direct call could no longer observe the eviction
// decision at all, and an "entry survived" assertion against it would be
// vacuous for every error class including a real deletion.
func i187RunRefreshCycle(t *testing.T, inputs cache.ResolvedKeyInputs, key string,
	saEP *endpoints.Endpoint, saRC *rest.Config, store *cache.ResolvedCacheStore) int64 {
	t.Helper()

	var invocations atomic.Int64
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		invocations.Add(1)
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(ctx)
	cache.EnqueueRefresh(key)

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		_, alive := store.Get(key)
		if !alive && invocations.Load() > 0 {
			break
		}
		if invocations.Load() > i187MaxRequeues {
			// Budget spent and the entry is still here — give the drop
			// path a moment to run, then stop waiting.
			time.Sleep(300 * time.Millisecond)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Tear the pool down HERE, not via t.Cleanup. ResetRefresherForTest
	// blocks until every worker goroutine has exited; without that barrier a
	// worker still inside resolveAndPopulateL1 races the caller's restore of
	// the resolveOnceFn seam (cleanups run LIFO, so a later-registered
	// restore would fire while the pool is still draining).
	cancel()
	cache.ResetRefresherForTest()
	return invocations.Load()
}

// i187MaxRequeues mirrors cache.maxRefreshRequeues, which is unexported. The
// B2E2E arm asserts the exact invocation count against it, so a change to the
// budget shows up here as a failing count rather than as a silent timing shift.
const i187MaxRequeues = 5

func i187RefresherEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
}

// --- E2E — the whole loop, ending at an HTTP 404 ----------------------------

func TestIssue187_B2E2E_RefresherSelfNotFoundEvictsThroughTheRealLoop(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusNotFound)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

	beforeEvictDelete := cache.Deps().Stats().EvictDeleteTotal
	beforeSelfGone := cache.Deps().Stats().EvictSelfGoneTotal

	invocations := i187RunRefreshCycle(t, inputs, key, saEP, saRC, store)

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B2 RED: the entry's own object 404s at the apiserver (%d object GETs "+
			"served) yet the refresher left the stale body resident after %d invocations. Once "+
			"the full requeue budget has re-confirmed the deletion, the drop point must EVICT, "+
			"not drop-to-TTL.", srv.objectGets.Load(), invocations)
	}
	if srv.objectGets.Load() == 0 {
		t.Fatalf("#187 B2: the apiserver never served the object GET — the arm is not driving "+
			"the real re-fetch boundary (invocations=%d)", invocations)
	}
	// The budget is the bound: the full requeue allowance is spent
	// re-confirming the deletion, then exactly one eviction. Not earlier
	// (that would evict on a transient) and not later (that would spin).
	if invocations != i187MaxRequeues+1 {
		t.Fatalf("#187 B2: refresh invocations = %d, want %d (the full requeue budget, then "+
			"evict at the drop point)", invocations, i187MaxRequeues+1)
	}
	if got := cache.Deps().Stats().EvictSelfGoneTotal; got != beforeSelfGone+1 {
		t.Fatalf("#187 B2: evict_self_gone_total = %d, want %d", got, beforeSelfGone+1)
	}
	// Architect Finding 2: the H1 live discriminator must NOT move.
	if got := cache.Deps().Stats().EvictDeleteTotal; got != beforeEvictDelete {
		t.Fatalf("#187 B2: evict_delete_total moved (%d -> %d) for a refresher-driven eviction; "+
			"it must stay informer-DELETE-driven", beforeEvictDelete, got)
	}
	if n := len(cache.Deps().CollectMatchesForTest(i187GVR(), i187NS, i187Name)); n != 0 {
		t.Fatalf("#187 B2: %d dep record(s) survived the eviction — RemoveL1Key did not run "+
			"alongside the store delete", n)
	}
}

// --- B500 / B403 — a transient must NOT evict, even across the whole budget --

func TestIssue187_B2Bound_TransientApiserverErrorDoesNotEvict(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{
		{"apiserver_500", http.StatusInternalServerError},
		{"rbac_403", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i187RefresherEnv(t)
			srv := newI187APIServer(t, tc.code)
			store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

			// Classification first, at the site that does it.
			err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
			if err == nil {
				t.Fatalf("#187 bound %s: want an error so the refresher REQUEUES; got nil", tc.name)
			}
			if errors.Is(err, cache.ErrSelfObjectGone) {
				t.Fatalf("#187 bound %s: a %d was classified as a deletion — only a definite "+
					"404 may ever reach the drop-point eviction", tc.name, tc.code)
			}

			// Then the whole loop: the budget is spent and the entry SURVIVES.
			invocations := i187RunRefreshCycle(t, inputs, key, saEP, saRC, store)
			if _, ok := store.Get(key); !ok {
				t.Fatalf("#187 bound %s: the entry was EVICTED after %d attempts on a %d. An "+
					"apiserver hiccup or an RBAC blip must never evict L1 — the key drops to "+
					"TTL and a later dirty-mark retries it.", tc.name, invocations, tc.code)
			}
			if got := cache.Deps().Stats().EvictSelfGoneTotal; got != 0 {
				t.Fatalf("#187 bound %s: evict_self_gone_total = %d, want 0", tc.name, got)
			}
		})
	}
}

// --- BINF — the SYNTHESISED 404 of an informer-only read must not evict -----

func TestIssue187_B2Bound_InformerOnlyMissIsNotADeletion(t *testing.T) {
	srv := newI187APIServer(t, http.StatusNotFound)
	_, inputs, _, _, saRC := i187Fixture(t, srv, true)

	// WithInformerOnlyReads makes objects.Get return a NotFound-SHAPED Err
	// WITHOUT reaching the apiserver (objects/get.go informerOnlyMiss). That
	// 404 means "absent from the indexer", which is exactly what a CRD
	// re-registration window produces — a transient, not a deletion.
	ctx := cache.WithInternalRESTConfig(cache.WithInformerOnlyReads(context.Background()), saRC)

	_, err := resolveOnceProd(ctx, inputs)
	if err == nil {
		t.Fatalf("#187 BINF: expected the informer-only miss to surface an error")
	}
	if errors.Is(err, cache.ErrSelfObjectGone) {
		t.Fatalf("#187 BINF: a SYNTHESISED informer-only 404 was classified as a deletion. "+
			"objects.Get never asked the apiserver, so this 404 carries no deletion evidence. "+
			"err=%v", err)
	}
	if srv.objectGets.Load() != 0 {
		t.Fatalf("#187 BINF: the apiserver was contacted (%d GETs) — the informer-only marker "+
			"did not take effect and the arm is not testing what it claims", srv.objectGets.Load())
	}
}

// --- BSTR — discrimination is structural, not textual -----------------------

func TestIssue187_B2Bound_NotFoundWordingWithoutTheSentinelDoesNotEvict(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusNotFound)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

	// An INNER call's NotFound reaches the refresher as an ordinary resolve
	// error carrying NotFound wording but no sentinel. Evicting on it would
	// evict a widget because one of its CHILDREN vanished — the opposite of
	// the OnDelete bucket-2/3 contract.
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return nil, fmt.Errorf(`inner call comps: compositions.composition.krateo.io "x" not found`)
	})
	t.Cleanup(restore)

	invocations := i187RunRefreshCycle(t, inputs, key, saEP, saRC, store)

	if _, ok := store.Get(key); !ok {
		t.Fatalf("#187 BSTR: the entry was evicted after %d attempts on an INNER call's "+
			"NotFound. The drop-point gate must key on the wrapped sentinel (errors.Is), never "+
			"on error text — a child vanishing must dirty-mark the parent, not evict it.",
			invocations)
	}
	if got := cache.Deps().Stats().EvictSelfGoneTotal; got != 0 {
		t.Fatalf("#187 BSTR: evict_self_gone_total = %d, want 0", got)
	}
}

// --- BCRD — the type-absent 404: PINS WHAT ACTUALLY HAPPENS ------------------
//
// This arm was written to pin the architect's Finding 1 fix (b): a type-exists
// conjunct, cache.Global().IsRegistered(gvr), meant to stop a deleted CRD's
// genuine 404s from evicting every entry of that GVR. Driven against the real
// path, that fix went RED — and the reason is in the log line the arm captures:
//
//	INFO cache.lazy_register gvr="…, Resource=flexes" path=full-unstructured
//	     hint="first resolver touch — informer registered + dep-tracker handlers wired"
//
// objects.Get's own not-servable branch calls rw.EnsureResourceType(gvr) BEFORE
// falling through to the apiserver, so the GVR is registered again by the time
// anything downstream can consult IsRegistered. EnsureResourceType's #119
// pre-check refuses only at GROUP granularity, and a single deleted widget CRD
// leaves its group served by every other widget CRD — exactly the portal-release
// case the conjunct was chosen for. The conjunct is kept as defence-in-depth for
// the case it does bind (a whole group gone), but it cannot be the bound.
//
// So the REAL bound is the requeue budget, and this arm pins that: a type-absent
// window shorter than the budget leaves the entry alive (that is B3, in the cache
// package); a type that stays absent across the whole budget is indistinguishable
// from a deleted object and does evict. This arm asserts the latter honestly
// rather than asserting a guarantee the code does not provide.
func TestIssue187_BCRD_TypeAbsentIsBoundedByTheBudgetNotTheConjunct(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusNotFound)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, false)

	if cache.Global().IsRegistered(i187GVR()) {
		t.Fatalf("setup: the GVR must start UNregistered for this arm")
	}

	invocations := i187RunRefreshCycle(t, inputs, key, saEP, saRC, store)

	// The conjunct did not hold: objects.Get re-registered the GVR itself.
	if !cache.Global().IsRegistered(i187GVR()) {
		t.Fatalf("#187 BCRD: the GVR was NOT re-registered by objects.Get. If this ever holds, " +
			"the type-exists conjunct becomes a real bound and this arm should be inverted back " +
			"to \"type absent must not evict\".")
	}
	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 BCRD: the entry survived %d attempts of a sustained type-absent 404. "+
			"That is not today's behaviour: the budget cannot tell a permanently absent type "+
			"from a deleted object, and pinning a guarantee the code does not provide would "+
			"hide the next regression.", invocations)
	}
	if invocations != i187MaxRequeues+1 {
		t.Fatalf("#187 BCRD: %d invocations, want %d — the budget must still be spent in full "+
			"before a type-absent window evicts, so a CRD re-registration that clears inside "+
			"it is safe (that is arm B3)", invocations, i187MaxRequeues+1)
	}
}
