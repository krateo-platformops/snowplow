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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
				Message: i187StatusMessage(s.objectStatus),
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

// i187Fixture wires a clean L1 store + dep tracker with ONE resident
// widgets-class entry whose own object is the CR the apiserver will answer
// for, and returns the SA transport pair resolveAndPopulateL1 expects.
func i187Fixture(t *testing.T, srv *i187APIServer) (store *cache.ResolvedCacheStore, inputs cache.ResolvedKeyInputs, key string, saEP *endpoints.Endpoint, saRC *rest.Config) {
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

// --- E2E — the whole loop, ending at an HTTP 404 ----------------------------

func TestIssue187_B2E2E_RefresherSelfNotFoundEvictsThroughTheRealLoop(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")

	srv := newI187APIServer(t, http.StatusNotFound)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv)

	beforeEvictDelete := cache.Deps().Stats().EvictDeleteTotal
	beforeSelfNotFound := cache.RefresherSelfNotFoundEvictTotal()

	// The PRODUCTION refresh closure, verbatim (dispatchers.go:79-87). The
	// only thing this test supplies that RegisterRefreshHandlers would
	// discover is the SA transport pair.
	var invocations atomic.Int64
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		invocations.Add(1)
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(ctx)
	cache.EnqueueRefresh(key)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(key); !ok && invocations.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, ok := store.Get(key); ok {
		t.Fatalf("#187 B2 RED: the entry's own object 404s at the apiserver (%d object GETs served) "+
			"yet the refresher left the stale body resident after %d invocations. A definite 404 on "+
			"the entry's OWN object means the object is gone — it must EVICT, not requeue-then-drop.",
			srv.objectGets.Load(), invocations.Load())
	}
	if srv.objectGets.Load() == 0 {
		t.Fatalf("#187 B2: the apiserver never served the object GET — the arm is not driving the "+
			"real re-fetch boundary (invocations=%d)", invocations.Load())
	}
	// The eviction must be BOUNDED: one re-fetch, not the five-requeue budget.
	if got := invocations.Load(); got != 1 {
		t.Fatalf("#187 B2: refresh invocations = %d, want exactly 1 — a self-object 404 is a "+
			"resolved outcome and must not burn the requeue budget re-confirming a deletion", got)
	}
	// Counted on BOTH surfaces an operator reads.
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != beforeSelfNotFound+1 {
		t.Fatalf("#187 B2: refresher self-NotFound evictions = %d, want %d",
			got, beforeSelfNotFound+1)
	}
	if got := cache.Deps().Stats().EvictDeleteTotal; got != beforeEvictDelete+1 {
		t.Fatalf("#187 B2: evict_delete_total = %d, want %d — the eviction must land on the same "+
			"counter an informer DELETE moves", got, beforeEvictDelete+1)
	}
	// Dep edges must go with the entry, not outlive it (the reason the
	// eviction routes through the tracker rather than the store directly).
	if n := len(cache.Deps().CollectMatchesForTest(i187GVR(), i187NS, i187Name)); n != 0 {
		t.Fatalf("#187 B2: %d dep record(s) survived the eviction — RemoveL1Key did not run "+
			"alongside the store delete", n)
	}
	_ = inputs
}

// --- B500 / B403 — a transient must NOT evict -------------------------------

func TestIssue187_B2Bound_TransientApiserverErrorDoesNotEvict(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{
		{"apiserver_500", http.StatusInternalServerError},
		{"rbac_403", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newI187APIServer(t, tc.code)
			store, inputs, key, saEP, saRC := i187Fixture(t, srv)

			err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
			if err == nil {
				t.Fatalf("#187 bound %s: want an error so the refresher REQUEUES; got nil", tc.name)
			}
			if errors.Is(err, errSelfObjectGone) {
				t.Fatalf("#187 bound %s: a %d was classified as a deletion — only a definite 404 may evict",
					tc.name, tc.code)
			}
			if _, ok := store.Get(key); !ok {
				t.Fatalf("#187 bound %s: the entry was EVICTED on a %d. An apiserver hiccup or an RBAC "+
					"blip must never evict a slice of L1 — it must stay retryable.", tc.name, tc.code)
			}
			if got := cache.RefresherSelfNotFoundEvictTotal(); got != 0 {
				t.Fatalf("#187 bound %s: self-NotFound evictions = %d, want 0", tc.name, got)
			}
		})
	}
}

// --- BINF — the SYNTHESISED 404 of an informer-only read must not evict -----

func TestIssue187_B2Bound_InformerOnlyMissIsNotADeletion(t *testing.T) {
	srv := newI187APIServer(t, http.StatusNotFound)
	_, inputs, _, _, saRC := i187Fixture(t, srv)

	// WithInformerOnlyReads makes objects.Get return a NotFound-SHAPED Err
	// WITHOUT reaching the apiserver (objects/get.go informerOnlyMiss). That
	// 404 means "absent from the indexer", which is exactly what a CRD
	// re-registration window produces — it is a transient, not a deletion.
	ctx := cache.WithInternalRESTConfig(cache.WithInformerOnlyReads(context.Background()), saRC)

	_, err := resolveOnceProd(ctx, inputs)
	if err == nil {
		t.Fatalf("#187 BINF: expected the informer-only miss to surface an error")
	}
	if errors.Is(err, errSelfObjectGone) {
		t.Fatalf("#187 BINF: a SYNTHESISED informer-only 404 was classified as a deletion. "+
			"objects.Get never asked the apiserver, so this 404 carries no deletion evidence; "+
			"treating it as one evicts L1 during a CRD re-registration window. err=%v", err)
	}
	if srv.objectGets.Load() != 0 {
		t.Fatalf("#187 BINF: the apiserver was contacted (%d GETs) — the informer-only marker "+
			"did not take effect and the arm is not testing what it claims", srv.objectGets.Load())
	}
}

// --- BSTR — discrimination is structural, not textual -----------------------

func TestIssue187_B2Bound_NotFoundWordingWithoutTheSentinelDoesNotEvict(t *testing.T) {
	srv := newI187APIServer(t, http.StatusNotFound)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv)

	// An INNER call's NotFound reaches resolveAndPopulateL1 as an ordinary
	// resolve error carrying NotFound wording but no sentinel. Evicting on
	// it would evict a widget because one of its CHILDREN vanished — the
	// opposite of the OnDelete bucket-2/3 contract (a child going away
	// dirty-marks the parent; it never evicts it).
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return nil, fmt.Errorf(`inner call comps: compositions.composition.krateo.io "x" not found`)
	})
	t.Cleanup(restore)

	err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
	if err == nil {
		t.Fatalf("#187 BSTR: want the inner-call error to propagate for a requeue; got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("#187 BSTR: fixture error lost its wording: %v", err)
	}
	if _, ok := store.Get(key); !ok {
		t.Fatalf("#187 BSTR: the entry was evicted on an INNER call's NotFound. The self-object " +
			"gate must key on the wrapped sentinel (errors.Is), never on error text — a child " +
			"vanishing must dirty-mark the parent, not evict it.")
	}
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Fatalf("#187 BSTR: self-NotFound evictions = %d, want 0", got)
	}
}
