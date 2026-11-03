package dispatchers

// issue1080_review_arms_test.go — #1080 arms the PR #1100 review asked for
// (reviewer arms RV-C6 and the CRD gone-then-recreated case), on the same
// c681 customer harness as issue1080_definite_notfound_test.go.
//
//	C6  the object is created DURING the fill, after the apiserver answered
//	    404 and before the Put. Either the resolve declines (the watcher
//	    already holds the object), or the 404 is stored AND a dirty mark
//	    reaches the cell after it is stored (the #375 put-then-remark). A
//	    mark that only lands before the Put is not enough: a refresher acting
//	    on it would be overwritten by the stale 404.
//	C8  the resource type goes away and comes back together with the
//	    object (a CRD deleted, then restored with its objects): the object
//	    arrives in the new informer's INITIAL list, whose ADD the pre-sync
//	    gate drops, so no exact-edge mark reaches the cached 404. The type
//	    removal's mark (OnResourceTypeRemoved) is what heals it: the
//	    refresher cannot re-establish the 404, the cell ends KNOWN-STALE
//	    (#547), and a read revalidates it under the requester. The next
//	    reads show the object.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestIssue1080_C6_CreateDuringFill_StoredCellIsRemarked(t *testing.T) {
	e := c681Setup(t)
	orig := e.user.Config.Handler
	var target atomic.Value
	target.Store("")
	e.user.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orig.ServeHTTP(w, r)
		if tp := target.Load().(string); tp != "" && r.URL.Path == "/api/v1/namespaces/pub/configmaps/"+tp {
			target.Store("")
			// The object appears after the apiserver answered 404 and before
			// the resolver classifies and Puts (informer delivery is async).
			_, _ = e.dyn.Resource(c681CMs).Namespace("pub").Create(context.Background(),
				n1080CM("pub", tp, "RACE"), metav1.CreateOptions{})
		}
	})
	t.Cleanup(func() { cache.Deps().SetRefreshHook(nil) })
	// The window is a race; iterate until the store side has been seen a few
	// times (each iteration is a fresh object name, so a fresh window).
	const wantStored, maxIter = 3, 400
	var declined, stored int
	for i := 0; i < maxIter && stored < wantStored; i++ {
		cache.ResetResolvedCacheForTest()
		name := fmt.Sprintf("race-%d", i)
		e.ra = c681RAObj("/api/v1/namespaces/pub/configmaps/" + name)
		key := e.key(t, c681A)
		var mu sync.Mutex
		var pre, post int
		cache.Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
			if k != key {
				return
			}
			_, present := cache.ResolvedCache().GetNoTouch(k)
			mu.Lock()
			if present {
				post++
			} else {
				pre++
			}
			mu.Unlock()
		})
		target.Store(name)
		body, _ := e.call(t, c681A)
		if _, ok := e.cell(c681A, t); !ok {
			declined++
			continue
		}
		stored++
		if !bytes.Contains(body, []byte("not found")) {
			t.Fatalf("iteration %d: the stored body is not the 404: %s", i, s425Trunc(body))
		}
		i1126WaitFor(t, 5*time.Second, fmt.Sprintf(
			"C6 RED (iteration %d): the 404 was stored after the object was created during its fill and no dirty mark reached the STORED cell — a refresher that acted on a pre-Put mark is overwritten by the stale 404", i),
			func() bool {
				mu.Lock()
				defer mu.Unlock()
				return post > 0
			})
		mu.Lock()
		t.Logf("iteration %d stored: post-Put marks=%d pre-Put marks=%d", i, post, pre)
		mu.Unlock()
	}
	t.Logf("RUN C6: declined=%d stored(all re-marked after the Put)=%d", declined, stored)
	if stored == 0 {
		t.Fatalf("C6: no iteration stored the 404 in %d tries — the arm never exercised the put-then-remark window", maxIter)
	}
}

// n1080Serve is c681Env.call without the per-call fetch seam: C8's seam is
// installed once for the whole arm, because the detached demand revalidation
// re-enters the handler after the call returns.
func n1080Serve(t *testing.T, e *c681Env, id ident681) ([]byte, int64) {
	t.Helper()
	d0 := e.dials.Load()
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(id.ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("/call for %s: status %d body=%s", id.user, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes(), e.dials.Load() - d0
}

// c1080Gizmos is a CRD-backed GVR: its group is navigation-discovered, so its
// informer is standalone and removable (watcher.go R6), as a composition
// CRD's is in production. A core type is never removed, so it cannot play
// this arm.
var c1080Gizmos = schema.GroupVersionResource{Group: "gizmos1080.example.io", Version: "v1", Resource: "gizmos"}

func n1080Gizmo(ns, name, v string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": c1080Gizmos.Group + "/" + c1080Gizmos.Version, "kind": "Gizmo",
		"metadata": map[string]any{"namespace": ns, "name": name},
		"spec":     map[string]any{"v": v},
	}}
}

func n1080WaitServable(t *testing.T, gvr schema.GroupVersionResource) {
	t.Helper()
	rw := cache.Global()
	if _, ch := rw.EnsureResourceType(gvr); ch != nil {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s informer did not sync", gvr)
		}
	}
	i1126WaitFor(t, 10*time.Second, gvr.String()+" servable", func() bool { return rw.IsServable(gvr) })
}

func TestIssue1080_C8_TypeGoneThenRecreatedWithObject_CellHeals(t *testing.T) {
	i187RefresherEnv(t)
	cache.ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(cache.ResetNavigationDiscoveredGroupsForTest)
	cache.AddNavigationDiscoveredGroup(c1080Gizmos.Group)
	e := c681Setup(t)
	n1080WaitServable(t, c1080Gizmos)
	const path = "/apis/gizmos1080.example.io/v1/namespaces/pub/gizmos/ghost"
	e.ra = c681RAObj(path)
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	})
	t.Cleanup(restore)
	cache.Deps().SetStore(cache.ResolvedCache())

	body, d := n1080Serve(t, e, c681A)
	key := e.key(t, c681A)
	if _, ok := e.cell(c681A, t); !ok || d != 1 || !bytes.Contains(body, []byte(`gizmos.gizmos1080.example.io \"ghost\" not found`)) {
		t.Fatalf("PRE: A's definite 404 must be cached (dials %d): %s", d, s425Trunc(body))
	}

	// The real refresher pool. Its SA endpoint carries no credential the
	// harness apiserver accepts: a background re-resolve that must dial
	// declines.
	_, stop := i1126Loop(t, "restactions", &endpoints.Endpoint{ServerURL: e.user.URL}, &rest.Config{Host: e.user.URL})
	t.Cleanup(stop)

	// The CRD is deleted (triggerCRDDelete's two calls, in its order) and
	// restored together with the object: the object is at the apiserver
	// before any new informer lists, so it reaches the informer only in its
	// INITIAL list, whose ADD the pre-sync gate drops.
	rw := cache.Global()
	rw.RemoveResourceType(c1080Gizmos)
	if _, err := e.dyn.Resource(c1080Gizmos).Namespace("pub").Create(context.Background(),
		n1080Gizmo("pub", "ghost", "CREATED-1080"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	cache.Deps().OnResourceTypeRemoved(c1080Gizmos)
	n1080WaitServable(t, c1080Gizmos)

	// Reads: the cell must come to show the object (a known-stale hit is
	// served and revalidated under the requester; a refresh that found the
	// informer synced has already replaced it).
	reads := 0
	i1126WaitFor(t, 20*time.Second, "C8 RED: after the type came back with the object, reads keep being served the 404", func() bool {
		reads++
		b, _ := n1080Serve(t, e, c681A)
		return bytes.Contains(b, []byte("CREATED-1080"))
	})
	t.Logf("RUN C8: the cell showed the object after %d read(s)", reads)
	// Let any revalidation in flight settle before the seam is restored.
	i1126WaitFor(t, 10*time.Second, "C8: the healed cell is fresh", func() bool {
		_, _, stale := cache.KnownStale(key)
		got, ok := cache.ResolvedCache().GetNoTouch(key)
		return !stale && ok && bytes.Contains(got.RawJSON, []byte("CREATED-1080"))
	})
}
