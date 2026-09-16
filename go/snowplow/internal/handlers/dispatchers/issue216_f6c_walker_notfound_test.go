// issue216_f6c_walker_notfound_test.go — 1.12.7 F6c: the walk's child-fetch
// failure branch must drop the harvested copy on a confirmed NotFound, and
// keep it on every other failure.
//
// HOW REAL THE ERRORS ARE. Nothing here hand-builds an error value. Each arm
// runs the production objects.Get against an httptest apiserver that answers
// discovery and then replies with the status code under test, so the
// *response.Status the decision sees travelled back through client-go and
// apierrors exactly as a real deletion, a real RBAC blip or a real outage
// does. The harness is the one issue187_self_notfound_evict_test.go already
// uses for the refresher's self-gone verdict; this file reuses it rather than
// building a second apiserver.
//
// The decision under test is the production method forgetChildIfGone, called
// from the walk's child-fetch branch with exactly these values. The walk's
// own recursion is not driven here: reaching the child fetch requires a
// successful parent widgets.Resolve, which needs a live CRD schema fetch (the
// same hermetic reach limit recorded in seed_resolves_counter_test.go).
//
// PARTIAL, NOT DORMANT. F6c fires for any child fetched under a walked root —
// most of the walk, since the app-shell root is alive — but cannot reach a
// widget only the routes loader would have found (#220). F6b is the closer.
//
// RED on the base: drop the NotFound split and forget on every error, and the
// 500/403/informer-only arms fail; drop the forget entirely and the 404 arm
// fails.
package dispatchers

import (
	"context"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func f6cGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: i187Group, Version: i187Version, Resource: i187Resource}
}

// f6cRef is the child reference the walk would have parsed out of a /call
// path, aimed at the coordinate the httptest apiserver answers for.
func f6cRef() templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: i187Name, Namespace: i187NS},
		APIVersion: i187Group + "/" + i187Version,
		Resource:   i187Resource,
	}
}

// f6cWalker returns a walker holding a harvested copy of the child, plus the
// harvester so the arm can inspect it.
func f6cWalker(t *testing.T) (*phase1Walker, *navWidgetHarvester) {
	t.Helper()
	nav := newNavWidgetHarvester()
	w := &unstructured.Unstructured{}
	w.SetNamespace(i187NS)
	w.SetName(i187Name)
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: i187Group, Version: i187Version, Kind: i187Kind,
	})
	nav.harvestNavWidget(w, f6cGVR(), -1, -1, -1, -1)
	if !f6bHarvestedStillHolds(nav, f6cGVR(), i187NS, i187Name) {
		t.Fatalf("premise: the child was not harvested")
	}
	return &phase1Walker{
		visited:            map[string]struct{}{},
		navWidgetHarvester: nav,
		apiRefHarvester:    newContentPrewarmHarvester(),
	}, nav
}

// f6cSACtx builds the ctx the walk's child fetch actually runs under, using
// the PRODUCTION builder, pointed at the httptest apiserver.
func f6cSACtx(serverURL string) context.Context {
	return withPhase1SAContext(context.Background(),
		endpoints.Endpoint{ServerURL: serverURL},
		&rest.Config{Host: serverURL})
}

func TestIssue216_F6c_ConfirmedNotFoundForgetsTheHarvestedChild(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	srv := newI187APIServer(t, http.StatusNotFound)
	ctx := f6cSACtx(srv.URL)

	got := objects.Get(ctx, f6cRef())
	if got.Err == nil {
		t.Fatalf("premise: objects.Get returned no error against a 404 apiserver")
	}
	if got.Err.Code != http.StatusNotFound {
		t.Fatalf("premise: expected a real 404 from the wire, got %d (%s)", got.Err.Code, got.Err.Message)
	}
	// The 404 must have come from the apiserver, not been synthesised — that
	// distinction is the whole point of the informer-only arm below.
	if n := srv.objectGets.Load(); n == 0 {
		t.Fatalf("premise: the object GET never reached the apiserver, so this 404 is not the " +
			"confirmed-deletion one under test")
	}

	w, nav := f6cWalker(t)
	if gone := w.forgetChildIfGone(ctx, f6cRef(), got.Err); !gone {
		t.Fatalf("RED (F6c): a confirmed apiserver 404 for a child was not treated as GONE")
	}
	if f6bHarvestedStillHolds(nav, f6cGVR(), i187NS, i187Name) {
		t.Fatalf("RED (F6c): the apiserver confirmed %s/%s is gone, but the walk kept its harvested "+
			"copy. The per-binding seed re-resolves that copy on every pass and never fetches the "+
			"object, so the deleted widget keeps being written back into L1", i187NS, i187Name)
	}
}

func TestIssue216_F6c_OtherErrorsKeepTheHarvestedChild(t *testing.T) {
	for _, tc := range []struct {
		label string
		code  int
	}{
		{"500 — an apiserver outage", http.StatusInternalServerError},
		{"403 — an RBAC blip", http.StatusForbidden},
	} {
		t.Run(tc.label, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			srv := newI187APIServer(t, tc.code)
			ctx := f6cSACtx(srv.URL)

			got := objects.Get(ctx, f6cRef())
			if got.Err == nil {
				t.Fatalf("premise: objects.Get returned no error against a %d apiserver", tc.code)
			}
			if got.Err.Code == http.StatusNotFound {
				t.Fatalf("premise: a %d reply arrived as a 404 — the arm would be vacuous", tc.code)
			}

			w, nav := f6cWalker(t)
			if gone := w.forgetChildIfGone(ctx, f6cRef(), got.Err); gone {
				t.Fatalf("RED (F6c): a %d was treated as GONE", tc.code)
			}
			if !f6bHarvestedStillHolds(nav, f6cGVR(), i187NS, i187Name) {
				t.Fatalf("RED (F6c): a %d dropped the harvested copy of %s/%s. Only a confirmed "+
					"deletion may — an apiserver hiccup that emptied the warm set would turn every "+
					"blip into a wave of cold navigations", tc.code, i187NS, i187Name)
			}
		})
	}
}

// TestIssue216_F6c_SynthesisedNotFoundKeepsTheHarvestedChild — the bound that
// is easiest to get wrong. Under cache.WithInformerOnlyReads objects.Get
// SYNTHESISES a 404 without asking the apiserver, and that 404 means "not in
// the indexer", not "deleted from the cluster" — precisely the transient a CRD
// re-registration produces. The code is identical to a real deletion's, so
// only the ctx can tell them apart.
func TestIssue216_F6c_SynthesisedNotFoundKeepsTheHarvestedChild(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	srv := newI187APIServer(t, http.StatusOK)
	ctx := cache.WithInformerOnlyReads(f6cSACtx(srv.URL))

	got := objects.Get(ctx, f6cRef())
	if got.Err == nil || got.Err.Code != http.StatusNotFound {
		t.Fatalf("premise: expected the synthesised informer-only 404, got %v", got.Err)
	}
	if n := srv.objectGets.Load(); n != 0 {
		t.Fatalf("premise: the informer-only path reached the apiserver %d time(s); the 404 under "+
			"test must be the synthesised one", n)
	}

	w, nav := f6cWalker(t)
	if gone := w.forgetChildIfGone(ctx, f6cRef(), got.Err); gone {
		t.Fatalf("RED (F6c): the SYNTHESISED informer-only 404 was treated as GONE")
	}
	if !f6bHarvestedStillHolds(nav, f6cGVR(), i187NS, i187Name) {
		t.Fatalf("RED (F6c): a synthesised informer-only 404 dropped the harvested copy. That 404 " +
			"means 'not in the indexer', not 'deleted' — it is the CRD re-registration transient, " +
			"and removing on it is removal on absence of evidence")
	}
}
