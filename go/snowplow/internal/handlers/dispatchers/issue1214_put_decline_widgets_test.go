package dispatchers

// issue1214_put_decline_widgets_test.go — #1214, the widgets twin of the arm in
// issue1214_put_decline_partial_snapshot_test.go: the same protection on the same
// partial snapshot, reached through a widget whose apiRef RESTAction has a stage
// dialled as the requester. It runs the REAL widgets.Resolve, status validation
// included: the resolver seam the other widget arms use would replace the dispatch
// site this pins.
//
// What alice's dial brought must reach no shared cell: not the widget's own key,
// and not any other cell the apiRef goes through. So besides that key this reads
// every resident cell, of any class, for the dial's own mark. The content cell does
// hold the object here on the informer shapes: the informer filled it, un-gated by
// design, and bob's serve is what shows the per-user gate kept it from him.
//
// Mutation → RED, on the sentinel in bob's body: the dispatch site stops marking
// the touch, the count ignores it, or the gate stops declining on it. The widgets
// Put has two layers, the site's own branch and the write that takes the verdict.
// Either one removed alone stays GREEN, masked by the other; both removed is RED.
//
// Not exercised here: a widget with the external-TTL opt-in.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// k1214Widget is a widget whose data is the apiRef stage's read.
func k1214Widget() *unstructured.Unstructured {
	return h1WidgetUnstructured(map[string]any{
		"apiRef": map[string]any{"name": psRAName, "namespace": h1NS},
		"widgetDataTemplate": []any{
			map[string]any{"forPath": "title", "expression": "${ .target.data.password }"},
		},
	})
}

// k1214WidgetCtx is a requester as the auth middleware leaves one: a widget's
// resourcesRefs step reads the user's own endpoint from the context.
func k1214WidgetCtx(a psArm, user, srvURL string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: psGroups(a)}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srvURL, Token: "tok-" + user}))
}

func k1214WidgetKey(t *testing.T, ctx context.Context, cr *unstructured.Unstructured) string {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
		h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, cr.Object, nil))
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRECONDITION: need a live handle and a non-empty BindingUID; handle=%v in=%v", h != nil, in)
	}
	return k
}

func k1214ServeWidget(cr *unstructured.Unstructured, srvURL string, ctx context.Context) *httptest.ResponseRecorder {
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: cr.DeepCopy()}
	})
	defer restore()
	h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: srvURL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
	return rec
}

// k1214CellsHoldingTheDial lists the class of every resident cell holding what a
// dial brought.
func k1214CellsHoldingTheDial(store *cache.ResolvedCacheStore) []string {
	var out []string
	for _, k := range store.KeysForTest() {
		e, ok := store.GetNoTouch(k)
		if !ok || e == nil {
			continue
		}
		if bytes.Contains(e.Raw(), []byte(k1214DialMark)) {
			class := "?"
			if e.Inputs != nil {
				class = e.Inputs.CacheEntryClass
			}
			out = append(out, class)
		}
	}
	return out
}

func TestIssue1214_Widgets_APutDeclinedOnAPerUserFallThroughKeepsAPartialSnapshotsSharedKeySafe(t *testing.T) {
	k423Env(t)
	for _, a := range []psArm{
		{name: "informer_servable", target: psConfigmapsGVR, watchShape: true},
		{name: "list_shape", target: psConfigmapsGVR, watchShape: false},
		{name: "informer_not_registered", target: psConfigmapsGVR, watchShape: true, noPrereg: true},
		{name: "user_bound", target: psConfigmapsGVR, watchShape: true, userBound: true},
	} {
		t.Run(a.name, func(t *testing.T) {
			cache.ResetPluralsStoreForTest()
			t.Cleanup(cache.ResetPluralsStoreForTest)
			crdschema.InvalidateCRDSchemaMemo()
			t.Cleanup(crdschema.InvalidateCRDSchemaMemo)
			cr := k1214Widget()
			psBuildWatcher(t, a, append(k423WidgetExtras(a), runtime.Object(psRACR(a)), runtime.Object(cr))...)
			if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
				select {
				case <-ch:
				case <-time.After(30 * time.Second):
					t.Fatal("PRECONDITION: the RESTAction informer did not sync")
				}
			}
			srv, wire := k1214RealRights(t, a)
			psSeedClientconfigs(t, srv.URL)
			aliceCtx, bobCtx := k1214WidgetCtx(a, psAlice, srv.URL), k1214WidgetCtx(a, psBob, srv.URL)

			if k1214WidgetKey(t, aliceCtx, cr) == k1214WidgetKey(t, bobCtx, cr) {
				t.Fatalf("PRECONDITION: alice and bob share a widget key on the complete snapshot")
			}
			k1214PublishPartial(t)
			key := k1214WidgetKey(t, aliceCtx, cr)
			if key != k1214WidgetKey(t, bobCtx, cr) {
				t.Fatalf("PRECONDITION: alice and bob must derive one widget key on the partial snapshot")
			}
			store := cache.ResolvedCache()
			if _, ok := store.GetNoTouch(key); ok {
				t.Fatalf("PRECONDITION: the shared key must be cold before alice's request")
			}

			alice := k1214ServeWidget(cr, srv.URL, aliceCtx)
			if alice.Code != http.StatusOK || !psHasSentinel(alice.Body.Bytes()) || wire.aliceOK.Load() == 0 {
				t.Fatalf("PRECONDITION: alice's own dial must bring the sentinel, or nothing here can leak (code=%d dials=%d body=%s)",
					alice.Code, wire.aliceOK.Load(), psTrunc(alice.Body.String(), 400))
			}
			if !bytes.Contains(alice.Body.Bytes(), []byte(k1214DialMark)) {
				t.Fatalf("PRECONDITION: alice's body must be what her dial brought, not the informer's object: %s", psTrunc(alice.Body.String(), 400))
			}
			// Not Fatal: with a decline gone the arm must go on and show the leak.
			if _, ok := store.GetNoTouch(key); ok {
				t.Errorf("RED (#1214): the shared widget key is resident: the per-user fall-through must decline its Put")
			}
			if held := k1214CellsHoldingTheDial(store); len(held) != 0 {
				t.Errorf("RED (#1214): alice's dial is in shared cell(s) %v: the per-user fall-through must decline every Put", held)
			}

			bob := k1214ServeWidget(cr, srv.URL, bobCtx)
			if psHasSentinel(bob.Body.Bytes()) {
				t.Fatalf("CROSS-IDENTITY SERVE (#1214): bob, who the apiserver refuses, was served alice's sentinel from a shared cell (bob's dials=%d, code=%d)",
					wire.bob403.Load(), bob.Code)
			}
			if wire.bob403.Load() == 0 {
				t.Fatalf("PRECONDITION: bob must have dialled as himself and been refused (403s=%d code=%d body=%s)",
					wire.bob403.Load(), bob.Code, psTrunc(bob.Body.String(), 400))
			}
			if wire.other.Load() != 0 {
				t.Fatalf("PRECONDITION: %d dial(s) under a credential that is neither user's: the stage must dial as the requester", wire.other.Load())
			}
		})
	}
}
