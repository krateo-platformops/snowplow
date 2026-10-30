// list_resolve_lists_988_test.go — #988 (#991 review finding 3, MY9): the
// /list handler's per-GVR LISTs are sent with the ctx RecordApiserverFallthrough
// returned, so the wire counter labels them client-build rather than
// unattributed. Driven over HTTP through the real /list route middleware and
// handlers.List() against the #517 fake apiserver.
package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/handlers"
)

func TestIssue988_List_PerGVRListsAreCountedAsClientBuild(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // else the /list scope (and the marker) is a no-op
	credhttp.ResetResolveCollectionListsForTest()
	t.Cleanup(credhttp.ResetResolveCollectionListsForTest)

	prev := xenv.TestMode()
	xenv.SetTestMode(true) // keep the injected ServerURL (plumbing rewrites it otherwise)
	f := &fakeAPI517{}
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})

	req := httptest.NewRequest(http.MethodGet, "/list?category="+cat517+"&ns="+ns517, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken("test-token-988"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL}),
	))
	rec := httptest.NewRecorder()
	cache.FallthroughScopeMiddleware(cache.ScopeList)(handlers.List()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PRECONDITION: /list answered %d: %s", rec.Code, rec.Body.String())
	}

	want := map[credhttp.ResolveListKey]uint64{
		{GVR: "/v1, Resource=configmaps", Reason: string(cache.ReasonClientBuild)}:                  1,
		{GVR: "good.example.io/v1, Resource=goodthings", Reason: string(cache.ReasonClientBuild)}:   1,
		{GVR: "stale.example.io/v1, Resource=stalethings", Reason: string(cache.ReasonClientBuild)}: 1,
	}
	got := credhttp.ResolveCollectionLists()
	if len(got) != len(want) {
		t.Fatalf("want the 3 per-GVR LISTs under client-build; got %v", got)
	}
	for k, n := range want {
		if got[k] != n {
			t.Fatalf("cell {%s|%s} = %d, want %d (all %v)", k.GVR, k.Reason, got[k], n, got)
		}
	}
}
