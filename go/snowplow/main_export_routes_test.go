package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestExport970_Routes — the REAL export route table (mountExportRoutes):
// GET /export and POST /export are both mounted, each through its lane
// (POST over the read-only /call/read lane: a resource no resolve handler
// serves reaches the apiserver as a GET), and both are registered as
// scoped routes, so the boot-assert (cache.AssertReadPathsScoped) holds with
// them and would report a lost registration.
func TestExport970_Routes(t *testing.T) {
	got, ep := fakeAPIMain443(t)
	cache.ResetRouteScopeRegistryForTest()
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)

	mux := http.NewServeMux()
	mountExportRoutes(mux, use.NewChain(), testUserConfig443(ep))

	target := "/export" + cmQ443 + "&format=json&page=1&perPage=5"
	get := httptest.NewRecorder()
	mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, target, nil))
	post := httptest.NewRecorder()
	mux.ServeHTTP(post, httptest.NewRequest(http.MethodPost, target, strings.NewReader(`{"extras":{}}`)))
	for name, rec := range map[string]*httptest.ResponseRecorder{"GET": get, "POST": post} {
		if rec.Code != http.StatusOK {
			t.Errorf("%s /export: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	for _, o := range got() {
		if o.method != http.MethodGet {
			t.Errorf("an export reached the apiserver as %s; both export lanes are reads", o.method)
		}
	}
	if len(got()) != 2 {
		t.Errorf("apiserver requests = %d, want one per export", len(got()))
	}

	// The boot-assert: register main's other required routes as main does,
	// then every required route must be present — the export routes
	// included, registered by mountExportRoutes alone.
	mountCallWriteRoutes(http.NewServeMux(), use.NewChain(), testUserConfig443(ep))
	cache.RegisterScopedRoute("GET /api-info/names", cache.ScopePlurals)
	cache.RegisterScopedRoute("GET /list", cache.ScopeList)
	cache.RegisterScopedRoute("GET /call", cache.ScopeCallGeneric)
	cache.RegisterScopedRoute("POST /call/read", cache.ScopeCallGeneric)
	prev := xenv.TestMode()
	xenv.SetTestMode(false) // count, don't panic
	defer xenv.SetTestMode(prev)
	if missing := cache.AssertReadPathsScoped(); missing != 0 {
		t.Errorf("required scoped routes missing: %d — mountExportRoutes must register GET and POST /export", missing)
	}
}
