package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/testsupport/pdfcheck"
)

// #970 — a pdf export through the REAL /call chain, composed exactly as
// main.go composes GET /export: Export(Dispatcher(dispatchers.All())(Call())).
// The apiserver is a fake that answers per bearer token, standing in for
// RBAC: the caller's own credential decides what the caller's /call returns.
// The RBAC arm: a row the caller's /call does not return never appears in
// the caller's PDF — while the control proves the same row IS exported for
// an identity whose /call returns it (the arm can fail).

// fakeRBACAPI serves the object at objPath, its items depending on the
// caller's token, and records the token of every request.
type fakeRBACAPI struct {
	mu     sync.Mutex
	tokens []string
}

const objPath970 = "/api/v1/namespaces/default/configmaps/inventory"

func (f *fakeRBACAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	f.mu.Lock()
	f.tokens = append(f.tokens, tok)
	f.mu.Unlock()
	if r.URL.Path != objPath970 {
		http.Error(w, `{"kind":"Status","code":404}`, http.StatusNotFound)
		return
	}
	items := `{"name":"visible-row","owner":"team-a"}`
	switch tok {
	case "token-admin":
		items += `,{"name":"restricted-row","owner":"team-secret"}`
	case "token-viewer":
	default:
		http.Error(w, `{"kind":"Status","code":401}`, http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"inventory","namespace":"default"},"items":[` + items + `]}`))
}

func exportChain() http.Handler {
	return handlers.Export(handlers.Dispatcher(dispatchers.All())(handlers.Call()))
}

func serveAs(t *testing.T, h http.Handler, srvURL, user, token, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken(token),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"devs"}}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srvURL, Token: token}),
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestExport970_RealCallChainRBAC(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(true) // keep the injected ServerURL
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	api := &fakeRBACAPI{}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	const q = "apiVersion=v1&resource=configmaps&namespace=default&name=inventory"
	h := exportChain()

	// The caller's own /call, through the same chain's /call handler.
	call := serveAs(t, handlers.Dispatcher(dispatchers.All())(handlers.Call()), srv.URL, "viewer", "token-viewer", "/call?"+q)
	if call.Code != http.StatusOK || strings.Contains(call.Body.String(), "restricted-row") {
		t.Fatalf("viewer /call: %d %s", call.Code, call.Body.String())
	}

	viewer := serveAs(t, h, srv.URL, "viewer", "token-viewer", "/export?"+q+"&format=pdf&path=.items&fields=name,owner")
	if viewer.Code != http.StatusOK || viewer.Header().Get("Content-Type") != "application/pdf" {
		t.Fatalf("viewer export: %d %s", viewer.Code, viewer.Body.String())
	}
	vd := pdfcheck.Parse(t, viewer.Body.Bytes())
	if !strings.Contains(vd.String(), "visible-row | team-a") {
		t.Errorf("viewer PDF lacks the row its /call returns:\n%s", vd)
	}
	if strings.Contains(vd.String(), "restricted-row") || strings.Contains(vd.String(), "team-secret") {
		t.Errorf("viewer PDF shows a row the viewer's /call does not return:\n%s", vd)
	}
	if !strings.Contains(vd.String(), "Rows: 1") {
		t.Errorf("viewer PDF row count is not its /call's 1:\n%s", vd)
	}

	// Control: the same export as an identity whose /call returns the row.
	admin := serveAs(t, h, srv.URL, "admin", "token-admin", "/export?"+q+"&format=pdf&path=.items&fields=name,owner")
	ad := pdfcheck.Parse(t, admin.Body.Bytes())
	if !strings.Contains(ad.String(), "restricted-row | team-secret") || !strings.Contains(ad.String(), "Rows: 2") {
		t.Errorf("control: the admin PDF must show the row admin's /call returns:\n%s", ad)
	}

	// Every apiserver request carried the caller's own credential.
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.tokens) < 3 {
		t.Fatalf("the apiserver saw %d requests: the exports did not go through the /call chain", len(api.tokens))
	}
	for i, tok := range api.tokens {
		if tok != "token-viewer" && tok != "token-admin" {
			t.Errorf("apiserver request %d carried token %q, not the caller's", i, tok)
		}
	}
}
