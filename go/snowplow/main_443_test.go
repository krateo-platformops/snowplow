// main_443_test.go — #443 arms that live next to production wiring:
//
//   - TestS443_Capabilities/c_CORSExposed: the browser can READ every #443
//     echo header and Warning off a cross-origin reply (through the real
//     use.CORS(snowplowCORSOptions())).
//   - TestS443_Capabilities/d_FailClosedRoute: an OLD snowplow's route table
//     (the pre-#443 write patterns only) answers POST /call/dry-run with 404
//     and makes zero apiserver calls. This is GREEN on main by design; it pins
//     the property the separate route exists for: during a rolling deploy a
//     dry run that lands on an old pod writes nothing.
//   - TestS443_Routes: the REAL route table (mountCallWriteRoutes) sends
//     POST|PUT|PATCH /call/dry-run to the dry-run handler (outbound
//     dryRun=All), has no DELETE /call/dry-run, and rejects
//     POST /call?dryRun=All with zero outbound requests.
package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/handlers"
)

type outbound443 struct{ method, rawQuery string }

// fakeAPIMain443 records every request a handler sends to the apiserver.
func fakeAPIMain443(t *testing.T) (func() []outbound443, endpoints.Endpoint) {
	t.Helper()
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	var mu sync.Mutex
	var got []outbound443
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, outbound443{r.Method, r.URL.RawQuery})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap"}`))
	}))
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})
	return func() []outbound443 {
		mu.Lock()
		defer mu.Unlock()
		return append([]outbound443(nil), got...)
	}, endpoints.Endpoint{ServerURL: srv.URL}
}

// testUserConfig443 stands in for middleware.UserConfig: it attaches the
// caller identity and endpoint instead of verifying a JWT and reading the
// caller's clientconfig Secret.
func testUserConfig443(ep endpoints.Endpoint) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := xcontext.BuildContext(r.Context(),
				xcontext.WithAccessToken("test-token"),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
				xcontext.WithUserConfig(ep),
			)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

const cm443 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default"}}`
const cmQ443 = "?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"

func do443(h http.Handler, method, target string) *httptest.ResponseRecorder {
	var body io.Reader = bytes.NewReader([]byte(cm443))
	req := httptest.NewRequest(method, target, body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestS443_Capabilities(t *testing.T) {
	t.Run("c_CORSExposed", func(t *testing.T) {
		hdr := serveCORS(t, snowplowCORSOptions())
		exposed := exposeList(hdr.Get("Access-Control-Expose-Headers"))
		for _, want := range []string{
			"X-Snowplow-Dry-Run", "X-Snowplow-Resolve-Source", "X-Snowplow-Raw",
			"X-Snowplow-Field-Validation", "Warning",
			// the pre-#443 set must survive the change
			"X-Snowplow-Refresh-Key", "X-Snowplow-Refresh-Class", "Link",
		} {
			if !exposed[strings.ToLower(want)] {
				t.Errorf("Access-Control-Expose-Headers must contain %q; got %q", want, hdr.Get("Access-Control-Expose-Headers"))
			}
		}
	})

	t.Run("d_FailClosedRoute", func(t *testing.T) {
		got, ep := fakeAPIMain443(t)
		// The route table of a pre-#443 snowplow: the four write patterns on
		// /call, nothing under /call/dry-run.
		old := http.NewServeMux()
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			old.Handle(m+" /call", testUserConfig443(ep)(handlers.Call()))
		}
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
			rec := do443(old, m, "/call/dry-run"+cmQ443+"&dryRun=All")
			if rec.Code != http.StatusNotFound {
				t.Errorf("old route table: %s /call/dry-run → %d, want 404", m, rec.Code)
			}
		}
		if n := len(got()); n != 0 {
			t.Fatalf("old route table: %d apiserver requests for a dry run, want 0 — a dry run on an old pod must write nothing", n)
		}
	})
}

func TestS443_Routes(t *testing.T) {
	got, ep := fakeAPIMain443(t)
	mux := http.NewServeMux()
	mountCallWriteRoutes(mux, use.NewChain(), testUserConfig443(ep))

	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		before := len(got())
		rec := do443(mux, m, "/call/dry-run"+cmQ443)
		after := got()
		if rec.Code != http.StatusOK || len(after) != before+1 {
			t.Fatalf("%s /call/dry-run: code=%d outbound=%d", m, rec.Code, len(after)-before)
		}
		last := after[len(after)-1]
		if last.method != m || !strings.Contains(last.rawQuery, "dryRun=All") {
			t.Errorf("%s /call/dry-run: outbound %s ?%s, want %s with dryRun=All", m, last.method, last.rawQuery, m)
		}
		if rec.Header().Get(handlers.HeaderDryRun) != "All" {
			t.Errorf("%s /call/dry-run: missing %s echo", m, handlers.HeaderDryRun)
		}
	}

	before := len(got())
	if rec := do443(mux, http.MethodDelete, "/call/dry-run"+cmQ443); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /call/dry-run: code=%d, want 405 (no dry-run DELETE route)", rec.Code)
	}
	if rec := do443(mux, http.MethodPost, "/call"+cmQ443+"&dryRun=All"); rec.Code != http.StatusBadRequest {
		t.Errorf("POST /call?dryRun=All: code=%d, want 400", rec.Code)
	}
	if n := len(got()) - before; n != 0 {
		t.Errorf("%d apiserver requests for a DELETE dry run / a plain-/call dryRun, want 0", n)
	}

	// The plain write route still writes (the arm-that-cannot-fail control:
	// the fake does see a real create, without dryRun).
	if rec := do443(mux, http.MethodPost, "/call"+cmQ443); rec.Code != http.StatusOK {
		t.Fatalf("POST /call control: code=%d", rec.Code)
	}
	after := got()
	if last := after[len(after)-1]; last.method != http.MethodPost || strings.Contains(last.rawQuery, "dryRun") {
		t.Errorf("POST /call control: outbound %s ?%s, want a plain POST", last.method, last.rawQuery)
	}
}
