package main

// main_cors_provenance_1333_test.go — #1333: the portal sends
// X-Krateo-Provenance on its cross-origin /call writes and dry runs, so their
// preflight must allow it, through the production CORS wrapper
// (use.CORS(snowplowCORSOptions())), or the browser never sends the write.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func TestS1333_CORSPreflightAllowsProvenance(t *testing.T) {
	reached := false
	h := use.CORS(snowplowCORSOptions())(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/call"}, {http.MethodPut, "/call"}, {http.MethodPatch, "/call"}, {http.MethodDelete, "/call"},
		{http.MethodPost, "/call/dry-run"}, {http.MethodPut, "/call/dry-run"}, {http.MethodPatch, "/call/dry-run"},
	} {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			reached = false
			req := httptest.NewRequest(http.MethodOptions, c.path, nil)
			req.Header.Set("Origin", "https://portal.example")
			req.Header.Set("Access-Control-Request-Method", c.method)
			req.Header.Set("Access-Control-Request-Headers", "authorization,content-type,x-krateo-provenance")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code/100 != 2 || rec.Header().Get("Access-Control-Allow-Origin") == "" {
				t.Fatalf("preflight %d, Allow-Origin %q: the browser would block the write",
					rec.Code, rec.Header().Get("Access-Control-Allow-Origin"))
			}
			allowed := map[string]bool{}
			for _, v := range rec.Header().Values("Access-Control-Allow-Headers") {
				for _, h := range strings.Split(v, ",") {
					allowed[strings.ToLower(strings.TrimSpace(h))] = true
				}
			}
			if !allowed[strings.ToLower(audit.ProvenanceHeader)] {
				t.Errorf("RED #1333: preflight does not allow %s (Access-Control-Allow-Headers=%q)",
					audit.ProvenanceHeader, rec.Header().Values("Access-Control-Allow-Headers"))
			}
			if reached {
				t.Error("a preflight reached the handler")
			}
		})
	}
}
