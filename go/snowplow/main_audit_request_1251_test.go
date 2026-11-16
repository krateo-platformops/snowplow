// main_audit_request_1251_test.go — AUDIT-3 (#1251): a request whose query and
// User-Agent carry a person's identity puts it in the audit record, which holds
// identity on purpose, and on no log line (#1192).
//
// PRODUCTION FUNCTIONS UNDER TEST: newServerHandler, newRequestChain,
// buildLogHandler and mountCallWriteRoutes, as main() composes them, through
// both log handlers at DEBUG, installed as the request logger AND as
// slog.Default (main() does both). Only UserConfig is a stand-in (testUserConfig443):
// the real one needs a cluster for the clientconfig Secret.
package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
)

const sentinel1251 = "sentinel-owner-zq1251@tenant.example"

func TestS1251_RequestFactsReachTheAuditRecordAndNoLogLine(t *testing.T) {
	for _, h := range handlers1192 {
		t.Run(h.name, func(t *testing.T) {
			rec := logtest.NewRecorder()
			audit.SetDefault(audit.New(rec.Logger("test-1251")))
			t.Cleanup(func() { audit.SetDefault(nil) })
			got, ep := fakeAPIMain443(t)

			capture := &lockedBuffer{}
			log := slog.New(buildLogHandler(h.pretty, slog.LevelDebug, capture))
			// main() installs the same logger as slog.Default (slog.SetDefault):
			// a line written through the default logger, not the request's, must
			// land in the capture too. The audit package has no request logger,
			// so the default one is where a leak from it would go.
			prev := slog.Default()
			slog.SetDefault(log)
			t.Cleanup(func() { slog.SetDefault(prev) })
			mux := http.NewServeMux()
			mountCallWriteRoutes(newRouteTable(mux), newRequestChain(log), testUserConfig443(ep))
			srv := newServerHandler(mux)

			target := "/call/dry-run" + cmQ443 + "&owner=" + url.QueryEscape(sentinel1251)
			req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader([]byte(cm443)))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("User-Agent", "portal/1.0 ("+sentinel1251+")")
			req.Header.Set("X-Forwarded-For", "203.0.113.7")
			req.RemoteAddr = "10.12.0.31:52344"
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)
			if w.Code != http.StatusOK || len(got()) != 1 {
				t.Fatalf("precondition: POST /call/dry-run → %d with %d apiserver requests: %s", w.Code, len(got()), w.Body.String())
			}

			// The record holds the facts, identity included.
			var recs []map[string]string
			for _, rs := range rec.Result() {
				for _, r := range rs {
					m := map[string]string{}
					for _, kv := range r.Attributes {
						m[string(kv.Key)] = kv.Value.String()
					}
					recs = append(recs, m)
				}
			}
			if len(recs) != 1 {
				t.Fatalf("records = %d, want 1", len(recs))
			}
			if v := recs[0][audit.AttrRequestURI]; v != target || !strings.Contains(v, url.QueryEscape(sentinel1251)) {
				t.Errorf("%s = %q, want the received target %q", audit.AttrRequestURI, v, target)
			}
			if v := recs[0][audit.AttrUserAgent]; !strings.Contains(v, sentinel1251) {
				t.Errorf("%s = %q, want the received User-Agent", audit.AttrUserAgent, v)
			}
			if v := recs[0][audit.AttrSourceIPs]; !strings.Contains(v, "203.0.113.7") || !strings.Contains(v, "10.12.0.31") {
				t.Errorf("%s = %q, want the chain and the peer", audit.AttrSourceIPs, v)
			}

			// No log line holds any of it.
			out := capture.String()
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if strings.TrimSpace(out) == "" {
				t.Fatal("CONTROL: the request wrote no log line, so the absence below proves nothing")
			}
			for _, tok := range []string{sentinel1251, url.QueryEscape(sentinel1251), "owner=", "portal/1.0", "203.0.113.7", "10.12.0.31"} {
				for _, line := range lines {
					if strings.Contains(line, tok) {
						t.Errorf("a log line carries %q: %s", tok, line)
					}
				}
			}
			if !strings.Contains(out, "endpoint call done") {
				t.Errorf("CONTROL: the handler's own line is not in the capture; lines=%v", lines)
			}
		})
	}
}
