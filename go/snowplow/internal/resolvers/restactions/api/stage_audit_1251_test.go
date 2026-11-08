package api

// stage_audit_1251_test.go — AUDIT-3 (#1251) at the third emit site: a
// RESTAction write stage, driven through the REAL api.Resolve on the context a
// request leaves after audit.Middleware. Its record carries the facts of the
// request snowplow RECEIVED (the /call that caused the stage), never the
// stage's own outbound path, which can be a secret (AUDIT-4).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// requestCtx1251 is the context audit.Middleware hands the handler for req,
// with the requester's identity on it, as UserConfig adds it.
func requestCtx1251(t *testing.T, req *http.Request) context.Context {
	t.Helper()
	var ctx context.Context
	audit.Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ctx = r.Context()
	})).ServeHTTP(httptest.NewRecorder(), req)
	if ctx == nil {
		t.Fatal("precondition: the middleware did not call the handler")
	}
	return xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: user410, Groups: []string{group410}}))
}

func TestStageAudit1251_StageRecordsCarryTheReceivedRequestNotTheStagePath(t *testing.T) {
	api, _ := upstream410(t)
	ext, extDials := upstream410(t)
	env410(t, api.URL, ext.URL)
	rec := records410(t)

	const inbound = "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=" + raNS410 + "&name=" + raName410
	req := httptest.NewRequest(http.MethodGet, inbound, nil)
	req.RemoteAddr = "10.12.0.31:52344"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("User-Agent", "sentinel-ua-1251")

	const base = "/apis/composition.krateo.io/v1alpha1/namespaces/team-a/fireworksapps"
	resolve410(requestCtx1251(t, req), api.URL,
		stage410("status", http.MethodPut, base+"/demo/status"),
		extStage410("notify", http.MethodPost, "/services/T000/B000/"+pathCanary410))

	if len(extDials()) != 1 {
		t.Fatalf("precondition: the webhook stage was not dialled: %v", extDials())
	}
	got := attrs410(rec)
	if len(got) != 2 {
		t.Fatalf("records = %d, want 2 (one per write stage): %v", len(got), got)
	}
	for _, r := range got {
		stage := r["krateo.restaction.stage"]
		if r[audit.AttrRequestURI] != inbound {
			t.Errorf("stage %s: %s = %q, want the received request %q", stage, audit.AttrRequestURI, r[audit.AttrRequestURI], inbound)
		}
		if r[audit.AttrUserAgent] != "sentinel-ua-1251" {
			t.Errorf("stage %s: %s = %q", stage, audit.AttrUserAgent, r[audit.AttrUserAgent])
		}
		ips := r[audit.AttrSourceIPs]
		if !strings.Contains(ips, "203.0.113.7") || !strings.HasSuffix(strings.TrimRight(ips, `"]`), "10.12.0.31") {
			t.Errorf("stage %s: %s = %s, want [203.0.113.7 10.12.0.31], the peer last", stage, audit.AttrSourceIPs, ips)
		}
		for k, v := range r {
			if strings.Contains(v, pathCanary410) || strings.Contains(v, "/services/") || strings.Contains(v, "/demo/status") {
				t.Errorf("stage %s: attribute %s carries the stage's outbound path: %q", stage, k, v)
			}
		}
	}
}
