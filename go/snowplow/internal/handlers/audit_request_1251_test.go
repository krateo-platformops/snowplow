package handlers

// audit_request_1251_test.go — AUDIT-3 (#1251) at the /call and /export emit
// sites, through the REAL handlers behind audit.Middleware, as the pod mounts
// them (newRequestChain), against a fake apiserver.
//
// One arm per shape of the post-release probe on krateo-057 (#1251, final
// plan): A dry-run POST; B, C, D dry-run PATCH, PATCH of the status
// subresource and PUT of a name that does not exist (a 404 at the dial); E, F
// GET and POST exports, each sent through the gateway (a forwarded chain, the
// gateway as the TCP peer); G an A sent directly (no forwarding header). Plus a
// 403 and a 404 at the dial on a real write, the uid control, and the exits
// before the dial (400, 401), which emit nothing. Each record must carry:
//
//   S  krateo.request.source_ips: the forwarded entries, then the peer, LAST;
//   U  user_agent.original: the header as received;
//   R  krateo.request.uri: the request target as received, query included;
//   O  k8s.resource.subresource=status on the status shape and on no other;
//      k8s.object.uid only where #957 records one (a successful real write,
//      from the response), never on a dry run, a failure or an export, though
//      the apiserver's answer carries a uid in every one of these arms.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/logtest"
)

const (
	gatewayPeer1251 = "10.12.0.31" // the agentgateway hop, the TCP peer
	browser1251     = "203.0.113.7"
	ua1251          = "Mozilla/5.0 (X11; Linux x86_64) sentinel-ua-1251"
)

// typedOne1251 is the only record rec holds, values typed.
func typedOne1251(t *testing.T, rec *logtest.Recorder) map[string]attribute.Value {
	t.Helper()
	var out []map[string]attribute.Value
	for _, recs := range rec.Result() {
		for _, r := range recs {
			m := map[string]attribute.Value{}
			for _, kv := range r.Attributes {
				m[string(kv.Key)] = kv.Value
			}
			out = append(out, m)
		}
	}
	if len(out) != 1 {
		t.Fatalf("records = %d, want 1", len(out))
	}
	return out[0]
}

// serve1251 sends method target (+ body) as alice, from peer, with the given
// X-Forwarded-For ("" = none), through audit.Middleware into h.
func serve1251(h http.Handler, ep endpoints.Endpoint, method, target, body, peer, xff string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := withCaller443(httptest.NewRequest(method, target, rdr), ep)
	req.RemoteAddr = peer + ":52344"
	req.Header.Set("User-Agent", ua1251)
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	audit.Middleware()(h).ServeHTTP(w, req)
	return w
}

func TestCall1251_EveryMeasuredShapeCarriesTheRequestFacts(t *testing.T) {
	const statusQ = cmQuery443 + "&subresource=status"
	cases := []struct {
		name    string
		h       func() http.Handler
		method  string
		target  string
		body    string
		answer  int
		xff     string // "" = sent directly
		action  string
		subres  string
		wantUID string // "" = absent
	}{
		// A: a dry-run create, answered by the apiserver.
		{"A dry-run POST", CallDryRun, http.MethodPost, "/call/dry-run?" + cmQuery443, cmRequest957, http.StatusCreated,
			browser1251, "call.dryRun", "", ""},
		// B, C, D target a name that does not exist: the 404 comes from the
		// dial, so the request still reaches the emit, as a failure.
		{"B dry-run PATCH, 404 at the dial", CallDryRun, http.MethodPatch, "/call/dry-run?" + cmQuery443, cmRequest957, http.StatusNotFound,
			browser1251, "call.dryRun", "", ""},
		{"C dry-run PATCH status, 404 at the dial", CallDryRun, http.MethodPatch, "/call/dry-run?" + statusQ, cmRequest957, http.StatusNotFound,
			browser1251, "call.dryRun", "status", ""},
		{"D dry-run PUT, 404 at the dial", CallDryRun, http.MethodPut, "/call/dry-run?" + cmQuery443, cmRequest957, http.StatusNotFound,
			browser1251, "call.dryRun", "", ""},
		// G: A sent directly, with no forwarding header: the peer alone.
		{"G dry-run POST sent directly", CallDryRun, http.MethodPost, "/call/dry-run?" + cmQuery443, cmRequest957, http.StatusCreated,
			"", "call.dryRun", "", ""},
		// A refused write: a failure record with every fact.
		{"real PATCH status, 403 at the dial", Call, http.MethodPatch, "/call?" + statusQ, cmRequest957, http.StatusForbidden,
			browser1251, "call", "status", ""},
		{"real DELETE, 404 at the dial", Call, http.MethodDelete, "/call?" + cmQuery443, "", http.StatusNotFound,
			browser1251, "call", "", ""},
		// The uid control: a real write that succeeds records the response's uid.
		{"real PUT (uid control)", Call, http.MethodPut, "/call?" + cmQuery443, cmRequest957, http.StatusOK,
			browser1251, "call", "", "uid-resp-957"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api, ep := newFakeAPI443(t)
			answer := cmResponse957
			switch c.answer {
			case http.StatusNotFound:
				answer = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"NotFound",` +
					`"code":404,"details":{"name":"cm-x","kind":"configmaps","uid":"uid-resp-957"}}`
			case http.StatusForbidden:
				answer = cmForbidden957
			}
			api.respond(c.answer, answer)
			rec := recorder957(t)

			w := serve1251(c.h(), ep, c.method, c.target, c.body, gatewayPeer1251, c.xff)
			if len(api.requests()) != 1 {
				t.Fatalf("precondition: the apiserver got %d requests, want 1 (code %d: %s)", len(api.requests()), w.Code, w.Body.String())
			}
			got := typedOne1251(t, rec)
			if a := got["krateo.action"].AsString(); a != c.action {
				t.Fatalf("precondition: krateo.action = %q, want %q", a, c.action)
			}
			wantOutcome := "success"
			if c.answer >= 300 {
				wantOutcome = "failure"
			}
			if o := got["outcome"].AsString(); o != wantOutcome {
				t.Fatalf("precondition: outcome = %q, want %q", o, wantOutcome)
			}
			if code := got["http.response.status_code"].AsInt64(); c.answer >= 300 && code != int64(c.answer) {
				t.Errorf("http.response.status_code = %d, want the dial's %d", code, c.answer)
			}
			wantIPs := []string{gatewayPeer1251}
			if c.xff != "" {
				wantIPs = []string{c.xff, gatewayPeer1251}
			}
			wantFacts1251(t, got, wantIPs, c.target)
			if v := got["k8s.resource.subresource"].AsString(); v != c.subres {
				t.Errorf("k8s.resource.subresource = %q, want %q", v, c.subres)
			}
			uid, has := got["k8s.object.uid"]
			switch {
			case c.wantUID == "" && has:
				t.Errorf("k8s.object.uid = %q, want absent (the answer carried one; this write recorded no object)", uid.AsString())
			case c.wantUID != "" && uid.AsString() != c.wantUID:
				t.Errorf("k8s.object.uid = %q, want the response's %q", uid.AsString(), c.wantUID)
			}
		})
	}
}

func TestExport1251_GETAndPOSTCarryTheRequestFacts(t *testing.T) {
	for _, c := range []struct {
		method string
		h      func() http.Handler
		body   string
	}{
		{http.MethodGet, func() http.Handler { return Export(Call()) }, ""},
		{http.MethodPost, ExportPOST, `{"extras":{}}`},
	} {
		t.Run(c.method, func(t *testing.T) {
			api, ep := newFakeAPI443(t)
			// The envelope names an object with a uid: an export is a read and
			// records none (#957), so its uid must not reach the record.
			api.respond(http.StatusOK, body1055)
			rec := recorder957(t)

			target := "/export?format=csv&" + target1055 + "&filename=team%20report"
			w := serve1251(c.h(), ep, c.method, target, c.body, gatewayPeer1251, browser1251)
			if w.Code != http.StatusOK {
				t.Fatalf("%s /export → %d: %s", c.method, w.Code, w.Body.String())
			}
			got := typedOne1251(t, rec)
			if a := got["krateo.action"].AsString(); a != "export" {
				t.Fatalf("precondition: krateo.action = %q", a)
			}
			if m := got["http.request.method"].AsString(); m != c.method {
				t.Fatalf("precondition: http.request.method = %q, want %q", m, c.method)
			}
			wantFacts1251(t, got, []string{browser1251, gatewayPeer1251}, target)
			if v, has := got["k8s.object.uid"]; has {
				t.Errorf("k8s.object.uid = %q on an export, want absent", v.AsString())
			}
			if v, has := got["k8s.resource.subresource"]; has {
				t.Errorf("k8s.resource.subresource = %q on a whole-object export, want absent", v.AsString())
			}
		})
	}
}

// The export's subresource reaches the record as the /call lane reads it.
func TestExport1251_SubresourceReachesTheRecord(t *testing.T) {
	api, ep := newFakeAPI443(t)
	api.respond(http.StatusOK, body1055)
	rec := recorder957(t)
	target := "/export?format=json&" + target1055 + "&subresource=status"
	if w := serve1251(Export(Call()), ep, http.MethodGet, target, "", gatewayPeer1251, ""); w.Code != http.StatusOK {
		t.Fatalf("GET /export → %d: %s", w.Code, w.Body.String())
	}
	got := typedOne1251(t, rec)
	if v := got["k8s.resource.subresource"].AsString(); v != "status" {
		t.Errorf("k8s.resource.subresource = %q, want status", v)
	}
	wantFacts1251(t, got, []string{gatewayPeer1251}, target)
}

func wantFacts1251(t *testing.T, got map[string]attribute.Value, ips []string, uri string) {
	t.Helper()
	src := got[audit.AttrSourceIPs]
	if src.Type() != attribute.STRINGSLICE || !slices.Equal(src.AsStringSlice(), ips) {
		t.Errorf("%s = %v, want %q", audit.AttrSourceIPs, src.AsStringSlice(), ips)
	}
	if s := src.AsStringSlice(); len(s) == 0 || s[len(s)-1] != gatewayPeer1251 {
		t.Errorf("%s = %q: the last entry must be the TCP peer %s", audit.AttrSourceIPs, s, gatewayPeer1251)
	}
	if v := got[audit.AttrUserAgent].AsString(); v != ua1251 {
		t.Errorf("%s = %q, want %q", audit.AttrUserAgent, v, ua1251)
	}
	if v := got[audit.AttrRequestURI].AsString(); v != uri {
		t.Errorf("%s = %q, want the request target %q", audit.AttrRequestURI, v, uri)
	}
}

// The exits before the dial — a request that does not validate (400) and one
// with no user endpoint (401) — emit no record: nothing was sent. The control
// in the same arm is a write that reaches the dial.
func TestCall1251_ExitsBeforeTheDialEmitNothing(t *testing.T) {
	api, ep := newFakeAPI443(t)
	api.respond(http.StatusNotFound, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)
	rec := recorder957(t)
	for _, c := range []struct {
		name   string
		h      http.Handler
		method string
		target string
	}{
		{"400 /call/dry-run without a name", CallDryRun(), http.MethodPatch, "/call/dry-run?apiVersion=v1&resource=configmaps&namespace=default"},
		{"400 /call without a name", Call(), http.MethodPut, "/call?apiVersion=v1&resource=configmaps&namespace=default"},
	} {
		if w := serve1251(c.h, ep, c.method, c.target, cmRequest957, gatewayPeer1251, browser1251); w.Code != http.StatusBadRequest {
			t.Fatalf("%s: code %d, want 400", c.name, w.Code)
		}
	}
	// 401: the caller is known (an actor is on the context, so the emitter
	// would not refuse a record) but no user endpoint is.
	req := httptest.NewRequest(http.MethodPatch, "/call/dry-run?"+cmQuery443, bytes.NewReader([]byte(cmRequest957)))
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}})))
	w := httptest.NewRecorder()
	audit.Middleware()(CallDryRun()).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no user endpoint: code %d, want 401", w.Code)
	}
	if n := len(records957(rec)); n != 0 {
		t.Fatalf("the exits before the dial emitted %d records, want 0", n)
	}
	if n := len(api.requests()); n != 0 {
		t.Fatalf("precondition: %d requests reached the apiserver before a 400/401", n)
	}
	// Control: the same handler, a request that reaches the dial.
	if w := serve1251(CallDryRun(), ep, http.MethodPatch, "/call/dry-run?"+cmQuery443, cmRequest957, gatewayPeer1251, browser1251); w.Code != http.StatusNotFound {
		t.Fatalf("control: code %d, want the dial's 404", w.Code)
	}
	wantFacts1251(t, typedOne1251(t, rec), []string{browser1251, gatewayPeer1251}, "/call/dry-run?"+cmQuery443)
}
