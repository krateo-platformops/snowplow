package audit

// request_1251_test.go — AUDIT-3 (#1251): every record carries what snowplow
// received with the request it was made on: the source IPs (the apiserver's
// audit semantics), the User-Agent and the request URI, read once by Middleware.
//
//   S  source_ips: X-Forwarded-For entries, then X-Real-Ip, then the TCP peer,
//      de-duplicated; the peer is always last, whatever the headers claim.
//   M  Middleware -> Emit stamps all three, typed; the URI keeps its query.
//   E  the facts are read at the edge: a handler that rewrites the request
//      afterwards does not change the record.
//   A  a record on a context that no request went through carries none, and
//      an absent User-Agent writes no attribute.

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log/logtest"
)

func TestRequest1251_S_SourceIPsKeepTheApiserverOrderAndThePeerLast(t *testing.T) {
	const peer = "10.0.0.9"
	cases := []struct {
		name   string
		remote string
		xff    []string // each element is one X-Forwarded-For header line
		realIP string
		want   []string
	}{
		{"no forwarding header", peer + ":41000", nil, "", []string{peer}},
		{"one entry", peer + ":41000", []string{"203.0.113.7"}, "", []string{"203.0.113.7", peer}},
		{"several entries", peer + ":41000", []string{"203.0.113.7,198.51.100.2,192.0.2.1"}, "",
			[]string{"203.0.113.7", "198.51.100.2", "192.0.2.1", peer}},
		{"entries with spaces", peer + ":41000", []string{" 203.0.113.7 ,  198.51.100.2"}, "",
			[]string{"203.0.113.7", "198.51.100.2", peer}},
		{"a malformed entry is dropped", peer + ":41000", []string{"203.0.113.7, not-an-ip, 999.1.1.1, ,198.51.100.2"}, "",
			[]string{"203.0.113.7", "198.51.100.2", peer}},
		{"only malformed entries", peer + ":41000", []string{"garbage"}, "", []string{peer}},
		{"X-Real-Ip after the chain", peer + ":41000", []string{"203.0.113.7"}, "198.51.100.5",
			[]string{"203.0.113.7", "198.51.100.5", peer}},
		{"X-Real-Ip already in the chain", peer + ":41000", []string{"203.0.113.7"}, "203.0.113.7",
			[]string{"203.0.113.7", peer}},
		{"peer equal to the last entry is not repeated", peer + ":41000", []string{"203.0.113.7, " + peer}, "",
			[]string{"203.0.113.7", peer}},
		// A forged chain that names the peer FIRST cannot move it: the peer is
		// still appended, last.
		{"a chain that claims the peer first", peer + ":41000", []string{peer + ", 203.0.113.7"}, "",
			[]string{peer, "203.0.113.7", peer}},
		{"IPv6 peer", "[2001:db8::1]:41000", []string{"203.0.113.7"}, "", []string{"203.0.113.7", "2001:db8::1"}},
		// Only the first X-Forwarded-For line is read (Header.Get), as the
		// apiserver reads it.
		{"two header lines", peer + ":41000", []string{"203.0.113.7", "198.51.100.2"}, "",
			[]string{"203.0.113.7", peer}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/call", nil)
			req.RemoteAddr = c.remote
			for _, v := range c.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if c.realIP != "" {
				req.Header.Set("X-Real-Ip", c.realIP)
			}
			got := requestFactsOf(req).sourceIPs
			if !slices.Equal(got, c.want) {
				t.Fatalf("source_ips = %q, want %q", got, c.want)
			}
		})
	}
}

// serveAndEmit sends req through Middleware to a handler that emits ev (after
// mutate, if any, has had its way with the request) and returns the record.
func serveAndEmit(t *testing.T, req *http.Request, mutate func(*http.Request)) map[string]attribute.Value {
	t.Helper()
	rec := logtest.NewRecorder()
	e := New(rec.Logger("test-1251"))
	h := Middleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if mutate != nil {
			mutate(r)
		}
		e.Emit(r.Context(), Event{Action: "call", Verb: http.MethodPost, Resource: "configmaps", Outcome: "success"})
	}))
	req = req.WithContext(xcontext.BuildContext(req.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}})))
	h.ServeHTTP(httptest.NewRecorder(), req)
	return attrsOf957(t, rec)
}

func TestRequest1251_M_MiddlewareStampsEveryFactOnTheRecord(t *testing.T) {
	const uri = "/call/dry-run?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x&note=a%20b"
	req := httptest.NewRequest(http.MethodPost, uri, nil)
	req.RemoteAddr = "10.0.0.9:41000"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("User-Agent", "Mozilla/5.0 (sentinel-ua-1251)")
	got := serveAndEmit(t, req, nil)

	ips := got[AttrSourceIPs]
	if ips.Type() != attribute.STRINGSLICE || !slices.Equal(ips.AsStringSlice(), []string{"203.0.113.7", "10.0.0.9"}) {
		t.Errorf("%s = %v (%v), want the string slice [203.0.113.7 10.0.0.9]", AttrSourceIPs, ips, ips.Type())
	}
	if v := got[AttrUserAgent]; v.AsString() != "Mozilla/5.0 (sentinel-ua-1251)" {
		t.Errorf("%s = %q, want the header as received", AttrUserAgent, v.AsString())
	}
	if v := got[AttrRequestURI]; v.AsString() != uri {
		t.Errorf("%s = %q, want the request target as received %q", AttrRequestURI, v.AsString(), uri)
	}
}

func TestRequest1251_E_TheFactsAreReadAtTheEdge(t *testing.T) {
	const uri = "/export?format=csv&name=demo"
	req := httptest.NewRequest(http.MethodGet, uri, nil)
	req.RemoteAddr = "10.0.0.9:41000"
	req.Header.Set("User-Agent", "edge-ua")
	got := serveAndEmit(t, req, func(r *http.Request) {
		// What a later handler may do to the request it was handed: rewrite
		// the URL, the headers and the peer. The record is of what was received.
		r.URL.RawQuery = "rewritten=1"
		r.RequestURI = "/rewritten"
		r.Header.Set("User-Agent", "rewritten-ua")
		r.Header.Set("X-Forwarded-For", "192.0.2.66")
		r.RemoteAddr = "192.0.2.77:1"
	})
	if v := got[AttrRequestURI].AsString(); v != uri {
		t.Errorf("%s = %q, want %q", AttrRequestURI, v, uri)
	}
	if v := got[AttrUserAgent].AsString(); v != "edge-ua" {
		t.Errorf("%s = %q, want edge-ua", AttrUserAgent, v)
	}
	if v := got[AttrSourceIPs].AsStringSlice(); !slices.Equal(v, []string{"10.0.0.9"}) {
		t.Errorf("%s = %q, want [10.0.0.9]", AttrSourceIPs, v)
	}
}

// A request built in process, never read off a connection, has no RequestURI:
// its URL is the request target.
func TestRequest1251_M_InProcessRequestFallsBackToItsURL(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://snowplow/export?format=json&name=demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if req.RequestURI != "" {
		t.Fatalf("precondition: a client request has RequestURI %q", req.RequestURI)
	}
	if got := requestFactsOf(req).requestURI; got != "/export?format=json&name=demo" {
		t.Fatalf("requestURI = %q, want the URL's path and query", got)
	}
}

func TestRequest1251_A_NoRequestNoFacts(t *testing.T) {
	// A context no request went through.
	rec := logtest.NewRecorder()
	New(rec.Logger("test-1251")).Emit(actorCtx(), Event{Action: "call", Verb: http.MethodPost, Resource: "configmaps", Outcome: "success"})
	got := attrsOf957(t, rec)
	for _, k := range []string{AttrSourceIPs, AttrUserAgent, AttrRequestURI} {
		if v, ok := got[k]; ok {
			t.Errorf("%s = %v on a record made with no request, want absent", k, v)
		}
	}
	// A request with no User-Agent: no attribute, the others still present.
	req := httptest.NewRequest(http.MethodPost, "/call?name=x", nil)
	req.Header.Del("User-Agent")
	got = serveAndEmit(t, req, nil)
	if v, ok := got[AttrUserAgent]; ok {
		t.Errorf("%s = %v with no User-Agent header, want absent", AttrUserAgent, v)
	}
	if _, ok := got[AttrRequestURI]; !ok {
		t.Errorf("CONTROL: %s missing on the same record", AttrRequestURI)
	}
	if _, ok := got[AttrSourceIPs]; !ok {
		t.Errorf("CONTROL: %s missing on the same record (httptest's peer is 192.0.2.1)", AttrSourceIPs)
	}
}
