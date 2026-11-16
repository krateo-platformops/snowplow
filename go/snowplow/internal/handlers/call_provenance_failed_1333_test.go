package handlers

// call_provenance_failed_1333_test.go — #1333 arm F (call_provenance_1333_test.go):
// the provenance is recorded whatever the write's outcome. A failed or refused
// write's record is the record it would be without the header, plus exactly
// the header's client keys, and the caller's answer is the same bytes.

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// rejections1333 is one header per rejection reason, with that reason.
var rejections1333 = []struct{ reason, value string }{
	// #1333's contract, 2 KiB, written out: never derived from the constant
	// under test.
	{"oversize", "v1." + strings.Repeat("A", 2048-len("v1.")+1)},
	{"version", "v2." + strings.TrimPrefix(header1333(prov1333), "v1.")},
	{"encoding", header1333(prov1333) + "="},
	{"schema", header1333(`{"source":"portal","user":"mallory"}`)},
}

// serveOn1333 serves method target (+ body) to h through audit.Middleware as
// alice, on ep, with a fixed session id and the given provenance header (""
// for none), into rec. It returns the caller's response and the one record's
// attributes and body.
func serveOn1333(t *testing.T, rec *audittest.Recorder, h http.Handler, ep endpoints.Endpoint,
	method, target, body, prov string) (*httptest.ResponseRecorder, map[string]string, string) {
	t.Helper()
	rec.Reset()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := withCaller443(httptest.NewRequest(method, target, rdr), ep)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if prov != "" {
		req.Header.Set(audit.ProvenanceHeader, prov)
	}
	req = req.WithContext(audit.WithSessionID(req.Context(), "session-1333"))
	w := httptest.NewRecorder()
	audit.Middleware()(h).ServeHTTP(w, req)
	r := rec.Only(t)
	return w, asStrings1291(r), r.Body().AsString()
}

func TestCall1333_F_AFailedWriteCarriesTheProvenanceToo(t *testing.T) {
	failures := []struct {
		name string
		ep   func(t *testing.T) endpoints.Endpoint
		// the record's outcome without the header (the precondition)
		outcome string
	}{
		{"apiserver 403", func(t *testing.T) endpoints.Endpoint {
			api, ep := newFakeAPI443(t)
			api.respond(http.StatusForbidden, forbidden1298)
			return ep
		}, "failure"},
		{"refused destination", func(t *testing.T) endpoints.Endpoint {
			_, ep := newFakeAPI443(t)
			ep.ServerURL = "http://169.254.169.254" // link-local: refused in the dialer, never sent
			return ep
		}, "denied"},
		{"connection refused", func(t *testing.T) endpoints.Endpoint {
			_, ep := newFakeAPI443(t)
			gone := httptest.NewServer(http.NotFoundHandler())
			ep.ServerURL = gone.URL
			gone.Close() // nothing listens
			return ep
		}, "failure"},
	}
	type header struct {
		name, value string
		want        map[string]string
	}
	headers := []header{{"valid", header1333(prov1333), want1333}}
	for _, r := range rejections1333 {
		headers = append(headers, header{"rejected " + r.reason, r.value, map[string]string{krateoaudit.KeyClientRejected: r.reason}})
	}
	writes := []struct{ method, body string }{{http.MethodPost, cmRequest957}, {http.MethodDelete, ""}}

	for _, f := range failures {
		for _, wr := range writes {
			for _, hd := range headers {
				t.Run(f.name+"/"+wr.method+"/"+hd.name, func(t *testing.T) {
					ep := f.ep(t)
					rec := audittest.NewRecorder(t)
					audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
					t.Cleanup(func() { audit.SetDefault(nil) })
					target := "/call?" + cmQuery443

					plainW, plain, plainBody := serveOn1333(t, rec, Call(), ep, wr.method, target, wr.body, "")
					if plain["krateo.audit.outcome"] != f.outcome || plainW.Code == http.StatusOK {
						t.Fatalf("SETUP: without the header the write answered %d, record outcome %q, want %s",
							plainW.Code, plain["krateo.audit.outcome"], f.outcome)
					}
					w, got, gotBody := serveOn1333(t, rec, Call(), ep, wr.method, target, wr.body, hd.value)

					if w.Code != plainW.Code || w.Body.String() != plainW.Body.String() {
						t.Errorf("RED #1333: the caller got %d %q with the header, %d %q without",
							w.Code, w.Body.String(), plainW.Code, plainW.Body.String())
					}
					if gotBody != plainBody {
						t.Errorf("RED #1333: record body %q with the header, %q without", gotBody, plainBody)
					}
					client := map[string]string{}
					for k, v := range got {
						if strings.HasPrefix(k, "krateo.audit.client.") {
							client[k] = v
							delete(got, k)
						}
					}
					if !maps.Equal(client, hd.want) {
						t.Errorf("RED #1333: client keys\n got %v\nwant %v", client, hd.want)
					}
					if !maps.Equal(got, plain) {
						t.Errorf("RED #1333: apart from client.*, the record differs:\nwith    %v\nwithout %v", got, plain)
					}
				})
			}
		}
	}
}
