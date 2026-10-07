//go:build unit
// +build unit

package api

// errortext_redaction_523_test.go — #523, the BEHAVIOURAL half.
//
// serverurl_redaction_503_test.go proved that no log site hands a ServerURL
// FIELD to slog unredacted. #523 is the second route to the same record: the
// same credential rides out inside an error's TEXT, because net/url.Error
// renders its URL verbatim. redact.URL cannot touch that — it is not a URL
// field — and url_error_test.go (internal/redact) proves redact.ErrorURL can.
//
// These arms close the gap between those two facts: they drive a
// credential-bearing ServerURL through the REAL failure paths that produce a
// url.Error and assert on what the production logger actually emitted.
//
// They are non-vacuous by construction, in three ways:
//   - each first asserts the site under test FIRED, with its attribute present,
//     so a refactor that stopped logging could not turn them green;
//   - each asserts an ERROR-level record, which is the level that is always on
//     in production and the reason #523 is a live leak;
//   - each asserts the diagnostic SURVIVED. A fix that redacted the message
//     into nothing would pass every leak check and be a different defect.
//
// They reuse the #503 capture harness (s503Capture, credServerURL and the
// sentinels) deliberately: one sink, one sentinel, so the two issues cannot
// drift apart on what counts as a leak.

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/client-go/rest"
)

// s523QueryToken is a SECOND sentinel, in the query rather than the userinfo.
// It is the shape net/http's own redaction misses: client.go wraps a transport
// failure in a url.Error whose URL went through stripPassword, which replaces
// the PASSWORD and keeps the username and the entire query. So an arm that only
// used the userinfo password would pass on the transport path even with #523
// unfixed, on the stdlib's work. This one does not.
const s523QueryToken = "qu3ry-t0k3n-DO-NOT-LEAK-523"

// malformedCredURL is the issue's own trigger: an ordinary credential-bearing
// endpoint URL that url.Parse REJECTS. A well-formed one parses and takes the
// success path, so the leak needs a malformed one — a control character in a
// tenant endpoint Secret, which is an operator typo away.
func malformedCredURL(t *testing.T) string {
	t.Helper()
	out := "https://" + s503User + ":" + s503Password + "@apiserver.example:6443/base\x7f"
	if _, err := url.Parse(out); err == nil {
		t.Fatalf("SETUP: %q parses cleanly, so url.Parse never fails and the #523 site never fires", out)
	}
	if !strings.Contains(out, s503Password) {
		t.Fatalf("SETUP: the malformed URL does not carry the sentinel: %q", out)
	}
	return out
}

// deadPort returns a host:port nothing is listening on, by opening a listener
// and closing it. Dialling it fails with a real transport error rather than a
// synthesised one.
func deadPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("SETUP: cannot open a listener: %v", err)
	}
	addr := ln.Addr().String()
	if cerr := ln.Close(); cerr != nil {
		t.Fatalf("SETUP: cannot close the listener: %v", cerr)
	}
	return addr
}

// assertNoSentinelAnywhere is assertNoPasswordAnywhere widened to the query
// token: neither sentinel may appear in any emitted record, at any level.
func assertNoSentinelAnywhere(t *testing.T, c *s503Capture) {
	t.Helper()
	c.assertNoPasswordAnywhere(t)
	for _, r := range c.snapshot() {
		if strings.Contains(r.all, s523QueryToken) {
			t.Errorf("#523 CREDENTIAL LEAK: the ServerURL QUERY token reached an emitted %s record: %s",
				r.level, r.all)
		}
	}
}

// ---------------------------------------------------------------------------
// Arm 1 — the issue's exact trace, through the real api.Resolve
// ---------------------------------------------------------------------------

// TestS523_ResolveErrorTextNeverCarriesAServerURLPassword drives api.Resolve
// with a ServerURL that carries a password AND is rejected by url.Parse. That is
// external_fetch.go's first site: the *url.Error is folded into the response
// envelope, and resolve.go logs the envelope's Message at Error with
// slog.String("error", res.Message) — the attribute #503 could not reach.
//
// Mutation evidence is recorded in the PR: dropping redact.ErrorURL from the
// url.Parse branch of external_fetch.go makes this RED with the password in the
// "error" attribute of an Error record.
func TestS523_ResolveErrorTextNeverCarriesAServerURLPassword(t *testing.T) {
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	credURL := malformedCredURL(t)

	rec := &s503Capture{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s523-user"}),
		xcontext.WithLogger(slog.New(rec.handler())),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: credURL})

	_ = Resolve(ctx, ResolveOptions{
		RC: &rest.Config{},
		Items: []*templates.API{{
			Name:            "bad",
			Path:            "/bad",
			Verb:            ptr.To(http.MethodGet),
			ContinueOnError: ptr.To(true),
			ErrorKey:        ptr.To("badErr"),
		}},
		RESTActionNamespace: "default",
		RESTActionName:      "s523-errortext-redaction",
	})

	// NON-VACUITY 1: the site must have fired, carrying an "error" attribute.
	recs := rec.withAttr("error")
	if len(recs) == 0 {
		t.Fatalf("SETUP/NON-VACUITY: the resolver emitted no record carrying an \"error\" attribute, so the "+
			"no-leak assertion would be vacuous. records=%d", len(rec.snapshot()))
	}
	// NON-VACUITY 2: specifically at ERROR level. Error is always on in
	// production; a Debug-only hit would not be the #523 leak.
	var errLevel []s503Record
	for _, r := range recs {
		if r.level >= slog.LevelError {
			errLevel = append(errLevel, r)
		}
	}
	if len(errLevel) == 0 {
		t.Fatalf("NON-VACUITY: no ERROR-level record carried an \"error\" attribute. The always-on site is the "+
			"point of #523, so this arm must exercise it. error-carrying records=%d", len(recs))
	}

	assertNoSentinelAnywhere(t, rec)

	// NON-VACUITY 3 and the OTHER direction: at least one of those Error records
	// must still SAY what went wrong. A fix that emptied the message would pass
	// every assertion above.
	useful := false
	for _, r := range errLevel {
		msg := r.attrs["error"]
		if strings.Contains(msg, "parse") && strings.Contains(msg, "invalid control character") {
			useful = true
		}
		if strings.Contains(msg, s503Password) || strings.Contains(msg, s503User) {
			t.Errorf("#523: an \"error\" attribute still carries the ServerURL userinfo: %q (record: %s)", msg, r.all)
		}
	}
	if !useful {
		var seen []string
		for _, r := range errLevel {
			seen = append(seen, r.attrs["error"])
		}
		t.Errorf("#523: no ERROR record names the parse failure any more, so the redaction destroyed the "+
			"diagnostic instead of cleaning it. error attributes seen: %q", seen)
	}
}

// ---------------------------------------------------------------------------
// Arm 2 — the transport path, where the stdlib's own redaction is not enough
// ---------------------------------------------------------------------------

// TestS523_TransportErrorTextNeverCarriesAServerURLQueryToken drives
// api.Resolve against a port nothing is listening on, with a ServerURL that
// carries BOTH a userinfo password and a query token. Client.Do fails with a
// real *url.Error; net/http has already stripped the password from it and has
// NOT stripped the query, so the token is the sentinel that makes this arm fail
// without the fix.
func TestS523_TransportErrorTextNeverCarriesAServerURLQueryToken(t *testing.T) {
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	credURL := "http://" + s503User + ":" + s503Password + "@" + deadPort(t) + "/base?token=" + s523QueryToken

	rec := &s503Capture{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s523-user"}),
		xcontext.WithLogger(slog.New(rec.handler())),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: credURL})

	_ = Resolve(ctx, ResolveOptions{
		RC: &rest.Config{},
		Items: []*templates.API{{
			Name:            "bad",
			Verb:            ptr.To(http.MethodGet),
			ContinueOnError: ptr.To(true),
			ErrorKey:        ptr.To("badErr"),
		}},
		RESTActionNamespace: "default",
		RESTActionName:      "s523-transport-redaction",
	})

	recs := rec.withAttr("error")
	if len(recs) == 0 {
		t.Fatalf("SETUP/NON-VACUITY: the resolver emitted no record carrying an \"error\" attribute. records=%d",
			len(rec.snapshot()))
	}
	var errLevel []s503Record
	for _, r := range recs {
		if r.level >= slog.LevelError {
			errLevel = append(errLevel, r)
		}
	}
	if len(errLevel) == 0 {
		t.Fatalf("NON-VACUITY: no ERROR-level record carried an \"error\" attribute. error-carrying records=%d",
			len(recs))
	}

	assertNoSentinelAnywhere(t, rec)

	// The diagnostic: the record must still say a dial failed.
	useful := false
	for _, r := range errLevel {
		msg := r.attrs["error"]
		if strings.Contains(msg, "127.0.0.1") && (strings.Contains(msg, "refused") || strings.Contains(msg, "dial")) {
			useful = true
		}
	}
	if !useful {
		var seen []string
		for _, r := range errLevel {
			seen = append(seen, r.attrs["error"])
		}
		t.Errorf("#523: no ERROR record names the dial failure any more, so the redaction destroyed the "+
			"diagnostic. error attributes seen: %q", seen)
	}
}

// ---------------------------------------------------------------------------
// Arm 3 — the OTHER two consumers of the same message
// ---------------------------------------------------------------------------

// TestS523_TheFetchEnvelopeAndGoErrorAreBothRedacted is why the fix is at the
// error-construction site and not at the log site.
//
// resolve.go does three things with the same text: it logs it, it hands it to
// recordItemError as the ErrorKey VALUE (which reaches the resolved dict and
// therefore the HTTP response body), and it wraps it into the item error.
// Sanitising slog.String("error", res.Message) alone would have left the other
// two leaking, which is not something a log-capture arm can see. So this one
// drives the production fetch directly and asserts on what it RETURNS.
func TestS523_TheFetchEnvelopeAndGoErrorAreBothRedacted(t *testing.T) {
	ac6FailFast(t)

	credURL := malformedCredURL(t)
	st, body, _, err := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
		Endpoint:    &endpoints.Endpoint{ServerURL: credURL},
		RequestInfo: httpcall.RequestInfo{Verb: ptr.To(http.MethodGet), Path: "/bad"},
	})

	// NON-VACUITY: the fetch must actually have FAILED on the parse, else the
	// assertions below hold trivially.
	if err == nil {
		t.Fatalf("SETUP/NON-VACUITY: the fetch returned no error for a ServerURL url.Parse rejects "+
			"(status=%+v body=%d bytes) — this arm is not exercising the #523 site", st, len(body))
	}
	if st == nil || st.Status != response.StatusFailure {
		t.Fatalf("SETUP/NON-VACUITY: the fetch did not produce a Failure envelope: %+v", st)
	}

	for name, got := range map[string]string{
		"the envelope Message (→ the ErrorKey dict value and the response body)": st.Message,
		"the returned go error (→ the joined item error)":                        err.Error(),
	} {
		if strings.Contains(got, s503Password) {
			t.Errorf("#523 CREDENTIAL LEAK in %s: %q", name, got)
		}
		if strings.Contains(got, s503User) {
			t.Errorf("#523: %s still names the userinfo USER: %q", name, got)
		}
		// Both directions: it must still be a usable diagnostic.
		if !strings.Contains(got, "invalid control character") {
			t.Errorf("#523: %s lost the failure reason: %q", name, got)
		}
	}

	// The chain is not broken by the redaction — resolve.go only reads the text,
	// but a future caller doing errors.Is/As must not be silently defeated.
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Errorf("#523: errors.As no longer finds the *url.Error the fetch failed with: %v", err)
	}
}

// TestS523_AnOrdinaryFetchErrorStillRendersUsefully is the arm for the other
// direction, over the real fetch: an error that carries no URL at all must come
// through the same boundary UNCHANGED. Without it, a redaction that quietly
// mangled every failure message would be invisible here.
func TestS523_AnOrdinaryFetchErrorStillRendersUsefully(t *testing.T) {
	ac6FailFast(t)

	// A 2xx body that is neither JSON nor convertible YAML: the envelope message
	// is composed by toJSONBytes from the BODY and the content-type, never from
	// the URI, and is deliberately NOT routed through redact.ErrorURL.
	f := newAC6Fixture(t, http.StatusOK, "text/yaml", "entries:\n  - a: 1\n   : broken indent\n\tbad")

	st, _, _, err := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
		Endpoint:    &endpoints.Endpoint{ServerURL: f.server.URL},
		RequestInfo: httpcall.RequestInfo{Verb: ptr.To(http.MethodGet), Path: "/bad"},
	})
	if err != nil {
		t.Fatalf("SETUP: the conversion-failure path must return no go error, got %v", err)
	}
	if st == nil || st.Status != response.StatusFailure {
		t.Fatalf("SETUP/NON-VACUITY: expected a Failure envelope from an unconvertible 2xx body, got %+v", st)
	}
	if !strings.Contains(strings.ToLower(st.Message), "yaml") {
		t.Errorf("#523: an ordinary conversion failure no longer explains itself: %q", st.Message)
	}
}
