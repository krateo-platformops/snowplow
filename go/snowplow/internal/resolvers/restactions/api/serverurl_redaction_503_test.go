//go:build unit
// +build unit

package api

// serverurl_redaction_503_test.go — #503, the BEHAVIOURAL half.
//
// endpoint_url_guard_test.go (internal/redact) proves STRUCTURALLY that no log
// site hands a URL-bearing endpoint field to slog without redact.URL. It proves
// nothing about what comes out: a redact.URL that had regressed to returning
// its input would satisfy it completely.
//
// So these arms drive a ServerURL that CARRIES A PASSWORD through the real
// resolver and the real TLS client builder, capture what the production logger
// actually emitted, and assert the password is in none of it.
//
// Both arms are non-vacuous by construction: each first asserts that the log
// site under test FIRED (the attribute is present), so a refactor that simply
// stopped logging the host could not turn these green.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"

	"k8s.io/client-go/rest"
)

// s503Password is the sentinel. It is deliberately NOT token-shaped (no JWT
// segments, no "Bearer"): the redaction must be STRUCTURAL — userinfo is
// dropped because of where it sits in a URL, never because it matched a
// known credential pattern (the same reasoning as #502's sentinel choice).
const s503Password = "pa55w0rd-DO-NOT-LEAK-503"

const s503User = "tenant-admin"

// s503Capture is the SINK. s503Handler is the slog.Handler bound to it.
//
// They are split because WithAttrs must be honoured rather than dropped. A
// handler that returns itself from WithAttrs silently discards every attribute
// added by logger.With(...), so a leak carried on one would never be captured
// and this arm would pass while the credential sat in the record — a guard
// that reads as health precisely when it has gone blind.
//
// No site in the api package uses logger.With today (checked: no `log.With(`
// in the non-test sources). This handler is written to survive one being
// added, because the arm must not depend on that staying true.
type s503Capture struct {
	mu   sync.Mutex
	recs []s503Record
}

type s503Handler struct {
	sink  *s503Capture
	base  []slog.Attr
	group string
}

type s503Record struct {
	level slog.Level
	msg   string
	attrs map[string]string
	all   string
}

func (h *s503Handler) Enabled(context.Context, slog.Level) bool { return true }

func (h *s503Handler) Handle(_ context.Context, r slog.Record) error {
	rec := s503Record{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	var b strings.Builder
	b.WriteString(r.Message)
	add := func(a slog.Attr) {
		key := a.Key
		if h.group != "" {
			key = h.group + "." + a.Key
		}
		rec.attrs[key] = a.Value.String()
		fmt.Fprintf(&b, " %s=%s", key, a.Value.String())
	}
	for _, a := range h.base { // attrs from WithAttrs — never dropped
		add(a)
	}
	r.Attrs(func(a slog.Attr) bool {
		add(a)
		return true
	})
	rec.all = b.String()
	h.sink.mu.Lock()
	h.sink.recs = append(h.sink.recs, rec)
	h.sink.mu.Unlock()
	return nil
}

func (h *s503Handler) WithAttrs(as []slog.Attr) slog.Handler {
	next := append(append([]slog.Attr(nil), h.base...), as...)
	return &s503Handler{sink: h.sink, base: next, group: h.group}
}

func (h *s503Handler) WithGroup(name string) slog.Handler {
	g := name
	if h.group != "" {
		g = h.group + "." + name
	}
	return &s503Handler{sink: h.sink, base: h.base, group: g}
}

// handler returns a logger handler bound to this sink.
func (c *s503Capture) handler() slog.Handler { return &s503Handler{sink: c} }

func (c *s503Capture) snapshot() []s503Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]s503Record(nil), c.recs...)
}

// withAttr returns the records carrying the given attribute key.
func (c *s503Capture) withAttr(key string) []s503Record {
	var out []s503Record
	for _, r := range c.snapshot() {
		if _, ok := r.attrs[key]; ok {
			out = append(out, r)
		}
	}
	return out
}

// assertNoPasswordAnywhere is the leak assertion: the sentinel must appear in
// no record, at any level, in the message or in any attribute value.
func (c *s503Capture) assertNoPasswordAnywhere(t *testing.T) {
	t.Helper()
	for _, r := range c.snapshot() {
		if strings.Contains(r.all, s503Password) {
			t.Errorf("#503 CREDENTIAL LEAK: the ServerURL password reached an emitted %s record: %s",
				r.level, r.all)
		}
	}
}

// credServerURL rewrites a base URL to carry user:password@ in its authority —
// the ordinary shape of a tenant endpoint Secret, and the shape #503 is about.
func credServerURL(t *testing.T, base string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("SETUP: base URL %q does not parse: %v", base, err)
	}
	u.User = url.UserPassword(s503User, s503Password)
	out := u.String()
	// Guard the guard: the arm is meaningless if the sentinel is not actually
	// in the input.
	if !strings.Contains(out, s503Password) {
		t.Fatalf("SETUP: the credential-bearing ServerURL does not carry the sentinel: %q", out)
	}
	return out
}

// ---------------------------------------------------------------------------
// Arm 1 — the Warn site in endpoints_tls_233.go, through the real client builder
// ---------------------------------------------------------------------------

// TestS503_UnparseableCAWarnNeverCarriesAServerURLPassword drives
// httpClientForEndpoint — the real production boundary — with an endpoint whose
// ServerURL carries a password and whose CA is unparseable, which is what makes
// warnUnparseableCADelegation fire. That WARN is one of the five always-on
// sites #503 names.
//
// Mutation evidence (PR #503): dropping redact.URL from
// endpoints_tls_233.go:78 makes this RED with the password in the record.
func TestS503_UnparseableCAWarnNeverCarriesAServerURLPassword(t *testing.T) {
	resetUnparseableCAStateForTest()
	rec := &s503Capture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec.handler()))
	t.Cleanup(func() { slog.SetDefault(prev) })

	srv := credServerURL(t, "https://apiserver.example:6443")
	ep := &endpoints.Endpoint{
		ServerURL:                srv,
		Token:                    saTestToken,
		CertificateAuthorityData: "!!! not pem and not base64 !!!",
	}
	if !endpointNeedsOwnedCAClient(ep) {
		t.Fatal("SETUP: the endpoint must take the owned-CA (capability) branch, else the WARN never fires")
	}
	drive233(t, ep)

	// NON-VACUITY: the site must actually have fired, with its server_url attr.
	recs := rec.withAttr("server_url")
	if len(recs) == 0 {
		t.Fatalf("SETUP/NON-VACUITY: no record carried a server_url attribute — the #233 WARN did not fire, "+
			"so the no-leak assertion below would be vacuous. records=%d", len(rec.snapshot()))
	}

	rec.assertNoPasswordAnywhere(t)

	// And the attribute is the redacted rendering, not an empty string: the
	// diagnostic must survive the redaction (the #502 "stripped, not refused"
	// lesson — a fix that logged nothing would also pass a pure leak check).
	for _, r := range recs {
		got := r.attrs["server_url"]
		if got != redact.URL(srv) {
			t.Errorf("#503: server_url must be exactly redact.URL(ServerURL); got %q want %q", got, redact.URL(srv))
		}
		if !strings.Contains(got, "apiserver.example") {
			t.Errorf("#503: the redacted server_url lost the host, so the diagnostic is gone: %q", got)
		}
		if strings.Contains(got, s503User) {
			t.Errorf("#503: the redacted server_url still names the userinfo USER: %q", got)
		}
	}
}

// ---------------------------------------------------------------------------
// Arm 2 — the Error site in resolve.go, through the real api.Resolve
// ---------------------------------------------------------------------------

// TestS503_ResolveErrorLogNeverCarriesAServerURLPassword drives the real
// api.Resolve over an httptest server whose ServerURL carries a password, with
// a stage that fails, so resolve.go's "api call response failure" Error site
// fires. Error is always on in production.
//
// It asserts on EVERY record the resolver emitted, not just that one, so the
// Debug sites (nine of the fourteen) are covered by the same run.
//
// Mutation evidence (PR #503): dropping redact.URL from the resolve.go host
// attribute makes this RED with the password in the record.
func TestS503_ResolveErrorLogNeverCarriesAServerURLPassword(t *testing.T) {
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "<html>upstream exploded</html>")
	}))
	t.Cleanup(srv.Close)

	credURL := credServerURL(t, srv.URL)

	rec := &s503Capture{}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s503-user"}),
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
		RESTActionName:      "s503-serverurl-redaction",
	})

	// NON-VACUITY: a host attribute must have been emitted by the resolver.
	recs := rec.withAttr("host")
	if len(recs) == 0 {
		t.Fatalf("SETUP/NON-VACUITY: the resolver emitted no record carrying a host attribute, so the "+
			"no-leak assertion would be vacuous. records=%d", len(rec.snapshot()))
	}
	// And specifically an ERROR-level one. Error is always on in production —
	// it is the level that makes #503 a live leak rather than a debug-only one
	// — so the arm must not be satisfied by the Debug sites alone.
	//
	// This is a REQUIREMENT the test enforces, not a transcript of a run: if
	// this scenario stops reaching an Error site with a host attribute, that is
	// a failure of the arm's relevance and it says so rather than passing on
	// the Debug sites.
	var errLevel []s503Record
	for _, r := range recs {
		if r.level >= slog.LevelError {
			errLevel = append(errLevel, r)
		}
	}
	if len(errLevel) == 0 {
		t.Fatalf("NON-VACUITY: no ERROR-level record carried a host attribute. The always-on site is the "+
			"whole point of #503, so this arm must exercise it, not just the Debug ones. "+
			"host-carrying records=%d", len(recs))
	}

	rec.assertNoPasswordAnywhere(t)

	// The credential-bearing URL must appear at a log site in its REDACTED
	// form at least once. Requiring it of EVERY host attribute would be wrong:
	// resolve.go:1594 logs the RESOLVED endpoint, which the isInternal path may
	// have rewritten to kubernetes.default.svc (endpoints.go:144,169) — a
	// different URL, and not a leak. So: at least one host is exactly
	// redact.URL of the credential URL, and no host anywhere carries the
	// password (asserted globally above, over every record at every level).
	want := redact.URL(credURL)
	matched := false
	for _, r := range recs {
		got := r.attrs["host"]
		if got == want {
			matched = true
		}
		if strings.Contains(got, s503Password) || strings.Contains(got, s503User) {
			t.Errorf("#503: a host attribute still carries the ServerURL userinfo: %q (record: %s)", got, r.all)
		}
	}
	if !matched {
		var seen []string
		for _, r := range recs {
			seen = append(seen, r.attrs["host"])
		}
		t.Errorf("#503: no log site rendered the credential-bearing ServerURL as redact.URL would (%q). "+
			"host attributes seen: %q — if the endpoint never reached a log site in this scenario the arm "+
			"is not exercising the #503 sites at all", want, seen)
	}
	// The redaction must keep the diagnostic: 127.0.0.1:<port> is still there.
	if !strings.Contains(want, "127.0.0.1") {
		t.Errorf("#503: redact.URL dropped the host from %q → %q, so the log site lost its diagnostic", credURL, want)
	}
}

// TestS503_RedactURLStripsTheSentinelFromAServerURL is the unit floor under
// both arms above: whatever the log sites do, redact.URL itself must remove
// this sentinel from this shape. Without it, a failure above is ambiguous
// between "the site does not redact" and "redact.URL does not work".
func TestS503_RedactURLStripsTheSentinelFromAServerURL(t *testing.T) {
	raw := "https://" + s503User + ":" + s503Password + "@apiserver.example:6443/base"
	got := redact.URL(raw)
	if strings.Contains(got, s503Password) {
		t.Fatalf("redact.URL left the password in: %q", got)
	}
	if strings.Contains(got, s503User) {
		t.Errorf("redact.URL left the userinfo user in: %q", got)
	}
	if got == redact.URLUnparseable {
		t.Errorf("redact.URL REFUSED an ordinary credential-bearing URL (%q); it must strip and still render, "+
			"else every #503 log site loses its diagnostic", raw)
	}
	if !strings.Contains(got, "apiserver.example:6443") {
		t.Errorf("redact.URL lost the host: %q", got)
	}
}
