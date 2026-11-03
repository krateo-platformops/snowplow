package credhttp

// wiredump.go — #1112 D4: an endpoint dial's wire dump never holds a
// credential or a body.
//
// THE DEFECT. An endpoint Secret with `debug: "true"` (or RESOLVER_VERBOSE_WIRE_
// DUMP=true plus the RESTAction's krateo.io/verbose annotation, which sets the
// same Endpoint.Debug per call) made plumbing's debuggingRoundTripper log, at
// DEBUG, httputil.DumpRequestOut and DumpResponse of every request: the
// `Authorization: Bearer <endpoint token>` header (plumbing installs the dump
// INSIDE its auth wrapper), any cookie, and the full request and response
// bodies. A debug surface never dumps per-identity data
// (feedback: metadata and a digest only).
//
// THE RULE. No endpoint client snowplow builds carries plumbing's dump: every
// builder hands plumbing a copy with Debug cleared (WithoutPlumbingWireDump),
// and an endpoint that asks for the dump gets WireDump instead, outermost on its
// client. WireDump logs metadata only:
//   - the method and the URL through redact.URL (no userinfo, no query values);
//   - every header NAME, with its value only for the few headers that describe
//     the message (wireDumpClearHeaders); every other value — Authorization,
//     Cookie, Set-Cookie, Impersonate-*, an API-key header — is "[REDACTED]".
//     A Location goes through redact.URL;
//   - the request body as its length and its keyed digest (redact.Digest: a
//     plain sha256 of a short body could be guessed), never its bytes; the response
//     body as its declared length only (it is never read here, so a large body
//     costs nothing, which was the R1 memory concern of the old dump).
//
// A client certificate never reaches a dump: it is TLS handshake material,
// below the HTTP layer either dump sees.

import (
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// wireDumpRedacted replaces a header value the dump does not show.
const wireDumpRedacted = "[REDACTED]"

// wireDumpClearHeaders are the headers whose value describes the message and
// never carries a credential or identity: shown in clear.
var wireDumpClearHeaders = map[string]bool{
	"Accept":            true,
	"Accept-Encoding":   true,
	"Content-Encoding":  true,
	"Content-Length":    true,
	"Content-Type":      true,
	"Retry-After":       true,
	"Transfer-Encoding": true,
}

// WithoutPlumbingWireDump returns ep for a plumbing builder: ep itself when it
// does not ask for the dump, else a copy with Debug cleared, so plumbing never
// installs its debuggingRoundTripper. The caller installs WireDump when
// ep.Debug. ep is not modified.
func WithoutPlumbingWireDump(ep *endpoints.Endpoint) *endpoints.Endpoint {
	if ep == nil || !ep.Debug {
		return ep
	}
	cp := *ep
	cp.Debug = false
	return &cp
}

// WireDump wraps rt so every request it sends and every response it receives
// is logged at DEBUG as metadata only (see the file comment).
func WireDump(rt http.RoundTripper) http.RoundTripper {
	return &wireDump{next: rt}
}

type wireDump struct{ next http.RoundTripper }

// WrappedRoundTripper lets client-go/utilnet walk the chain.
func (t *wireDump) WrappedRoundTripper() http.RoundTripper { return t.next }

func (t *wireDump) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	log := xcontext.Logger(req.Context())
	log.Debug("request details",
		slog.String("method", req.Method),
		slog.String("url", redact.URL(req.URL.String())),
		slog.Any("headers", dumpHeaders(req.Header)),
		slog.String("body", requestBodySummary(req)))
	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	log.Debug("response details",
		slog.Int("status", resp.StatusCode),
		slog.Any("headers", dumpHeaders(resp.Header)),
		slog.String("body", "declared length "+strconv.FormatInt(resp.ContentLength, 10)))
	return resp, nil
}

// dumpHeaders is h as the dump shows it: every name, in order, with its value
// in clear only for wireDumpClearHeaders.
func dumpHeaders(h http.Header) []string {
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, k := range names {
		ck := http.CanonicalHeaderKey(k)
		v := wireDumpRedacted
		switch {
		case wireDumpClearHeaders[ck]:
			v = h.Get(k)
		case ck == "Location":
			v = redact.URL(h.Get(k))
		}
		out = append(out, ck+": "+v)
	}
	return out
}

// requestBodySummary is the request body as its length and keyed digest, read
// from a fresh copy (GetBody), never from the body being sent.
func requestBodySummary(req *http.Request) string {
	if req.Body == nil || req.Body == http.NoBody {
		return "none"
	}
	if req.GetBody == nil {
		return "length " + strconv.FormatInt(req.ContentLength, 10) + ", not re-readable"
	}
	rc, err := req.GetBody()
	if err != nil {
		return "length " + strconv.FormatInt(req.ContentLength, 10) + ", not re-readable"
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "length " + strconv.FormatInt(req.ContentLength, 10) + ", not re-readable"
	}
	return "length " + strconv.Itoa(len(b)) + ", digest " + redact.Digest(string(b))
}
