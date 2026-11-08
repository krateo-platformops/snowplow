// request.go — AUDIT-3 (#1251): what snowplow RECEIVED, on every record.
//
// A record says who did what to which target (audit.go). These attributes say
// where the request came from and how it was sent, with the semantics of the
// Kubernetes apiserver's audit Event (sourceIPs, userAgent, requestURI;
// k8s.io/apiserver pkg/audit/request.go LogRequestMetadata):
//
//   - krateo.request.source_ips: k8s.io/apimachinery utilnet.SourceIPs, the
//     function the apiserver's own audit calls. In order: each entry of the
//     first X-Forwarded-For header line, trimmed, dropped when it is not an IP
//     (repeated entries are KEPT); then X-Real-Ip, when it is an IP not already
//     in that list; then the address of the TCP peer, unless it equals the last
//     entry so far.
//   - user_agent.original: the User-Agent header, truncated as the apiserver
//     truncates it (maybeTruncateUserAgent): beyond maxUserAgentLength bytes it
//     is cut there and userAgentTruncateSuffix is appended.
//   - krateo.request.uri: req.URL.RequestURI(), as the apiserver records it:
//     the path and query snowplow received, never a scheme or host, even when
//     the request target is in absolute form. The apiserver does not bound it,
//     and neither does this package: net/http bounds the request line together
//     with the headers (http.Server.MaxHeaderBytes, unset in main.go, so
//     http.DefaultMaxHeaderBytes, 1 MiB) and answers 431 beyond that.
//
// THE TRUST MODEL OF source_ips. Only the LAST entry is established by snowplow:
// it is the address of the TCP peer that sent the request (on krateo-057, the
// agentgateway hop). Every earlier entry was written into a header by a client
// or a proxy, and snowplow cannot tell a true one from a forged one. There is
// no trusted-proxy list and no special case for the gateway: a reader of the
// trail decides which hops to believe, knowing the topology at the time. The
// peer is not repeated when it equals the last forwarded entry, so the last
// entry is the peer's either way.
//
// WHERE IT IS READ. Middleware captures the request's raw values once, at the
// edge of the chain, before any handler can rewrite the request: the header
// strings (immutable once read), the peer address and a copy of the URL. It
// carries them on the request context next to the session id. Nothing is
// parsed there, so a request that never emits a record (most are reads) pays
// only the capture. Emit derives the attributes from the captured values on
// every record made on that context, so every emit site — the /call write, the
// export, a RESTAction stage write — carries the same facts of the same
// request, and no site can supply them from anything else, such as a body
// field a client asserts. A record made on a context with no request (none
// today) carries none of them, which is the correct record, not a gap.
//
// IDENTITY. The request URI can carry identity in its query. The audit trail
// holds identity on purpose; the log lines must not (#1192). These values live
// only on the context under an unexported key and are written only by Emit to
// the audit LoggerProvider, never to slog.
package audit

import (
	"context"
	"net/http"
	"net/url"

	utilnet "k8s.io/apimachinery/pkg/util/net"
)

// The attribute keys of the request facts.
const (
	AttrSourceIPs  = "krateo.request.source_ips"
	AttrUserAgent  = "user_agent.original"
	AttrRequestURI = "krateo.request.uri"
)

// The apiserver's User-Agent bound and marker (k8s.io/apiserver
// pkg/audit/request.go maxUserAgentLength, userAgentTruncateSuffix).
const (
	maxUserAgentLength      = 1024
	userAgentTruncateSuffix = "...TRUNCATED"
)

// The header names utilnet.SourceIPs reads, in canonical form.
const (
	headerForwardedFor = "X-Forwarded-For"
	headerRealIP       = "X-Real-Ip"
)

// receivedRequest is what one request was received with, captured raw at the
// edge (Middleware) and turned into attributes only by Emit.
type receivedRequest struct {
	remoteAddr   string
	forwardedFor string // the first X-Forwarded-For line, as utilnet.SourceIPs reads it
	realIP       string
	userAgent    string
	url          url.URL // a copy: a later handler's change to req.URL does not reach it
	hasURL       bool
}

type receivedRequestKey struct{}

// withReceivedRequest returns ctx carrying the raw values req was received with.
func withReceivedRequest(ctx context.Context, req *http.Request) context.Context {
	r := &receivedRequest{
		remoteAddr:   req.RemoteAddr,
		forwardedFor: req.Header.Get(headerForwardedFor),
		realIP:       req.Header.Get(headerRealIP),
		userAgent:    req.Header.Get("User-Agent"),
	}
	if req.URL != nil {
		r.url, r.hasURL = *req.URL, true
	}
	return context.WithValue(ctx, receivedRequestKey{}, r)
}

// requestFacts are the attribute values of one received request.
type requestFacts struct {
	sourceIPs  []string
	userAgent  string
	requestURI string
}

// requestFactsFrom derives the request facts from the values captured on ctx,
// if any.
func requestFactsFrom(ctx context.Context) (requestFacts, bool) {
	r, ok := ctx.Value(receivedRequestKey{}).(*receivedRequest)
	if !ok || r == nil {
		return requestFacts{}, false
	}
	return r.facts(), true
}

// facts applies the apiserver's audit semantics to the captured values.
func (r *receivedRequest) facts() requestFacts {
	// utilnet.SourceIPs reads only these two headers and RemoteAddr: give it
	// exactly what was captured.
	h := http.Header{}
	if r.forwardedFor != "" {
		h[headerForwardedFor] = []string{r.forwardedFor}
	}
	if r.realIP != "" {
		h[headerRealIP] = []string{r.realIP}
	}
	ips := utilnet.SourceIPs(&http.Request{Header: h, RemoteAddr: r.remoteAddr})
	src := make([]string, len(ips))
	for i, ip := range ips {
		src[i] = ip.String()
	}
	f := requestFacts{sourceIPs: src, userAgent: truncateUserAgent(r.userAgent)}
	if r.hasURL {
		f.requestURI = r.url.RequestURI()
	}
	return f
}

// truncateUserAgent is the apiserver's maybeTruncateUserAgent.
func truncateUserAgent(ua string) string {
	if len(ua) > maxUserAgentLength {
		return ua[:maxUserAgentLength] + userAgentTruncateSuffix
	}
	return ua
}
