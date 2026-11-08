// request.go — AUDIT-3 (#1251): what snowplow RECEIVED, on every record.
//
// A record says who did what to which target (audit.go). These attributes say
// where the request came from and how it was sent, as the Kubernetes apiserver's
// audit Event does (sourceIPs, userAgent, requestURI; k8s.io/apiserver
// pkg/audit/request.go LogRequestMetadata):
//
//   - krateo.request.source_ips: the X-Forwarded-For entries, then X-Real-Ip,
//     then the connection's remote address, de-duplicated, in that order. It is
//     k8s.io/apimachinery's utilnet.SourceIPs, the function the apiserver's own
//     audit calls, so the list means what it means in a Kubernetes audit log.
//   - user_agent.original: the User-Agent header, as received.
//   - krateo.request.uri: the request target, path and query, as received.
//
// THE TRUST MODEL OF source_ips. Only the LAST entry is established by snowplow:
// it is the address of the TCP peer that sent the request (on krateo-057, the
// agentgateway hop). Every earlier entry was written into a header by a client
// or a proxy, and snowplow cannot tell a true one from a forged one. There is
// no trusted-proxy list and no special case for the gateway: a reader of the
// trail decides which hops to believe, knowing the topology at the time. When
// the peer's address repeats the last forwarded entry it is not repeated (the
// apiserver's rule), so the last entry is still the peer's.
//
// WHERE IT IS READ. Middleware reads the request once, at the edge of the
// chain, before any handler can rewrite it, and carries the facts on the
// request context next to the session id. Emit stamps them on every record
// made on that context, so every emit site — the /call write, the export, a
// RESTAction stage write — carries the same facts of the same request, and no
// site can supply them from anything else, such as a body field a client
// asserts. A record made on a context with no request (none today) carries
// none of them, which is the correct record, not a gap.
//
// IDENTITY. The request URI can carry identity in its query. The audit trail
// holds identity on purpose; the log lines must not (#1192). These facts live
// only on the context under an unexported key and are written only by Emit to
// the audit LoggerProvider, never to slog.
package audit

import (
	"context"
	"net/http"

	utilnet "k8s.io/apimachinery/pkg/util/net"
)

// The attribute keys of the request facts.
const (
	AttrSourceIPs  = "krateo.request.source_ips"
	AttrUserAgent  = "user_agent.original"
	AttrRequestURI = "krateo.request.uri"
)

// requestFacts is what snowplow received with one request.
type requestFacts struct {
	sourceIPs  []string
	userAgent  string
	requestURI string
}

type requestFactsKey struct{}

// requestFactsOf reads req as received.
func requestFactsOf(req *http.Request) requestFacts {
	ips := utilnet.SourceIPs(req)
	src := make([]string, len(ips))
	for i, ip := range ips {
		src[i] = ip.String()
	}
	// RequestURI is the request target exactly as the client sent it, set by
	// the server. A request built in process (never read off a connection)
	// has none; its URL is then all there is.
	uri := req.RequestURI
	if uri == "" && req.URL != nil {
		uri = req.URL.RequestURI()
	}
	return requestFacts{
		sourceIPs:  src,
		userAgent:  req.Header.Get("User-Agent"),
		requestURI: uri,
	}
}

// withRequestFacts returns ctx carrying what req was received with.
func withRequestFacts(ctx context.Context, req *http.Request) context.Context {
	return context.WithValue(ctx, requestFactsKey{}, requestFactsOf(req))
}

// requestFactsFrom returns the request facts on ctx, if any.
func requestFactsFrom(ctx context.Context) (requestFacts, bool) {
	f, ok := ctx.Value(requestFactsKey{}).(requestFacts)
	return f, ok
}
