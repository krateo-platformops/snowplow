package credhttp

// connections.go — #847: how many connections each egress opened, and how many
// times a request was handed one:
//
//	snowplow_egress_connections_opened_total{egress,protocol}
//	snowplow_egress_connection_handovers_total{egress,protocol}
//
// THE READING. reuse share = 1 − opened / handovers, per egress and protocol. A
// connection that is thrown away costs the next request a TCP connect and, on
// https, a TLS handshake. A latency cannot stand in for that, because a latency
// moves with load. The share holds still under load as long as a pool's requests
// in flight stay at or under its idle cap (below), so a paired bench run that
// changes only the toolchain (#847: Go 1.27 drains an HTTP/1 body closed early and
// keeps the connection) can read the effect directly.
//
// THE IDLE CAP. Past it the share moves with load, and rightly: a connection that
// comes free with the pool's idle slots full is closed, and the next request opens
// another one. opened counts them all, as it should, but that drop is load, not
// reuse being lost. The cap is per host: 25 on a kept stage client (client-go's
// MaxIdleConnsPerHost) and net/http's default of 2 on authn and the RUM forwarder,
// which are serial. A toolchain that changes latency changes what is in flight, so
// a pair read past the cap can show a change the toolchain's reuse did not cause.
//
// WHY TWO COUNTERS AND NOT GotConnInfo.Reused. Reused does not say "this
// connection has carried a request before". A dial whose waiter was served by
// something else, another connection coming free or a cancelled wait, parks its
// connection in the idle pool, and net/http marks it reused there, before it has
// carried anything. That is the normal way a lost connection is replaced on a
// busy pool, so on a concurrent pool most new connections read as reused. And
// nothing at hand-over time sees a surplus dial, which nobody ever uses and which
// still cost a connect. So the two facts are read where each one happens:
//
//   - opened: a connection came up, read from the dial itself. A plain http://
//     connection is up at httptrace ConnectDone; an https:// one at
//     TLSHandshakeDone, which is also what names its protocol. Both fire on the
//     dial's own goroutine, so they do not depend on the request that started the
//     dial still waiting for it. A dial that fails, or is given up before it is
//     up, does not count: there was no connection. One that comes up after its
//     request left does: the server has accepted it and the cost is paid, whether
//     it later serves someone or is closed unused.
//   - handovers: a request was handed a connection (httptrace GotConn). On HTTP/2
//     that is one per stream. A retry above the transport and a redirect hop take
//     one each, and so does a request net/http retries by itself after a pooled
//     connection turned out dead.
//
// opened then equals what the server accepted, however concurrent the pool. An
// interval can read more opened than handovers, when dials land after the
// requests that started them have finished, so read the share over a whole phase.
//
// The one shape that overcounts: an https:// request through an https:// proxy
// does two TLS handshakes on one connection, and each counts as opened, under the
// protocol that handshake negotiated. So an HTTP/2 target through such a proxy also
// adds the proxy's handshake to the http1 cell, with no handover to go with it.
//
// WHY PROTOCOL IS A LABEL. Go 1.27's change is HTTP/1 only, and one egress can
// carry both: a stage reaches the apiserver over HTTP/2 and an in-cluster
// http:// service over HTTP/1. A share over both would dilute the HTTP/1 reading
// with connections the change cannot affect.
//
// WHERE. One hook per request, and only one, whichever layers it crosses. Send
// is where every stage and /call request leaves, a write included, so it counts
// there, and its egress wins. Harden gives every *rest.Config the apiserver
// egress, and authn and the RUM forwarder name theirs with CountConnectionsOn.
// The hook marks the context, so a chain that carries it twice (Harden on a copy
// of a hardened config, Send on a hardened client) still counts once.
//
// WHAT IS NOT COUNTED. The three OTLP exporters (metrics, traces, logs): they
// build their own clients and do not go through this package.
//
// It only observes: the hooks read what net/http reports and add to an atomic.
// A request is sent exactly as before.
//
// LABELS. Two closed, code-defined sets; a value outside egress's folds into
// EgressOther, so no data can grow the label set. Never a host, a path, an
// identity or a status: nothing here reads a header or logs, and the URL's
// scheme is only compared.

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
)

// Egress names (closed set): where a connection leads, and who sends on it.
const (
	EgressStage      = SendSiteStage // the RESTAction branch-E stage fetch
	EgressCall       = SendSiteCall  // the /call raw passthrough (credhttp.Do)
	EgressAPIServer  = "apiserver"   // every client-go client (a *rest.Config Harden returned)
	EgressAuthn      = "authn"       // the authn service login
	EgressRUMForward = "rum_forward" // the browser-span forwarder
	// EgressOther: a name outside this set. No caller sends one; the cell keeps
	// the count, and keeps the set closed, if one ever did.
	EgressOther = "other"
)

// Protocols of a connection (closed set).
const (
	ConnProtocolHTTP1 = "http1"
	ConnProtocolHTTP2 = "http2"
)

// Egresses and ConnProtocols are the closed label sets, in publication order.
var (
	Egresses      = []string{EgressStage, EgressCall, EgressAPIServer, EgressAuthn, EgressRUMForward, EgressOther}
	ConnProtocols = []string{ConnProtocolHTTP1, ConnProtocolHTTP2}
)

type connectionCell struct{ opened, handovers atomic.Uint64 }

// connectionCounter is one cell per closed {egress, protocol}, all present from
// construction. The maps are never written after that, so reading them takes no
// lock.
type connectionCounter map[string]map[string]*connectionCell

func newConnectionCounter() connectionCounter {
	m := make(connectionCounter, len(Egresses))
	for _, e := range Egresses {
		m[e] = make(map[string]*connectionCell, len(ConnProtocols))
		for _, p := range ConnProtocols {
			m[e][p] = new(connectionCell)
		}
	}
	return m
}

func (m connectionCounter) cell(egress, protocol string) *connectionCell {
	byProtocol, ok := m[egress]
	if !ok {
		byProtocol = m[EgressOther]
	}
	return byProtocol[protocol]
}

// alpnProtocol reads a negotiated ALPN protocol: "h2" is HTTP/2. Anything else is
// HTTP/1: a server may negotiate "http/1.1" or nothing.
func alpnProtocol(negotiated string) string {
	if negotiated == "h2" {
		return ConnProtocolHTTP2
	}
	return ConnProtocolHTTP1
}

// connProtocol is the protocol of a connection handed to a request. One with no
// TLS state is HTTP/1: snowplow never speaks unencrypted HTTP/2.
func connProtocol(c net.Conn) string {
	if t, ok := c.(interface{ ConnectionState() tls.ConnectionState }); ok {
		return alpnProtocol(t.ConnectionState().NegotiatedProtocol)
	}
	return ConnProtocolHTTP1
}

var egressConnections = newConnectionCounter()

// connectionsCountedKey marks a context whose requests are already counted.
type connectionsCountedKey struct{}

// countConnections returns req's context with the connection hooks for egress,
// or that context itself when it already has them: the outermost egress wins,
// once. The scheme decides when a connection is up, so a hook is only valid for
// requests of that scheme: Send's is, because no redirect hop leaves the origin.
func countConnections(req *http.Request, egress string) context.Context {
	ctx := req.Context()
	if ctx.Value(connectionsCountedKey{}) != nil {
		return ctx
	}
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			egressConnections.cell(egress, connProtocol(info.Conn)).handovers.Add(1)
		},
	}
	if req.URL != nil && req.URL.Scheme == "https" {
		trace.TLSHandshakeDone = func(state tls.ConnectionState, err error) {
			if err == nil {
				egressConnections.cell(egress, alpnProtocol(state.NegotiatedProtocol)).opened.Add(1)
			}
		}
	} else {
		trace.ConnectDone = func(_, _ string, err error) {
			if err == nil {
				egressConnections.cell(egress, ConnProtocolHTTP1).opened.Add(1)
			}
		}
	}
	return httptrace.WithClientTrace(context.WithValue(ctx, connectionsCountedKey{}, true), trace)
}

// countConnectionsTransport wraps rt so its requests count under egress.
func countConnectionsTransport(egress string, rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &connectionCountingTransport{next: rt, egress: egress}
}

type connectionCountingTransport struct {
	next   http.RoundTripper
	egress string
}

func (t *connectionCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(req.WithContext(countConnections(req, t.egress)))
}

// WrappedRoundTripper keeps the chain walkable (client-go cancellation,
// CloseIdleConnectionsFor).
func (t *connectionCountingTransport) WrappedRoundTripper() http.RoundTripper { return t.next }

// CountConnectionsOn counts c's connections under egress: c's transport is
// wrapped in place, and c returned. For a client that does not send through
// Send and is not built from a *rest.Config.
func CountConnectionsOn(egress string, c *http.Client) *http.Client {
	c.Transport = countConnectionsTransport(egress, c.Transport)
	return c
}

// EgressConnectionCount is one {egress, protocol} cell of
// snowplow_egress_connections_opened_total and
// snowplow_egress_connection_handovers_total.
type EgressConnectionCount struct {
	Egress, Protocol  string
	Opened, Handovers uint64
}

// EgressConnectionsSnapshot returns every {egress, protocol} cell, zeros
// included, in Egresses × ConnProtocols order.
func EgressConnectionsSnapshot() []EgressConnectionCount {
	out := make([]EgressConnectionCount, 0, len(Egresses)*len(ConnProtocols))
	for _, e := range Egresses {
		for _, p := range ConnProtocols {
			c := egressConnections[e][p]
			out = append(out, EgressConnectionCount{Egress: e, Protocol: p, Opened: c.opened.Load(), Handovers: c.handovers.Load()})
		}
	}
	return out
}
