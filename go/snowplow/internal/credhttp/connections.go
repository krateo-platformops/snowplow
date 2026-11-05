package credhttp

// connections.go — #847: snowplow_egress_connections_total{egress,protocol,connection},
// how many exchanges took a new connection and how many reused one, by egress.
//
// WHY. A connection that is thrown away costs the next request a TCP connect
// and, on https, a TLS handshake. Nothing counted that: httptrace reported
// GotConnInfo.Reused to the budget and the slow-call record, per attempt, and
// neither adds it up. A latency cannot stand in for it, because a latency moves
// with load. The share reused ÷ (new + reused) does not, so a paired bench run
// that changes only the toolchain (#847: Go 1.27 drains an HTTP/1 body closed
// early and keeps the connection) can read the effect directly.
//
// WHAT ONE COUNT IS. One httptrace GotConn: a request was handed a connection.
//   - new: the connection had carried no request before this one. So "new" is
//     the number of connections that served at least one request; a dial whose
//     connection nobody ended up using is not counted.
//   - reused: it had. On HTTP/2 that is every stream after a connection's first.
//
// It is per connection handed over, not per request: a request net/http retries
// by itself after a pooled connection turned out dead counts one reused and one
// new, which is what happened on the wire. A retry above the transport and a
// redirect hop each take a connection of their own and count one each.
//
// WHY PROTOCOL IS A LABEL. Go 1.27's change is HTTP/1 only, and one egress can
// carry both: a stage reaches the apiserver over HTTP/2 and an in-cluster
// http:// service over HTTP/1. A share over both would dilute the HTTP/1 reading
// with connections the change cannot affect.
//
// WHERE. One hook per request, and only one, whichever layers it crosses. Send
// is where every stage and /call request leaves, a write included, so it counts
// there, and its egress wins. Everything else goes through a client that names
// its egress: Harden gives every *rest.Config the apiserver egress, and a client
// with an egress of its own says so with CountConnectionsOn. The hook marks the
// context, so a chain that carries it twice (Harden on a copy of a hardened
// config, Send on a hardened client) still counts once.
//
// It only observes: the hook reads what net/http reports and adds to an atomic.
// A request is sent exactly as before.
//
// LABELS. Three closed, code-defined sets; a value outside egress's folds into
// EgressOther, so no data can grow the label set. Never a host, a path, an
// identity or a status: nothing here reads the request or logs.

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

// What a request got (closed set).
const (
	ConnNew    = "new"
	ConnReused = "reused"
)

// Egresses, ConnProtocols and ConnStates are the closed label sets, in
// publication order.
var (
	Egresses      = []string{EgressStage, EgressCall, EgressAPIServer, EgressAuthn, EgressRUMForward, EgressOther}
	ConnProtocols = []string{ConnProtocolHTTP1, ConnProtocolHTTP2}
	ConnStates    = []string{ConnNew, ConnReused}
)

// connectionCounter is one counter per closed {egress, protocol, connection}
// cell, all present from construction. The maps are never written after that,
// so reading them takes no lock.
type connectionCounter map[string]map[string]map[string]*atomic.Uint64

func newConnectionCounter() connectionCounter {
	m := make(connectionCounter, len(Egresses))
	for _, e := range Egresses {
		m[e] = make(map[string]map[string]*atomic.Uint64, len(ConnProtocols))
		for _, p := range ConnProtocols {
			m[e][p] = make(map[string]*atomic.Uint64, len(ConnStates))
			for _, s := range ConnStates {
				m[e][p][s] = new(atomic.Uint64)
			}
		}
	}
	return m
}

func (m connectionCounter) add(egress string, info httptrace.GotConnInfo) {
	byProtocol, ok := m[egress]
	if !ok {
		byProtocol = m[EgressOther]
	}
	state := ConnNew
	if info.Reused {
		state = ConnReused
	}
	byProtocol[connProtocol(info.Conn)][state].Add(1)
}

// connProtocol reads the protocol the connection negotiated: ALPN "h2" is
// HTTP/2. Anything else is HTTP/1: a plain http:// connection has no ALPN, and
// snowplow never speaks unencrypted HTTP/2.
func connProtocol(c net.Conn) string {
	if t, ok := c.(interface{ ConnectionState() tls.ConnectionState }); ok && t.ConnectionState().NegotiatedProtocol == "h2" {
		return ConnProtocolHTTP2
	}
	return ConnProtocolHTTP1
}

var egressConnections = newConnectionCounter()

// connectionsCountedKey marks a context whose requests are already counted.
type connectionsCountedKey struct{}

// countConnections returns ctx with the connection hook for egress, or ctx
// itself when it already has one: the outermost egress wins, once.
func countConnections(ctx context.Context, egress string) context.Context {
	if ctx.Value(connectionsCountedKey{}) != nil {
		return ctx
	}
	return httptrace.WithClientTrace(context.WithValue(ctx, connectionsCountedKey{}, true), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { egressConnections.add(egress, info) },
	})
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
	return t.next.RoundTrip(req.WithContext(countConnections(req.Context(), t.egress)))
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

// EgressConnectionCount is one snowplow_egress_connections_total cell.
type EgressConnectionCount struct {
	Egress, Protocol, Connection string
	Count                        uint64
}

// EgressConnectionsSnapshot returns every snowplow_egress_connections_total
// cell, zeros included, in Egresses × ConnProtocols × ConnStates order.
func EgressConnectionsSnapshot() []EgressConnectionCount {
	out := make([]EgressConnectionCount, 0, len(Egresses)*len(ConnProtocols)*len(ConnStates))
	for _, e := range Egresses {
		for _, p := range ConnProtocols {
			for _, s := range ConnStates {
				out = append(out, EgressConnectionCount{Egress: e, Protocol: p, Connection: s, Count: egressConnections[e][p][s].Load()})
			}
		}
	}
	return out
}
