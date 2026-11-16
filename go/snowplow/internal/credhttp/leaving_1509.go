package credhttp

// leaving_1509.go — #1509 (a): a 429 that also closes its connection is the
// server leaving, not the server asking us to slow down.
//
// THE CASE. kube-apiserver's graceful shutdown answers every new request with
// 429, `Retry-After: 5` and `Connection: close` (k8s.io/apiserver
// pkg/server/filters/with_retry_after.go), "so client can retry after N
// seconds, hopefully on a new apiserver instance". On HTTP/1.1 the client sees
// the header (Response.Close). On HTTP/2 the header is not allowed on the wire;
// Go's server turns it into a GOAWAY (net/http's HTTP/2 server, writeChunk), and
// the client transport then closes that connection as soon as it carries no
// stream (golang.org/x/net/http2 ClientConn.forgetStreamID: closeOnIdle when
// goAway != nil). In run27 N2 one customer request honoured that 5 s on a
// regional control plane and then spent its budget on a fresh connect (#1509).
//
// THE RULE. For a safe request of a budgeted send, a 429 whose connection the
// protocol says is closing is not an answer: the attempt is made again at once,
// on a fresh connection, and everything after it (the connect cut, the retry
// decision, Retry-After) runs as for a first attempt. So Retry-After applies
// only when the fresh attempt answers 429 too. Every other 429 is handled
// exactly as before: its connection stays open, and the send waits Retry-After.
//
// THE SIGNALS, both the protocol's own, no timer:
//   - HTTP/1.x: Response.Close, set by net/http when the server sent
//     `Connection: close` (the connection will not be reused).
//   - HTTP/2: the connection the 429 arrived on is closed by the client
//     transport. Send closes the 429's body first; a connection the server sent
//     GOAWAY on is then closed once it carries no stream, while an ordinary
//     connection stays pooled. The wait for that close is the Retry-After wait
//     itself: the attempt is redone when the connection closes or when
//     Retry-After ends, whichever comes first, so no 429 ever waits longer than
//     it did before.
//
// The HTTP/2 signal needs the attempt's connection, which only the dialer
// snowplow owns can identify (endpointDial wraps every connection it returns).
// A connection it did not dial (a plumbing-built client, or an attempt answered
// by a request another reader had on the wire, #1320) gives no signal, and its
// 429 is handled as before.

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// endpointConn is a connection endpointDial returned. closed is closed when the
// transport (or anyone) closes it.
type endpointConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func newEndpointConn(c net.Conn) *endpointConn {
	return &endpointConn{Conn: c, closed: make(chan struct{})}
}

func (c *endpointConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

// NetConn returns the connection endpointConn wraps (as tls.Conn.NetConn does).
func (c *endpointConn) NetConn() net.Conn { return c.Conn }

// connClosedSignal returns the channel closed when the connection httptrace
// reported for an attempt (GotConnInfo.Conn) is closed, or nil when that
// connection was not dialled by endpointDial.
func connClosedSignal(c net.Conn) <-chan struct{} {
	for c != nil {
		switch v := c.(type) {
		case *endpointConn:
			return v.closed
		case *tls.Conn:
			c = v.NetConn()
		default:
			return nil
		}
	}
	return nil
}

// leavingSignal reports how a 429 answer says its server is leaving: closesNow
// when the protocol said so on the answer itself (HTTP/1.x Connection: close),
// else a channel closed when its connection is closed (HTTP/2 GOAWAY), else nil
// (no signal: the 429 is handled as before). Any non-429 answer has none.
func leavingSignal(resp *http.Response, conn net.Conn) (closesNow bool, closed <-chan struct{}) {
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		return false, nil
	}
	if resp.ProtoMajor < 2 {
		return resp.Close, nil
	}
	return false, connClosedSignal(conn)
}

// closedChan is an already-closed channel: a leaving signal that has fired
// (the HTTP/1.x answer said so itself).
var closedChan = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// sleepBudgetOrLeaving is sleepBudget that also ends when leaving is closed.
// left reports that it ended because of leaving (the caller redoes the attempt
// at once); ok is sleepBudget's: false when the request's own context ended or
// the budget ran out meanwhile.
func sleepBudgetOrLeaving(ctx context.Context, b *pagebudget.Budget, d time.Duration, leaving <-chan struct{}) (left, ok bool) {
	if leaving == nil {
		return false, sleepBudget(ctx, b, d)
	}
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-leaving:
			left = true
		case <-t.C:
		case <-ctx.Done():
			return false, false
		}
	} else {
		select {
		case <-leaving:
			left = true
		default:
		}
	}
	return left, ctx.Err() == nil && budgetNow().Before(b.Deadline())
}
