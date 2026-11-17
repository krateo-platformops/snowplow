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
// THE RULE. For a safe request, on both send paths (sendBudgeted;
// sendRetrying, the unbudgeted path), a 429 whose connection is closed is not
// an answer: the attempt is made again at once, on a fresh connection, once
// per send, and everything after it (the connect cut, the retry decision,
// Retry-After) runs as for a first attempt. So Retry-After applies only when
// the fresh attempt answers 429 too. A 429 whose connection stays open is
// handled exactly as before.
//
// WHAT THE SIGNAL IS, exactly: "the connection this 429 arrived on is closed".
//   - HTTP/1.x: Response.Close, set by net/http when the answer said
//     `Connection: close` — and also for an HTTP/1.0 answer without
//     keep-alive: such a 429 also reads as leaving (its connection is not
//     reused either, so the redo costs the fresh connect the next attempt
//     would have paid anyway).
//   - HTTP/2: the client transport closes the connection. Send closes (or, on
//     the budgeted path, reads) the 429's body first; a connection the server
//     sent GOAWAY on is then closed once it carries no stream, while an
//     ordinary one stays pooled. ANY close counts, not only a GOAWAY's: one
//     closed by CloseIdleConnections, the HTTP/2 health check or a broken
//     socket during the wait reads as leaving too, and its 429 is redone at
//     once on a fresh connection (the wait it skips was for an instance that
//     the closed connection no longer reaches).
//   WHY NOT GOAWAY ITSELF: x/net/http2 gives the http.Transport above it no
//   view of a GOAWAY. The ClientConn is owned by the transport's connection
//   pool, which net/http's HTTP/2 integration (http2.ConfigureTransports) does
//   not expose; ClientConn.State().Closing would not tell either, as it is also
//   true for a connection marked do-not-reuse after an error; and the GOAWAY
//   frame may reach the client after the 429's headers, so even a direct view
//   could not decide it when the answer arrives. The close is the first fact
//   the client can observe that a GOAWAY caused, and it reaches every waiter.
//
// WHEN THE SIGNAL IS WAITED FOR. On HTTP/2 the close comes a moment after the
// 429, so the redo waits for it, never longer than the send would wait anyway:
//   - with a retry the send will make, during its Retry-After wait (the redo
//     comes at the close or at the wait's end, whichever is first);
//   - sendRetrying's last attempt (no retry left; plumbing returns at once):
//     only a close that has already happened counts (#1521 B1);
//   - sendBudgeted, when Retry-After does not fit the budget left (owner ruling
//     on #1521: other 429s unchanged): only a signal ALREADY there when the
//     answer is processed counts (HTTP/1.x Connection: close; an HTTP/2
//     connection already closed). Nothing is waited for, so an ordinary 429
//     returns at once, as before; an HTTP/2 leaving answer whose close comes a
//     moment later is returned, not redone (the accepted cost).
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
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// endpointConn is a connection endpointDial returned. closed is closed when the
// transport (or anyone) closes it.
type endpointConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
	// onRead, while set, is called on the first read that returns bytes (a
	// raced TLS setup's progress, race_1509.go); one atomic load per read.
	onRead atomic.Pointer[func()]
}

// watchRead sets (f) or clears (nil) the progress callback.
func (c *endpointConn) watchRead(f func()) {
	if f == nil {
		c.onRead.Store(nil)
		return
	}
	c.onRead.Store(&f)
}

func (c *endpointConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		if f := c.onRead.Swap(nil); f != nil {
			(*f)()
		}
	}
	return n, err
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

// alreadyLeft reports whether a leaving signal has already fired (never waits).
func alreadyLeft(leaving <-chan struct{}) bool {
	if leaving == nil {
		return false
	}
	select {
	case <-leaving:
		return true
	default:
		return false
	}
}

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
