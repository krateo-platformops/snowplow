package credhttp

// race_1509.go — #1509 (b): a connection setup that is late is raced by
// another one on a new socket, instead of waiting on a SYN in the kernel's
// retransmit backoff or on a TLS handshake that never answers.
//
// THE CASE. A SYN that is lost (a listener whose accept queue is full, an
// endpoint mid-replacement) is sent again only when the socket's retransmit
// timer fires: 1 s after the first, then doubling (RFC 6298 §2.1, §5.5;
// Linux's initial RTO). A destination that is back after 4 s is therefore not
// reached before the 7 s retransmit. In run27 N2, connects completed ~3.1 s
// and ~7.1 s after they started. And a connect can succeed while the TLS
// handshake on it never completes: in run28 N1, 1,249 retries ended at the 10 s
// handshake bound with cause tls_handshake (#1509). A connection is usable
// only when its setup is done, so the race covers the whole setup: the connect
// and, for https, the TLS handshake.
//
// THE RULE. A setup that has not completed within this destination's measured
// setup time starts one more setup on a new socket (a new source port: a new
// SYN, a new connection-tracking / load-balancing decision, a new handshake).
// The first connection up wins; every other one is closed when it arrives. If
// that one is late too, the next one starts after twice the wait before it, as
// TCP doubles its own retransmit timer, so the attempts thin out at the rate
// the protocol already uses: at most log2(bound / trigger) + 1 sockets inside
// the dial's bound (PageMaxGoal from its start, endpointDial / endpointDialTLS).
//
// THE TRIGGER, measured, never tuned: the goal quantile (PageGoalQuantile) of
// this destination's recent successful setup times (connect, or connect + TLS),
// over the same window and by the same rule as the retry reserve (budget.go
// reserveRing). A destination with no sample yet uses the goal's typical bound
// (PageP95Goal), as the reserve does before it has one; a partial window reads
// its maximum. Plain and TLS setups of one address are separate destinations.
//
// EVERY ATTEMPT IS THE SAME DIAL. Each attempt's connect is endpointDialFn, the
// owned dialer with the destination policy (dialguard.Control, #1131) on every
// address, under the same deadline. A failed attempt ends the race only when no
// attempt is left: a fast refusal does not cancel a slower attempt that may
// still come up.
//
// WHAT THE REQUEST'S TRACE SEES: one setup, not one per attempt. The attempts
// run without the request's httptrace, and the race reports to it: ConnectStart
// when it starts; ConnectDone when the first attempt's connect succeeds, or
// with the error when none does; for TLS, TLSHandshakeStart at that first
// connect and, when no handshake succeeds, TLSHandshakeDone with the error. A
// successful handshake is reported by net/http itself, which runs (and finds
// done) the handshake of the connection a TLS dialer returns. So the retry
// cause (send.go), the slow-call phases and the connection counters
// (connections.go) read one setup per dial as before; a losing connection that
// came up is counted as opened directly (noteRacedConnectionUp).

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http/httptrace"
	"sync"
	"time"
)

// connectTimes is the per-destination setup-time window.
var connectTimes = &destinationConnectTimes{rings: map[string]*reserveRing{}}

type destinationConnectTimes struct {
	mu    sync.Mutex
	rings map[string]*reserveRing
}

func (d *destinationConnectTimes) ring(key string) *reserveRing {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, ok := d.rings[key]
	if !ok {
		r = &reserveRing{}
		d.rings[key] = r
	}
	return r
}

// trigger is how long a setup to key may take before another one is started:
// the goal quantile of key's recent setup times, or the goal's typical bound
// when none is measured (measured false).
func (d *destinationConnectTimes) trigger(key string) (time.Duration, bool) {
	return d.ring(key).reserve()
}

func (d *destinationConnectTimes) record(key string, took time.Duration) {
	d.ring(key).record(took)
}

func (d *destinationConnectTimes) resetForTest() {
	d.mu.Lock()
	d.rings = map[string]*reserveRing{}
	d.mu.Unlock()
}

// setupKey is the destination of a setup: the address the dial was asked for
// (host:port, before resolution), and whether the setup includes TLS.
func setupKey(addr string, withTLS bool) string {
	if withTLS {
		return "tls " + addr
	}
	return "tcp " + addr
}

// raceDial is the plain (connect-only) raced setup of addr.
func raceDial(ctx context.Context, network, addr string) (net.Conn, error) {
	return raceSetup(ctx, network, addr, nil)
}

// raceDialTLS is the raced connect + TLS handshake setup of addr, with cfg
// (its ServerName defaulting to addr's host, as net/http's own handshake does).
func raceDialTLS(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	return raceSetup(ctx, network, addr, cfg)
}

type racedConn struct {
	c      net.Conn
	err    error
	tlsErr bool // the connect succeeded and the TLS handshake failed
}

// raceSetup runs the race (see the file comment). tlsCfg nil: a plain setup.
// It returns the first connection up, or, when every attempt failed, the error
// of the latest phase any attempt reached (TLS over connect), first first.
func raceSetup(ctx context.Context, network, addr string, tlsCfg *tls.Config) (net.Conn, error) {
	trace := httptrace.ContextClientTrace(ctx)
	key := setupKey(addr, tlsCfg != nil)
	actx, cancel := withoutTrace(ctx)
	defer cancel()

	results := make(chan racedConn)
	tcpUp := make(chan struct{}, 1)
	attempt := func() {
		start := dialNow()
		c, err := endpointDialFn(actx, network, addr)
		if err != nil {
			results <- racedConn{err: err}
			return
		}
		select {
		case tcpUp <- struct{}{}:
		default:
		}
		conn := net.Conn(newEndpointConn(c))
		if tlsCfg != nil {
			cfg := tlsCfg.Clone()
			if cfg.ServerName == "" {
				if host, _, herr := net.SplitHostPort(addr); herr == nil {
					cfg.ServerName = host
				}
			}
			tc := tls.Client(conn, cfg)
			if err := tc.HandshakeContext(actx); err != nil {
				_ = tc.Close()
				results <- racedConn{err: err, tlsErr: true}
				return
			}
			conn = tc
		}
		connectTimes.record(key, dialNow().Sub(start))
		results <- racedConn{c: conn}
	}

	if trace != nil && trace.ConnectStart != nil {
		trace.ConnectStart(network, addr)
	}
	go attempt()
	outstanding := 1
	connected := false
	onConnected := func() {
		if connected {
			return
		}
		connected = true
		if trace != nil && trace.ConnectDone != nil {
			trace.ConnectDone(network, addr, nil)
		}
		if tlsCfg != nil && trace != nil && trace.TLSHandshakeStart != nil {
			trace.TLSHandshakeStart()
		}
	}

	wait, _ := connectTimes.trigger(key)
	timer := time.NewTimer(wait)
	defer timer.Stop()

	var firstConnErr, firstTLSErr error
	for {
		select {
		case <-tcpUp:
			onConnected()
		case r := <-results:
			outstanding--
			if r.err == nil {
				onConnected()
				go closeLosers(ctx, results, outstanding, tlsCfg != nil)
				return r.c, nil
			}
			if r.tlsErr {
				onConnected()
				if firstTLSErr == nil {
					firstTLSErr = r.err
				}
			} else if firstConnErr == nil {
				firstConnErr = r.err
			}
			if outstanding == 0 {
				if firstTLSErr != nil {
					if trace != nil && trace.TLSHandshakeDone != nil {
						trace.TLSHandshakeDone(tls.ConnectionState{}, firstTLSErr)
					}
					return nil, firstTLSErr
				}
				if trace != nil && trace.ConnectDone != nil {
					trace.ConnectDone(network, addr, firstConnErr)
				}
				return nil, firstConnErr
			}
		case <-timer.C:
			if actx.Err() != nil {
				continue // the dial's bound ended: the attempts racing return by it
			}
			go attempt()
			outstanding++
			wait *= 2
			timer.Reset(wait)
		}
	}
}

// withoutTrace returns a context with ctx's deadline and cancellation but none
// of its values, so the attempts report nothing to the request's httptrace
// (the race reports for them).
func withoutTrace(ctx context.Context) (context.Context, context.CancelFunc) {
	var actx context.Context
	var cancel context.CancelFunc
	if dl, ok := ctx.Deadline(); ok {
		actx, cancel = context.WithDeadline(context.Background(), dl)
	} else {
		actx, cancel = context.WithCancel(context.Background())
	}
	// The deadline is ctx's own, set above: an attempt the deadline ends reads
	// it (DeadlineExceeded), as it would on ctx. Only a cancellation of ctx is
	// carried over.
	stop := context.AfterFunc(ctx, func() {
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cancel()
		}
	})
	return actx, func() {
		stop()
		cancel()
	}
}

// closeLosers receives the n attempts still racing and closes every connection
// one of them brings up, counting it as opened (it was accepted and paid for).
func closeLosers(ctx context.Context, results <-chan racedConn, n int, withTLS bool) {
	for range n {
		r := <-results
		if r.err != nil {
			continue
		}
		negotiated := ""
		if tc, ok := r.c.(*tls.Conn); ok {
			negotiated = tc.ConnectionState().NegotiatedProtocol
		}
		noteRacedConnectionUp(ctx, withTLS, negotiated)
		_ = r.c.Close()
	}
}
