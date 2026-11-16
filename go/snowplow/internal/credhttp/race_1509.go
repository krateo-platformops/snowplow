package credhttp

// race_1509.go — #1509 (b): a connect that is late is raced by another one on
// a new socket, instead of waiting on a SYN in the kernel's retransmit backoff.
//
// THE CASE. A SYN that is lost (a listener whose accept queue is full, an
// endpoint mid-replacement) is sent again only when the socket's retransmit
// timer fires: 1 s after the first, then doubling (RFC 6298 §2.1, §5.5;
// Linux's initial RTO). A destination that is back after 4 s is therefore not
// reached before the 7 s retransmit, whatever happened at 4 s. In run27 N2,
// connects completed ~3.1 s and ~7.1 s after they started, the second and third
// SYN instants (#1509).
//
// THE RULE. A connect that has not completed within this destination's
// measured connect time starts one more connect, on a new socket (a new source
// port: a new SYN, and a new connection-tracking / load-balancing decision on
// the way). The first connection up wins; every other one is closed when it
// arrives. If that one is late too, the next one starts after twice the wait
// before it, as TCP doubles its own retransmit timer: the attempts thin out at
// the rate the protocol already uses, and every racing attempt stays inside the
// dial's bound (PageMaxGoal from its start, endpointDial).
//
// THE TRIGGER, measured, never tuned: the goal quantile (PageGoalQuantile) of
// this destination's recent successful connect times, over the same window and
// by the same rule as the retry reserve (budget.go reserveRing). A destination
// with no sample yet uses the goal's typical bound (PageP95Goal), as the
// reserve does before it has one; a partial window reads its maximum.
//
// EVERY ATTEMPT IS THE SAME DIAL. Each racing connect is endpointDialFn, the
// owned dialer with the destination policy (dialguard.Control, #1131) on every
// address, under the same deadline. A failed attempt ends the race only when no
// attempt is left: a fast refusal does not cancel a slower attempt that may
// still connect.

import (
	"context"
	"net"
	"sync"
	"time"
)

// connectTimes is the per-destination connect-time window, keyed by the
// address the dial was asked for (host:port, before resolution).
var connectTimes = &destinationConnectTimes{rings: map[string]*reserveRing{}}

type destinationConnectTimes struct {
	mu    sync.Mutex
	rings map[string]*reserveRing
}

func (d *destinationConnectTimes) ring(addr string) *reserveRing {
	d.mu.Lock()
	defer d.mu.Unlock()
	r, ok := d.rings[addr]
	if !ok {
		r = &reserveRing{}
		d.rings[addr] = r
	}
	return r
}

// trigger is how long a connect to addr may take before another one is
// started: the goal quantile of addr's recent connect times, or the goal's
// typical bound when none is measured (measured false).
func (d *destinationConnectTimes) trigger(addr string) (time.Duration, bool) {
	return d.ring(addr).reserve()
}

func (d *destinationConnectTimes) record(addr string, took time.Duration) {
	d.ring(addr).record(took)
}

func (d *destinationConnectTimes) resetForTest() {
	d.mu.Lock()
	d.rings = map[string]*reserveRing{}
	d.mu.Unlock()
}

type racedConn struct {
	c   net.Conn
	err error
}

// raceDial dials addr with endpointDialFn under ctx (the dial's bound), racing
// late attempts as the file comment says. It returns the first connection up,
// or, when every attempt failed, the first attempt's error.
func raceDial(ctx context.Context, network, addr string) (net.Conn, error) {
	results := make(chan racedConn)
	attempt := func() {
		start := dialNow()
		c, err := endpointDialFn(ctx, network, addr)
		if err == nil {
			connectTimes.record(addr, dialNow().Sub(start))
		}
		results <- racedConn{c: c, err: err}
	}
	go attempt()
	outstanding := 1

	wait, _ := connectTimes.trigger(addr)
	timer := time.NewTimer(wait)
	defer timer.Stop()

	var firstErr error
	for {
		select {
		case r := <-results:
			outstanding--
			if r.err == nil {
				// The winner. Every attempt still racing is closed when it ends.
				go closeLosers(results, outstanding)
				return r.c, nil
			}
			if firstErr == nil {
				firstErr = r.err
			}
			if outstanding == 0 {
				return nil, firstErr
			}
		case <-timer.C:
			if ctx.Err() != nil {
				continue // the dial's bound ended: the attempts racing return by it
			}
			go attempt()
			outstanding++
			wait *= 2
			timer.Reset(wait)
		}
	}
}

// closeLosers receives the n attempts still racing and closes every connection
// one of them brings up.
func closeLosers(results <-chan racedConn, n int) {
	for range n {
		if r := <-results; r.err == nil {
			_ = r.c.Close()
		}
	}
}
