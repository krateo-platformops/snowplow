package credhttp

// race_1509.go — #1509 (b): a connection setup that is late is raced by
// another one on a new socket, instead of waiting on a SYN in the kernel's
// retransmit backoff or on a TLS handshake that never answers; and every setup
// waiting on one destination joins ONE race (owner ruling on #1509), so a
// struggling destination sees a bounded number of attempts however many
// requests wait on it.
//
// THE CASE. A SYN that is lost (an endpoint mid-replacement) is sent again
// only when the socket's retransmit timer fires: 1 s after the first, then
// doubling (RFC 6298 §2.1, §5.5; Linux's initial RTO). A destination that is
// back after 4 s is therefore not reached before the 7 s retransmit. In run27
// N2, connects completed ~3.1 s and ~7.1 s after they started. And a connect
// can succeed while the TLS handshake on it never completes: in run28 N1, 1,249
// retries ended at the 10 s handshake bound with cause tls_handshake (#1509). A
// connection is usable only when its setup is done, so the race covers the
// whole setup: the connect and, for https, the TLS handshake.
//
// THE CHAIN. One chain per destination runs the race for every setup waiting
// on it (a waiter):
//   - the first waiter starts it with one attempt; a later waiter JOINS it
//     instead of starting attempts of its own;
//   - while no attempt has brought a connection up within the destination's
//     measured setup time (the trigger), one more attempt starts on a new
//     socket (a new source port: a new SYN, a new connection-tracking /
//     load-balancing decision, a new handshake), after twice the wait before
//     it each time, as TCP doubles its own retransmit timer. So a stuck
//     destination sees at most log2(bound / trigger) + 1 attempts per attempt
//     bound, however many requests wait on it;
//   - each connection that comes up goes to ONE waiter, the longest waiting;
//   - once a connection has come up, the destination is ANSWERING (#1521):
//     every waiter left, and every one that joins, is covered by an attempt —
//     a live one, or a new one started at once, exactly as net/http dials one
//     connection per waiting request — and no race attempt is added while
//     the destination answers. So the setups a destination sees beyond one
//     per waiting request are at most the race's own attempts (racesFiredAt:
//     each fired after a trigger interval with no evidence — before the
//     first answer, a setup slower than the trigger; while answering, an
//     interval in which nothing came up and no handshake read bytes), plus
//     the attempts replaced when an answering period begins (replacedAt,
//     below), plus the connects that stopped covering while still connecting
//     (stuckDroppedAt: each can cost its waiter one more attempt, whether or
//     not the stuck connect completes later); and many waiters are served in
//     parallel, not one after another;
//   - "answering" holds only on FRESH evidence: within every trigger interval
//     some attempt must come up or show progress (a TLS attempt reading its
//     server's handshake bytes in that interval: a handshake still receiving
//     bytes is evidence in every interval it receives them, #1521 review).
//     An interval with neither means the destination stalled again — a busy
//     destination's later control-plane change, with waiters that never let
//     the chain empty — and the chain returns to racing
//     for the waiters still waiting, with the same bound, from the trigger. No
//     new number: the interval is the measured trigger;
//   - demand is covered per ATTEMPT (owner decision on #1521, "rule D"). The
//     chain's answering PERIODS are numbered: one begins each time the
//     destination answers after not answering (its first answer, and each
//     answer after the chain returned to racing). An attempt whose TCP
//     connect has completed covers a waiter while the period it connected in
//     lasts — until it comes up, fails, is cancelled or reaches its own
//     bound, whatever intervals pass. When a new period begins, every
//     attempt that connected before it stops covering, and the waiters it
//     covered get attempts of their own at once (the replaced attempts are
//     counted, replacedAt). An attempt still connecting stops covering after
//     a trigger interval with no evidence of its own (it neither started nor
//     connected in it: a connect stuck in SYN backoff through a stall), and
//     covers again when its connect completes. So within a period a
//     connected handshake waiting on a busy server is never replaced (#1521
//     review: dropping such handshakes as cover at every interval started a
//     replacement for each: 37 of 200 solo-warmed bursts), and a handshake
//     the destination never answered during a stall covers nobody after the
//     comeback (#1521 review: under "rule B", which kept it, up to 15
//     waiters per run were left to the race's attempts one at a time and 118
//     of them hit their deadline in 12 of 30 runs on 2 CPUs; rule D: 0 in
//     30). The cost: a burst whose chain goes quiet for an interval mid-burst
//     replaces its live handshakes once at the next answer (2 of 200
//     solo-warmed bursts, 0 of 200 warm ones);
//   - each waiter leaves at its own deadline or cancellation, whatever the
//     chain does; a connection that comes up with nobody waiting is closed
//     (counted as opened, noteRacedConnectionUp); and when the last waiter
//     leaves, the chain cancels its attempts and ends: the next setup starts a
//     new one;
//   - a failed attempt ends the chain only when no attempt is left (a fast
//     refusal does not cancel a slower attempt that may still come up), and
//     then every waiter gets that failure.
//
// EVERY ATTEMPT IS THE SAME DIAL, BOUNDED AS BEFORE. Each attempt's connect is
// endpointDialFn, the owned dialer with the destination policy
// (dialguard.Control, #1131) on every address, and gives up PageMaxGoal after
// it starts, whoever started it (#1115).
//
// TWO LEVELS. A connect carries no identity, so connect chains are per
// address, process-wide: every https setup's connect joins its address's
// connect chain too, so the SYNs a destination sees are bounded across every
// identity. A TLS handshake carries the identity (the client certificate), so
// its connection can only go to a setup with the same TLS configuration: TLS
// chains are per (TLS configuration, address), i.e. per kept identity client.
// A TLS attempt takes its connection from the connect chain, then handshakes.
//
// THE TRIGGER, measured, never tuned: the goal quantile (PageGoalQuantile) of
// the destination's recent successful setup times (connect, or connect + TLS),
// over the same window and by the same rule as the retry reserve (budget.go
// reserveRing) — once the window holds enough setups to estimate that
// quantile (owner ruling on #1521, "after enough samples"). Until then the
// goal's typical bound (PageP95Goal) applies, as for a destination never
// measured. Enough is set by the quantile itself: the fewest samples n in
// which the q-quantile is not simply the window's maximum, i.e. at least one
// sample lies above it: floor(n·(1−q)) ≥ 1, n = ⌈1/(1−q)⌉ = 20 for q = 0.95
// (triggerMinSamples, computed with the reserve's own rounding, quantileAbove).
// Fewer samples are not a quantile: one solo setup (≈2.5 ms on the build VM)
// read as the trigger raced every handshake of a 25-request burst, whose
// concurrent setups take 2–3× a solo one (#1521 review: p50 7.4 ms, p95 10 ms
// vs a solo p50 3.0 ms), and doubled the burst's setups. The window holds
// reserveWindow samples, derived from the same quantile, so a full window is
// exactly enough (an init check fails the build's tests otherwise).
//
// WHAT THE REQUEST'S TRACE SEES: one setup, not one per attempt. The attempts
// run without the request's httptrace, and the chain reports to each waiter:
// ConnectStart when it waits; ConnectDone once a connect is up (or with the
// error when none came up); for TLS, TLSHandshakeStart at that point and, when
// the chain fails in TLS, TLSHandshakeDone with the error. A successful
// handshake is reported by net/http itself, which runs (and finds done) the
// handshake of the connection a TLS dialer returns. So the retry cause
// (send.go), the slow-call phases and the connection counters (connections.go)
// read one setup per dial as before.

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// raceCounts counts the race's own attempts per destination window (setupKey:
// the address and the level), each one an attempt started because the setups
// before it ran past the trigger with no evidence (onTimer). Every other
// attempt is a waiter's own: the chain's first, or one covering a waiter, of
// which a waiter gets a second only when its first stopped covering — while
// still connecting (a connect stuck through an interval), or because it
// connected before a new answering period began (counted in replaceCounts,
// replacedAt). So the setups a destination sees beyond one per waiting
// request are at most the races fired AT THAT DESTINATION (racesFiredAt),
// plus the attempts replaced there at a new period (replacedAt), plus the
// connects that stopped covering there while still connecting
// (stuckDroppedAt); a count across destinations would let another
// destination's races excuse this one's extra setups.
var raceCounts sync.Map // setupKey → *atomic.Uint64

// replaceCounts counts, per destination window, the attempts started at a new
// answering period because attempts that connected before it stopped
// covering (rule D): the starts beyond what the waiters needed without that.
var replaceCounts sync.Map // setupKey → *atomic.Uint64

// stuckCounts counts, per destination window, the attempts that stopped
// covering while still connecting (onTimer: a trigger interval with no
// evidence of their own). Each leaves its waiter uncovered, so the waiter can
// get one more attempt (coverDemandLocked), whether or not the stuck connect
// completes later (#1536: the burst bound omitted this term, and a burst on 2
// CPUs under -race, whose connects outlast the trigger, read as a doubling).
var stuckCounts sync.Map // setupKey → *atomic.Uint64

func noteRaceFired(key string) { countAt(&raceCounts, key, 1) }

func countAt(m *sync.Map, key string, n uint64) {
	v, _ := m.LoadOrStore(key, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(n)
}

func countsAt(m *sync.Map, addr string) uint64 {
	var n uint64
	for _, key := range []string{setupKey(addr, false), setupKey(addr, true)} {
		if v, ok := m.Load(key); ok {
			n += v.(*atomic.Uint64).Load()
		}
	}
	return n
}

// racesFiredAt is how many race attempts addr's chains have fired, connect
// and TLS levels together.
func racesFiredAt(addr string) uint64 { return countsAt(&raceCounts, addr) }

// replacedAt is how many attempts addr's chains started at a new answering
// period to replace attempts that connected before it, both levels.
func replacedAt(addr string) uint64 { return countsAt(&replaceCounts, addr) }

// stuckDroppedAt is how many of addr's attempts stopped covering while still
// connecting, both levels.
func stuckDroppedAt(addr string) uint64 { return countsAt(&stuckCounts, addr) }

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
	r := d.ring(key)
	if r.count() < triggerMinSamples {
		return pagebudget.PageP95Goal, false
	}
	return r.reserve()
}

// triggerMinSamples is the fewest setups in a destination's window from which
// its trigger is measured (see the file comment): the smallest n at which the
// quantile's sample is not the window's maximum (quantileAbove(n) ≥ 1).
var triggerMinSamples = func() int {
	n := 1
	for quantileAbove(n) < 1 {
		n++
	}
	return n
}()

func (d *destinationConnectTimes) record(key string, took time.Duration) {
	d.ring(key).record(took)
}

func (d *destinationConnectTimes) resetForTest() {
	d.mu.Lock()
	d.rings = map[string]*reserveRing{}
	d.mu.Unlock()
}

// setupKey is the destination of a setup's timing window: the address the
// dial was asked for (host:port, before resolution), and whether the setup
// includes TLS.
func setupKey(addr string, withTLS bool) string {
	if withTLS {
		return "tls " + addr
	}
	return "tcp " + addr
}

type racedConn struct {
	c      net.Conn
	err    error
	tlsErr bool // the connect succeeded and the TLS handshake failed
}

// chainKey names a chain: its address and, for TLS, its configuration (by
// identity: a kept client's transport holds one *tls.Config for its life).
type chainKey struct {
	network, addr string
	tlsCfg        *tls.Config
}

// setupChains is the chains of one level.
type setupChains struct {
	mu     sync.Mutex
	chains map[chainKey]*setupChain
}

// dialFunc is a connect, as endpointDialFn.
type dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// connectChains is the process-wide connect level (a connect has no identity).
var connectChains = &setupChains{chains: map[chainKey]*setupChain{}}

// tlsChains is the TLS level, keyed by configuration.
var tlsChains = &setupChains{chains: map[chainKey]*setupChain{}}

// raceDial is the plain (connect-only) raced setup of addr, bounded by ctx.
func raceDial(ctx context.Context, network, addr string) (net.Conn, error) {
	return connectChains.dial(ctx, chainKey{network: network, addr: addr}, endpointDialFn)
}

// raceDialTLS is the raced connect + TLS handshake setup of addr, with cfg
// (its ServerName defaulting to addr's host, as net/http's own handshake does).
func raceDialTLS(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	return tlsChains.dial(ctx, chainKey{network: network, addr: addr, tlsCfg: cfg}, endpointDialFn)
}

// dial waits on k's chain (starting it if none runs) until a connection comes
// up for this waiter, the chain fails, or ctx ends. connect is the connect a
// chain started here uses (endpointDialFn, read by the caller): attempts run
// on goroutines of their own and never read the package-level seam.
//
// No waiter is ever added to a done chain: join refuses a done chain under
// the chain's own lock, the same lock under which a chain is finished, so
// "is it done" and "add the waiter" are one step. On a refusal dial starts a
// new chain (still under s.mu, so no other dial can race it); a new chain is
// never done before its first join, which starts its first attempt. (#1521
// review: checking done and joining as two steps let an attempt finish the
// chain in between, and the waiter then waited out its whole budget on a
// chain that would never start an attempt for it.)
func (s *setupChains) dial(ctx context.Context, k chainKey, connect dialFunc) (net.Conn, error) {
	s.mu.Lock()
	var w *chainWaiter
	c := s.chains[k]
	if c != nil {
		if f := joinGapForTest.Load(); f != nil {
			(*f)(c)
		}
		w = c.join(ctx)
	}
	if w == nil {
		c = newSetupChain(s, k, connect)
		s.chains[k] = c
		w = c.join(ctx)
	}
	s.mu.Unlock()

	select {
	case r := <-w.ch:
		return r.c, r.err
	case <-ctx.Done():
		r, delivered := c.leave(w)
		if delivered {
			if r.err == nil {
				c.giveBack(r.c)
			}
			// The chain already reported this waiter's outcome to its trace.
			return nil, ctx.Err()
		}
		// Left before the chain ended: its setup ended by its deadline, in
		// the phase it was in, reported as a dial's own timeout would be.
		c.reportLeft(w, ctx.Err())
		return nil, ctx.Err()
	}
}

// reportLeft tells a waiter's trace that its setup ended with err in the phase
// it had reached (TLS once a connect came up for it, else the connect).
func (c *setupChain) reportLeft(w *chainWaiter, err error) {
	c.mu.Lock()
	notified := w.notified
	c.mu.Unlock()
	if w.trace == nil {
		return
	}
	if notified && c.k.tlsCfg != nil {
		if w.trace.TLSHandshakeDone != nil {
			w.trace.TLSHandshakeDone(tls.ConnectionState{}, err)
		}
		return
	}
	if !notified && w.trace.ConnectDone != nil {
		w.trace.ConnectDone(c.k.network, c.k.addr, err)
	}
}

func (s *setupChains) forget(k chainKey, c *setupChain) {
	s.mu.Lock()
	if s.chains[k] == c {
		delete(s.chains, k)
	}
	s.mu.Unlock()
}

type chainWaiter struct {
	ch       chan racedConn // buffered 1: the chain never blocks on a waiter
	trace    *httptrace.ClientTrace
	notified bool // ConnectDone (and, for TLS, TLSHandshakeStart) reported
}

type setupChain struct {
	owner   *setupChains
	k       chainKey
	ringKey string
	dial    dialFunc // the connect its attempts use (endpointDialFn when the chain began)

	ctx    context.Context // ended when the chain ends: its attempts stop
	cancel context.CancelFunc

	mu          sync.Mutex
	done        bool
	waiters     []*chainWaiter
	outstanding int
	// attempts are the outstanding attempts; covering counts those that cover
	// a waiter (attemptState.covers).
	attempts  map[*attemptState]struct{}
	covering  int
	wait      time.Duration
	timer     *time.Timer
	connected bool // some attempt's connect came up
	answered  bool // a setup completed recently: attempts follow demand, no race
	progress  bool // since the last check: an attempt came up or read handshake bytes
	firstConn error
	firstTLS  error
	countCtx  context.Context // a waiter's ctx: its connection counting (connections.go)
	period    int             // answering periods begun (rule D)
}

func newSetupChain(s *setupChains, k chainKey, connect dialFunc) *setupChain {
	ctx, cancel := context.WithCancel(context.Background())
	return &setupChain{owner: s, k: k, ringKey: setupKey(k.addr, k.tlsCfg != nil), ctx: ctx, cancel: cancel, dial: connect,
		attempts: map[*attemptState]struct{}{}}
}

// attemptState is one outstanding attempt's cover state (see the file
// comment): connected is whether its TCP connect has completed, and period
// the chain's answering period then; evidence is whether it started since the
// chain's last interval check (it only matters while connecting); covers is
// whether it counts as a waiter's cover.
type attemptState struct {
	connected, evidence, covers bool
	period                      int // the chain's answering period when it connected
}

// joinGapForTest, when set (tests only), runs between dial finding a chain and
// joining it: where a chain could once finish unseen.
var joinGapForTest atomic.Pointer[func(*setupChain)]

// join adds a waiter; the first one starts the chain. It refuses (nil) a
// chain that is done: no waiter is ever added to a done chain.
func (c *setupChain) join(ctx context.Context) *chainWaiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return nil
	}
	w := &chainWaiter{ch: make(chan racedConn, 1), trace: httptrace.ContextClientTrace(ctx)}
	if w.trace != nil && w.trace.ConnectStart != nil {
		w.trace.ConnectStart(c.k.network, c.k.addr)
	}
	c.waiters = append(c.waiters, w)
	c.countCtx = ctx
	c.notifyLocked(w)
	switch {
	case c.answered:
		c.coverDemandLocked()
	case c.outstanding == 0 && c.timer == nil:
		c.wait, _ = connectTimes.trigger(c.ringKey)
		c.startAttemptLocked()
		c.timer = time.AfterFunc(c.wait, c.onTimer)
	}
	return w
}

// coverDemandLocked gives every waiter an attempt of its own, once the
// destination has answered (see the file comment).
func (c *setupChain) coverDemandLocked() {
	for !c.done && c.covering < len(c.waiters) {
		c.startAttemptLocked()
	}
}

// leave removes w; delivered reports that the chain had already handed it r.
func (c *setupChain) leave(w *chainWaiter) (r racedConn, delivered bool) {
	c.mu.Lock()
	found := false
	for i, x := range c.waiters {
		if x == w {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			found = true
			break
		}
	}
	if len(c.waiters) == 0 {
		c.finishLocked()
	}
	c.mu.Unlock()
	if found {
		return racedConn{}, false
	}
	return <-w.ch, true
}

// giveBack hands a connection a waiter could not take to the next waiter, or
// closes it.
func (c *setupChain) giveBack(conn net.Conn) {
	c.mu.Lock()
	if w := c.popLocked(); w != nil {
		c.deliverLocked(w, racedConn{c: conn})
		c.mu.Unlock()
		return
	}
	countCtx := c.countCtx
	c.mu.Unlock()
	closeLoser(countCtx, conn, c.k.tlsCfg != nil)
}

func (c *setupChain) popLocked() *chainWaiter {
	if len(c.waiters) == 0 {
		return nil
	}
	w := c.waiters[0]
	c.waiters = c.waiters[1:]
	return w
}

func (c *setupChain) notifyLocked(w *chainWaiter) {
	if w.notified || !c.connected {
		return
	}
	w.notified = true
	if w.trace == nil {
		return
	}
	if w.trace.ConnectDone != nil {
		w.trace.ConnectDone(c.k.network, c.k.addr, nil)
	}
	if c.k.tlsCfg != nil && w.trace.TLSHandshakeStart != nil {
		w.trace.TLSHandshakeStart()
	}
}

func (c *setupChain) deliverLocked(w *chainWaiter, r racedConn) {
	if r.err == nil {
		c.connected = true
		c.notifyLocked(w)
	} else if w.trace != nil {
		if r.tlsErr {
			c.notifyLocked(w)
			if w.trace.TLSHandshakeDone != nil {
				w.trace.TLSHandshakeDone(tls.ConnectionState{}, r.err)
			}
		} else if !w.notified && w.trace.ConnectDone != nil {
			w.trace.ConnectDone(c.k.network, c.k.addr, r.err)
		}
	}
	w.ch <- r
}

func (c *setupChain) onConnected() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = true
	for _, w := range c.waiters {
		c.notifyLocked(w)
	}
}

func (c *setupChain) startAttemptLocked() {
	c.outstanding++
	a := &attemptState{evidence: true, covers: true}
	c.attempts[a] = struct{}{}
	c.covering++
	answered := c.answered
	actx, acancel := context.WithDeadline(c.ctx, dialNow().Add(pagebudget.PageMaxGoal))
	go func() {
		defer acancel()
		start := dialNow()
		conn, err, tlsErr := c.attempt(actx, answered, a)
		if err == nil {
			connectTimes.record(c.ringKey, dialNow().Sub(start))
		}
		c.result(racedConn{c: conn, err: err, tlsErr: tlsErr}, a)
	}()
}

// attempt is one setup on a new socket: the connect (endpointDialFn) for a
// connect chain; for a TLS chain, a connection from the address's connect
// chain, then the handshake. Once the TLS chain's destination has answered
// (answered: the attempt serves demand, not the race), its connect is its own,
// like the attempt itself: the connect chain bounds racing, and there is no
// race left to bound.
func (c *setupChain) attempt(ctx context.Context, answered bool, a *attemptState) (net.Conn, error, bool) {
	if c.k.tlsCfg == nil || answered {
		raw, err := c.dial(ctx, c.k.network, c.k.addr)
		if err != nil {
			return nil, err, false
		}
		c.onAttemptConnected(a)
		c.onConnected()
		conn := net.Conn(newEndpointConn(raw))
		if c.k.tlsCfg == nil {
			return conn, nil, false
		}
		return c.handshake(ctx, conn)
	}
	conn, err := connectChains.dial(ctx, chainKey{network: c.k.network, addr: c.k.addr}, c.dial)
	if err != nil {
		return nil, err, false
	}
	c.onAttemptConnected(a)
	c.onConnected()
	return c.handshake(ctx, conn)
}

// handshake runs the TLS handshake of a TLS chain's attempt on conn.
func (c *setupChain) handshake(ctx context.Context, conn net.Conn) (net.Conn, error, bool) {
	if ec, ok := conn.(*endpointConn); ok {
		ec.watchRead(c.onProgress)
		defer ec.watchRead(nil)
	}
	cfg := c.k.tlsCfg.Clone()
	if cfg.ServerName == "" {
		if host, _, herr := net.SplitHostPort(c.k.addr); herr == nil {
			cfg.ServerName = host
		}
	}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		if tc.ConnectionState().HandshakeComplete {
			// #1544: the handshake COMPLETED and then the attempt's ctx ended (its chain finished, its last waiter
			// served by another attempt: result -> finishLocked -> c.cancel). crypto/tls's HandshakeContext then
			// returns the ctx's error for a handshake that completed, so this connection was set up, the server
			// counted it, and nothing counted it opened. It is a raced connection like any other loser: counted
			// opened with the chain's counting (closeLoser), then closed. (TLS 1.2 keeps a residual: there the
			// server finishes first, so a server-complete, client-incomplete setup is still not counted;
			// client-go to the apiserver negotiates TLS 1.3.)
			c.mu.Lock()
			countCtx := c.countCtx
			c.mu.Unlock()
			closeLoser(countCtx, tc, true)
			return nil, err, true
		}
		_ = tc.Close()
		return nil, err, true
	}
	return tc, nil, false
}

// result takes attempt a's outcome.
func (c *setupChain) result(r racedConn, a *attemptState) {
	c.mu.Lock()
	c.outstanding--
	delete(c.attempts, a)
	if a.covers {
		c.covering--
	}
	if r.err == nil {
		w := c.popLocked()
		if w == nil {
			countCtx := c.countCtx
			c.mu.Unlock()
			closeLoser(countCtx, r.c, c.k.tlsCfg != nil)
			return
		}
		c.deliverLocked(w, r)
		// The destination answered: every waiter left is covered by an attempt
		// (a live one, or a new one), and the timer now watches for fresh
		// evidence (onTimer). If it was not answering, a new answering period
		// begins: attempts that connected before it stop covering (rule D).
		began := !c.answered
		if began {
			c.beginPeriodLocked()
		}
		c.answered = true
		c.progress = true
		if c.timer != nil {
			trigger, _ := connectTimes.trigger(c.ringKey)
			c.timer.Reset(trigger)
		}
		if len(c.waiters) == 0 {
			c.finishLocked()
		} else {
			c.coverDemandLocked()
		}
		if f := newPeriodForTest.Load(); began && f != nil {
			(*f)(c)
		}
		c.mu.Unlock()
		return
	}
	if r.tlsErr {
		if c.firstTLS == nil {
			c.firstTLS = r.err
		}
	} else if c.firstConn == nil {
		c.firstConn = r.err
	}
	if c.outstanding == 0 && !c.done {
		// No attempt left: every waiter gets the failure of the latest phase
		// any attempt reached (TLS over connect), the first of its kind.
		fail := racedConn{err: c.firstConn}
		if c.firstTLS != nil {
			fail = racedConn{err: c.firstTLS, tlsErr: true}
		}
		for _, w := range c.waiters {
			c.deliverLocked(w, fail)
		}
		c.waiters = nil
		c.finishLocked()
	}
	c.mu.Unlock()
}

// beginPeriodLocked starts a new answering period: every attempt that
// connected before it stops covering, and the starts that costs (beyond what
// the waiters needed anyway) are counted as replacements (replacedAt).
func (c *setupChain) beginPeriodLocked() {
	c.period++
	before := c.covering
	for a := range c.attempts {
		if a.covers && a.connected && a.period < c.period {
			a.covers = false
			c.covering--
		}
	}
	need := func(covering int) int { return max(0, len(c.waiters)-covering) }
	if n := need(c.covering) - need(before); n > 0 {
		countAt(&replaceCounts, c.ringKey, uint64(n))
	}
}

// newPeriodForTest, when set (tests only), runs (under c.mu) once a new
// answering period has begun and its waiters are covered: at the end of the
// result that began it.
var newPeriodForTest atomic.Pointer[func(*setupChain)]

func (c *setupChain) onTimer() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done || len(c.waiters) == 0 {
		return
	}
	// Per attempt: one still connecting with no evidence in the interval just
	// ended stops covering; a connected one always covers. Evidence starts
	// over for the next interval.
	for a := range c.attempts {
		if !a.connected && !a.evidence && a.covers {
			a.covers = false
			c.covering--
			countAt(&stuckCounts, c.ringKey, 1)
		}
		a.evidence = false
	}
	if c.answered {
		trigger, _ := connectTimes.trigger(c.ringKey)
		if c.progress {
			// Fresh evidence in the last interval: still answering.
			c.progress = false
			c.timer.Reset(trigger)
			return
		}
		// An interval with no attempt up and no progress while waiters wait:
		// the destination stalled again. Back to racing, from the trigger.
		c.answered = false
		c.wait = trigger
	}
	noteRaceFired(c.ringKey)
	c.startAttemptLocked()
	c.wait *= 2
	c.timer.Reset(c.wait)
}

// onProgress records an attempt reading its server's handshake bytes (each
// read): the destination answers even if no setup has completed yet. (The
// attempt is connected, so it covers already: onAttemptConnected.)
func (c *setupChain) onProgress() {
	c.mu.Lock()
	c.progress = true
	c.mu.Unlock()
}

// onAttemptConnected records attempt a's TCP connect completing: from now on
// it covers a waiter for the rest of the current answering period (rule D),
// again if it had stopped while connecting.
func (c *setupChain) onAttemptConnected(a *attemptState) {
	c.mu.Lock()
	a.connected, a.period = true, c.period
	if _, live := c.attempts[a]; live && !a.covers {
		a.covers = true
		c.covering++
	}
	c.mu.Unlock()
}

// finishLocked ends the chain: its attempts are cancelled, and the next setup
// starts a new chain.
func (c *setupChain) finishLocked() {
	if c.done {
		return
	}
	c.done = true
	c.cancel()
	if c.timer != nil {
		c.timer.Stop()
	}
	go c.owner.forget(c.k, c)
}

// closeLoser closes a connection that came up with nobody waiting for it,
// counting it as opened (it was accepted and paid for).
func closeLoser(countCtx context.Context, conn net.Conn, withTLS bool) {
	if countCtx != nil {
		negotiated := ""
		if tc, ok := conn.(*tls.Conn); ok {
			negotiated = tc.ConnectionState().NegotiatedProtocol
		}
		noteRacedConnectionUp(countCtx, withTLS, negotiated)
	}
	_ = conn.Close()
}

// resetChainsForTest forgets every chain and every count of the burst bound's
// three terms (races, replacements, stuck connects dropped: each test's
// destinations are its own; this keeps a test's leftovers, or a reused port's,
// from another's counts).
func resetChainsForTest() {
	raceCounts.Clear()
	replaceCounts.Clear()
	stuckCounts.Clear()
	for _, s := range []*setupChains{connectChains, tlsChains} {
		s.mu.Lock()
		for _, c := range s.chains {
			c.mu.Lock()
			c.finishLocked()
			c.mu.Unlock()
		}
		s.chains = map[chainKey]*setupChain{}
		s.mu.Unlock()
	}
}
