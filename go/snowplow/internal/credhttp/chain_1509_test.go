package credhttp

// chain_1509_test.go — #1521 review: the race is bounded PER DESTINATION
// (owner ruling): concurrent setups to one destination join one chain; each
// waiter leaves at its own deadline; nothing leaks. Plus the review's other
// arms: an ordinary HTTP/2 429 on the last background attempt returns at once
// (B1), a budgeted leaving 429 whose Retry-After does not fit is still redone,
// WrapTransport is refused on a server build, and a raced loser that came up
// is counted as opened.

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/transport"
)

// countingRaceDial is scriptedRaceDial that also counts the attempts running.
func countingRaceDial(t *testing.T, script ...string) (*scriptedRaceDial, *atomic.Int32) {
	t.Helper()
	s := &scriptedRaceDial{script: script}
	var live atomic.Int32
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		live.Add(1)
		defer live.Add(-1)
		return s.dial(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })
	return s, &live
}

// K concurrent setups stuck on one destination open at most
// log2(cap / trigger) + 1 sockets IN TOTAL, not K times that.
func TestIssue1509_Chain_ConcurrentStuckDialsShareOneBound(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	const trigger, bound, k = 10 * time.Millisecond, 640 * time.Millisecond, 8
	seedConnectTimes(setupKey(addr, false), trigger)
	s, live := countingRaceDial(t, "hang")

	var wg sync.WaitGroup
	for range k {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), bound)
			defer cancel()
			if _, err := raceDial(ctx, "tcp", addr); err == nil {
				t.Error("a stuck setup returned a connection")
			}
		}()
	}
	wg.Wait()
	limit := int(math.Log2(float64(bound)/float64(trigger))) + 1
	if n := len(s.startOffsets()); n > limit || n < 2 {
		t.Fatalf("%d concurrent stuck setups opened %d sockets; want 2..%d in total (one chain per destination)", k, n, limit)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && live.Load() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if v := live.Load(); v != 0 {
		t.Fatalf("%d attempts still running after every waiter left", v)
	}
}

// Each waiter leaves at ITS deadline: a joiner with a shorter one is not held
// by the chain, and one with a longer one is not cut by another's.
func TestIssue1509_Chain_JoinersReturnByTheirOwnDeadlines(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(setupKey(addr, false), 20*time.Millisecond)
	countingRaceDial(t, "hang")

	type out struct {
		took time.Duration
		err  error
	}
	run := func(d time.Duration, res chan<- out) {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		start := time.Now()
		_, err := raceDial(ctx, "tcp", addr)
		res <- out{time.Since(start), err}
	}
	short, long := make(chan out, 1), make(chan out, 1)
	go run(600*time.Millisecond, long)
	time.Sleep(20 * time.Millisecond)
	go run(150*time.Millisecond, short)
	s, lo := <-short, <-long
	if !errors.Is(s.err, context.DeadlineExceeded) || s.took > 400*time.Millisecond {
		t.Fatalf("short joiner: %v after %v; want its own 150ms deadline", s.err, s.took)
	}
	if !errors.Is(lo.err, context.DeadlineExceeded) || lo.took < 550*time.Millisecond || lo.took > 900*time.Millisecond {
		t.Fatalf("long waiter: %v after %v; want its own 600ms deadline, not the joiner's", lo.err, lo.took)
	}
}

// Every waiter gets its own connection, the longest waiting first; and nothing
// leaks: every connection that came up is either handed out or closed.
func TestIssue1509_Chain_EachConnectionGoesToOneWaiterAndNoneLeaks(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(setupKey(addr, false), 20*time.Millisecond)
	// Attempt 1 hangs; the raced ones connect: the destination is back.
	countingRaceDial(t, "hang", "ok")

	const k = 4
	conns := make(chan net.Conn, k)
	var wg sync.WaitGroup
	for range k {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := raceDial(ctx, "tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			conns <- c
		}()
	}
	wg.Wait()
	close(conns)
	seen := map[net.Conn]bool{}
	for c := range conns {
		if seen[c] {
			t.Fatal("one connection was handed to two waiters")
		}
		seen[c] = true
		_ = c.Close()
	}
	if len(seen) != k {
		t.Fatalf("%d waiters got %d connections", k, len(seen))
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && l.accepted.Load() != l.closed.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if a, c := l.accepted.Load(), l.closed.Load(); a != c {
		t.Fatalf("accepted %d, closed %d: a connection leaked", a, c)
	}
}

// B1: on the background path's LAST attempt an ordinary 429 returns at once,
// over HTTP/2 too, as plumbing's RetryClient does; it never waits for its
// connection to close. And a retry's wait is plumbing's Retry-After, once.
func TestIssue1509_Background_LastAttemptOrdinary429ReturnsAtOnceOverHTTP2(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Setenv("CLIENT_MAX_RETRIES", "0")
		s := &leavingScript{steps: []string{"busy", "busy"}, retryAfter: "2"}
		srv := leavingServer(t, s, h2)
		cli := srv.Client()
		if h2 {
			cli = h2EndpointClient(t, srv)
		}
		resp, took, err := backgroundSend(t, cli, srv.URL)
		if err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests || s.n.Load() != 1 {
			t.Fatalf("h2=%v MaxRetries=0: resp %v, err %v, %d requests; want plumbing's 429 after 1", h2, resp, err, s.n.Load())
		}
		if took > 500*time.Millisecond {
			t.Fatalf("h2=%v MaxRetries=0: took %v; the last attempt's ordinary 429 must return at once (Retry-After 2 s)", h2, took)
		}

		t.Setenv("CLIENT_MAX_RETRIES", "1")
		s2 := &leavingScript{steps: []string{"busy", "busy"}, retryAfter: "1"}
		srv2 := leavingServer(t, s2, h2)
		cli2 := srv2.Client()
		if h2 {
			cli2 = h2EndpointClient(t, srv2)
		}
		_, took2, err2 := backgroundSend(t, cli2, srv2.URL)
		if err2 == nil || s2.n.Load() != 2 || took2 < 900*time.Millisecond || took2 > 1600*time.Millisecond {
			t.Fatalf("h2=%v MaxRetries=1: err %v, %d requests, took %v; want plumbing's one 1 s wait then the 429", h2, err2, s2.n.Load(), took2)
		}
	}
}

// The budgeted path: an HTTP/2 leaving 429 whose Retry-After does not fit the
// budget left is redone at its connection's close; an ordinary one is returned,
// body intact, by the budget left − reserve.
func TestIssue1509_A_BudgetedLeavingRedoneWhenRetryAfterDoesNotFit(t *testing.T) {
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, true)
	cli := h2EndpointClient(t, srv)
	seedReserve(t, 50*time.Millisecond)
	start := time.Now()
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(3*time.Second), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if took := time.Since(start); resp.StatusCode != http.StatusOK || s.n.Load() != 2 || s.distinctConns() != 2 || took > time.Second {
		t.Fatalf("status %d, %d requests on %d connections, %v; want the leaving 429 redone at once on a new connection", resp.StatusCode, s.n.Load(), s.distinctConns(), took)
	}

	s2 := &leavingScript{steps: []string{"busy"}, retryAfter: "5"}
	srv2 := leavingServer(t, s2, true)
	cli2 := h2EndpointClient(t, srv2)
	start = time.Now()
	resp2, err := Send(SendSiteStage, cli2, getReq(t, budgetLeft(2*time.Second), srv2.URL))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 64)
	n, _ := resp2.Body.Read(body)
	_ = resp2.Body.Close()
	took := time.Since(start)
	if resp2.StatusCode != http.StatusTooManyRequests || s2.n.Load() != 1 || n == 0 {
		t.Fatalf("ordinary 429: status %d after %d requests, body %q; want the 429 with its body", resp2.StatusCode, s2.n.Load(), body[:n])
	}
	if took > 2*time.Second {
		t.Fatalf("ordinary 429 returned after %v; want by the budget left − reserve", took)
	}
}

// A server build refuses a WrapTransport: it could replace the base transport
// and the guarded dial with it.
func TestIssue1509_EndpointTransportRefusesAWrapperOnAServerBuild(t *testing.T) {
	if browserBuild {
		t.Fatal("this arm runs on a server build")
	}
	_, err := NewEndpointTransport(&transport.Config{
		WrapTransport: func(http.RoundTripper) http.RoundTripper { return http.DefaultTransport },
	})
	if err == nil {
		t.Fatal("a WrapTransport was accepted on a server build")
	}
}

// A raced loser that came up is counted as opened (it was accepted and paid
// for), besides the winner: through noteRacedConnectionUp.
func TestIssue1509_Chain_RacedLoserThatCameUpIsCountedOpened(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(setupKey(addr, false), 20*time.Millisecond)
	// Attempt 1 connects late (after the raced one won and its waiter left).
	countingRaceDial(t, "after:300ms", "ok")

	req, err := http.NewRequest(http.MethodGet, "http://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := countConnections(req, SendSiteStage)
	before := egressConnections.cell(SendSiteStage, ConnProtocolHTTP1).opened.Load()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && l.accepted.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	for time.Now().Before(deadline) && l.closed.Load() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := egressConnections.cell(SendSiteStage, ConnProtocolHTTP1).opened.Load() - before; got != 2 {
		t.Fatalf("opened moved by %d; want 2: the winner (its ConnectDone) and the loser that came up (noteRacedConnectionUp)", got)
	}
}

// A busy destination stalls a SECOND time (#1521 review): it answers, a
// continuous stream of joiners keeps the chain from ever emptying, then it
// stops answering (connects started now never complete, as a SYN in backoff)
// and comes back. The waiters that joined after the stall must be raced: a
// connection attempt started after the comeback serves them. With "answering
// until the chain empties" every attempt they get started during the stall
// and never completes: they wait out their deadlines (RED).
func TestIssue1509_Chain_ABusyDestinationsSecondStallIsRaced(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(setupKey(addr, false), 20*time.Millisecond)

	var stalled atomic.Bool
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		if stalled.Load() {
			<-ctx.Done() // started during the stall: never completes
			return nil, ctx.Err()
		}
		select {
		case <-time.After(10 * time.Millisecond): // a connect that takes 10 ms
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })

	type result struct {
		joined, took time.Duration
		err          error
	}
	var mu sync.Mutex
	var results []result
	var wg sync.WaitGroup
	t0 := time.Now()
	join := func() {
		defer wg.Done()
		joined := time.Since(t0)
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		c, err := raceDial(ctx, "tcp", addr)
		if c != nil {
			_ = c.Close()
		}
		mu.Lock()
		results = append(results, result{joined, time.Since(t0), err})
		mu.Unlock()
	}

	const stallAt, joinUntil, comeback = 300 * time.Millisecond, 340 * time.Millisecond, 900 * time.Millisecond
	for time.Since(t0) < joinUntil {
		if time.Since(t0) >= stallAt {
			stalled.Store(true)
		}
		wg.Add(1)
		go join()
		time.Sleep(2 * time.Millisecond) // joiners overlap: the chain never empties
	}
	time.Sleep(time.Until(t0.Add(comeback)))
	stalled.Store(false)
	wg.Wait()

	after := 0
	for _, r := range results {
		if r.joined < stallAt+15*time.Millisecond {
			continue // before the stall, or served by an attempt started before it
		}
		after++
		if r.err != nil {
			t.Errorf("a waiter that joined %v (after the stall) failed: %v — it was not raced after the comeback", r.joined, r.err)
			continue
		}
		if r.took > comeback+500*time.Millisecond {
			t.Errorf("a waiter that joined %v got its connection at %v; want by comeback + 500ms (%v)", r.joined, r.took, comeback+500*time.Millisecond)
		}
	}
	if after == 0 {
		t.Fatal("SETUP: no waiter joined after the stall")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && l.accepted.Load() != l.closed.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if a, c := l.accepted.Load(), l.closed.Load(); a != c {
		t.Fatalf("accepted %d, closed %d: a connection leaked", a, c)
	}
}
