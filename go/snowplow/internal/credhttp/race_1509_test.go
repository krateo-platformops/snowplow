package credhttp

// race_1509_test.go — #1509 (b) unit arms: the trigger is the destination's
// measured connect-time quantile (cold and partial windows included), a late
// connect is raced by a new socket, the race thins out by doubling, every
// losing connection is closed, and a failure ends the race only when no
// attempt is left.

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func resetConnectTimes(t *testing.T) {
	t.Helper()
	connectTimes.resetForTest()
	t.Cleanup(connectTimes.resetForTest)
}

// A destination with no sample races only after the goal's typical bound, as
// the retry reserve does before it has measured anything.
func TestIssue1509_B_TriggerColdDestinationIsTheP95Goal(t *testing.T) {
	resetConnectTimes(t)
	d, measured := connectTimes.trigger("cold.invalid:443")
	if measured || d != pagebudget.PageP95Goal {
		t.Fatalf("cold trigger = %v (measured %v), want %v unmeasured", d, measured, pagebudget.PageP95Goal)
	}
}

// Too few samples for the quantile to exclude anything: the window's maximum,
// so one fast sample cannot make every later connect race.
func TestIssue1509_B_TriggerPartialWindowIsItsMaximum(t *testing.T) {
	resetConnectTimes(t)
	for _, ms := range []int{3, 40, 7} {
		connectTimes.record("few.invalid:443", time.Duration(ms)*time.Millisecond)
	}
	if d, measured := connectTimes.trigger("few.invalid:443"); !measured || d != 40*time.Millisecond {
		t.Fatalf("trigger = %v (measured %v), want the maximum 40ms", d, measured)
	}
}

// A full window: the goal quantile, so one slow outlier among 20 does not set
// it. And windows are per destination.
func TestIssue1509_B_TriggerIsTheGoalQuantilePerDestination(t *testing.T) {
	resetConnectTimes(t)
	for i := range reserveWindow {
		d := 2 * time.Millisecond
		if i == 0 {
			d = 5 * time.Second // one outlier
		}
		connectTimes.record("full.invalid:443", d)
	}
	if d, _ := connectTimes.trigger("full.invalid:443"); d != 2*time.Millisecond {
		t.Fatalf("trigger = %v, want 2ms (the outlier above the 0.95 quantile is excluded)", d)
	}
	if d, measured := connectTimes.trigger("other.invalid:443"); measured || d != pagebudget.PageP95Goal {
		t.Fatalf("another destination's trigger = %v (measured %v), want it cold", d, measured)
	}
}

// raceListener counts accepted connections and how many of them the client
// closed (read to EOF).
type raceListener struct {
	ln       net.Listener
	accepted atomic.Int32
	closed   atomic.Int32
	wg       sync.WaitGroup
}

func newRaceListener(t *testing.T) *raceListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &raceListener{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			l.accepted.Add(1)
			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				_, _ = io.Copy(io.Discard, c) // returns at the client's close
				l.closed.Add(1)
				_ = c.Close()
			}()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return l
}

// scriptedRaceDial makes endpointDialFn follow script, one step per attempt:
// "hang" (until ctx ends), "refuse", "ok" (connect now), or "after:<dur>"
// (connect after dur, whatever ctx says: a connect the kernel completes late).
// It records each attempt's start.
type scriptedRaceDial struct {
	script []string
	mu     sync.Mutex
	starts []time.Time
}

func (s *scriptedRaceDial) install(t *testing.T) {
	t.Helper()
	prev := endpointDialFn
	endpointDialFn = s.dial
	t.Cleanup(func() { endpointDialFn = prev })
}

func (s *scriptedRaceDial) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	s.mu.Lock()
	i := len(s.starts)
	s.starts = append(s.starts, time.Now())
	s.mu.Unlock()
	step := s.script[min(i, len(s.script)-1)]
	switch {
	case step == "hang":
		<-ctx.Done()
		return nil, ctx.Err()
	case step == "refuse":
		return nil, &net.OpError{Op: "dial", Net: network, Err: errors.New("connection refused")}
	case len(step) > 6 && step[:6] == "after:":
		d, _ := time.ParseDuration(step[6:])
		time.Sleep(d)
	}
	return (&net.Dialer{}).Dial(network, addr)
}

func (s *scriptedRaceDial) startOffsets() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, len(s.starts))
	for i, st := range s.starts {
		out[i] = st.Sub(s.starts[0])
	}
	return out
}

func seedConnectTimes(addr string, d time.Duration) {
	for range reserveWindow {
		connectTimes.record(addr, d)
	}
}

// A late connect is raced by a new socket at the trigger, and the race is won
// by whichever connects first.
func TestIssue1509_B_LateConnectIsRacedAtTheTrigger(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(addr, 50*time.Millisecond)
	s := &scriptedRaceDial{script: []string{"hang", "ok"}}
	s.install(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if took := time.Since(start); took >= time.Second {
		t.Fatalf("took %v; a hung connect must be raced at its 50ms trigger", took)
	}
	if offs := s.startOffsets(); len(offs) != 2 || offs[1] < 40*time.Millisecond {
		t.Fatalf("attempt starts %v; want 2, the second at the 50ms trigger", offs)
	}
}

// Each further attempt waits twice as long as the one before it: starts at
// q, 3q, 7q, … (q = the trigger), never one per q.
func TestIssue1509_B_RaceThinsOutByDoubling(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	q := 40 * time.Millisecond
	seedConnectTimes(addr, q)
	s := &scriptedRaceDial{script: []string{"hang", "hang", "hang", "ok"}}
	s.install(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	offs := s.startOffsets()
	want := []time.Duration{0, q, 3 * q, 7 * q}
	if len(offs) != len(want) {
		t.Fatalf("attempt starts %v; want %d attempts", offs, len(want))
	}
	for i := range want {
		if offs[i] < want[i]-5*time.Millisecond || offs[i] > want[i]+q {
			t.Fatalf("attempt %d started at %v, want ≈%v (starts %v)", i, offs[i], want[i], offs)
		}
	}
}

// A destination with no sample is not raced before the P95 goal: a connect
// that answers within it is the only attempt.
func TestIssue1509_B_ColdDestinationIsNotRacedEarly(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	s := &scriptedRaceDial{script: []string{"after:300ms", "ok"}}
	s.install(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if n := len(s.startOffsets()); n != 1 {
		t.Fatalf("%d attempts; a cold destination races only after %v", n, pagebudget.PageP95Goal)
	}
}

// No leaked socket: a losing attempt that connects after the race was won is
// closed by the race.
func TestIssue1509_B_LosingConnectionIsClosed(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(addr, 30*time.Millisecond)
	s := &scriptedRaceDial{script: []string{"after:300ms", "ok"}}
	s.install(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the loser to connect and be closed: two accepted, one closed
	// (the loser), the winner still open.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (l.accepted.Load() < 2 || l.closed.Load() < 1) {
		time.Sleep(10 * time.Millisecond)
	}
	if a, cl := l.accepted.Load(), l.closed.Load(); a != 2 || cl != 1 {
		t.Fatalf("accepted %d, closed %d; want the loser's connection closed and only the winner open", a, cl)
	}
	_ = c.Close()
	l.wg.Wait()
	if a, cl := l.accepted.Load(), l.closed.Load(); a != cl {
		t.Fatalf("accepted %d, closed %d after the winner closed", a, cl)
	}
}

// A fast failure does not end a race another attempt can still win.
func TestIssue1509_B_FailureEndsTheRaceOnlyWhenNoAttemptIsLeft(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(addr, 20*time.Millisecond)

	// attempt 1 connects late, attempt 2 (raced) is refused: attempt 1 wins.
	s := &scriptedRaceDial{script: []string{"after:150ms", "refuse"}}
	s.install(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := raceDial(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("err %v; the late attempt must still win after the raced one was refused", err)
	}
	_ = c.Close()

	// Every attempt refused: the first attempt's error.
	s2 := &scriptedRaceDial{script: []string{"refuse"}}
	s2.install(t)
	if _, err := raceDial(ctx, "tcp", addr); err == nil {
		t.Fatal("every attempt refused, want an error")
	}
}

// Every attempt is bounded by the dial's own deadline: none starts after it.
func TestIssue1509_B_NoAttemptAfterTheBound(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(addr, 20*time.Millisecond)
	s := &scriptedRaceDial{script: []string{"hang"}}
	s.install(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := raceDial(ctx, "tcp", addr); err == nil {
		t.Fatal("every attempt hung past the bound, want an error")
	}
	offs := s.startOffsets()
	for _, o := range offs {
		if o > 200*time.Millisecond {
			t.Fatalf("an attempt started at %v, after the 200ms bound (starts %v)", o, offs)
		}
	}
}

// The owned dialer races: endpointDial goes through raceDial, so a late
// connect behind a kept client is raced (the production wiring).
func TestIssue1509_B_EndpointDialRaces(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	seedConnectTimes(addr, 30*time.Millisecond)
	s := &scriptedRaceDial{script: []string{"hang", "ok"}}
	s.install(t)
	start := time.Now()
	c, err := endpointDial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if took := time.Since(start); took >= time.Second || len(s.startOffsets()) != 2 {
		t.Fatalf("took %v with %d attempts; endpointDial must race a hung connect", took, len(s.startOffsets()))
	}
	if connClosedSignal(c) == nil {
		t.Fatal("endpointDial's connection carries no close signal (#1509 a)")
	}
}

// The cost bound (owner ruling on #1509): a dial that never connects opens at
// most log2(cap / trigger) + 1 sockets inside its cap, and every attempt has
// returned when raceDial does (nothing left racing).
func TestIssue1509_B_SocketsPerStuckDialAreLogarithmic(t *testing.T) {
	for _, tc := range []struct{ trigger, bound time.Duration }{
		{10 * time.Millisecond, 640 * time.Millisecond},
		{5 * time.Millisecond, 1300 * time.Millisecond},
	} {
		resetConnectTimes(t)
		l := newRaceListener(t)
		addr := l.ln.Addr().String()
		seedConnectTimes(addr, tc.trigger)
		var live atomic.Int32
		s := &scriptedRaceDial{script: []string{"hang"}}
		prev := endpointDialFn
		endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
			live.Add(1)
			defer live.Add(-1)
			return s.dial(ctx, network, a)
		}
		ctx, cancel := context.WithTimeout(context.Background(), tc.bound)
		_, err := raceDial(ctx, "tcp", addr)
		cancel()
		endpointDialFn = prev
		if err == nil {
			t.Fatal("every attempt hung, want an error")
		}
		n := len(s.startOffsets())
		limit := int(math.Log2(float64(tc.bound)/float64(tc.trigger))) + 1
		if n > limit || n < 2 {
			t.Fatalf("trigger %v, cap %v: %d sockets; want 2..%d (log2(cap/trigger)+1)", tc.trigger, tc.bound, n, limit)
		}
		if v := live.Load(); v != 0 {
			t.Fatalf("%d attempts still running after raceDial returned", v)
		}
	}
}
