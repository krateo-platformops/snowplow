package credhttp

// trigger_samples_1521_test.go — #1521, owner ruling "after enough samples":
// a destination's race trigger is its measured setup time only once its
// window holds enough setups to estimate the trigger's quantile; until then
// the cold default (PageP95Goal) applies.

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/transport"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// The threshold is the quantile's own: the fewest samples in which one lies
// above the q-quantile's sample, ⌈1/(1−q)⌉ (20 for q = 0.95), and the window
// can hold it.
func TestIssue1521_TriggerMinSamplesIsTheQuantilesOwnAndFitsTheWindow(t *testing.T) {
	if quantileAbove(triggerMinSamples) < 1 || quantileAbove(triggerMinSamples-1) != 0 {
		t.Fatalf("triggerMinSamples %d is not the fewest samples with one above the %v-quantile", triggerMinSamples, reserveQuantile)
	}
	if reserveQuantile == 0.95 && triggerMinSamples != 20 {
		t.Fatalf("triggerMinSamples = %d for q = 0.95; want ⌈1/(1−q)⌉ = 20", triggerMinSamples)
	}
	if triggerMinSamples > reserveWindow {
		t.Fatalf("the window holds %d setups, fewer than the %d the trigger needs: it could never be measured", reserveWindow, triggerMinSamples)
	}
}

// Both edges: one setup short of the threshold, the trigger is the cold
// default and not measured; at the threshold it is the measured quantile.
func TestIssue1521_TriggerIsMeasuredOnlyAfterEnoughSamples(t *testing.T) {
	resetConnectTimes(t)
	key := setupKey("127.0.0.1:1", true)
	for i := 1; i < triggerMinSamples; i++ {
		connectTimes.record(key, time.Duration(i)*time.Millisecond)
	}
	if d, measured := connectTimes.trigger(key); measured || d != pagebudget.PageP95Goal {
		t.Fatalf("with %d samples the trigger is %v (measured %v); want the cold default %v, not measured",
			triggerMinSamples-1, d, measured, pagebudget.PageP95Goal)
	}
	connectTimes.record(key, time.Duration(triggerMinSamples)*time.Millisecond)
	// 1..20 ms: the 0.95 quantile's sample skips the one above it: 19 ms.
	want := time.Duration(triggerMinSamples-quantileAbove(triggerMinSamples)) * time.Millisecond
	if d, measured := connectTimes.trigger(key); !measured || d != want {
		t.Fatalf("with %d samples the trigger is %v (measured %v); want the measured quantile %v", triggerMinSamples, d, measured, want)
	}
}

// warmDestination1521 brings up n separate TLS setups to srv, each through a
// transport of its own (so each is a new connection, its connect and
// handshake recorded in the destination's windows); the server then closes
// every connection it holds, so the arm starts from none. It returns how many
// samples each window then holds.
func warmDestination1521(t *testing.T, srv *httptest.Server, n int) (tlsSamples, tcpSamples int) {
	t.Helper()
	for range n {
		rt, err := NewEndpointTransport(&transport.Config{TLS: transport.TLSConfig{CAData: caPEMOf(srv)}, Proxy: http.ProxyFromEnvironment})
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("warming the destination: %v", err)
		}
		_ = resp.Body.Close()
	}
	srv.CloseClientConnections()
	addr := srv.Listener.Addr().String()
	return connectTimes.ring(setupKey(addr, true)).count(), connectTimes.ring(setupKey(addr, false)).count()
}

// requireWarm1521 fails unless both of srv's windows (TLS setup and connect)
// hold enough samples for a measured trigger.
func requireWarm1521(t *testing.T, srv *httptest.Server) {
	t.Helper()
	tlsN, tcpN := warmDestination1521(t, srv, triggerMinSamples)
	if tlsN < triggerMinSamples || tcpN < triggerMinSamples {
		t.Fatalf("after warming, the destination's windows hold %d TLS and %d connect samples; the race needs %d in each for a measured trigger",
			tlsN, tcpN, triggerMinSamples)
	}
	for _, key := range []string{setupKey(srv.Listener.Addr().String(), true), setupKey(srv.Listener.Addr().String(), false)} {
		if _, measured := connectTimes.trigger(key); !measured {
			t.Fatalf("after warming, %s's trigger is not measured", key)
		}
	}
}

// The threshold is the race trigger's only (trigger, not reserveRing): the
// customer path's retry reserve still reads a partial window's maximum
// (#1521 review N3; budget.go reserve, unchanged).
func TestIssue1521_RetryReserveStillReadsAPartialWindowsMaximum(t *testing.T) {
	r := &reserveRing{}
	for _, ms := range []int{3, 40, 7} {
		r.record(time.Duration(ms) * time.Millisecond)
	}
	if d, measured := r.reserve(); !measured || d != 40*time.Millisecond {
		t.Fatalf("a 3-sample reserve is %v (measured %v); want the partial window's maximum 40ms, measured", d, measured)
	}
}

// Rule D stamps an attempt's answering period when it CONNECTS (#1521
// review): an attempt started before the comeback that is still connecting
// when the new period begins keeps covering, and once it connects it belongs
// to the new period; only an attempt that had connected before the period
// began stops covering. Driven on the chain's own state, step by step.
func TestIssue1521_RuleDStampsThePeriodAtConnect(t *testing.T) {
	resetConnectTimes(t)
	c := newSetupChain(tlsChains, chainKey{network: "tcp", addr: "127.0.0.1:1", tlsCfg: &tls.Config{}}, nil)
	connectedEarly := &attemptState{covers: true, evidence: true}
	stillConnecting := &attemptState{covers: true, evidence: true}
	c.mu.Lock()
	c.period = 1 // answering
	c.attempts = map[*attemptState]struct{}{connectedEarly: {}, stillConnecting: {}}
	c.covering = 2
	c.waiters = []*chainWaiter{{ch: make(chan racedConn, 1)}, {ch: make(chan racedConn, 1)}}
	c.mu.Unlock()
	c.onAttemptConnected(connectedEarly) // connects in period 1

	// The destination answers after a stall: period 2 begins.
	c.mu.Lock()
	c.beginPeriodLocked()
	early, still, period := connectedEarly.covers, stillConnecting.covers, c.period
	c.mu.Unlock()
	if period != 2 || early || !still {
		t.Fatalf("at period %d: the attempt connected in period 1 covers=%v (want false), the one still connecting covers=%v (want true)", period, early, still)
	}
	if got := replacedAt("127.0.0.1:1"); got != 1 {
		t.Fatalf("replaced at the new period: %d; want 1 (one waiter lost its cover)", got)
	}

	// The still-connecting attempt connects after the comeback: it belongs to
	// period 2 and keeps covering.
	c.onAttemptConnected(stillConnecting)
	c.mu.Lock()
	stamped, still := stillConnecting.period, stillConnecting.covers
	c.mu.Unlock()
	if stamped != 2 || !still {
		t.Fatalf("an attempt started before the comeback that connected after it: period %d covers=%v; want period 2, covering", stamped, still)
	}

	// The next period: it connected before that one, so it stops covering.
	c.mu.Lock()
	c.beginPeriodLocked()
	still = stillConnecting.covers
	c.mu.Unlock()
	if still {
		t.Fatal("at period 3 the attempt connected in period 2 still covers; want it dropped")
	}
	c.mu.Lock()
	c.finishLocked()
	c.mu.Unlock()
}
