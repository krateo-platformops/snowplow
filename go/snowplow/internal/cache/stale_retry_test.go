// stale_retry_test.go — #740 viewer-gated retry engine, on a manual clock (no
// real minute-long sleeps). The viewer is a REAL /refreshes subscription
// (SubscribeRefreshStream), so the gate reads the production predicate.
//
//	V1   retries while armed: genuine failures back off 1, 2, 4, … s, flat at 60 s.
//	V1b  the floor self-adapts: an attempt that takes 5 s is never retried sooner.
//	V2   stops on disarm: the connection closes → the pending wake is stopped,
//	     abandoned{no_viewer} +1, no further attempt; chains_active back to 0.
//	V2b  lazy: a genuine wake with no viewer abandons without an attempt.
//	V5   arm resumes: an abandoned chain gets an immediate attempt on arm.
//	V6   one chain per cell: ten triggers, two viewers, one schedule.
//	V7   transient vs genuine: a refused Put retries at once, then #690's.
//	E    an evicted cell ends its chain (abandoned, reason evicted).
//	T    a join replaces the spec: the next attempt runs the newest template.
package cache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func retryEngineSetup(t *testing.T) *StaleRetryManualClock {
	t.Helper()
	withRefreshLayer(t)
	resetStaleRetryForTest()
	t.Cleanup(resetStaleRetryForTest)
	c := NewStaleRetryManualClockForTest()
	SetStaleRetryClockForTest(c)
	t.Cleanup(func() { SetStaleRetryClockForTest(nil) })
	return c
}

func fireOne(t *testing.T, c *StaleRetryManualClock) time.Duration {
	t.Helper()
	if p := c.Pending(); len(p) != 1 {
		t.Fatalf("want exactly 1 pending wake, have %v", p)
	}
	d, _ := c.FireNext()
	return d
}

type retryProbe struct {
	mu       sync.Mutex
	attempts int
	tags     []string
	out      StaleRetryOutcome
	cost     time.Duration
	stale    bool
	resident bool
}

func (p *retryProbe) spec(clock *StaleRetryManualClock, key, tag string) StaleRetrySpec {
	return StaleRetrySpec{
		Source: StaleRetryBackground,
		Attempt: func(ctx context.Context) StaleRetryOutcome {
			if !CustomerTriggeredRefreshFromContext(ctx) {
				panic("a viewer-gated retry must be customer-triggered")
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			p.attempts++
			p.tags = append(p.tags, tag)
			clock.Advance(p.cost)
			return p.out
		},
		Dependents: func() []string { return []string{key} },
		StillStale: func() (bool, bool) {
			p.mu.Lock()
			defer p.mu.Unlock()
			return p.stale, p.resident
		},
	}
}

func (p *retryProbe) n() int { p.mu.Lock(); defer p.mu.Unlock(); return p.attempts }

func TestStaleRetry_V1_RetriesWhileArmedDoublingToTheCap(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v1"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}

	StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "a"))
	want := []time.Duration{1, 2, 4, 8, 16, 32, 60, 60, 60}
	for i, w := range want {
		if got := fireOne(t, clock); got != w*time.Second {
			t.Fatalf("V1 RED: retry #%d waited %v, want %v (1 s doubling, flat at the 60 s cap)", i+1, got, w*time.Second)
		}
	}
	if p.n() != len(want) {
		t.Fatalf("V1: %d attempts, want %d", p.n(), len(want))
	}
	s := StaleRetryStatsSnapshot()
	if got := s.StaleRetryTotalFor(StaleRetryGenuine, StaleRetryBackground); got != uint64(len(want))+1 {
		t.Fatalf("V1: retry_total{genuine,background} = %d, want %d (the trigger + every failed retry)", got, len(want)+1)
	}
	if s.ChainsActive != 1 || s.Viewers != 1 || s.BackoffMS != 60_000 {
		t.Fatalf("V1: chains_active=%d viewers=%d backoff_ms=%d, want 1/1/60000", s.ChainsActive, s.Viewers, s.BackoffMS)
	}
}

func TestStaleRetry_V1b_TheFloorIsTheFailedAttemptsOwnDuration(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v1b"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true, cost: 5 * time.Second}

	StaleRetryFailed(key, StaleRetryGenuine, 5*time.Second, p.spec(clock, key, "a"))
	for i, w := range []time.Duration{5, 10, 20, 40, 60} {
		if got := fireOne(t, clock); got != w*time.Second {
			t.Fatalf("V1b RED: retry #%d waited %v, want %v — an upstream that takes 5 s to fail is never retried "+
				"sooner than every 5 s", i+1, got, w*time.Second)
		}
	}
}

func TestStaleRetry_V2_StopsWhenTheLastViewerLeaves(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v2"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}

	StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "a"))
	fireOne(t, clock)
	fireOne(t, clock)
	st.Unsub() // the tab closes
	if p := clock.Pending(); len(p) != 0 {
		t.Fatalf("V2 RED: %d wake(s) still pending after the last viewer left — the chain keeps retrying", len(p))
	}
	s := StaleRetryStatsSnapshot()
	if s.AbandonedNoViewer != 1 || s.ChainsActive != 0 {
		t.Fatalf("V2: abandoned{no_viewer}=%d (want 1), chains_active=%d (want 0)", s.AbandonedNoViewer, s.ChainsActive)
	}
	if p.n() != 2 {
		t.Fatalf("V2: %d attempts, want 2", p.n())
	}
}

func TestStaleRetry_V2b_AWakeWithNoViewerAbandonsLazily(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v2b"
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}
	StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "a")) // nobody armed
	fireOne(t, clock)
	if p.n() != 0 {
		t.Fatalf("V2b RED: a genuine retry ran with no viewer")
	}
	if len(clock.Pending()) != 0 || StaleRetryStatsSnapshot().AbandonedNoViewer != 1 {
		t.Fatalf("V2b: the chain was not abandoned")
	}
}

func TestStaleRetry_V5_AnArmResumesAnAbandonedChainAtOnce(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v5"
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}
	StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "a"))
	fireOne(t, clock) // no viewer: abandoned

	p.mu.Lock()
	p.out = StaleRetryFresh // the upstream is up again
	p.mu.Unlock()
	st := SubscribeRefreshStream(map[string]struct{}{key: {}}) // a tab arms
	defer st.Unsub()
	if got := fireOne(t, clock); got != 0 {
		t.Fatalf("V5 RED: the resumed attempt waited %v, want an immediate attempt", got)
	}
	if p.n() != 1 || StaleRetryStatsSnapshot().Resumed != 1 {
		t.Fatalf("V5: attempts %d (want 1), resumed %d (want 1)", p.n(), StaleRetryStatsSnapshot().Resumed)
	}
	if open, _ := StaleRetryChainOpenForTest(key); open || len(clock.Pending()) != 0 {
		t.Fatalf("V5: the healed chain did not end")
	}
}

func TestStaleRetry_V6_OneChainPerCell(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v6"
	st1 := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st1.Unsub()
	st2 := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st2.Unsub()
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}
	for i := 0; i < 10; i++ {
		StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "a"))
	}
	if pend := clock.Pending(); len(pend) != 1 {
		t.Fatalf("V6 RED: %d pending wakes for one cell, want 1 (one chain, whatever the triggers and viewers)", len(pend))
	}
	fireOne(t, clock)
	if p.n() != 1 || len(clock.Pending()) != 1 {
		t.Fatalf("V6: attempts %d / pending %d after one wake, want 1/1", p.n(), len(clock.Pending()))
	}
}

func TestStaleRetry_V7_ATransientRefusalRetriesAtOnce(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v7"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &retryProbe{out: StaleRetryTransient, stale: true, resident: true}
	StaleRetryFailed(key, StaleRetryTransient, 0, p.spec(clock, key, "a"))
	for i, w := range []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond} {
		if got := fireOne(t, clock); got != w {
			t.Fatalf("V7 RED: transient retry #%d waited %v, want %v (at once, then #690's backoff)", i+1, got, w)
		}
	}
	if got := StaleRetryStatsSnapshot().StaleRetryTotalFor(StaleRetryTransient, StaleRetryBackground); got != 5 {
		t.Fatalf("V7: retry_total{transient} = %d, want 5", got)
	}
}

func TestStaleRetry_E_AnEvictedCellEndsItsChain(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-e"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: false}
	evictedHook := 0
	spec := p.spec(clock, key, "a")
	spec.Evicted = func() { evictedHook++ }
	StaleRetryFailed(key, StaleRetryGenuine, 0, spec)
	fireOne(t, clock)
	open, _ := StaleRetryChainOpenForTest(key)
	if evictedHook != 1 {
		t.Fatalf("E: the Evicted hook ran %d times, want 1", evictedHook)
	}
	if p.n() != 0 || StaleRetryStatsSnapshot().AbandonedEvicted != 1 || len(clock.Pending()) != 0 || open {
		t.Fatalf("E: attempts %d (want 0), abandoned{evicted} %d (want 1), pending %d (want 0), open %v",
			p.n(), StaleRetryStatsSnapshot().AbandonedEvicted, len(clock.Pending()), open)
	}
}

func TestStaleRetry_T_AJoinMakesTheNewestSpecTheTemplate(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-t"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}
	StaleRetryFailed(key, StaleRetryGenuine, 0, p.spec(clock, key, "first"))
	StaleRetryRetemplate(key, p.spec(clock, key, "second"))
	fireOne(t, clock)
	if len(p.tags) != 1 || p.tags[0] != "second" {
		t.Fatalf("T RED: the retry ran template %v, want the newest (second)", p.tags)
	}
}

// R — StaleRetryResume restarts an abandoned chain at once and runs its Park /
// Resume hooks; StaleRetryEnd ends a pending chain with no further wake.
func TestStaleRetry_R_ResumeAndEndOnRequest(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-r"
	p := &retryProbe{out: StaleRetryGenuine, stale: true, resident: true}
	var parks, resumes int
	spec := p.spec(clock, key, "a")
	spec.Park = func() { parks++ }
	spec.Resume = func() { resumes++ }
	StaleRetryFailed(key, StaleRetryGenuine, 0, spec)
	if StaleRetryResume(key) {
		t.Fatalf("R: a scheduled (not abandoned) chain was resumed")
	}
	fireOne(t, clock) // no viewer: abandoned (parked)
	if parks != 1 {
		t.Fatalf("R: Park ran %d times on abandon, want 1", parks)
	}
	if !StaleRetryResume(key) || resumes != 1 {
		t.Fatalf("R RED: StaleRetryResume did not resume the abandoned chain (resumes %d)", resumes)
	}
	if got := fireOne(t, clock); got != 0 {
		t.Fatalf("R: the resumed attempt waits %v, want at once", got)
	}
	StaleRetryEnd(key)
	if open, _ := StaleRetryChainOpenForTest(key); open || len(clock.Pending()) != 0 {
		t.Fatalf("R RED: StaleRetryEnd left the chain (pending %v)", clock.Pending())
	}
}

// V7b — A2: only a transient refusal's FIRST, immediate retry skips the viewer
// gate; the next transient retry with nobody viewing abandons the chain — no
// endless <= 5 s loop for nobody.
func TestStaleRetry_V7b_TransientRetriesAreViewerGatedAfterTheFirst(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-v7b"
	p := &retryProbe{out: StaleRetryTransient, stale: true, resident: true}
	StaleRetryFailed(key, StaleRetryTransient, 0, p.spec(clock, key, "a")) // nobody armed
	if got := fireOne(t, clock); got != 0 || p.n() != 1 {
		t.Fatalf("V7b: the first transient retry waited %v / ran %d times, want at once / 1", got, p.n())
	}
	fireOne(t, clock) // the second (10 ms) transient wake: nobody views it
	if p.n() != 1 || len(clock.Pending()) != 0 {
		t.Fatalf("V7b RED: a transient retry ran with no viewer (attempts %d, pending %v) — only the first, immediate "+
			"one is ungated", p.n(), clock.Pending())
	}
	if open, abandoned := StaleRetryChainOpenForTest(key); !open || !abandoned {
		t.Fatalf("V7b: the chain is not abandoned (open=%v abandoned=%v)", open, abandoned)
	}
}
