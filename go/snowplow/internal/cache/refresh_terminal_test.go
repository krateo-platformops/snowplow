// refresh_terminal_test.go — 1.12.6 C4 falsifiers at the refresher frame
// (design-1.12.6-event-hardening §6.6; PM gate C1 = the two-regime breaker
// pin).
//
// The frame under test is processNext's drop point and its suppression
// consult. Every arm drives the REAL refresher loop (StartRefresher → worker
// pool → processNext) against a real ResolvedCacheStore and a real
// DepTracker; the only seam is the RefreshFunc handler, which sits BELOW the
// frame under test (the handler is what fails; the decision under test is
// what the refresher does with the failure).
//
// ARMS
//
//	F5a   few deterministic failures (3 keys, budget 64) → ALL evicted at the
//	      drop point, breaker never engaged (suspended == 0). RED on main: the
//	      entries survive (drop-to-TTL).
//	F5b   mass failure (budget "1", 6 keys) → the first evicts, the rest are
//	      SUSPENDED: resident AND SERVED, suspended_total == 5, exactly ONE
//	      suspension WARN. Assertion is on the served state, never on the
//	      arithmetic. An unbounded evict passes F5a and FAILS this arm; a
//	      never-evict passes this arm and fails F5a.
//	KILL  budget "0" → 1.12.5 byte-for-byte: dropped, resident.
//	F6m   the suppression mechanism end-to-end: K consecutive declines noted
//	      by the handler → the (K+1)th dequeue does NOT invoke the handler;
//	      one real Put → the next dequeue DOES. RED on main: every dequeue
//	      resolves forever (#191).
//	F6p   permanent declines (external / UAF / unsupported) suppress on the
//	      FIRST occurrence.
//	F6e   an eviction clears the marker (it cannot outlive its key).
//	B1    a stats read (the expvar / OTLP scrape) racing the FIRST
//	      construction of the refresher singleton is race-free and never
//	      constructs the singleton itself. RED on 6ee779d under -race: the
//	      snapshot read refresherInstance unsynchronised against
//	      refresherInit.Do's store.
//
// ORDER OF ASSERTIONS. Every arm asserts the BEHAVIOUR first (was the
// handler invoked? is the entry resident?) and consults the marker /
// counter API only afterwards, so the RED transcript against a tree without
// the mechanism reads as the symptom (#191: the 4th dequeue resolved), not
// as "the marker API returned false".
package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func terminalEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envRefresherBaseDelayMS, "1")
	t.Setenv(envRefresherMaxDelayMS, "2")
	t.Setenv(envRefresherRateFloorSeconds, "0")
}

// terminalFixture: clean store + deps + refresher, N resident widgets entries
// with a self dep edge each (so EvictSelfGone has edges to clear).
func terminalFixture(t *testing.T, n int) (*ResolvedCacheStore, []string) {
	t.Helper()
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	resetRefresherForTest()
	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled")
	}
	Deps().SetStore(c)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Group: "widgets.templates.krateo.io",
			Version: "v1beta1", Resource: "flexes", Namespace: "ns", Name: fmt.Sprintf("w-%d", i)}
		key := ComputeKey(inputs)
		c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"old"}`), Inputs: &inputs})
		keys = append(keys, key)
	}
	return c, keys
}

func terminalRun(t *testing.T, c *ResolvedCacheStore, keys []string, handler RefreshFunc, until func() bool) {
	t.Helper()
	RegisterRefreshFunc("widgets", handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	for _, k := range keys {
		enqueueRefreshForTest(k)
	}
	waitFor(t, 10*time.Second, "refresh loop settled", until)
	cancel()
	resetRefresherForTestKeepTerminalCounters()
}

// resetRefresherForTestKeepTerminalCounters drains the pool but leaves the
// breaker counters readable for the assertions that follow.
func resetRefresherForTestKeepTerminalCounters() {
	if refresherInstance != nil {
		refresherInstance.queue.ShutDown()
		if refresherInstance.clusterListQueue != nil {
			refresherInstance.clusterListQueue.ShutDown()
		}
		done := make(chan struct{})
		go func() { refresherInstance.workersWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}

func TestRefreshTerminal_F5a_FewDeterministicFailuresAreEvicted(t *testing.T) {
	terminalEnv(t)
	t.Setenv(envRefreshDropEvictMaxPerMinute, "64")
	c, keys := terminalFixture(t, 3)

	var attempts atomic.Int32
	terminalRun(t, c, keys, func(context.Context, string, ResolvedKeyInputs) error {
		attempts.Add(1)
		return errors.New("deterministic 500 — never succeeds")
	}, func() bool {
		for _, k := range keys {
			if _, ok := c.Get(k); ok {
				return false
			}
		}
		return true
	})

	for _, k := range keys {
		if _, ok := c.Get(k); ok {
			t.Fatalf("F5a RED: entry %s survived a deterministic failure across the whole requeue "+
				"budget — the drop point must EVICT (rule: no outcome may end in forget-and-keep)", k[:8])
		}
	}
	st := RefreshTerminalStatsSnapshot()
	if st.DropEvictTotal != uint64(len(keys)) {
		t.Fatalf("F5a: drop_evict_total = %d, want %d", st.DropEvictTotal, len(keys))
	}
	if st.DropEvictSuspendedTotal != 0 || st.DropEvictSuspendWarns != 0 {
		t.Fatalf("F5a: breaker engaged for %d keys (suspended=%d warns=%d) — a handful of failures "+
			"is the CORRECTNESS regime, the breaker must stay idle", len(keys), st.DropEvictSuspendedTotal, st.DropEvictSuspendWarns)
	}
	// Each key spent its full budget before the eviction: 1 initial + maxRefreshRequeues.
	if want := int32(len(keys) * (1 + maxRefreshRequeues)); attempts.Load() != want {
		t.Fatalf("F5a: attempts = %d, want %d (full budget per key, then evict)", attempts.Load(), want)
	}
	if got := RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Fatalf("F5a: self_notfound_evict_total = %d, want 0 — the C4 route must not be counted as the 404 route", got)
	}
}

func TestRefreshTerminal_F5b_MassFailureIsSuspendedAndStillServed(t *testing.T) {
	terminalEnv(t)
	// Drive the regime by LOWERING the knob, not by generating 129 keys: the
	// arm is independent of the default and survives a re-tune.
	t.Setenv(envRefreshDropEvictMaxPerMinute, "1")
	const n = 6
	c, keys := terminalFixture(t, n)

	var attempts atomic.Int32
	terminalRun(t, c, keys, func(context.Context, string, ResolvedKeyInputs) error {
		attempts.Add(1)
		return errors.New("mass failure — apiserver down")
	}, func() bool {
		// settled == every key has spent its budget (1 + maxRefreshRequeues each)
		return attempts.Load() >= int32(n*(1+maxRefreshRequeues)) &&
			refresherSingleton().droppedTotal.Load() >= n
	})

	resident := 0
	for _, k := range keys {
		if e, ok := c.Get(k); ok {
			resident++
			if string(e.RawJSON) != `{"body":"old"}` {
				t.Fatalf("F5b: a suspended entry's body changed")
			}
		}
	}
	st := RefreshTerminalStatsSnapshot()
	// Budget 1/min: exactly ONE eviction is allowed inside the window; the
	// other n-1 must stay resident AND served.
	if resident != n-1 {
		t.Fatalf("F5b RED: %d of %d entries resident after a mass failure with budget 1/min; want %d "+
			"— an unbounded drop-point evict turns a stale portal into a broken one during an outage",
			resident, n, n-1)
	}
	if st.DropEvictTotal != 1 {
		t.Fatalf("F5b: drop_evict_total = %d, want 1", st.DropEvictTotal)
	}
	if st.DropEvictSuspendedTotal != n-1 {
		t.Fatalf("F5b: drop_evict_suspended_total = %d, want %d", st.DropEvictSuspendedTotal, n-1)
	}
	if st.DropEvictSuspendWarns != 1 {
		t.Fatalf("F5b: suspension WARNs = %d, want exactly 1 per window", st.DropEvictSuspendWarns)
	}
}

func TestRefreshTerminal_KillSwitch_ZeroRestoresDropToTTL(t *testing.T) {
	terminalEnv(t)
	t.Setenv(envRefreshDropEvictMaxPerMinute, "0")
	c, keys := terminalFixture(t, 2)

	terminalRun(t, c, keys, func(context.Context, string, ResolvedKeyInputs) error {
		return errors.New("deterministic failure")
	}, func() bool { return refresherSingleton().droppedTotal.Load() >= 2 })

	for _, k := range keys {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("KILL: entry %s evicted with REFRESH_DROP_EVICT_MAX_PER_MINUTE=0 — the kill switch "+
				"must restore the 1.12.5 drop-to-TTL byte-for-byte", k[:8])
		}
	}
	st := RefreshTerminalStatsSnapshot()
	if st.DropEvictTotal != 0 || st.DropEvictSuspendedTotal != 0 || st.DropEvictSuspendWarns != 0 {
		t.Fatalf("KILL: breaker counters moved (%+v) with the knob at 0", st)
	}
}

// F6m — suppression mechanism end-to-end through the real loop.
func TestRefreshTerminal_F6m_SuppressAfterKDeclinesAndClearOnPut(t *testing.T) {
	terminalEnv(t)
	t.Setenv(envRefreshSuppressAfterDeclines, "3")
	c, keys := terminalFixture(t, 1)
	key := keys[0]

	var invocations atomic.Int32
	// The handler models resolveAndPopulateL1's stage-error decline: it notes
	// the decline and returns nil (a decline is not an error — no requeue).
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		invocations.Add(1)
		NoteRefreshDecline(k, "stage_error", false)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// K=3 declines.
	for i := 1; i <= 3; i++ {
		enqueueRefreshForTest(key)
		want := int32(i)
		waitFor(t, 5*time.Second, "handler invoked", func() bool { return invocations.Load() >= want })
	}
	// Architect N3: a dirty-mark GVR recorded at enqueue must be CONSUMED by
	// a suppressed skip, not left to force-miss an arbitrarily later dequeue.
	// Stored before the enqueue so the worker cannot outrun the store.
	n3GVR := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	refresherInstance.triggerGVRByKey.Store(key, n3GVR)

	// The (K+1)th dequeue must NOT invoke the handler. BEHAVIOUR FIRST: wait
	// for either outcome (the skip was counted, or the handler ran a 4th
	// time) and then assert on the invocation count — on a tree without the
	// mechanism this reads "handler invoked 4 times" (#191), not "marker API
	// returned false".
	enqueueRefreshForTest(key)
	waitFor(t, 5*time.Second, "4th dequeue settled (skipped or resolved)", func() bool {
		return RefreshTerminalStatsSnapshot().SuppressedSkipsTotal >= 1 || invocations.Load() >= 4
	})
	time.Sleep(50 * time.Millisecond)
	if invocations.Load() != 3 {
		t.Fatalf("F6m RED: handler invoked %d times; a suppressed key must be skipped without a resolve (#191)", invocations.Load())
	}
	if _, ok := c.Get(key); !ok {
		t.Fatalf("F6m: suppression must keep the entry resident (refresh-by-traffic-only), it was evicted")
	}
	if reason, ok := RefreshSuppressedReason(key); !ok || reason != "stage_error" {
		t.Fatalf("F6m: key not marked suppressed after 3 consecutive declines (reason=%q ok=%v)", reason, ok)
	}
	if v, present := refresherInstance.triggerGVRByKey.Load(key); present {
		t.Fatalf("F6m RED (N3): the suppressed skip left the trigger GVR %v in triggerGVRByKey — "+
			"it would force-miss whatever dequeue first runs after the next real Put", v)
	}

	// One real Put clears the marker → the next dequeue resolves again.
	e, _ := c.Get(key)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"traffic"}`), Inputs: e.Inputs})
	if _, still := RefreshSuppressedReason(key); still {
		t.Fatalf("F6m RED: marker survived a real Put — suppress-and-never-recover")
	}
	enqueueRefreshForTest(key)
	waitFor(t, 5*time.Second, "handler resumes after Put", func() bool { return invocations.Load() >= 4 })
	cancel()
	resetRefresherForTestKeepTerminalCounters()
}

// B1 — the expvar / OTLP scrape (RefreshTerminalStatsSnapshot) racing the
// FIRST construction of the refresher singleton. Two invariants: no data race
// under -race, and the read never constructs the singleton. RED on 6ee779d:
// the snapshot read refresherInstance directly while refresherInit.Do stored
// it (architect B1, blocking).
func TestRefreshTerminal_B1_SnapshotRacesFirstConstructionWithoutConstructing(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	resetRefresherForTest()
	if refresherInstance != nil {
		t.Fatal("precondition: singleton must not exist")
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		refresherSingleton()
	}()
	var st RefreshTerminalStats
	go func() {
		defer wg.Done()
		<-start
		st = RefreshTerminalStatsSnapshot()
	}()
	close(start)
	wg.Wait()
	if st.SuppressAfterDeclines != RefreshSuppressAfterDeclines() {
		t.Fatalf("snapshot = %+v, knob = %d", st, RefreshSuppressAfterDeclines())
	}

	// And in isolation: a read on a clean process must not construct.
	resetRefresherForTest()
	_ = RefreshTerminalStatsSnapshot()
	if refresherInstance != nil {
		t.Fatal("B1: RefreshTerminalStatsSnapshot constructed the refresher singleton")
	}
}

// F6p — structurally permanent declines suppress on the FIRST occurrence.
func TestRefreshTerminal_F6p_PermanentDeclinesSuppressImmediately(t *testing.T) {
	t.Setenv(envRefreshSuppressAfterDeclines, "3")
	resetRefreshTerminalForTest()
	t.Cleanup(resetRefreshTerminalForTest)
	for _, reason := range []string{"external_touched", "uaf", "unsupported_kind"} {
		k := "k-" + reason
		if !NoteRefreshDecline(k, reason, true) {
			t.Fatalf("F6p: %s not suppressed on the first occurrence", reason)
		}
		if got, _ := RefreshSuppressedReason(k); got != reason {
			t.Fatalf("F6p: reason = %q, want %q", got, reason)
		}
	}
	// A non-permanent decline needs K.
	if NoteRefreshDecline("k-stage", "stage_error", false) || NoteRefreshDecline("k-stage", "stage_error", false) {
		t.Fatalf("F6p: stage_error suppressed before K=3 declines")
	}
	if !NoteRefreshDecline("k-stage", "stage_error", false) {
		t.Fatalf("F6p: stage_error not suppressed at the 3rd decline")
	}
	if st := RefreshTerminalStatsSnapshot(); st.SuppressedKeys != 4 || st.SuppressedSetTotal != 4 {
		t.Fatalf("F6p: stats = %+v, want 4 suppressed keys", st)
	}
}

// F6e — an eviction clears the marker: it cannot outlive its key.
func TestRefreshTerminal_F6e_EvictionClearsTheMarker(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	resetRefreshTerminalForTest()
	t.Cleanup(resetRefreshTerminalForTest)
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "gone"}
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})
	NoteRefreshDecline(key, "uaf", true)
	if _, ok := RefreshSuppressedReason(key); !ok {
		t.Fatal("F6e: setup — key should be suppressed")
	}
	c.deleteForDep(key)
	if _, ok := RefreshSuppressedReason(key); ok {
		t.Fatalf("F6e RED: the suppression marker survived the eviction of its key")
	}
}
