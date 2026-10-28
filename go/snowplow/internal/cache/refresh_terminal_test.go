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
// ARMS (#547 amended the drop point and the decline suppression)
//
//	F5a   few deterministic failures across the whole requeue budget -> every
//	      entry stays RESIDENT and KNOWN-STALE (reason dropped), never evicted
//	      and never forgotten as fresh. RED on 9c233de9: evicted (breaker
//	      budget) or the stamp erased (suspended / budget 0).
//	F5b   mass failure (6 keys) -> all resident, all served, all known-stale:
//	      no eviction storm and no breaker to engage (REFRESH_DROP_EVICT_MAX_
//	      PER_MINUTE is retired).
//	F6m   a stage-error decline (non-permanent) NEVER suppresses: every dirty
//	      mark re-resolves (retry on change) and the cell is known-stale
//	      (reason declined). RED on 9c233de9: suppressed after K=3.
//	F6s   a structurally-permanent decline suppresses; the next mark is
//	      skipped without a resolve, the window ends known-stale (reason
//	      suppressed) and the trigger GVR is consumed (architect N3); one real
//	      Put clears the marker.
//	F6p   permanent declines (external / UAF / unsupported) suppress on the
//	      FIRST occurrence; a non-permanent one never does.
//	F6e   an eviction clears the marker (it cannot outlive its key).
//	B1    a stats read (the expvar / OTLP scrape) racing the FIRST
//	      construction of the refresher singleton is race-free and never
//	      constructs the singleton itself.
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
		c.put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"old"}`), Inputs: &inputs})
		keys = append(keys, key)
	}
	return c, keys
}

// resetRefresherForTestKeepTerminalCounters drains the pool but leaves the
// breaker counters readable for the assertions that follow.
func resetRefresherForTestKeepTerminalCounters() {
	if r := refresherPeek(); r != nil {
		r.queue.ShutDown()
		if r.clusterListQueue != nil {
			r.clusterListQueue.ShutDown()
		}
		done := make(chan struct{})
		go func() { r.workersWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}

// terminalMark dirty-marks keys through the refresher's real hook (onDirtyMark:
// trigger merge + window open + enqueue), so each dequeue runs inside a window.
func terminalMark(keys []string) {
	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	for _, k := range keys {
		refresherSingleton().onDirtyMark(k, gvr, nil)
	}
}

func terminalRunMarked(t *testing.T, keys []string, handler RefreshFunc, until func() bool) {
	t.Helper()
	RegisterRefreshFunc("widgets", handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	terminalMark(keys)
	waitFor(t, 10*time.Second, "refresh loop settled", until)
	cancel()
	resetRefresherForTestKeepTerminalCounters()
}

func TestRefreshTerminal_F5a_DeterministicFailuresLeaveEntriesKnownStale(t *testing.T) {
	terminalEnv(t)
	c, keys := terminalFixture(t, 3)

	var attempts atomic.Int32
	terminalRunMarked(t, keys, func(context.Context, string, ResolvedKeyInputs) error {
		attempts.Add(1)
		return errors.New("deterministic 500 — never succeeds")
	}, func() bool { return refresherSingleton().droppedTotal.Load() >= uint64(len(keys)) })

	for _, k := range keys {
		e, ok := c.GetNoTouch(k)
		if !ok {
			t.Fatalf("F5a: entry %s EVICTED at the drop point — #547: a non-404 failure is not a deletion; "+
				"the cell stays resident (zero cold navigations)", k[:8])
		}
		if string(e.RawJSON) != `{"body":"old"}` {
			t.Fatalf("F5a: the dropped entry's body changed")
		}
		if _, reason, ok := KnownStale(k); !ok || reason != "dropped" {
			t.Fatalf("F5a RED: entry %s is resident but NOT known-stale after its refresh was dropped "+
				"(ok=%v reason=%q) — forget-as-fresh, the #547 defect", k[:8], ok, reason)
		}
	}
	// Each key spent its full budget: 1 initial + maxRefreshRequeues.
	if want := int32(len(keys) * (1 + maxRefreshRequeues)); attempts.Load() != want {
		t.Fatalf("F5a: attempts = %d, want %d (full budget per key)", attempts.Load(), want)
	}
	if got := RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Fatalf("F5a: self_notfound_evict_total = %d, want 0 — a 500 is not a confirmed 404", got)
	}
}

func TestRefreshTerminal_F5b_MassFailureStaysResidentServedAndKnownStale(t *testing.T) {
	terminalEnv(t)
	const n = 6
	c, keys := terminalFixture(t, n)
	served0 := ResolvedCacheStatsByStat()["known_stale_served_dropped_total"]

	terminalRunMarked(t, keys, func(context.Context, string, ResolvedKeyInputs) error {
		return errors.New("mass failure — apiserver down")
	}, func() bool { return refresherSingleton().droppedTotal.Load() >= n })

	for _, k := range keys {
		e, ok := c.Get(k) // a customer serve: it must still be served
		if !ok || string(e.RawJSON) != `{"body":"old"}` {
			t.Fatalf("F5b: a mass failure must leave every entry resident AND served (ok=%v)", ok)
		}
		if _, _, ks := KnownStale(k); !ks {
			t.Fatalf("F5b: entry %s served but not known-stale", k[:8])
		}
	}
	if got := ResolvedCacheStatsByStat()["known_stale_served_dropped_total"] - served0; got != n {
		t.Fatalf("F5b: known_stale_served_dropped_total moved %d, want %d (one per customer serve above)", got, n)
	}
}

// F6m — a non-permanent (stage-error) decline never suppresses since #547:
// every change retries, and the cell is known-stale meanwhile.
func TestRefreshTerminal_F6m_StageErrorDeclinesNeverSuppressAndStayKnownStale(t *testing.T) {
	terminalEnv(t)
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

	for i := 1; i <= 4; i++ {
		terminalMark(keys)
		want := int32(i)
		waitFor(t, 5*time.Second, "handler invoked", func() bool { return invocations.Load() >= want })
	}
	waitFor(t, 5*time.Second, "window settled known-stale", func() bool {
		_, r, ok := KnownStale(key)
		return ok && r == "declined"
	})
	if _, ok := RefreshSuppressedReason(key); ok {
		t.Fatalf("F6m: a non-permanent decline suppressed the key — #547 retired REFRESH_SUPPRESS_AFTER_DECLINES")
	}
	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatalf("F6m: a decline evicted the entry")
	}
	cancel()
	resetRefresherForTestKeepTerminalCounters()
}

// F6s — the structurally-permanent suppression end-to-end through the real loop.
func TestRefreshTerminal_F6s_PermanentDeclineSuppressesSkipEndsKnownStale(t *testing.T) {
	terminalEnv(t)
	c, keys := terminalFixture(t, 1)
	key := keys[0]

	var invocations atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		invocations.Add(1)
		NoteRefreshDecline(k, "uaf", true)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	terminalMark(keys)
	waitFor(t, 5*time.Second, "handler invoked", func() bool { return invocations.Load() >= 1 })
	waitFor(t, 5*time.Second, "first window settled", func() bool { _, _, ok := KnownStale(key); return ok })

	// Architect N3: a dirty-mark GVR recorded at enqueue must be CONSUMED by a
	// suppressed skip. terminalMark merges it before the enqueue.
	terminalMark(keys)
	waitFor(t, 5*time.Second, "2nd dequeue settled (skipped or resolved)", func() bool {
		return RefreshTerminalStatsSnapshot().SuppressedSkipsTotal >= 1 || invocations.Load() >= 2
	})
	time.Sleep(50 * time.Millisecond)
	if invocations.Load() != 1 {
		t.Fatalf("F6s: handler invoked %d times; a suppressed key must be skipped without a resolve (#191)", invocations.Load())
	}
	if _, r, ok := KnownStale(key); !ok || r != "suppressed" {
		t.Fatalf("F6s: a suppressed skip must leave the cell known-stale (reason suppressed), got ok=%v reason=%q", ok, r)
	}
	if v, present := refresherPeek().triggerGVRByKey.Load(key); present {
		t.Fatalf("F6s (N3): the suppressed skip left the trigger GVR %v in triggerGVRByKey", v)
	}

	// One real Put clears the marker → the next dequeue resolves again.
	e, _ := c.GetNoTouch(key)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"traffic"}`), Inputs: e.Inputs})
	if _, still := RefreshSuppressedReason(key); still {
		t.Fatalf("F6s: marker survived a real Put — suppress-and-never-recover")
	}
	terminalMark(keys)
	waitFor(t, 5*time.Second, "handler resumes after Put", func() bool { return invocations.Load() >= 2 })
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
	if refresherPeek() != nil {
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
	if st.SuppressedKeys < 0 {
		t.Fatalf("snapshot = %+v", st)
	}

	// And in isolation: a read on a clean process must not construct.
	resetRefresherForTest()
	_ = RefreshTerminalStatsSnapshot()
	if refresherPeek() != nil {
		t.Fatal("B1: RefreshTerminalStatsSnapshot constructed the refresher singleton")
	}
}

// F6p — structurally permanent declines suppress on the FIRST occurrence.
func TestRefreshTerminal_F6p_PermanentDeclinesSuppressImmediately(t *testing.T) {
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
	// #547 — a non-permanent decline never suppresses (no K).
	for i := 0; i < 5; i++ {
		if NoteRefreshDecline("k-stage", "stage_error", false) {
			t.Fatalf("F6p: stage_error suppressed at decline %d — REFRESH_SUPPRESS_AFTER_DECLINES is retired", i+1)
		}
	}
	if st := RefreshTerminalStatsSnapshot(); st.SuppressedKeys != 3 || st.SuppressedSetTotal != 3 {
		t.Fatalf("F6p: stats = %+v, want 3 suppressed keys", st)
	}
}

// F6e — an eviction clears the marker: it cannot outlive its key.
func TestRefreshTerminal_F6e_EvictionClearsTheMarker(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	resetRefreshTerminalForTest()
	t.Cleanup(resetRefreshTerminalForTest)
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "gone"}
	key := ComputeKey(inputs)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})
	NoteRefreshDecline(key, "uaf", true)
	if _, ok := RefreshSuppressedReason(key); !ok {
		t.Fatal("F6e: setup — key should be suppressed")
	}
	c.deleteForDep(key)
	if _, ok := RefreshSuppressedReason(key); ok {
		t.Fatalf("F6e RED: the suppression marker survived the eviction of its key")
	}
}
