// issue544_retry_trigger_restore_test.go — #544: a refresher RETRY must not
// silently downgrade a re-resolve to a stale-input read.
//
// UNTAGGED on purpose, like refresher_test.go / refresh_terminal_test.go /
// issue375_dep_gen_guard_test.go (196 of 207 test files in this package carry no
// build tag): an untagged file compiles under a bare `go test` AND under
// -tags unit and -tags integration, so the arm cannot be skipped by a tag
// (feedback_falsifier_must_actually_run_under_gate_tag_env).
//
// THE DEFECT (refresher.go, processNext). The key's trigger-GVR set is taken out
// of triggerGVRByKey with LoadAndDelete BEFORE processOne runs. On handler error
// the key is re-queued with AddRateLimited, and nothing restored the set — every
// writer of that map (mergeTriggerGVR via SetRefreshTriggerMergeHook /
// SetRefreshHook / SetRefreshMultiTriggerHook) sits on the dirty-mark path only.
//
// The set has exactly one consumer, apistage.go:527-528:
//
//	_, refresherDriven := cache.RefreshTriggerGVRFromContext(ctx)
//	forceContentMiss := refresherDriven && cache.RefreshTriggerHas(ctx, gvr)
//
// so with the set gone the retried re-resolve has refresherDriven=false and
// every stage reads its input from the CONTENT CACHE — including the stage whose
// change triggered the refresh. The result is then written through
// ReplaceIfGenRefresh, the one entry point trusted to reset the birth stamp.
// The retried refresh therefore completes, advances the counters, slides
// CreatedAt and looks healthy in every observable way while holding the stale
// content that triggered it.
//
// It self-selects for the worst case: a retry means the first attempt FAILED,
// i.e. exactly when the upstream was unhealthy and the inputs most likely moved.
//
// SHAPE. TWO distinct trigger GVRs, not one: a single-GVR arm cannot tell a full
// restore from one that drops all but the first member of the set (#375 B merges
// several marks into one set, so a partial restore is a live failure mode).
//
// NEUTER that must turn arm 1 RED: delete the
//
//	for _, g := range consumedTriggers { r.mergeTriggerGVR(key, g) }
//
// restore from the retry branch of processNext.
//
// NOT NAMED *_arm_test.go on purpose: Go reads a trailing _arm as the GOARCH
// implicit build constraint and would silently exclude the whole file
// (reference_go_filename_goos_goarch_build_constraint).
package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	g544A = schema.GroupVersionResource{Group: "a544.example.io", Version: "v1", Resource: "alphas"}
	g544B = schema.GroupVersionResource{Group: "b544.example.io", Version: "v1", Resource: "betas"}
)

// i544Triggers is what one dispatch saw of the trigger set.
type i544Triggers struct{ a, b bool }

// TestIssue544_RetryStillForcesContentMiss is THE arm. A refresh carrying two
// trigger GVRs fails once; the retry must still force a content miss for BOTH.
//
// Real boundary, not installed state: the marks come from Deps().OnUpdate
// through the real dirty-mark hook, the failure comes from a real handler error,
// and the retry comes from the real AddRateLimited re-queue
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state).
func TestIssue544_RetryStillForcesContentMiss(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	// Tight backoff so the retry lands inside the arm's budget; the floor is
	// already 0 from withCleanRefresher. Set BEFORE the singleton is rebuilt.
	t.Setenv(envRefresherBaseDelayMS, "1")
	t.Setenv(envRefresherMaxDelayMS, "2")
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled — the fixture cannot build a resident entry")
	}
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "i544-retry"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now().Add(-agedBackoff)})
	Deps().Record(context.Background(), key, g544A, "ns", "a")
	Deps().Record(context.Background(), key, g544B, "ns", "b")

	gate := make(chan struct{})
	var attempts atomic.Int32
	var mu sync.Mutex
	saw := map[int32]i544Triggers{}

	RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, _ ResolvedKeyInputs) error {
		n := attempts.Add(1)
		mu.Lock()
		saw[n] = i544Triggers{a: RefreshTriggerHas(ctx, g544A), b: RefreshTriggerHas(ctx, g544B)}
		mu.Unlock()
		switch n {
		case 1:
			// The priming dequeue carries NO trigger. Holding it here is what
			// makes both marks accumulate into ONE later dispatch (the #375 (C)
			// recipe), so the failing attempt carries a two-member set.
			<-gate
			return nil
		case 2:
			// The first REAL attempt fails: the upstream was unhealthy.
			return errors.New("#544 arm: upstream unhealthy on the first real attempt")
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(key)
	waitFor(t, 5*time.Second, "the priming dequeue to be in flight", func() bool { return attempts.Load() == 1 })
	Deps().OnUpdate(g544A, "ns", "a") // mark 1, while the key is processing
	Deps().OnUpdate(g544B, "ns", "b") // mark 2, same re-queue
	close(gate)
	waitFor(t, 15*time.Second, "the failed dispatch and its retry",
		func() bool { return attempts.Load() >= 3 && refresherPeek().completedTotal.Load() >= 2 })
	cancel()

	mu.Lock()
	defer mu.Unlock()

	// INSTRUMENT CHECK at the "all" extreme before trusting the RED below: the
	// FAILING dispatch must itself have carried both triggers. If it did not,
	// the fixture never built the two-trigger shape and the retry assertion
	// would be measuring the fixture, not the restore.
	if a2 := saw[2]; !a2.a || !a2.b {
		t.Fatalf("#544 arm 1 AMBIGUOUS (fixture, NOT the defect): the failing dispatch must itself carry both "+
			"triggers, got A=%v B=%v. The two dirty-marks did not accumulate into one dispatch, so the retry "+
			"assertion below would not be exercising the restore at all.", a2.a, a2.b)
	}
	// INSTRUMENT CHECK at the "zero" extreme: the priming dequeue carried no
	// trigger, so a harness that stamps every ctx would show up here.
	if a1 := saw[1]; a1.a || a1.b {
		t.Fatalf("#544 arm 1 AMBIGUOUS (fixture, NOT the defect): the priming dequeue carries no dirty-mark and "+
			"must see NO trigger, got A=%v B=%v — RefreshTriggerHas is not discriminating.", a1.a, a1.b)
	}

	if a3 := saw[3]; !a3.a || !a3.b {
		t.Fatalf("#544 arm 1 RED: the RETRY of a failed refresh must still force a content miss, but the "+
			"re-dispatch carried A=%v B=%v. processNext's LoadAndDelete consumed the trigger set before "+
			"processOne and the retry re-queued without restoring it, so apistage sees refresherDriven=false "+
			"and every stage reads its input from the content cache — including the one whose change triggered "+
			"the refresh — after which ReplaceIfGenRefresh resets the birth stamp on that stale content.",
			a3.a, a3.b)
	}

	// The restore must be CONSUMED by the dispatch it was restored for, not
	// accumulate: attempt 3 succeeded, so the map is empty again.
	if v, present := refresherPeek().triggerGVRByKey.Load(key); present {
		t.Fatalf("#544 arm 1 RED: the successful retry left the restored trigger set %+v resident in "+
			"triggerGVRByKey — a restored set must be consumed by the dispatch it was restored for, "+
			"otherwise it force-misses every later refresh of this key forever.", v)
	}
}

// TestIssue544_FirstAttemptSuccessLeavesNoRestoredTrigger is the OTHER
// direction: a refresh that succeeds first time must behave exactly as before
// the fix — the trigger set is consumed and NOTHING is put back. An over-broad
// fix that restored on every exit (rather than only on the retry) would leave
// the set resident and force-miss every subsequent refresh of the key.
func TestIssue544_FirstAttemptSuccessLeavesNoRestoredTrigger(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled — the fixture cannot build a resident entry")
	}
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "i544-success"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now().Add(-agedBackoff)})
	Deps().Record(context.Background(), key, g544A, "ns", "a")

	var sawA atomic.Bool
	RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, _ ResolvedKeyInputs) error {
		if RefreshTriggerHas(ctx, g544A) {
			sawA.Store(true)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	before := refresherPeek().completedTotal.Load()
	Deps().OnUpdate(g544A, "ns", "a")
	// completedTotal advances AFTER the success branch has run, so this is the
	// state assertion's happens-before — no polling for an absence.
	waitFor(t, 10*time.Second, "the first-time-successful dispatch to settle",
		func() bool { return refresherPeek().completedTotal.Load() > before })
	cancel()

	if !sawA.Load() {
		t.Fatalf("#544 arm 2 AMBIGUOUS (fixture, NOT the defect): the single dispatch must carry the trigger " +
			"GVR, so the dirty-mark never reached the refresher with its trigger — nothing about the success " +
			"path was measured.")
	}
	if v, present := refresherPeek().triggerGVRByKey.Load(key); present {
		t.Fatalf("#544 arm 2 RED: a refresh that SUCCEEDED on its first attempt left %+v in triggerGVRByKey. "+
			"The retry restore must be scoped to the retry branch; restoring on the success path makes every "+
			"later refresh of this key force a content miss it has no reason to force.", v)
	}
	if rt := refresherPeek().retriedTotal.Load(); rt != 0 {
		t.Fatalf("#544 arm 2 AMBIGUOUS (fixture): expected a clean first-time success, but retriedTotal=%d — "+
			"this arm was not measuring the success path", rt)
	}
}

// TestIssue544_FloorDeferredKeyConsumesTriggerAtEventualDispatch pins the
// property the LoadAndDelete's placement comment protects: the consume sits
// AFTER the rate-floor branch, so a floor-deferred key keeps its trigger in the
// map across the deferral and consumes it at the eventual REAL dispatch.
//
// This is a regression guard on the fix's blast radius, not a RED arm for #544 —
// it passes before and after, and a fix that moved the LoadAndDelete to after
// processOne (the other candidate) is what it exists to catch.
func TestIssue544_FloorDeferredKeyConsumesTriggerAtEventualDispatch(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	// Floor ON (seconds granularity, so 1s is the shortest real deferral).
	t.Setenv(envRefresherRateFloorSeconds, "1")
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled — the fixture cannot build a resident entry")
	}
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "i544-floored"}
	key := ComputeKey(in)
	// FRESH CreatedAt → the first dequeue is inside the floor and is deferred.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now()})
	Deps().Record(context.Background(), key, g544A, "ns", "a")

	var attempts atomic.Int32
	var sawA atomic.Bool
	RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, _ ResolvedKeyInputs) error {
		attempts.Add(1)
		if RefreshTriggerHas(ctx, g544A) {
			sawA.Store(true)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	Deps().OnUpdate(g544A, "ns", "a")
	waitFor(t, 10*time.Second, "the rate-floor deferral",
		func() bool { return refresherPeek().flooredTotal.Load() >= 1 })

	// WHILE DEFERRED the trigger must still be in the map — the floored branch
	// returns before the consume precisely so the mark survives the deferral.
	if _, present := refresherPeek().triggerGVRByKey.Load(key); !present {
		t.Fatalf("#544 arm 3 RED: the rate-floor deferral consumed the trigger set. The floored branch must " +
			"return BEFORE the LoadAndDelete so the GVR survives the deferral and is consumed at the eventual " +
			"real dispatch; a fix that moved the consume past processOne breaks exactly this.")
	}
	// The handler has not run yet: the floor deferred the work, it did not do it.
	if n := attempts.Load(); n != 0 {
		t.Fatalf("#544 arm 3 AMBIGUOUS (fixture): the floored dequeue must not reach the handler, got %d "+
			"handler calls — the entry was not inside the floor", n)
	}

	waitFor(t, 15*time.Second, "the eventual real dispatch at floor expiry",
		func() bool { return attempts.Load() >= 1 })
	cancel()

	if !sawA.Load() {
		t.Fatalf("#544 arm 3 RED: the dispatch that eventually ran after the floor deferral did NOT carry the " +
			"trigger GVR, so the deferred mark lost its force-miss on the way through the floor.")
	}
}
