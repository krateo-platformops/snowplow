// apistage_fill_admission_review_710_test.go — #710 PR-2a, the architect
// review's arms (issuecomment-6184535627):
//
//   - R2 JOIN-BYPASS (users never queue): a customer that joins a QUEUED
//     background flight admits it at once — it dispatches with no release.
//     Mutations "no promotion" and "relabel only" → RED.
//   - R3 LOCK ORDER: with the nested-resolve gate full (one admitted tree, zero
//     headroom), that tree's content fill still completes — a fill never enters
//     the nested gate. Mutation "the fill takes a nested admission" → it never
//     completes.
//   - R4 NOISE: shrunken calibration samples (a partial GC mid-fill) do not
//     ratchet the expansion down. Mutation "symmetric EWMA" → RED.
//   - R5 NOT BAKED (driven by a BACKGROUND resolve — only background
//     queues): an admission timeout is a stage error (the dispatcher
//     declines its Put, restactions.go: stageErrSink.Count() > 0, not
//     denial-only), nothing is stored, and once the pressure clears the next
//     read of the same call resolves content. Mutation "the error is not
//     recorded as a stage error" → RED.
//   - R5 OWN BUDGET (background leader and joiner): a joiner of a flight that
//     timed out on its LEADER's deadline, with its own request alive, starts a new flight and is served
//     once the pressure clears. Mutation "joiners inherit the failure" → RED.
package api

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

// TestContentFill710_R2_CustomerJoinDispatchesAQueuedBackgroundFlightAtOnce:
// a background flight is queued at zero headroom behind a holder that is
// NEVER released during the arm. A customer joins it: the flight is admitted
// at once and dispatches, and the customer is served — no release needed.
// Mutations "no promotion" and "promotion only relabels" → the flight stays
// queued, the customer waits → RED.
func TestContentFill710_R2_CustomerJoinDispatchesAQueuedBackgroundFlightAtOnce(t *testing.T) {
	newFill710MultiNSWatcher(t, 1, 20)
	store := cache.ResolvedCache()
	fill710Admission(t)
	fill710SeamCeiling(t, 1)

	var dispatched atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		dispatched.Add(1)
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })

	holder, _, err := admitContentFillForTest(context.Background(), r1FsaGVR.String(), contentFillSeedRaw, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder) // released only after the arm
	served := make(chan string, 2)
	read := func(name string, ctx context.Context) {
		go func() {
			if _, s, ok, err := apistageContentServe(ctx, store, fill710NSListCall(0), false); ok && s && err == nil {
				served <- name
				return
			}
			served <- name + ":failed"
		}()
	}
	read("background", fill710BgCtx())
	if !fill710WaitFor(5*time.Second, func() bool { return ContentFillAdmissionStatsSnapshot().Queued["background"] == 1 }) {
		t.Fatalf("PRECONDITION: the background flight did not queue")
	}
	if dispatched.Load() != 0 {
		t.Fatalf("PRECONDITION: the queued background flight dispatched")
	}
	read("customer", r1Ctx())
	for i := 0; i < 2; i++ {
		select {
		case s := <-served:
			if len(s) > 7 && s[len(s)-7:] == ":failed" {
				t.Fatalf("%s", s)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("R2 RED: a customer joined the queued background flight and still waited (dispatches %d, queued %v) — the join must admit the flight at once",
				dispatched.Load(), ContentFillAdmissionStatsSnapshot().Queued)
		}
	}
	if d := dispatched.Load(); d != 1 {
		t.Fatalf("dispatches = %d, want 1 (the promoted flight, shared)", d)
	}
	if q := ContentFillAdmissionStatsSnapshot().Queued; q["background"] != 0 || q["customer"] != 0 {
		t.Fatalf("queued %v after the promoted flight ran", q)
	}
}

func TestContentFill710_R3_NestedGateFullTheTreesFillStillCompletes(t *testing.T) {
	newFill710MultiNSWatcher(t, 1, 5)
	store := cache.ResolvedCache()
	fill710Admission(t)
	store.SetTombstoneTTL(500 * time.Millisecond)
	// The nested-resolve gate at zero headroom, one tree admitted (the
	// empty-gate escape) and held: any further nested admission would wait.
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)
	setRuntimeSeamsForTest(func() int64 { return 8 }, func() int64 { return 8 })
	tree, err := enterNestedResolveUnit(context.Background())
	if err != nil {
		t.Fatalf("admit tree: %v", err)
	}
	defer tree()
	if _, n := inFlightWeightForTest(); n != 1 {
		t.Fatalf("PRECONDITION: nested gate not holding one tree (%d)", n)
	}
	// The fill gate at zero headroom too: only its own empty-gate escape.
	fill710SeamCeiling(t, 1)

	done := make(chan error, 1)
	go func() {
		_, s, ok, err := apistageContentServe(r1Ctx(), store, fill710NSListCall(0), false)
		if err == nil && (!ok || !s) {
			err = errors.New("not served")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("R3 RED: the admitted tree's content fill failed (%v) — a fill must never wait on the nested gate (lock order nested → fill)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("R3 RED: the admitted tree's content fill never completed")
	}
}

func TestContentFill710_R4_ShrunkenSamplesDoNotRatchetTheExpansionDown(t *testing.T) {
	fill710Admission(t)
	g := theContentFillGate()
	const gvr = "r4/v1, Resource=things"
	const truth = 10.0
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calibrateLocked(gvr, truth)
	for round := 0; round < 3; round++ {
		for i := 0; i < 10; i++ {
			g.calibrateLocked(gvr, 0.3*truth) // a partial GC shrank the delta
			if e := g.expansion[gvr]; e < truth/2 {
				t.Fatalf("R4 RED: after %d shrunken samples the expansion is %.2f, under half the true %.0f — the calibration ratchets down on noise", i+1, e, truth)
			}
		}
		g.calibrateLocked(gvr, truth)
		if e := g.expansion[gvr]; e != truth {
			t.Fatalf("a true sample must restore the expansion at once: %.2f", e)
		}
	}
}

func TestContentFill710_R5_TimeoutIsAStageErrorNeverStored_NextReadResolves(t *testing.T) {
	newF1Watcher(t)
	store := cache.ResolvedCache()
	fill710Admission(t)
	store.SetTombstoneTTL(300 * time.Millisecond)
	fill710SeamCeiling(t, 1)
	holder, _, err := admitContentFillForTest(context.Background(), "r5/v1, Resource=held", 1, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)

	// Under pressure: the resolve carries a stage error the dispatcher's Put
	// decline reads (restactions.go / widgets.go: stageErrSink.Count() > 0 and
	// not denial-only ⇒ "declining to cache the partial result").
	sinkCount, denialOnly, dict := f1ResolveWithStageSink(t, f1BroadUser, f1ListStage("widgets"), true)
	if sinkCount == 0 || denialOnly {
		t.Fatalf("R5 RED: an admission timeout was not recorded as a (non-denial) stage error (count=%d denialOnly=%v) — the customer cell would be Put with the error baked in: %v",
			sinkCount, denialOnly, dict)
	}
	if _, ok := store.GetNoTouch(cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))); ok {
		t.Fatalf("R5 RED: the content cell was stored by a flight that was never admitted")
	}

	// The pressure clears: the next read of the same call resolves content.
	holder()
	sinkCount, _, dict = f1ResolveWithStageSink(t, f1BroadUser, f1ListStage("widgets"), true)
	if sinkCount != 0 {
		t.Fatalf("R5: with the pressure gone the resolve still errors (%d): %v", sinkCount, dict)
	}
	if items, ok := dict["widgets"].([]any); !ok || len(items) == 0 {
		t.Fatalf("R5 RED: the next read after the pressure cleared did not resolve content: %v", dict)
	}
}

func TestContentFill710_R5_JoinerWithItsOwnBudgetStartsANewFlight(t *testing.T) {
	newFill710MultiNSWatcher(t, 1, 5)
	store := cache.ResolvedCache()
	fill710Admission(t)
	store.SetTombstoneTTL(300 * time.Millisecond) // the LEADER's fill deadline
	fill710SeamCeiling(t, 1)
	holder, _, err := admitContentFillForTest(context.Background(), "r5b/v1, Resource=held", 1, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)
	var released atomic.Bool
	release := func() {
		if released.CompareAndSwap(false, true) {
			holder()
		}
	}
	t.Cleanup(release)

	type out struct {
		ok, served bool
		err        error
	}
	leader := make(chan out, 1)
	go func() {
		_, s, ok, err := apistageContentServe(fill710BgCtx(), store, fill710NSListCall(0), false)
		leader <- out{ok, s, err}
	}()
	if !fill710WaitFor(5*time.Second, func() bool { return ContentFillAdmissionStatsSnapshot().Queued["background"] == 1 }) {
		t.Fatalf("PRECONDITION: the leader did not queue")
	}
	joiner := make(chan out, 1)
	go func() {
		_, s, ok, err := apistageContentServe(fill710BgCtx(), store, fill710NSListCall(0), false)
		joiner <- out{ok, s, err}
	}()
	if !fill710WaitFor(5*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == 1 }) {
		t.Fatalf("PRECONDITION: the second reader did not join")
	}
	// The leader's deadline passes: its flight fails.
	r := <-leader
	if !errors.Is(r.err, ErrContentFillAdmissionTimeout) {
		t.Fatalf("PRECONDITION: the leader got %v, want the admission timeout", r.err)
	}
	// The pressure clears while the joiner — whose own request is alive — is
	// on its own new flight.
	release()
	select {
	case j := <-joiner:
		if j.err != nil || !j.ok || !j.served {
			t.Fatalf("R5 RED: a joiner with its own budget inherited its leader's deadline failure (err=%v ok=%v served=%v) instead of starting a new flight", j.err, j.ok, j.served)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the joiner was never served")
	}
}

// f1ResolveWithStageSink is f1ResolveAs under a stage-error sink, as the
// dispatchers run it; it returns the sink's count, whether its errors are all
// per-identity denials, and the dict.
func f1ResolveWithStageSink(t *testing.T, user string, stage *templates.API, background bool) (int64, bool, map[string]any) {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: user}))
	if background {
		// The refresher's re-resolve: the only class the cap queues.
		ctx = cache.WithBackgroundResolve(ctx)
	}
	ctx, sink := cache.WithStageErrorSink(ctx)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "default",
		RESTActionName:      "f1-widget-restaction",
	})
	return sink.Count(), cache.DenialOnly(sink, nil, nil, "restactions"), dict
}
