package dispatchers

// issue643_prc_f4_f5_test.go — #643 PR-C falsifiers (changes 2 + 3: coalesce
// gvr-discovered into one pending pass; resume a cut pass without redoing its
// prefix), on the shared #643 harness (real engine queue + worker, real seed,
// real resolve).
//
//   F4   coalescing — a worker is blocked inside a gvr-discovered pass while the
//        REAL cache→engine hook fires for 51 distinct GVRs: after release at
//        most ONE more gvr-discovered pass runs (≤ 2 in total).
//   F5   resume — attempt 0 is cut after j units; across attempts every key is
//        resolved exactly once (attempt 1 resolves K·N − j), and the resume
//        positive control reads > 0.
//   F5-e epoch (I-3) — a GVR discovered between attempt 0 and attempt 1 makes
//        attempt 1 re-resolve EVERY cacheable key.
//   F5-z zero-cold — a unit attempt 0 resolved is re-resolved by attempt 1 when
//        its cell was evicted, its input changed (dirty), or its cell was
//        re-Put by someone else.
//
// RED on PR-B's base (5129e33e): F4 runs ~51 passes; F5 redoes the prefix.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// h643ResolveCounter counts real widget resolves per (widget, identity).
type h643ResolveCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *h643ResolveCounter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := 0
	for _, v := range c.n {
		t += v
	}
	return t
}

func (c *h643ResolveCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.n))
	for k, v := range c.n {
		out[k] = v
	}
	return out
}

// h643CtxGroups returns the resolving identity's groups (the cohort).
func h643CtxGroups(ctx context.Context) []string {
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return nil
	}
	return ui.Groups
}

// h643CountResolves wraps the harness's widgetsResolveFn to count resolves.
func h643CountResolves(t *testing.T) *h643ResolveCounter {
	t.Helper()
	c := &h643ResolveCounter{n: map[string]int{}}
	prev := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		id := strings.Join(h643CtxGroups(ctx), ",")
		c.mu.Lock()
		c.n[opts.In.GetName()+"|"+id]++
		c.mu.Unlock()
		return prev(ctx, opts)
	}
	t.Cleanup(func() { widgetsResolveFn = prev })
	return c
}

func h643PassesRun(kind prewarmScopeKind) uint64 {
	return h643PassCount(string(kind), prewarmPassOutcomeComplete) + h643PassCount(string(kind), prewarmPassOutcomeCut)
}

func h643ResumeSkips() uint64 {
	for _, c := range PrewarmSkipResumeDoneSnapshot() {
		if c.Kind == string(scopeKindGVRDiscovered) {
			return c.Count
		}
	}
	return 0
}

func TestIssue643_F4_GVRDiscoveredCoalescesIntoOnePass(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	h643Budget(t, nil)
	env.pass(prewarmScope{kind: scopeKindBoot})

	cache.ResetGVRDiscoveredHooksForTest()
	t.Cleanup(cache.ResetGVRDiscoveredHooksForTest)
	registerEngineGVRDiscoveredHook(env.engine)

	// Block the worker inside the first gvr-discovered pass.
	release := make(chan struct{})
	var blocked atomic.Bool
	entered := make(chan struct{})
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return prevW(ctx, e, authnNS, mode)
	}
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); env.engine.runWorker(wctx) }()
	t.Cleanup(func() {
		wcancel()
		<-done
	})

	runs0 := h643PassesRun(scopeKindGVRDiscovered)
	gvr := func(i int) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "x" + itoaLine(i) + ".cnrm.cloud.google.com", Version: "v1beta1", Resource: "things"}
	}
	cache.NotifyGVRDiscoveredForReprewarmTest(gvr(0))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never entered the first gvr-discovered pass")
	}
	for i := 1; i < 51; i++ {
		cache.NotifyGVRDiscoveredForReprewarmTest(gvr(i))
	}
	close(release)

	// Wait until the queue is drained and the worker idle for a while.
	deadline := time.Now().Add(60 * time.Second)
	last, stableSince := h643PassesRun(scopeKindGVRDiscovered), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		cur := h643PassesRun(scopeKindGVRDiscovered)
		if cur != last {
			last, stableSince = cur, time.Now()
			continue
		}
		if env.engine.queue.Len() == 0 && time.Since(stableSince) > 2*time.Second {
			break
		}
	}
	got := h643PassesRun(scopeKindGVRDiscovered) - runs0
	if got > 2 {
		t.Fatalf("F4: 51 discovered GVRs produced %d gvr-discovered passes, want ≤ 2 (the in-flight one + ONE coalesced re-run) — the pass ignores its GVR, so N GVRs must fold into one pending pass", got)
	}
	if got < 2 {
		t.Fatalf("F4: only %d gvr-discovered passes ran; a GVR discovered DURING a pass must still trigger exactly one more", got)
	}
	if e := env.engine.gvrEpoch.Load(); e != 51 {
		t.Fatalf("F4: discovery epoch = %d after 51 discoveries, want 51", e)
	}
	// The coalesced item completed: its backoff history is reset (Forget), so a
	// later cut starts from the base delay, not the tail of a streak.
	if n := env.engine.queue.NumRequeues(prewarmScope{kind: scopeKindGVRDiscovered}); n != 0 {
		t.Fatalf("F4: NumRequeues(gvr-discovered) = %d after the coalesced pass completed, want 0 (Forget on success)", n)
	}
}

// h643F5 drives: attempt 0 cut after j real resolves, then attempt 1 to
// completion. between runs after attempt 0. Returns resolves per attempt.
func h643F5(t *testing.T, j int, between func(env *h643Env)) (env *h643Env, a0, a1 int, cnt *h643ResolveCounter) {
	t.Helper()
	env = h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)
	env.pass(prewarmScope{kind: scopeKindBoot})
	cnt = h643CountResolves(t)

	// Attempt 0: the (j+1)-th unit holds until the budget expires (a cut).
	var units atomic.Int64
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered && units.Add(1) == int64(j+1) {
			<-ctx.Done()
			return ctx.Err()
		}
		return prevW(ctx, e, authnNS, mode)
	}
	budgets[scopeKindGVRDiscovered] = 3 * time.Second
	base := cnt.total()
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})
	a0 = cnt.total() - base
	if a0 != j {
		t.Fatalf("precondition: attempt 0 resolved %d units before the cut, want %d", a0, j)
	}
	seedOneWidgetFn = prevW
	budgets[scopeKindGVRDiscovered] = time.Hour
	if between != nil {
		between(env)
	}
	base = cnt.total()
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})
	a1 = cnt.total() - base
	return env, a0, a1, cnt
}

func TestIssue643_F5_ResumeDoesNotRedoThePrefix(t *testing.T) {
	const j = 5
	skip0 := h643ResumeSkips()
	_, a0, a1, cnt := h643F5(t, j, nil)
	if a1 != h643K*h643N-j {
		t.Fatalf("F5: attempt 1 resolved %d units, want %d (K·N − j): a resumed pass must not redo the units attempt 0 already re-resolved", a1, h643K*h643N-j)
	}
	for k, n := range cnt.snapshot() {
		if n != 1 {
			t.Fatalf("F5: %s resolved %d times across attempts 0+1, want exactly 1", k, n)
		}
	}
	if got := h643ResumeSkips() - skip0; got != uint64(a0) {
		t.Fatalf("F5: skip_resume_done_total{gvr-discovered} moved by %d, want %d (the positive control)", got, a0)
	}
}

func TestIssue643_F5e_NewGVRBetweenAttemptsRedoesEveryKey(t *testing.T) {
	const j = 5
	_, _, a1, _ := h643F5(t, j, func(env *h643Env) {
		cache.ResetGVRDiscoveredHooksForTest()
		t.Cleanup(cache.ResetGVRDiscoveredHooksForTest)
		registerEngineGVRDiscoveredHook(env.engine)
		cache.NotifyGVRDiscoveredForReprewarmTest(schema.GroupVersionResource{Group: "late.krateo.io", Version: "v1", Resource: "lates"})
	})
	if a1 != h643K*h643N {
		t.Fatalf("F5-e (I-3): a GVR discovered between attempts must make attempt 1 re-resolve EVERY cacheable key (%d), it resolved %d — a resume that ignores the epoch skips a dep edge", h643K*h643N, a1)
	}
}
