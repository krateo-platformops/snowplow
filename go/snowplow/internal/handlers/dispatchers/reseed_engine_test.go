package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// reseed_engine_test.go — #258 reseed composition arms: reseedTargets drives the
// shared core (reseedUnderCurrentIdentity → seedOneWidgetFn) end-to-end and mints
// the target under its CURRENT sub-generation key; a cancelled ctx re-enqueues the
// unprocessed tail rather than dropping it.

func reseedTestDeps() rePrewarmDeps {
	return rePrewarmDeps{saEP: endpoints.Endpoint{}, saRC: nil, authnNS: h1NS}
}

// TestReseedTargets_MintsCurrentSubGenCell — the #258 happy path: a resident
// widget target, reseeded under its cohort identity, lands under the exact key the
// dispatcher derives for that identity's CURRENT sub-generation (a warm hit for the
// rotated subject's next navigation). Nothing is left to re-enqueue.
func TestReseedTargets_MintsCurrentSubGenCell(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)

	e := reseedWidgetEntry()
	ctx := reseedSeedCtx()
	key, handle, _ := reseedWidgetKey(t, ctx, e)
	if _, ok := handle.Get(key); ok {
		t.Fatal("precondition: the current-sub-gen widgets key must be cold before the reseed")
	}

	req := reseedRequest{
		identity: seedTarget{Username: a1Alice, Groups: []string{a1Group}},
		isWidget: true,
		widget:   e,
	}
	reEnqueue := reseedTargets(context.Background(), reseedTestDeps(), []reseedRequest{req})
	if len(reEnqueue) != 0 {
		t.Fatalf("a successful reseed must leave nothing to re-enqueue; got %d", len(reEnqueue))
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("reseedTargets must mint the cell under the current-sub-gen key %q", key)
	}
}

// TestReseedTargets_CancelReEnqueuesTail — a ctx cancel must not drop work: the
// unprocessed targets are returned for re-enqueue (customer-priority yield can cut
// a batch; the cut tail is re-armed, never lost).
func TestReseedTargets_CancelReEnqueuesTail(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)

	e := reseedWidgetEntry()
	req := reseedRequest{
		identity: seedTarget{Username: a1Alice, Groups: []string{a1Group}},
		isWidget: true,
		widget:   e,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled → the first loop iteration re-enqueues the whole batch

	reEnqueue := reseedTargets(ctx, reseedTestDeps(), []reseedRequest{req, req, req})
	if len(reEnqueue) != 3 {
		t.Fatalf("a cancelled reseed must re-enqueue the entire unprocessed batch; got %d want 3", len(reEnqueue))
	}
}

// TestReseedTargets_ReArmedRunSkipsMintedKeys — the #258 work bound. A re-armed
// run (a ctx cut at the per-scope budget, or the coalesced follow-up after a
// second flush) re-enumerates the WHOLE rotated set; targets an earlier run
// already minted must cost a liveness read, not a re-resolve. Without the
// seedModeRBACShift liveness skip a cut run would restart its prefix on every
// budget and, past ~one budget of targets, never progress (the C1 storm shape).
// RED if the skip is removed: the second run resolves again (resolves=2).
func TestReseedTargets_ReArmedRunSkipsMintedKeys(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	resolves := 0
	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })
	widgetsResolveFn = func(_ context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		resolves++
		return h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}), nil
	}

	e := reseedWidgetEntry()
	key, handle, _ := reseedWidgetKey(t, reseedSeedCtx(), e)
	req := reseedRequest{identity: seedTarget{Username: a1Alice, Groups: []string{a1Group}}, isWidget: true, widget: e}

	if re := reseedTargets(context.Background(), reseedTestDeps(), []reseedRequest{req}); len(re) != 0 {
		t.Fatalf("first run: re-enqueue=%d", len(re))
	}
	if _, ok := handle.Get(key); !ok || resolves != 1 {
		t.Fatalf("first run must mint the cell with one resolve; live=%v resolves=%d", ok, resolves)
	}
	if re := reseedTargets(context.Background(), reseedTestDeps(), []reseedRequest{req}); len(re) != 0 {
		t.Fatalf("re-armed run: re-enqueue=%d", len(re))
	}
	if resolves != 1 {
		t.Fatalf("a re-armed run must SKIP the already-minted new-sub-gen key; resolves=%d, want 1", resolves)
	}
}
