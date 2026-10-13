package dispatchers

// issue643_prc_f4c_stale_redo_progress_test.go — #643 PR-C F4-c (architect-ch
// C4 must-fix): REDOING a stale record is progress. Attempt 0 is cut after j
// units; then a GVR is discovered (epoch+1 — every record is stale); attempt 1
// redoes previously-recorded keys only (no NEW key) and is cut. It must read
// progress=true and requeue at NumRequeues == 1 — a len()-delta measure would
// read 0 new keys and let the backoff climb toward 1000s. The source mutation
// back to the len() delta (RED) is recorded in the PR.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue643_F4c_RedoOfStaleRecordsIsProgress(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)
	env.pass(prewarmScope{kind: scopeKindBoot})
	budgets[scopeKindGVRDiscovered] = 2 * time.Second

	var calls, blockAt int
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered {
			calls++
			if calls == blockAt {
				<-ctx.Done()
				return ctx.Err()
			}
		}
		return prevW(ctx, e, authnNS, mode)
	}
	done := env.engine.passDoneSetFor(prewarmScope{kind: scopeKindGVRDiscovered}.key())
	s := prewarmScope{kind: scopeKindGVRDiscovered}
	attempt := func(block int) int {
		calls, blockAt = 0, block
		env.engine.enqueueScope(s)
		var got prewarmScope
		for {
			got, _ = env.engine.queue.Get()
			if got == s {
				break
			}
			env.engine.queue.Forget(got)
			env.engine.queue.Done(got)
		}
		env.engine.processScope(context.Background(), got)
		return env.engine.queue.NumRequeues(s)
	}

	const j = 4
	if n := attempt(j + 1); n != 1 {
		t.Fatalf("precondition: attempt 0 (progress) requeued at NumRequeues %d, want 1", n)
	}
	keys0, marks0 := done.len(), done.marksTotal()
	if keys0 != j {
		t.Fatalf("precondition: attempt 0 left %d records, want %d", keys0, j)
	}

	// A GVR is discovered: the epoch moves, every record goes stale.
	cache.ResetGVRDiscoveredHooksForTest()
	t.Cleanup(cache.ResetGVRDiscoveredHooksForTest)
	registerEngineGVRDiscoveredHook(env.engine)
	cache.NotifyGVRDiscoveredForReprewarmTest(schema.GroupVersionResource{Group: "f4c.krateo.io", Version: "v1", Resource: "things"})

	// Attempt 1 redoes the first 2 recorded units (no skips: all stale) and is
	// cut before reaching any unit it never did.
	n := attempt(3)
	if got := done.len(); got != keys0 {
		t.Fatalf("precondition: attempt 1 added %d NEW keys, want 0 (it only redoes stale records)", got-keys0)
	}
	if got := done.marksTotal() - marks0; got != 2 {
		t.Fatalf("precondition: attempt 1 made %d marks, want 2 redone records", got)
	}
	if n != 1 {
		t.Fatalf("F4-c: an attempt that redid stale records (real resolves, real Puts) requeued at NumRequeues %d, want 1 — a redo IS progress; counting only new keys lets the backoff climb", n)
	}
}
