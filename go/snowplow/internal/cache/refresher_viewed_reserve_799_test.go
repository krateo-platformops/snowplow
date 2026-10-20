// refresher_viewed_reserve_799_test.go — #799 fix 2, arms F3 / F4
// (architect-623's ruling, #799 issuecomment-6289159553).
//
// dev-765h's pair at 6bd30396 (RUN): under constant churn on viewed cells the
// viewed tier never emptied, strict priority kept every worker on it, and the
// normal tier completed ZERO refreshes (vs 1,339 / 1,018 on main).
//
// Both arms hold the viewed tier SATURATED for their whole run: a goroutine
// re-marks every viewed key every 2ms while each viewed re-resolve takes 20ms,
// so the tier always holds more keys than workers. Meanwhile N normal keys are
// marked once. The normal tier must keep pace — every normal key completed
// within a bound a single worker meets easily — not merely "eventually 1".
//
//   - F3 (p=4): all N normal keys complete, and at most p−1 workers are ever in
//     viewed work at once. Mutation: no reservation → normal completions 0.
//   - F4 (p=1): the tiers ALTERNATE: all N normal keys complete while viewed
//     keys keep completing too. Mutation: no alternation → normal completions 0.
//   - F5 (p=1, #799 N4): the same with the N keys on the CLUSTER_LIST tier: the
//     alternation must see that tier too (it once checked only the normal one).
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func reserve799Run(t *testing.T, p int, clusterList bool) (normalDone, viewedDone, maxViewed int32, elapsed time.Duration) {
	t.Helper()
	const (
		nViewed = 6
		nNormal = 30
	)
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, nViewed+nNormal)
	viewed, normal := keys[:nViewed], keys[nViewed:]
	isViewed := map[string]bool{}
	for _, k := range viewed {
		isViewed[k] = true
	}
	var nDone, vDone, inViewed, maxV atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		if isViewed[k] {
			n := inViewed.Add(1)
			for m := maxV.Load(); n > m && !maxV.CompareAndSwap(m, n); m = maxV.Load() {
			}
			time.Sleep(20 * time.Millisecond)
			inViewed.Add(-1)
			vDone.Add(1)
			return nil
		}
		time.Sleep(5 * time.Millisecond)
		nDone.Add(1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, viewed...)

	// Constant churn on the viewed cells for the whole run.
	stop := make(chan struct{})
	churned := make(chan struct{})
	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	go func() {
		defer close(churned)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, k := range viewed {
				refresherSingleton().onDirtyMark(k, gvr, nil)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	defer func() { close(stop); <-churned }()
	// Let the viewed tier saturate first.
	waitFor(t, 10*time.Second, "the viewed tier saturated", func() bool { return vDone.Load() >= int32(2*p) })

	if clusterList {
		for _, k := range normal {
			RegisterClusterListKey(k)
		}
	}
	t0 := time.Now()
	v0 := vDone.Load()
	terminalMark(normal)
	deadline := t0.Add(5 * time.Second)
	for nDone.Load() < nNormal && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// viewedDone: viewed completions WHILE the other tier drained, so an arm can
	// assert both tiers progressed in the same window.
	return nDone.Load(), vDone.Load() - v0, maxV.Load(), time.Since(t0)
}

// TestViewed799_F3_ReservationKeepsTheNormalTierRunning is F3.
func TestViewed799_F3_ReservationKeepsTheNormalTierRunning(t *testing.T) {
	const p = 4
	normalDone, viewedDone, maxViewed, elapsed := reserve799Run(t, p, false)
	if normalDone < 30 {
		t.Fatalf("F3 RED (#799): with the viewed tier saturated, the normal tier completed %d of 30 refreshes in %v "+
			"(viewed completed %d) — strict priority starves every unviewed cell", normalDone, elapsed, viewedDone)
	}
	if maxViewed > p-1 {
		t.Fatalf("F3 RED (#799): %d workers were in viewed work at once, want at most p-1 = %d", maxViewed, p-1)
	}
	t.Logf("F3: 30 normal refreshes in %v under a saturated viewed tier; viewed completed %d; max viewed workers %d of %d",
		elapsed, viewedDone, maxViewed, p)
}

// TestViewed799_F4_SingleWorkerAlternatesTiers is F4.
func TestViewed799_F4_SingleWorkerAlternatesTiers(t *testing.T) {
	normalDone, viewedDone, _, elapsed := reserve799Run(t, 1, false)
	if normalDone < 30 {
		t.Fatalf("F4 RED (#799): with one worker and the viewed tier saturated, the normal tier completed %d of 30 in %v "+
			"(viewed %d) — the tiers must alternate", normalDone, elapsed, viewedDone)
	}
	if viewedDone < 10 {
		t.Fatalf("F4 RED (#799): while the normal tier drained 30 keys the viewed tier completed only %d — the tiers must alternate, not swap starvation", viewedDone)
	}
	t.Logf("F4: 30 normal refreshes in %v with one worker; viewed completed %d meanwhile", elapsed, viewedDone)
}

// TestViewed799_F5_SingleWorkerAlternatesWithTheClusterListTier is F5.
func TestViewed799_F5_SingleWorkerAlternatesWithTheClusterListTier(t *testing.T) {
	clDone, viewedDone, _, elapsed := reserve799Run(t, 1, true)
	if clDone < 30 {
		t.Fatalf("F5 RED (#799 N4): with one worker and the viewed tier saturated, the cluster_list tier completed %d of 30 in %v "+
			"(viewed %d) — the alternation must give the cluster_list tier turns too", clDone, elapsed, viewedDone)
	}
	if viewedDone < 10 {
		t.Fatalf("F5 RED (#799 N4): while the cluster_list tier drained 30 keys the viewed tier completed only %d — "+
			"a cluster_list dequeue must count as the other tier's turn", viewedDone)
	}
	t.Logf("F5: 30 cluster_list refreshes in %v with one worker; viewed completed %d meanwhile", elapsed, viewedDone)
}
