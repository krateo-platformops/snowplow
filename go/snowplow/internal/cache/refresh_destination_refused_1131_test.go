package cache

// refresh_destination_refused_1131_test.go — #1131 through the real refresher
// loop: a refused destination cannot clear, so one dirty mark costs one resolve
// and the entry stays resident as known-stale. An ordinary 502 is the control: it
// spends the whole requeue budget, so the arm can fail.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// x1131Status is a stage failure's typed status with reason.
func x1131Status(reason string) error {
	se := &apierrors.StatusError{}
	se.ErrStatus.Status = "Failure"
	se.ErrStatus.Code = 502
	se.ErrStatus.Reason = metav1.StatusReason(reason)
	se.ErrStatus.Message = `restaction ns/ra stage "r" failed: x`
	return se
}

func x1131Refused() error { return x1131Status(credhttp.DestinationRefusedReason) }

func TestIssue1131_ARefusedDestinationReachesTheDropPointOnItsFirstAttempt(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	for _, tc := range []struct {
		name      string
		err       error
		wantRuns  int64
		wantClass string
	}{
		{"refused", x1131Refused(), 1, RefreshErrDestinationRefused},
		{"control_ordinary_502", x1131Status("BadGateway"), maxRefreshRequeues + 1, RefreshErrUnclassified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
			t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
			t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
			t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
			resetRefresherForTest()
			defer resetRefresherForTest()
			defer withCleanDepWatch(t)()
			ResetRefreshFailedByClassForTest()
			t.Cleanup(ResetRefreshFailedByClassForTest)

			store := ResolvedCache()
			if store == nil {
				t.Skip("resolved cache disabled in this environment")
			}
			Deps().SetStore(store)
			inputs := widgetInputs(gvrFlexes(), "krateo-system", "refused-"+tc.name)
			key := ComputeKey(*inputs)
			store.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"served"}`), Inputs: inputs})

			var runs atomic.Int64
			wrapped := fmt.Errorf("resolveAndPopulateL1 widgets/x: resolve widget: %w",
				WrapResolveStage(ResolveStageApiRef, "apiref", tc.err))
			RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
				runs.Add(1)
				return wrapped
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			StartRefresher(ctx)
			EnqueueRefresh(key)

			class := func() uint64 {
				for _, c := range RefreshFailedByClassSnapshot() {
					if c.Class == tc.wantClass && c.Stage == ResolveStageApiRef {
						return c.N
					}
				}
				return 0
			}
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) && (runs.Load() < tc.wantRuns || class() < uint64(tc.wantRuns)) {
				time.Sleep(10 * time.Millisecond)
			}
			// Long enough for a requeue that should not exist to show: the
			// backoff is 1 to 2 ms here.
			time.Sleep(200 * time.Millisecond)

			if got := runs.Load(); got != tc.wantRuns {
				t.Fatalf("RED: %d resolves for one dirty mark, want %d", got, tc.wantRuns)
			}
			if got := class(); got != uint64(tc.wantRuns) {
				t.Fatalf("RED: %s/%s counted %d, want %d: %+v", tc.wantClass, ResolveStageApiRef, got, tc.wantRuns, RefreshFailedByClassSnapshot())
			}
			// The drop point never evicts: the entry stays resident.
			if body := served(t, store, key); body != `{"v":"served"}` {
				t.Fatalf("RED: the entry did not stay resident: %q", body)
			}
		})
	}
}

// A corrected endpoint is picked up by the next dirty mark: nothing about the
// drop remembers the failure.
func TestIssue1131_TheNextDirtyMarkTriesAgain(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	defer resetRefresherForTest()
	defer withCleanDepWatch(t)()

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(store)
	inputs := widgetInputs(gvrFlexes(), "krateo-system", "refused-then-fixed")
	key := ComputeKey(*inputs)
	store.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"served"}`), Inputs: inputs})

	var runs atomic.Int64
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
		if runs.Add(1) == 1 {
			return x1131Refused()
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	EnqueueRefresh(key)
	waitFor := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) && runs.Load() < n {
			time.Sleep(5 * time.Millisecond)
		}
		if got := runs.Load(); got != n {
			t.Fatalf("RED: %d resolves, want %d", got, n)
		}
	}
	waitFor(1)
	time.Sleep(100 * time.Millisecond) // the drop, not a requeue
	if got := runs.Load(); got != 1 {
		t.Fatalf("RED: a refused destination was requeued (%d resolves)", got)
	}
	EnqueueRefresh(key)
	waitFor(2)
}
