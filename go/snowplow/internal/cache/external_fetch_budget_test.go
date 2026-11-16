// external_fetch_budget_test.go — the shared external-fetch layer's byte bound
// follows the memory headroom, and is never unbounded.
package cache

import (
	"math"
	"testing"
)

func TestExternalFetchCacheBudget_FollowsHeadroom(t *testing.T) {
	const gib = int64(1) << 30
	live := int64(2 * gib)
	restore := SetAdmissionRuntimeSeamsForTest(func() int64 { return 8 * gib }, func() int64 { return live })
	defer restore()

	// ceiling = (8 − 2) − 8/8 = 5 GiB; holding 0 → 5/8 GiB.
	if got, want := ExternalFetchCacheBudget(0), 5*gib/8; got != want {
		t.Fatalf("budget at 2 GiB live = %d; want %d", got, want)
	}
	// The heap fills to the reserve: the ceiling is 0 and the layer may keep
	// only a share of what it holds — it shrinks on its next store.
	live = 7 * gib
	if got, want := ExternalFetchCacheBudget(gib/4), (gib/4)/8; got != want {
		t.Fatalf("budget at the reserve = %d; want %d (shrink)", got, want)
	}
}

func TestExternalFetchCacheBudget_BoundedWithoutGOMEMLIMIT(t *testing.T) {
	restore := SetAdmissionRuntimeSeamsForTest(func() int64 { return math.MaxInt64 }, func() int64 { return 0 })
	defer restore()
	got := ExternalFetchCacheBudget(0)
	if got <= 0 || got > defaultResolvedCacheMaxBytes {
		t.Fatalf("budget without GOMEMLIMIT = %d; want a positive share of the L1 byte budget", got)
	}
}
