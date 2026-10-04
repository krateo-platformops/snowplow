package apiref

// inline443_ra_full_list_test.go — #443 part 2, TestS443_Inline/c4: the
// raFullList carrier of a nested widget apiRef under an inert (dry-run)
// resolve. Two suppressions are exercised on the REAL raFullListServe:
//
//   - (e) a warm raKey cell is read with GetNoTouch (no warmth stamp, no
//     representative-pool join): its LastReadSeconds must not reset;
//   - (a) a cold raKey cell is not written: PutRAFullListIfGen refuses.
//
// Control arms prove each observation can fail: the same calls WITHOUT the
// flag do touch the warm cell and do store the cold one.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestS443_Inline_c4_RAFullList(t *testing.T) {
	_ = r435Base(t)
	const ns, name = "krateo-system", "inline443-ra"
	alice := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	_, aKey, _ := seedFullListRAKey(alice, gvr(), ns, name, nil)
	var calls atomic.Int64
	rows := k423PerUserRows(t, &calls)
	serve := func(ctx context.Context) {
		t.Helper()
		if _, ok, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows); err != nil || !ok {
			t.Fatalf("raFullListServe ok=%v err=%v", ok, err)
		}
	}
	lastRead := func() int64 {
		m, ok := cache.ResolvedCache().MetadataForKey(aKey)
		if !ok {
			t.Fatalf("raKey cell absent")
		}
		return m.LastReadSeconds
	}
	// Prime: a stored first sight records the sliceable verdict and the cell.
	serve(alice)
	if !cache.ResolvedCache().Has(aKey) {
		t.Fatal("PRIME: the first sight must store the raKey cell")
	}

	t.Run("e_WarmCellNotTouched", func(t *testing.T) {
		time.Sleep(1200 * time.Millisecond) // one-second resolution: make a touch visible
		before := lastRead()
		if before < 1 {
			t.Fatalf("PRE: LastReadSeconds=%d, want >= 1 after the wait", before)
		}
		serve(cache.WithInert(alice))
		if got := lastRead(); got < before {
			t.Fatalf("an inert resolve touched the warm raKey cell (LastReadSeconds %d → %d)", before, got)
		}
		// Control: a non-inert serve does touch it.
		serve(alice)
		if got := lastRead(); got >= before {
			t.Fatalf("CONTROL: a stored serve did not touch the cell (LastReadSeconds %d → %d) — the arm cannot fail", before, got)
		}
	})

	t.Run("a_ColdCellNotStored", func(t *testing.T) {
		cache.ResolvedCache().DeleteForTest(aKey)
		// The served/ok outcome is not the observation here (the inert dep
		// Record is refused too, which can turn a stored cell's serve into a
		// fall-back); the observation is whether the cell was written.
		if _, _, err := raFullListServe(cache.WithInert(alice), gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows); err != nil {
			t.Fatalf("inert raFullListServe: %v", err)
		}
		if cache.ResolvedCache().Has(aKey) {
			t.Fatal("an inert resolve stored the raKey cell (PutRAFullListIfGen must refuse under the flag)")
		}
		// Control: a non-inert serve stores it.
		serve(alice)
		if !cache.ResolvedCache().Has(aKey) {
			t.Fatal("CONTROL: a stored serve did not store the cold cell — the arm cannot fail")
		}
	})
}
