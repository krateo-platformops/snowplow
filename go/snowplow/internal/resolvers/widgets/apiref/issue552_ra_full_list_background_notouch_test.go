// issue552_ra_full_list_background_notouch_test.go — #552 / #488, the raFullList
// carrier of the same defect the apistage content read had.
//
// THE SITE. raFullListServe's known-sliceable fast path asked ONE question —
// cache.Inert — and used its answer twice: which read primitive to use, and
// whether the requester joins the cell's #444 representative pool. A refresher
// re-resolve is NOT excluded from this serve (boot_walk_skip_rafulllist_test.go
// pins that the exclusion keys on ScopeBootPrewarmWalk only), so a background
// paginated read took c.Get — stamping lastRead, bumping hit_total, tripping the
// dirty-serve note — AND called entry.NoteHitter with the representative
// identity it happened to be resolving under, putting a non-customer into the
// pool the refresher later picks its representative from.
//
// NoteHitter is NOT reached inside Get (it is a separate call in this function),
// so the read-primitive change alone does NOT cover the pool. Both lines now ask
// the same predicate; this arm asserts BOTH consequences.
//
// Hermetic, on the real raFullListServe, reusing the #423/#435 fixture: alice
// and carol share ONE raKey (same binding, same identity class — the key folds
// the binding set and the class, never the username), which is what makes the
// representative pool observable at all.
//
// RED on ae2fdc7d (this PR's first commit, which fixes only the apistage site):
// hit_total +1 and carol in the pool after a BACKGROUND read.

package apiref

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue552_RAFullList_BackgroundReadIsInternalAndJoinsNoPool(t *testing.T) {
	_ = r435Base(t)
	const ns, name = "krateo-system", "issue552-ra"
	alice := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	carol := f6CtxWithUser(t, "carol-423", []string{"portal-423"})

	_, aKey, aOK := seedFullListRAKey(alice, gvr(), ns, name, nil)
	_, cKey, cOK := seedFullListRAKey(carol, gvr(), ns, name, nil)
	// PREMISE: one cell, two identities. If they keyed apart, every assertion
	// below would be about two different cells and the pool arm would be vacuous.
	if !aOK || !cOK || aKey == "" || aKey != cKey {
		t.Fatalf("PRE: alice and carol must share one raKey (aOK=%v cOK=%v %q vs %q)", aOK, cOK, aKey, cKey)
	}

	var calls atomic.Int64
	rows := k423PerUserRows(t, &calls)
	serve := func(t *testing.T, ctx context.Context) {
		t.Helper()
		if _, ok, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows); err != nil || !ok {
			t.Fatalf("ARM DID NOT RUN: raFullListServe ok=%v err=%v", ok, err)
		}
	}
	store := cache.ResolvedCache()
	poolHas := func(t *testing.T, username string) bool {
		t.Helper()
		entry, ok := store.GetNoTouch(aKey) // the probe itself must not stamp
		if !ok {
			t.Fatal("raKey cell absent")
		}
		for _, h := range entry.RecentHitters() {
			if h.Username == username {
				return true
			}
		}
		return false
	}
	lastRead := func(t *testing.T) int64 {
		t.Helper()
		m, ok := store.MetadataForKey(aKey)
		if !ok {
			t.Fatal("raKey cell absent")
		}
		return m.LastReadSeconds
	}

	// Prime: a customer first sight records the sliceable verdict and the cell.
	serve(t, alice)
	if !store.Has(aKey) {
		t.Fatal("PRIME: the first sight must store the raKey cell")
	}
	// A SECOND customer serve is needed to seed the pool: NoteHitter fires on the
	// HIT branch only, and the first sight is a cold MISS that resolves and Puts.
	// (This premise failing is what told me so — it is not a formality.)
	serve(t, alice)
	if !poolHas(t, "alice-423") {
		t.Fatal("PRIME: the priming customer must be in the representative pool — " +
			"without this the pool assertions below cannot discriminate")
	}
	if poolHas(t, "carol-423") {
		t.Fatal("PRE: carol must not be in the pool yet")
	}

	t.Run("background_read_is_internal", func(t *testing.T) {
		time.Sleep(1200 * time.Millisecond) // LastReadSeconds has one-second resolution
		before := lastRead(t)
		if before < 1 {
			t.Fatalf("PRE: LastReadSeconds=%d, want >= 1 after the wait — a stamp must be visible as a RESET", before)
		}
		h0, m0 := store.Stats().HitTotal, store.Stats().MissTotal

		serve(t, cache.WithBackgroundResolve(carol))

		if s := store.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
			t.Errorf("#552: a background raFullList read moved the ratio counters (hit %d->%d, miss %d->%d) — "+
				"snowplow read its own cell and it was accounted as a customer serve", h0, s.HitTotal, m0, s.MissTotal)
		}
		if got := lastRead(t); got < before {
			t.Errorf("#552: a background raFullList read stamped the cell warm (LastReadSeconds %d -> %d)", before, got)
		}
		if poolHas(t, "carol-423") {
			t.Errorf("#444/#552: a background raFullList read joined the representative pool — " +
				"the refresher picks its representative from that pool, so a non-customer in it " +
				"re-resolves the cell under an identity no customer presented")
		}
	})

	// CONTROL (non-vacuity): the same identity, the same cell, no marker. It must
	// do every one of the three things the arm above asserts did not happen, or
	// that arm is reading a dead instrument.
	t.Run("customer_read_stamps_and_joins", func(t *testing.T) {
		before := lastRead(t)
		h0 := store.Stats().HitTotal

		serve(t, carol)

		if s := store.Stats(); s.HitTotal != h0+1 {
			t.Errorf("control: a customer raFullList serve must bump hit_total (%d->%d)", h0, s.HitTotal)
		}
		if got := lastRead(t); got >= before && before > 0 {
			t.Errorf("control: a customer serve must stamp the cell warm (LastReadSeconds %d -> %d)", before, got)
		}
		if !poolHas(t, "carol-423") {
			t.Error("control: a customer serve must join the representative pool (#444) — " +
				"if it does not, the pool assertion in the arm above proves nothing")
		}
	})
}
