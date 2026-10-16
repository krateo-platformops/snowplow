// issue734_known_stale_gap_demand_test.go — #734 K3: the consequence of the
// Put→settle gap on an IDENTITY-BOUND cell, at the dispatcher.
//
// A per-cohort widgets cell is KNOWN-STALE (its background refresh was dropped).
// The refresher identity recovers and the next change re-resolves it: the
// production RefreshFunc (resolveAndPopulateL1) performs the accepted terminal
// re-Put. Then, still INSIDE that dequeue — before processNext settles the
// window — the cohort's customer GET is served through the real Widgets()
// handler. The hit delivers the fresh body; before #734 the cell still read
// known-stale there, so revalidateKnownStale claimed a demand revalidation and
// re-ran the customer cold path for a body that was already fresh. The arm
// asserts no demand revalidation was claimed and no customer-shaped resolve ran.
package dispatchers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue734_K3_AReadInThePutSettleGapStartsNoDemandRevalidation(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	if _, _, ks := cache.KnownStale(key); !ks {
		t.Fatalf("PRECONDITION: cohort 0's cell is not known-stale")
	}

	type gapRead struct {
		reason   string
		code     int
		inflight int
	}
	seen := make(chan gapRead, 1)
	var fired atomic.Bool
	refresh := cache.RefreshFuncForTest("widgets")
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, k string, in cache.ResolvedKeyInputs) error {
		err := refresh(ctx, k, in)
		if err == nil && k == key && fired.CompareAndSwap(false, true) {
			// The accepted re-Put has landed; the dequeue has not settled.
			r, code := h.get(0, k547Changed)
			seen <- gapRead{reason: r, code: code, inflight: cache.DemandRevalidationsInFlight()}
		}
		return err
	})

	st0 := cache.ResolvedCacheStatsByStat()
	cust0 := h.custCalls.Load()
	h.allowBackground.Store(true) // the refresher identity recovers
	h.update(k547Changed, "v2")   // the next change re-resolves the cell

	var g gapRead
	select {
	case g = <-seen:
	case <-time.After(30 * time.Second):
		t.Fatalf("PRECONDITION: cohort 0's cell was never re-resolved by an accepted refresh")
	}
	if g.code != 200 || g.reason != "v2" {
		t.Fatalf("K3: the GET in the gap served %q (code %d), want the re-Put body v2", g.reason, g.code)
	}
	if !k547WaitFor(t, 10*time.Second, "demand revalidations drained", func() bool {
		return cache.DemandRevalidationsInFlight() == 0
	}) {
		t.Fatalf("K3: a demand revalidation is still in flight")
	}
	st := cache.ResolvedCacheStatsByStat()
	claimed := st["known_stale_revalidation_fresh_total"] - st0["known_stale_revalidation_fresh_total"] +
		st["known_stale_revalidation_failed_total"] - st0["known_stale_revalidation_failed_total"]
	if g.inflight != 0 || claimed != 0 {
		t.Fatalf("K3 RED: a customer read of the FRESH body, after the refresher's accepted re-Put and before its "+
			"dequeue settled, started a demand revalidation (in flight at the read: %d, settled: %d) — the cell "+
			"still read known-stale in the Put→settle gap (#734)", g.inflight, claimed)
	}
	if got := h.custCalls.Load() - cust0; got != 0 {
		t.Fatalf("K3 RED: %d customer-shaped resolve(s) ran for a cell the refresher had already made fresh (#734)", got)
	}
}
