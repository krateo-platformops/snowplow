//go:build unit || integration

package dispatchers

// issue796_refused_list_seed_test.go — #796 W4, at the #790 shape: the Seed425
// unservable fixture (the configmaps informer's LIST is refused with 403, so it
// never syncs) used to stall every internal-dispatch LIST on it for the full 5 s
// sync-wait before the same live LIST ran. With the wait ending on the terminal
// refusal, a seed through it finishes well under the old bound, falls through
// to the same SA-scoped live path (identical outcome: nothing leaks to the
// denied group), and counts list_refused once per LIST.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func w796ListRefused() uint64 {
	for _, c := range restactionsapi.ServeMissSnapshot() {
		if c.Reason == "list_refused" {
			return c.Count
		}
	}
	return 0
}

func TestIssue796_W4_RefusedInformerDoesNotStallTheSeed(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: false})
	var saDials atomic.Int64
	saSrv := s425SAServer(t, &saDials)
	cr := s425RA(s425PathListX())

	for round := 1; round <= 2; round++ {
		// Each round must reach the internal-dispatch LIST again, not a cell
		// or memo the first round left behind.
		cache.ResetResolvedCacheForTest()
		before := w796ListRefused()
		start := time.Now()
		_, cell := s425Seed(t, seedTarget{Groups: []string{s425Group}}, cr, saSrv.URL)
		took := time.Since(start)
		if took >= 2*time.Second {
			t.Fatalf("#796 W4 RED (round %d): the seed took %s on a refused informer — the 5 s sync-wait was run out", round, took)
		}
		if got := w796ListRefused() - before; got < 1 {
			t.Fatalf("W4 (round %d): list_refused moved %d, want >= 1 (the miss is not counted as a refusal)", round, got)
		}
		// The fall-through outcome is unchanged: the group-only cohort is denied
		// the namespace, so the SA-fetched rows never reach its cell (#425).
		if s425Has(cell, s425SentinelY) {
			t.Fatalf("W4 (round %d): the fall-through changed — the denied cohort's cell carries the object", round)
		}
	}
	if saDials.Load() == 0 {
		t.Fatal("PRECONDITION W4: the seed never reached the SA live LIST (the arm proves nothing about the fall-through)")
	}
}
