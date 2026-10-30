// list_cell_854_more_test.go — #854: the remaining unanchor triggers and the
// reader paths.
//
//	F2t  every type-level event on the GVR (available / removed; #870 deleted
//	     the schema-relisted and store-repaired ones) unanchors the cell.
//	F3c  C5: a CUSTOMER read after a change is served the change (a customer
//	     read drains pending with the current seq as its need).
//	F5r  the comparator is read-driven: reads alone schedule passes (no timer),
//	     which the real refresher runs.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestListCell854_F2t_TypeLevelEventsUnanchor(t *testing.T) {
	events := map[string]func(){
		"available":       func() { cache.Deps().OnResourceTypeAvailable(r1FsaGVR) },
		"removed":         func() { cache.Deps().OnResourceTypeRemoved(r1FsaGVR) },
	}
	for name, fire := range events {
		t.Run(name, func(t *testing.T) {
			newInc854Watcher(t, 3)
			c := inc854Anchor(t, cache.ResolvedCache(), fill710ListCall())
			before := cache.ListCellStatsSnapshot().Unanchors["type_event"]
			fire()
			if c.Anchored() {
				t.Fatalf("F2t RED (%s): the cell survived a type-level event — not a per-object delta it can apply", name)
			}
			if got := cache.ListCellStatsSnapshot().Unanchors["type_event"] - before; got != 1 {
				t.Fatalf("F2t (%s): unanchors{type_event} moved by %d, want 1", name, got)
			}
		})
	}
}

func TestListCell854_F3c_CustomerReadIsServedTheChange(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	inc854Anchor(t, store, fill710ListCall())
	for m := 2; m <= 5; m++ {
		stamp765Change(t, dyn, m)
		v, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
		if !ok || !served {
			t.Fatalf("customer read %d not served", m)
		}
		if got := fill710ManagedOf(t, v, stamp765Item); got != m {
			t.Fatalf("F3c RED (C5): a customer read after change managed=%d was served managed=%d — the customer got a generation older than a fill would read", m, got)
		}
	}
}

func TestListCell854_F5r_ReadsAloneScheduleComparatorPasses(t *testing.T) {
	newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	inc854Anchor(t, store, fill710ListCall())
	// A pass is refresher work (#865): the reads make it due and the real
	// refresher runs it. No refresh handler is registered here, so the dequeue
	// runs the pass and then skips the key.
	cache.ResetRefresherForTest()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })
	cache.StartRefresher(ctx)
	p0 := cache.ListCellStatsSnapshot().Passes
	deadline := time.Now().Add(10 * time.Second)
	for cache.ListCellStatsSnapshot().Passes == p0 {
		if time.Now().After(deadline) {
			t.Fatal("F5r RED: 10s of reads scheduled no comparator pass — the comparator never runs on its own")
		}
		_, _, _, _ = apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
	}
}
