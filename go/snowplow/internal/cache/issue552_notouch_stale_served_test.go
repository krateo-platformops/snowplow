// issue552_notouch_stale_served_test.go — #552, the stale_served_total half,
// pinned at the read PRIMITIVE.
//
// WHY HERE AND NOT IN THE RESOLVER PACKAGE. #552's resolver arms
// (issue552_apistage_background_notouch_test.go) prove the apistage read picks
// GetNoTouch for a background dispatch, and they assert hit_total, miss_total
// and lastRead directly. They CANNOT make the stale_served_total assertion
// non-vacuous: opening a cell's invalidation→fresh window needs the refresher's
// markDirty, which has no exported seam, so from package api the counter is
// 0-before/0-after whatever the code does — a passing assertion that measures
// nothing.
//
// So the chain is closed in two halves, both non-vacuous:
//   1. (api) a background dispatch reads through GetNoTouch — asserted on
//      hit_total / miss_total / lastRead, with a customer control that moves all
//      of them.
//   2. (here) on a cell whose dirty window IS open, Get counts a stale serve and
//      GetNoTouch does not — so choosing the internal primitive is exactly what
//      keeps snowplow's own reads out of the #354 measurement.
//
// Together: no background read can reach stale_served_total. The structural
// complement already exists — noteServeWhileDirty is called ONLY from Get
// (resolved.go), which TestIssue354_P3_StaleServeNoteCoversEveryCountedHit pins.

package cache

import (
	"context"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue552_StaleServeIsCountedOnlyThroughTheCustomerGet(t *testing.T) {
	c := p3CacheEnv(t)
	gvr := schema.GroupVersionResource{Group: "p552.example", Version: "v1", Resource: "things"}
	RegisterRefreshFunc("restactions", func(context.Context, string, ResolvedKeyInputs) error { return nil })

	// Park the worker so the window stays OPEN for the duration of the arm: a
	// drained window would close before the reads and the arm would measure a
	// clean cell.
	var hold atomic.Bool
	hold.Store(true)
	SetCustomerInflightHook(hold.Load)
	t.Cleanup(func() { hold.Store(false) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	in := ResolvedKeyInputs{CacheEntryClass: "restactions", Group: gvr.Group, Version: gvr.Version,
		Resource: gvr.Resource, Namespace: "p552", Name: "cell", BindingUID: "uid"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	Deps().Record(context.Background(), key, gvr, "p552", in.Name)
	Deps().OnUpdate(gvr, "p552", in.Name)

	// PREMISE (the whole arm is vacuous without it): the cell's
	// invalidation→fresh window is OPEN, so a serve of it IS a stale serve.
	if _, dirty := dirtySince(key); !dirty {
		t.Fatal("premise: the dep mark must open the dirty window — without an open window neither " +
			"primitive can count a stale serve and this arm measures nothing")
	}

	// INTERNAL read: serves the cell as input, counts nothing.
	s0, h0 := c.Stats().StaleServedTotal, c.Stats().HitTotal
	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatal("premise: GetNoTouch must find the resident cell")
	}
	if s := c.Stats(); s.StaleServedTotal != s0 {
		t.Errorf("#552: GetNoTouch on a DIRTY cell counted a stale serve (%d->%d) — "+
			"snowplow's own read would land in the #354 window measurement", s0, s.StaleServedTotal)
	}
	if s := c.Stats(); s.HitTotal != h0 {
		t.Errorf("#376: GetNoTouch bumped hit_total (%d->%d)", h0, s.HitTotal)
	}

	// CUSTOMER read: the same cell, through the customer primitive. This is the
	// control — if it does not move, the no-movement assertions above are
	// measuring a dead instrument rather than the fix.
	if _, ok := c.Get(key); !ok {
		t.Fatal("premise: Get must find the resident cell")
	}
	if s := c.Stats(); s.StaleServedTotal != s0+1 {
		t.Errorf("control: a CUSTOMER Get of a dirty cell must count exactly one stale serve (%d->%d) — "+
			"the #354 window is measured on this population", s0, s.StaleServedTotal)
	}
	if s := c.Stats(); s.HitTotal != h0+1 {
		t.Errorf("control: a customer Get must bump hit_total (%d->%d)", h0, s.HitTotal)
	}
}
