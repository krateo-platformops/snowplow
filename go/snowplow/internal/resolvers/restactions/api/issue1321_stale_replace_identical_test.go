package api

// issue1321_stale_replace_identical_test.go — #1321 at the D2 publish site
// (finishStaleReplace): a stale-derived dependent whose replacement resolves
// to exactly the body the response was written with publishes no frame. The
// client already holds those bytes: the stale-push ledger closes its serve at
// the replacing Put ("identical"), the owed-push mark is settled, and
// stale_replace_identical_total counts it. A later CHANGED refresh of the same
// key still reaches the same stream (the silence is scoped).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue1321_D2_AStaleDependentReplacedByTheServedBytesPublishesNoFrame(t *testing.T) {
	f := newD2Fixture(t)
	const key = "dep-1321"
	const served = `{"fresh":false}`
	var body atomic.Pointer[string]
	b := served
	body.Store(&b)
	// The refresh terminal's shape (resolve_populate.go): the gen-guarded
	// refresh replace, then the gated publish with its outcome.
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, k string, in cache.ResolvedKeyInputs) error {
		put := f.store.ReplaceResolvedIfGenRefresh(ctx, fixtureStoreVerdict, k,
			&cache.ResolvedEntry{RawJSON: []byte(*body.Load()), Inputs: &in}, f.store.CaptureGen(k))
		cache.PublishRefreshed(ctx, k, put)
		return nil
	})
	keys := d2WrittenStream(t, key)
	_, sink := f.customer(t, key)
	f.store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{
		RawJSON:      []byte(served),
		Inputs:       &cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Namespace: "d2", Name: key},
		StaleDerived: true,
	})
	CommitStaleServe(sink, []byte(served))
	d2WaitDebtsClosed(t)
	if !fill710WaitFor(15*time.Second, func() bool { return StaleServeStatsSnapshot().ReplaceIdentical == 1 }) {
		e, _ := f.store.GetNoTouch(key)
		t.Fatalf("D2 RED: the dependent's replace by the served bytes was not recognised (stats %+v, cell stale-derived=%v)",
			StaleServeStatsSnapshot(), e != nil && e.StaleDerived)
	}
	if e, _ := f.store.GetNoTouch(key); e == nil || e.StaleDerived || string(e.RawJSON) != served {
		t.Fatalf("PRE: the dependent was not replaced by a fresh entry with the served bytes")
	}
	select {
	case k := <-keys:
		t.Fatalf("D2 RED: a frame for %q was published although the client holds the replacing bytes", k)
	case <-time.After(300 * time.Millisecond):
	}
	s := StaleServeStatsSnapshot()
	if s.PushTotal != 0 || s.PushUnarmed != 0 {
		t.Fatalf("D2 RED: push_total=%d push_unarmed=%d, want 0/0 (nothing was published)", s.PushTotal, s.PushUnarmed)
	}
	if cache.StaleOwed(key) || s.Owed.Settled != 1 || s.Owed.Dropped != 0 {
		t.Fatalf("D2 RED: owed mark still set=%v settled=%d dropped=%d — a mark nobody will ever deliver would end in "+
			"the capacity drop, read as a guarantee failure", cache.StaleOwed(key), s.Owed.Settled, s.Owed.Dropped)
	}
	var ap cache.StalePushLayerStats
	for _, l := range cache.StalePushStatsSnapshot() {
		if l.Layer == cache.StaleLayerAPIStage {
			ap = l
		}
	}
	if ap.Open != 0 || ap.Closes["identical"] != 1 {
		t.Fatalf("D2 RED: the apistage serve is open=%d identical=%d, want 0/1", ap.Open, ap.Closes["identical"])
	}

	// Scope: the same stream receives a real change.
	changed := `{"fresh":true}`
	body.Store(&changed)
	if err := cache.RunRefreshNow(context.Background(), key); err != nil {
		t.Fatalf("PRE: RunRefreshNow: %v", err)
	}
	d2Push(t, keys, key, 10*time.Second)
}
