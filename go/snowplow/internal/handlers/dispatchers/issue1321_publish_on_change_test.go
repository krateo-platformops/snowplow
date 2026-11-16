package dispatchers

// issue1321_publish_on_change_test.go — #1321, the wiring arms: the REAL
// refresh terminal (resolveAndPopulateL1, driven through cache.RunRefreshNow)
// publishes a /refreshes frame only when its accepted Put changed the stored
// bytes. The refresh and its Put still run either way.
//
//	R1  an identical refresh Put → the Put is accepted (the cell is replaced,
//	    put_total{identical} moves) and NO frame reaches the open stream;
//	    withheld_identical counts it, published does not move.
//	R2  a changed refresh Put on the same live stream → exactly one frame.
//	    R2 is also R1's scope: the stream R1 found silent is shown live.
//	R3  the cold-dispatch fill (publishIfSubscribed) takes the same rule: a
//	    PutIfGen over a resident cell with the same bytes (a #547 demand
//	    revalidation) announces nothing; a fill with new bytes announces once.
//
// Publishing is synchronous into the stream's buffered sink (coalesce window
// 0), so "no frame" is read at once after the call returned, never waited for.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

type r1321Env struct {
	c   *cache.ResolvedCacheStore
	key string
	in  cache.ResolvedKeyInputs
	st  cache.RefreshStream
}

func newR1321(t *testing.T, name, resident string) *r1321Env {
	t.Helper()
	cache.CompleteRBACSnapshotForTest(t)
	c := instrumentCacheOn(t)
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	cache.ResetRefreshCycleForTest()
	t.Cleanup(cache.ResetRefreshCycleForTest)
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	in := instrumentInputs(name)
	key := cache.ComputeKey(in)
	c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(resident), Inputs: &in})
	st := cache.SubscribeRefreshStream(map[string]struct{}{key: {}})
	t.Cleanup(st.Unsub)
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, nil, nil)
	})
	return &r1321Env{c: c, key: key, in: in, st: st}
}

// refresh runs one real refresh terminal whose resolve produces body.
func (e *r1321Env) refresh(t *testing.T, body string) {
	t.Helper()
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(body), nil
	})
	defer restore()
	if err := cache.RunRefreshNow(context.Background(), e.key); err != nil {
		t.Fatalf("PRE: RunRefreshNow: %v", err)
	}
}

// frames drains the frames already offered to the stream (publishing is
// synchronous: anything published is in the sink when the call returns).
func (e *r1321Env) frames() int {
	n := 0
	for {
		select {
		case <-e.st.Keys:
			n++
		default:
			return n
		}
	}
}

func r1321Puts(content string) uint64 {
	var n uint64
	for _, p := range cache.RefreshCycleStatsSnapshot().Puts {
		if p.Class == "restactions" && p.Tier == cache.RefreshTierDirect && p.Content == content {
			n += p.N
		}
	}
	return n
}

func TestIssue1321_R1_AnIdenticalRefreshPutPublishesNoFrame_R2_AChangedOnePublishesExactlyOne(t *testing.T) {
	e := newR1321(t, "r1-1321", `{"v":"same"}`)
	before, _ := e.c.GetNoTouch(e.key)
	b0 := cache.RefreshBroadcasterStatsSnapshot()

	// R1 — the resolve produced the bytes the cell already holds.
	e.refresh(t, `{"v":"same"}`)
	after, ok := e.c.GetNoTouch(e.key)
	if !ok || after == before {
		t.Fatal("R1 PRE: the identical refresh did not Put (the cell was not replaced) — the refresh and its Put must still run")
	}
	if got := r1321Puts("identical"); got != 1 {
		t.Fatalf("R1 PRE: put_total{restactions,direct,identical}=%d, want 1", got)
	}
	if n := e.frames(); n != 0 {
		t.Fatalf("R1 RED: an identical refresh Put published %d frame(s) to the armed stream, want 0 — the subscriber "+
			"already holds these bytes, and each frame makes the browser refetch them (#1321)", n)
	}
	b1 := cache.RefreshBroadcasterStatsSnapshot()
	if b1.Published != b0.Published || b1.WithheldIdentical != b0.WithheldIdentical+1 {
		t.Fatalf("R1 RED: published %d→%d, withheld_identical %d→%d; want published unchanged and withheld_identical +1, "+
			"so a frame and an identical Put are distinguishable in /debug/vars", b0.Published, b1.Published,
			b0.WithheldIdentical, b1.WithheldIdentical)
	}

	// R2 — the same stream, a real change: exactly one frame (and R1's silence
	// is scoped: this stream does receive frames).
	e.refresh(t, `{"v":"changed"}`)
	if n := e.frames(); n != 1 {
		t.Fatalf("R2 RED: a changed refresh Put published %d frame(s), want exactly 1", n)
	}
	if got := r1321Puts("changed"); got != 1 {
		t.Fatalf("R2 PRE: put_total{restactions,direct,changed}=%d, want 1", got)
	}
	b2 := cache.RefreshBroadcasterStatsSnapshot()
	if b2.Published != b1.Published+1 || b2.WithheldIdentical != b1.WithheldIdentical {
		t.Fatalf("R2 RED: published %d→%d, withheld_identical %d→%d; want +1 and unchanged", b1.Published, b2.Published,
			b1.WithheldIdentical, b2.WithheldIdentical)
	}
}

func TestIssue1321_R3_TheColdFillPublishesOnlyAChange(t *testing.T) {
	e := newR1321(t, "r3-1321", `{"v":"same"}`)
	ctx := context.Background()
	put := e.c.PutResolvedIfGenOutcome(ctx, fixtureStoreVerdict, e.key,
		&cache.ResolvedEntry{RawJSON: []byte(`{"v":"same"}`), Inputs: &e.in}, e.c.CaptureGen(e.key))
	if put != cache.PutIdentical {
		t.Fatalf("R3 RED: a PutIfGen over a resident cell with the same bytes reported %v, want identical", put)
	}
	publishIfSubscribed(e.key, put)
	if n := e.frames(); n != 0 {
		t.Fatalf("R3 RED: the cold fill published %d frame(s) for bytes the subscriber holds, want 0", n)
	}
	put = e.c.PutResolvedIfGenOutcome(ctx, fixtureStoreVerdict, e.key,
		&cache.ResolvedEntry{RawJSON: []byte(`{"v":"new"}`), Inputs: &e.in}, e.c.CaptureGen(e.key))
	if put != cache.PutChanged {
		t.Fatalf("R3 RED: a PutIfGen with new bytes reported %v, want changed", put)
	}
	publishIfSubscribed(e.key, put)
	if n := e.frames(); n != 1 {
		t.Fatalf("R3 RED: the cold fill of new bytes published %d frame(s), want exactly 1", n)
	}
	// A fill of an ABSENT key (the designed #62 case: evicted, re-armed) has no
	// prior, so it is always a change.
	gone := instrumentInputs("r3-1321-absent")
	gk := cache.ComputeKey(gone)
	st := cache.SubscribeRefreshStream(map[string]struct{}{gk: {}})
	defer st.Unsub()
	put = e.c.PutResolvedIfGenOutcome(ctx, fixtureStoreVerdict, gk,
		&cache.ResolvedEntry{RawJSON: []byte(`{"v":"same"}`), Inputs: &gone}, e.c.CaptureGen(gk))
	if put != cache.PutChanged {
		t.Fatalf("R3 RED: a fill of an absent key reported %v, want changed (there is no prior)", put)
	}
	publishIfSubscribed(gk, put)
	select {
	case <-st.Keys:
	case <-time.After(5 * time.Second):
		t.Fatal("R3 RED: the fill of an absent armed key published no frame")
	}
}
