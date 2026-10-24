//go:build unit || integration

// issue890_overlap_and_decline_test.go — #890 / #893 redesign (after the
// reviewer FAILED a13371f1): no generation fence. A strip never refuses an
// overlapping fill; an accepted write re-asserts the edges its own resolve
// recorded; a resolve whose cell is not stored releases its edges at its end.

package cache

import (
	"context"
	"fmt"
	"testing"
)

// TestIssue890_AcceptedWriteReassertsItsOwnEdges — R1 (refresher, old
// generation) and R2 (cold fill after the eviction) both record under K. R1 is
// refused and strips every edge of the non-resident K, R2's included. R2's
// write is accepted (nothing refuses an overlapping fill) and must come back
// WITH its edge: a stored cell that no dep event can reach is the failure.
func TestIssue890_AcceptedWriteReassertsItsOwnEdges(t *testing.T) {
	d, store, _ := setup890(t)
	const k = "L1_890_reassert"
	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "reassert"}

	ctxR1 := WithL1KeyContext(context.Background(), k)
	d.Record(ctxR1, k, dep.GVR, dep.Namespace, dep.Name)
	old := &ResolvedEntry{RawJSON: []byte(`{}`)}
	store.Put(k, old)
	gen1 := store.CaptureGen(k)
	store.EvictUnrefreshable(k, old)

	ctxR2 := WithL1KeyContext(context.Background(), k)
	gen2 := store.CaptureGen(k)
	d.Record(ctxR2, k, dep.GVR, dep.Namespace, dep.Name)
	d.Record(ctxR1, k, dep.GVR, dep.Namespace, dep.Name) // R1 is still recording too

	if store.ReplaceIfGen(ctxR1, k, &ResolvedEntry{RawJSON: []byte(`{"r":1}`)}, gen1) {
		t.Fatal("harness: R1 must be refused")
	}
	if d.hasEdge(k, dep) {
		t.Fatal("harness: R1's refusal of non-resident K must strip its edges")
	}
	before := d.reassertedEdgesTotal.Load()
	if !store.PutIfGen(ctxR2, k, &ResolvedEntry{RawJSON: []byte(`{"r":2}`)}, gen2) {
		t.Fatal("#890 RED: R2 overlapped R1's refusal and was refused — a strip must never refuse a fill")
	}
	if !d.hasEdge(k, dep) {
		t.Fatal("#890 RED: R2 was stored without the edge its resolve recorded — a resident cell no dep event can reach")
	}
	if _, ok := d.CollectMatchesForTest(dep.GVR, dep.Namespace, dep.Name)[k]; !ok {
		t.Fatal("an event on R2's dep must reach K")
	}
	if got := d.reassertedEdgesTotal.Load() - before; got != 1 {
		t.Fatalf("re-asserted %d edges, want exactly the 1 the strip took", got)
	}
}

// TestIssue890_OverlappingColdFillsCacheTheKey — the reviewer's falsifier. A
// refusal on a non-resident key lands while cold fills are in flight, and fills
// keep arriving W at a time (fill i starts at arrival i, captures the generation,
// records, and writes at arrival i+W; fills of a hot key are not coalesced). The
// key must be cached within one resolve duration — by arrival W+1, counted in
// arrivals — and stay cached. The first #890 design (bump the generation on every refusal) refused
// every overlapping fill and never cached the key again.
func TestIssue890_OverlappingColdFillsCacheTheKey(t *testing.T) {
	for _, w := range []int{2, 3, 10} {
		t.Run(fmt.Sprintf("W=%d", w), func(t *testing.T) {
			d, store, _ := setup890(t)
			k := fmt.Sprintf("L1_890_hot_w%d", w)
			dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "hot"}

			// A refresher resolve is in flight; K is evicted under it.
			ctx0 := WithL1KeyContext(context.Background(), k)
			d.Record(ctx0, k, dep.GVR, dep.Namespace, dep.Name)
			old := &ResolvedEntry{RawJSON: []byte(`{}`)}
			store.Put(k, old)
			gen0 := store.CaptureGen(k)
			store.EvictUnrefreshable(k, old)
			d.Record(ctx0, k, dep.GVR, dep.Namespace, dep.Name)

			type fill struct {
				ctx context.Context
				gen uint64
				at  int
			}
			var inflight []fill
			const arrivals = 200
			cachedAt, refused := 0, 0
			for i := 1; i <= arrivals; i++ {
				if !resident890(store, k) { // a cold miss starts a fill
					c := WithL1KeyContext(context.Background(), k)
					f := fill{ctx: c, gen: store.CaptureGen(k), at: i}
					d.Record(c, k, dep.GVR, dep.Namespace, dep.Name)
					inflight = append(inflight, f)
				}
				if i == 1 { // the in-flight refresher's write-back lands DURING fill 1: refused
					if store.ReplaceIfGen(ctx0, k, &ResolvedEntry{RawJSON: []byte(`{"r":0}`)}, gen0) {
						t.Fatal("harness: the refresher's write-back of evicted K must be refused")
					}
					d.ReleaseUnstored(context.Background(), k) // its deferred release
				}
				var rest []fill
				for _, f := range inflight {
					if i-f.at < w {
						rest = append(rest, f)
						continue
					}
					if !store.PutIfGen(f.ctx, k, &ResolvedEntry{RawJSON: []byte(`{"v":1}`)}, f.gen) {
						refused++
					}
					d.ReleaseUnstored(context.Background(), k) // the fill's deferred release
				}
				inflight = rest
				if cachedAt == 0 && resident890(store, k) {
					cachedAt = i
				}
			}
			if cachedAt == 0 || cachedAt > w+1 {
				t.Fatalf("#890 RED (W=%d): K was cached at arrival %d (0 = never; %d fills refused), want by arrival %d",
					w, cachedAt, refused, w+1)
			}
			if !resident890(store, k) || !d.hasEdge(k, dep) {
				t.Fatalf("K must stay cached with its edge (resident=%v edge=%v)", resident890(store, k), d.hasEdge(k, dep))
			}
		})
	}
}

// TestIssue890_DeclinedResolveReleasesItsEdges — #893: a resolve that records
// under K and then DECLINES its write (a Put gate: UAF, stage error, external,
// sensitive...) never reaches the store, so no refusal strips anything. The
// resolve's end — DepTracker.ReleaseUnstored, deferred at every site that
// installs an L1 resolve scope — leaves no edge and no memo link for a K that is
// not resident; a resident K keeps everything.
func TestIssue890_DeclinedResolveReleasesItsEdges(t *testing.T) {
	d, store, m := setup890(t)
	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "declined"}

	const cold = "L1_890_declined"
	ctx := WithL1KeyContext(context.Background(), cold)
	d.Record(ctx, cold, dep.GVR, dep.Namespace, dep.Name)
	m.StoreLayeredWithStates("memo-declined", cold, memoBody(map[string]any{"x": 1.0}), []DepKey{dep}, nil, DepGenEpochNow(), nil)
	// ... the Put gate declines: no write.
	d.ReleaseUnstored(context.Background(), cold)
	if d.hasEdges(cold) {
		t.Fatal("#893 RED: a declined resolve of a non-resident key left dep edges")
	}
	m.Load("probe-none")
	if keys := memoKeys883(m); len(keys) != 0 {
		t.Fatalf("#893 RED: the memo still holds %v for the declined, non-resident key", keys)
	}

	const warm = "L1_890_declined_resident"
	d.Record(context.Background(), warm, dep.GVR, dep.Namespace, dep.Name)
	store.Put(warm, &ResolvedEntry{RawJSON: []byte(`{}`)})
	ctxW := WithL1KeyContext(context.Background(), warm) // a refresh that declines
	d.Record(ctxW, warm, dep.GVR, dep.Namespace, dep.Name)
	d.ReleaseUnstored(context.Background(), warm)
	if !d.hasEdge(warm, dep) {
		t.Fatal("#893 RED: a declined refresh of a RESIDENT key stripped that cell's edges")
	}
}
