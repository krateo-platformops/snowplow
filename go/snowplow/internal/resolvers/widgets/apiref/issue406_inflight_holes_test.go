// issue406_inflight_holes_test.go — #406 review holes P1–P3 (reviewer-420 on ab617f36):
// a widget resolve that Go-sliced raKey and whose Put is still IN FLIGHT when raKey
// changes under it. Each arm drives the real raFullListServe fast path and the real
// gen-guarded widget Put; raKey changes through the production refresh re-Put
// (ReplaceIfGen) or a real store removal.
//   P1 raKey is evicted while the widget is in flight;
//   P2 the widget is in flight across TWO raKey content changes;
//   P3 two resolves of one widget key race and the OLDER one Puts last.
// In every case the widget's Put must remark it, so its body cannot stay stale.

package apiref

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// inflight406 is one widget resolve that sliced raKey and has not Put yet.
type inflight406 struct {
	ctx  context.Context
	gen0 uint64
	body []byte
}

func (h *layered406) sliceInFlight(wKey string) inflight406 {
	h.t.Helper()
	ctx := cache.WithL1KeyContext(h.base, wKey)
	gen0 := cache.ResolvedCache().CaptureGen(wKey)
	hit0 := cache.RAFullListServeSnapshot().Hit
	cache.Deps().Record(ctx, wKey, gvr(), layered406NS, h.raName)
	got, ok, err := raFullListServe(ctx, gvr(), layered406NS, h.raName, ra(raSliceJQ), 5, 1, nil, h.resolve)
	if err != nil || !ok {
		h.t.Fatalf("slice %s: ok=%v err=%v", wKey, ok, err)
	}
	if cache.RAFullListServeSnapshot().Hit == hit0 {
		h.t.Fatalf("[prove-path] %s must be served by the 4a fast-path HIT", wKey)
	}
	return inflight406{ctx: ctx, gen0: gen0, body: edge3MustJSON(h.t, got)}
}

func (h *layered406) putInFlight(wKey string, f inflight406) {
	h.t.Helper()
	if !cache.ResolvedCache().PutIfGen(f.ctx, wKey, &cache.ResolvedEntry{RawJSON: f.body, Inputs: edge3WidgetInputs(wKey)}, f.gen0) {
		h.t.Fatalf("PutIfGen %s refused", wKey)
	}
}

// P1 — raKey leaves the store while the widget resolve is in flight. The refresher's
// raKey re-Put then has nothing to replace (ReplaceIfGen refuses an absent key), so the
// widget's own Put is the only place left to catch the stale slice.
func TestIssue406_P1_RAKeyEvictedWhileWidgetInFlight_PutRemarks(t *testing.T) {
	marker := "OLD"
	h := newLayered406(t, "ra-406-p1", "L1_w406_p1_seed", &marker)
	h.coldFill()
	const w = "L1_w406_p1"
	f := h.sliceInFlight(w)
	cache.ResolvedCache().DeleteForTest(h.raKey)
	h.putInFlight(w, f)
	if h.layeredBy[w] != 1 {
		t.Fatalf("#406 P1 RED: the widget Put a slice of a raKey that left the store mid-resolve and was "+
			"remarked %d times (want 1) — nothing else can refresh it", h.layeredBy[w])
	}
}

// P2 — the widget resolve is in flight across two raKey content changes.
func TestIssue406_P2_WidgetInFlightAcrossTwoChanges_PutRemarks(t *testing.T) {
	marker := "V1"
	h := newLayered406(t, "ra-406-p2", "L1_w406_p2_seed", &marker)
	h.coldFill()
	const w = "L1_w406_p2"
	f := h.sliceInFlight(w) // slices V1, cell w not resident yet
	marker = "V2"
	h.refresh(h.raKey)
	marker = "V3"
	h.refresh(h.raKey)
	h.putInFlight(w, f)
	if h.layeredBy[w] != 1 {
		t.Fatalf("#406 P2 RED: the widget Put a V1 slice after raKey moved V1→V2→V3 and was remarked %d "+
			"times (want 1)", h.layeredBy[w])
	}
}

// P3 — two resolves of the same widget key: A slices the old raKey, raKey is re-Put
// with new content, B slices the new one and Puts first, then A Puts its stale body last.
func TestIssue406_P3_OlderConcurrentResolvePutsLast_PutRemarks(t *testing.T) {
	marker := "OLD"
	h := newLayered406(t, "ra-406-p3", "L1_w406_p3_seed", &marker)
	h.coldFill()
	const w = "L1_w406_p3"
	a := h.sliceInFlight(w)
	marker = "NEW"
	h.refresh(h.raKey)
	b := h.sliceInFlight(w)
	h.putInFlight(w, b)
	if h.layeredBy[w] != 0 {
		t.Fatalf("setup: B sliced the current raKey; its Put must not remark, got %d", h.layeredBy[w])
	}
	a.gen0 = cache.ResolvedCache().CaptureGen(w) // A's write is accepted (no DELETE between)
	h.putInFlight(w, a)
	if h.layeredBy[w] != 1 {
		t.Fatalf("#406 P3 RED: the older resolve Put its OLD slice last and the widget was remarked %d "+
			"times (want 1) — the index must judge each Put by what THAT Put's body sliced", h.layeredBy[w])
	}
	if !h.widgetHasKey(w, "OLD") {
		t.Fatalf("setup: the cell must hold A's OLD body (last writer)")
	}
}

// C1 — ONE widget resolve slices the same raKey twice (two apiRefs on one widget) with
// a raKey re-Put in between: the first slice is stale, so the Put must remark.
func TestIssue406_C1_OneResolveSlicesRAKeyAtTwoVersions_PutRemarks(t *testing.T) {
	marker := "OLD"
	h := newLayered406(t, "ra-406-c1", "L1_w406_c1_seed", &marker)
	h.coldFill()
	const w = "L1_w406_c1"
	ctx := cache.WithL1KeyContext(h.base, w)
	gen0 := cache.ResolvedCache().CaptureGen(w)
	serve := func(page int) {
		t.Helper()
		hit0 := cache.RAFullListServeSnapshot().Hit
		if _, ok, err := raFullListServe(ctx, gvr(), layered406NS, h.raName, ra(raSliceJQ), 5, page, nil, h.resolve); err != nil || !ok {
			t.Fatalf("serve page %d: ok=%v err=%v", page, ok, err)
		}
		if cache.RAFullListServeSnapshot().Hit == hit0 {
			t.Fatalf("[prove-path] page %d must be a 4a fast-path HIT", page)
		}
	}
	serve(1) // first apiRef slices OLD
	marker = "NEW"
	h.refresh(h.raKey)
	serve(2) // second apiRef slices NEW (current)
	if !cache.ResolvedCache().PutIfGen(ctx, w, &cache.ResolvedEntry{RawJSON: []byte(`{"w":1}`), Inputs: edge3WidgetInputs(w)}, gen0) {
		t.Fatalf("PutIfGen refused")
	}
	if h.layeredBy[w] != 1 {
		t.Fatalf("#406 C1 RED: the widget body embeds an OLD slice and a NEW one; its Put must remark once, got %d",
			h.layeredBy[w])
	}
}

func (h *layered406) widgetHasKey(key, marker string) bool {
	h.t.Helper()
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok {
		h.t.Fatalf("%s not resident", key)
	}
	var body map[string]any
	if err := json.Unmarshal(e.RawJSON, &body); err != nil {
		h.t.Fatalf("decode %s: %v", key, err)
	}
	return f6ContainsMarker(h.t, body, marker)
}
