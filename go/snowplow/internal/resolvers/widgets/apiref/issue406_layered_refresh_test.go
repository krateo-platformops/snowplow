// issue406_layered_refresh_test.go — #406: a RESTAction edit dirty-marks BOTH a widget
// cell and the RAFullList cell (raKey) it is Go-sliced from. If the refresher dequeues
// the WIDGET first, its re-resolve takes the 4a fast path (SliceabilityLookup
// known+sliceable, raKey hit, ra_full_list.go fast path) and slices the STILL-STALE
// raKey. When raKey is then re-Put fresh, nothing used to re-mark the widget, so the
// widget stayed stale until its next mark or TTL.
//
// HARNESS. The established apiref serve-path idiom (edge3_serve_seam_test.go): a real
// ResourceWatcher over a dynamic.fake seeded with the RBAC grant, so raFullListServe
// derives a real BindingUID; the real DepTracker + ResolvedCacheStore; the RA edit is a
// real OnUpdate on the RESTAction coordinate (OnObjectEvent, the informer bridge's
// decision site). The two refreshes are the production resolveAndPopulateL1 sequence
// (CaptureGen → WithL1KeyContext(key) → re-resolve → ReplaceIfGen), driven from a FIFO
// with workqueue dedup semantics so the dequeue ORDER is the variable under test: the
// fan-out iterates a map, so in production either order happens. Lever C (the
// sliceability-memo invalidation the refresh hook submits for every marked key) is
// applied SYNCHRONOUSLY here, its strongest form; it is rate-floored and refuses a
// verdict recorded inside the floor, so it cannot rescue the widget.

package apiref

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type layered406 struct {
	t        *testing.T
	base     context.Context
	raName   string
	raKey    string
	raInputs cache.ResolvedKeyInputs
	wKey     string
	resolve  func(context.Context, int, int) (map[string]any, error)

	queue     []string        // FIFO of dirty-marked keys (workqueue order)
	queued    map[string]bool // workqueue dedup: a queued key is not re-added
	refreshes map[string]int  // completed refreshes per key
	layered   int             // #406 remarks observed for wKey
	layeredBy map[string]int  // #406 remarks observed, any key
}

const layered406NS = edge3BackingNS

func newLayered406(t *testing.T, raName, wKey string, markerPtr *string) *layered406 {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	edge3NewWatcher(t, f6BuildFixture()...)
	cache.Deps().SetStore(cache.ResolvedCache())

	h := &layered406{
		t:         t,
		base:      f6CtxWithUser(t, "admin", []string{"system:masters"}),
		raName:    raName,
		wKey:      wKey,
		resolve:   edge3StubResolveRA(t, markerPtr, edge3BackingGVR()),
		queued:    map[string]bool{},
		refreshes: map[string]int{},
		layeredBy: map[string]int{},
	}
	h.raInputs = f6KeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		layered406NS, raName, "C:crb-a-f6-uid", nil)
	h.raKey = cache.ComputeKey(h.raInputs)

	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
		if reason != "layered" {
			return
		}
		h.layeredBy[k]++
		if k == h.wKey {
			h.layered++
		}
	}))
	cache.Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		// Lever C, synchronously (production submits it async to a rate-floored worker).
		cache.InvalidateSliceabilityForKey(k)
		if h.queued[k] {
			return
		}
		h.queued[k] = true
		h.queue = append(h.queue, k)
	})
	return h
}

// serveWidget is the widget resolve through the apiRef chokepoint: the RA fetch under
// the widget ctx records the RA self-dep (objects.Get, get.go), then raFullListServe.
func (h *layered406) serveWidget(ctx context.Context) []byte {
	h.t.Helper()
	cache.Deps().Record(ctx, h.wKey, gvr(), layered406NS, h.raName)
	got, ok, err := raFullListServe(ctx, gvr(), layered406NS, h.raName, ra(raSliceJQ), 5, 1, nil, h.resolve)
	if err != nil || !ok {
		h.t.Fatalf("widget serve failed: ok=%v err=%v", ok, err)
	}
	return edge3MustJSON(h.t, got)
}

// coldFill is the first customer /call: first-sight verdict, raKey Put, widget PutIfGen.
func (h *layered406) coldFill() {
	h.t.Helper()
	c := cache.ResolvedCache()
	ctx := cache.WithL1KeyContext(h.base, h.wKey)
	gen0 := c.CaptureGen(h.wKey)
	body := h.serveWidget(ctx)
	if !c.PutIfGen(ctx, h.wKey, &cache.ResolvedEntry{RawJSON: body, Inputs: edge3WidgetInputs("w406")}, gen0) {
		h.t.Fatalf("setup: widget cold PutIfGen refused")
	}
	if _, ok := c.GetNoTouch(h.raKey); !ok {
		h.t.Fatalf("setup: raKey not resident after the cold fill")
	}
	if s, known := cache.SliceabilityLookup(h.raKey, seedFullListShape(gvr(), layered406NS, h.raName, ra(raSliceJQ))); !known || !s {
		h.t.Fatalf("setup: the verdict must be known+sliceable (known=%v sliceable=%v)", known, s)
	}
}

// refresh is resolveAndPopulateL1 for one dequeued key.
func (h *layered406) refresh(k string) {
	h.t.Helper()
	c := cache.ResolvedCache()
	gen0 := c.CaptureGen(k)
	rctx := cache.WithL1KeyContext(h.base, k)
	var entry *cache.ResolvedEntry
	switch k {
	case h.wKey:
		entry = &cache.ResolvedEntry{RawJSON: h.serveWidget(rctx), Inputs: edge3WidgetInputs("w406")}
	case h.raKey:
		full, err := h.resolve(rctx, 0, 0) // resolveRAFullListForRefresh: PerPage=0/Page=0
		if err != nil {
			h.t.Fatalf("raKey refresh resolve: %v", err)
		}
		in := h.raInputs
		pinned := false
		if prior, ok := c.GetNoTouch(k); ok {
			pinned = prior.Pinned
		}
		entry = &cache.ResolvedEntry{RawJSON: edge3MustJSON(h.t, full), Inputs: &in, Pinned: pinned}
	default:
		return
	}
	if !c.ReplaceIfGen(rctx, k, entry, gen0) {
		h.t.Fatalf("refresh of %q: ReplaceIfGen refused", k)
	}
	h.refreshes[k]++
}

// drain processes the queue FIFO with `first` dequeued first among the initial batch.
// Bounded: a remark storm fails instead of looping.
func (h *layered406) drain(first string) {
	h.t.Helper()
	for i, k := range h.queue {
		if k == first {
			h.queue[0], h.queue[i] = h.queue[i], h.queue[0]
			break
		}
	}
	for n := 0; len(h.queue) > 0; n++ {
		if n > 16 {
			h.t.Fatalf("remark storm: more than 16 refreshes for one RA edit (queue=%v)", h.queue)
		}
		k := h.queue[0]
		h.queue = h.queue[1:]
		delete(h.queued, k) // dequeued: a mark during processing re-adds it (workqueue Done)
		h.refresh(k)
	}
}

func (h *layered406) widgetHas(marker string) bool {
	h.t.Helper()
	e, ok := cache.ResolvedCache().GetNoTouch(h.wKey)
	if !ok {
		h.t.Fatalf("widget cell not resident")
	}
	var body map[string]any
	if err := json.Unmarshal(e.RawJSON, &body); err != nil {
		h.t.Fatalf("decode widget: %v", err)
	}
	return f6ContainsMarker(h.t, body, marker)
}

// editRA models a RESTAction spec edit: the output changes iff newMarker differs, and
// the RA coordinate gets a real UPDATE event. Both cells must be dirty-marked.
func (h *layered406) editRA(markerPtr *string, newMarker string) {
	h.t.Helper()
	*markerPtr = newMarker
	cache.Deps().OnUpdate(gvr(), layered406NS, h.raName)
	if !h.queued[h.wKey] || !h.queued[h.raKey] {
		h.t.Fatalf("PRECONDITION: the RA edit must dirty-mark BOTH the widget and raKey; queued=%v", h.queued)
	}
}

// RED on main for widget-first: the widget converges fresh whichever cell is dequeued
// first. raKey-first is the control (green on main too) and pins zero amplification:
// the raKey re-Put's remark coalesces with the widget's pending mark.
func TestIssue406_RAEdit_WidgetConvergesFresh_EitherDequeueOrder(t *testing.T) {
	for _, tc := range []struct {
		name            string
		widgetFirst     bool
		wantWidgetRefs  int
		wantLayeredRems int
	}{
		{"widget-dequeued-first", true, 2, 1},
		{"raKey-dequeued-first", false, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := "OLD"
			h := newLayered406(t, "ra-406-"+tc.name, "L1_w406_"+tc.name, &marker)
			h.coldFill()
			if !h.widgetHas("OLD") {
				t.Fatalf("setup: widget must hold OLD")
			}

			h.editRA(&marker, "NEW")
			first := h.raKey
			if tc.widgetFirst {
				first = h.wKey
			}
			hit0 := cache.RAFullListServeSnapshot().Hit
			h.drain(first)
			if tc.widgetFirst && cache.RAFullListServeSnapshot().Hit == hit0 {
				t.Fatalf("[prove-path] the widget refresh must take the 4a fast path (RAFullListServeHit)")
			}
			if h.refreshes[h.raKey] != 1 {
				t.Fatalf("raKey must refresh exactly once, got %d", h.refreshes[h.raKey])
			}
			if !h.widgetHas("NEW") {
				t.Fatalf("#406 RED (%s): the widget cell is STALE (still OLD) after both dirty-marked cells refreshed — "+
					"its re-resolve Go-sliced the not-yet-refreshed raKey and nothing re-marked it when raKey was "+
					"re-Put with changed content (refreshes=%v)", tc.name, h.refreshes)
			}
			if got := h.refreshes[h.wKey]; got != tc.wantWidgetRefs {
				t.Fatalf("amplification (%s): widget refreshed %d times, want %d", tc.name, got, tc.wantWidgetRefs)
			}
			if h.layered != tc.wantLayeredRems {
				t.Fatalf("(%s): want %d layered remark of the widget, got %d", tc.name, tc.wantLayeredRems, h.layered)
			}
		})
	}
}

// An RA edit that leaves the output IDENTICAL (a label/annotation touch) re-Puts raKey
// with the same bytes. With the widget dequeued first it re-slices the same content;
// the raKey re-Put must NOT remark it — exactly one widget refresh, zero layered remarks.
func TestIssue406_RAEdit_IdenticalContent_NoExtraRemark(t *testing.T) {
	marker := "SAME"
	h := newLayered406(t, "ra-406-identical", "L1_w406_identical", &marker)
	h.coldFill()

	h.editRA(&marker, "SAME")
	h.drain(h.wKey)
	if h.refreshes[h.raKey] != 1 || h.refreshes[h.wKey] != 1 {
		t.Fatalf("identical content: want one refresh per cell, got %v", h.refreshes)
	}
	if h.layered != 0 {
		t.Fatalf("identical content: the raKey re-Put remarked the widget %d times; a remark is due only when "+
			"the content changed", h.layered)
	}
	if !h.widgetHas("SAME") {
		t.Fatalf("widget lost its content")
	}
}

// A widget whose re-resolve REPOPULATES raKey (cell miss under a known verdict) builds its
// body from that fresh resolve, so the raKey Put it makes must not remark the widget
// itself, even though it was a consumer of the evicted raKey version.
func TestIssue406_RepopulateBranch_NoSelfRemark(t *testing.T) {
	marker := "OLD"
	h := newLayered406(t, "ra-406-repopulate", "L1_w406_repopulate", &marker)
	h.coldFill()
	h.refresh(h.wKey) // fast path: the widget is now a consumer of raKey@OLD
	if h.layered != 0 {
		t.Fatalf("setup: unexpected layered remark %d", h.layered)
	}

	cache.ResolvedCache().DeleteForTest(h.raKey) // raKey gone; the widget stays resident
	marker = "NEW"
	rep0 := cache.RAFullListServeSnapshot().Repopulate
	h.refresh(h.wKey) // cell miss under a known verdict: repopulate + PutRAFullListIfGen
	if cache.RAFullListServeSnapshot().Repopulate == rep0 {
		t.Fatalf("[prove-path] the widget refresh must take the repopulate branch")
	}
	if !h.widgetHas("NEW") {
		t.Fatalf("setup: the repopulated widget must hold NEW")
	}
	if h.layered != 0 || len(h.queue) != 0 {
		t.Fatalf("amplification: the widget repopulated raKey from its own fresh resolve and was remarked "+
			"%d times (queue=%v); want 0", h.layered, h.queue)
	}
}
