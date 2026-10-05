// prewarm_engine_seed_order_test.go — #130 F3b-r2: the NavOrder-FLAT (total
// order), class-tail seed falsifier. SUPERSEDES the #42 FIX-E rank-major /
// class-interleaved model for the boot/gvr-discovered path.
//
// ── ORDER MODEL CHANGE (2026-07-12, arch ruling B) ──
// F3b-r2 replaced the rank-major widget loop (per identity rank r → widgets(r)
// in NavOrder → restactions(r)) with a SINGLE FLAT PASS over all (widget ×
// cohort) units sorted by (NavOrder ASC, cohortRankIndex ASC, ns/name ASC),
// followed by the RA tail. The dashboard seeds first because it is the
// low-NavOrder prefix in the DATA — no RootIndex/rank branch. Cohort interleave:
// because NavOrder is the PRIMARY key, every cohort's NavOrder-0 widget seeds
// before ANY cohort's NavOrder-1 widget (the "reachable cohorts' dashboards
// complete early" property, now by data order not a phase).
//
// This inverts the old FIX-E rank-major invariant (which required ALL of rank-1
// devs before ANY rank-2 ops). Under NavOrder-major, ops' NavOrder-0 widget
// seeds before devs' NavOrder-1 widget. The properties RE-EXPRESSED here:
//   (nav)  NavOrder-major: every cohort's widget at NavOrder N seeds before any
//          cohort's widget at NavOrder N+1 (the load-bearing F3b-r2 property).
//   (rank) within a NavOrder tie, cohorts interleave by rank (devs before ops)
//          — the cohortRankIndex tie-break (largest-cohort-first per nav slot).
//   (tail) RAs seed AFTER the whole widget list (RA content tail), and within
//          the tail in ascending len(targets) order (cheap RA before fan-out).
//
// Condition 3: NavOrder is derived via the REAL harvest-stamping
// (newNavWidgetHarvester + harvestNavWidget in walk order), NOT hand-assigned —
// so it fails if the harvest-order stamping breaks, not only the seed loop.
//
// Hermetic, -race, seams only (no cluster). TestFixD_IdentityRankMajorSeedOrder
// (all widgets at NavOrder 0 → the flat sort degenerates to rank-major via the
// cohortRankIndex tie-break) stays GREEN unmodified in its own file.

package dispatchers

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// ── recorder: ordered (class,label,identity) seed events ────────────────────
type eSeedEvent struct {
	class    string // "widget" | "restaction"
	label    string // ns/name
	identity string // "user:<u>" | "group:<g>" | "anon"
}

type eSeedRecorder struct {
	mu     sync.Mutex
	events []eSeedEvent
}

func (r *eSeedRecorder) record(class, label, identity string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, eSeedEvent{class: class, label: label, identity: identity})
}

func (r *eSeedRecorder) snapshot() []eSeedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]eSeedEvent(nil), r.events...)
}

// eIdentityLabel renders the seeding identity from the cohort ctx the seam runs
// under (withCohortSeedContext installs WithUserInfo). Mirrors the rank key
// domain (Username, else first group).
func eIdentityLabel(ctx context.Context) string {
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return "anon"
	}
	if ui.Username != "" {
		return "user:" + ui.Username
	}
	if len(ui.Groups) > 0 {
		return "group:" + ui.Groups[0]
	}
	return "anon"
}

// quietLoggingE silences seed info logging for a run (assert on the recorder).
func quietLoggingE(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(eDiscard{}, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

type eDiscard struct{}

func (eDiscard) Write(p []byte) (int, error) { return len(p), nil }

// ── E fixture GVRs ──────────────────────────────────────────────────────────
// Each widget has its OWN GVR so the enumerator seam can give it a distinct
// identity set; the two RA target GVRs get distinct-sized sets for the RA
// ascending-len tiebreak assert (property (b)).
func eWidgetGVR(res string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: res}
}

var (
	eCheapRAGVR  = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1", Resource: "cheaps"}
	eFanoutRAGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

type eID struct {
	name      string
	group     bool
	collapsed int
}

func (e eID) subject() cache.SubjectIdentity {
	if e.group {
		return cache.SubjectIdentity{Groups: []string{e.name}}
	}
	return cache.SubjectIdentity{Username: e.name}
}

// eIdentityTargets builds one PrewarmTarget per identity for a GVR; the
// CollapsedBindings count drives the rank.
func eIdentityTargets(gvr schema.GroupVersionResource, ids ...eID) []cache.PrewarmTarget {
	out := make([]cache.PrewarmTarget, 0, len(ids))
	for _, id := range ids {
		out = append(out, cache.PrewarmTarget{
			BindingUID:        "uid-" + id.name,
			Subject:           id.subject(),
			GVR:               gvr,
			Verb:              "list",
			CollapsedBindings: id.collapsed,
		})
	}
	return out
}

// eHarvestWidget stamps a widget through the REAL harvester (condition 3) so
// NavOrder comes from harvest sequence, not a hand-assigned value.
func eHarvestWidget(h *navWidgetHarvester, name string, gvr schema.GroupVersionResource) {
	w := &unstructured.Unstructured{}
	w.SetNamespace("krateo-system")
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: "W"})
	h.harvestNavWidget(w, gvr, -1, 1, -1, -1)
}

func eRA(name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
}

// TestF3bR2_NavOrderFlatSeedOrder — the #130 F3b-r2 NavOrder-flat falsifier.
//
// Fixture: 2 identity ranks (devs collapsed=442 = rank 0; ops collapsed=5 =
// rank 1), 3 widgets harvested in a deliberate NAV ORDER (dashboard-flex first,
// then obs-panel, then settings-card), each widget carrying BOTH identities; 2
// restactions (cheap 1-target, fanout 3-target) each carrying devs.
func TestF3bR2_NavOrderFlatSeedOrder(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	devs := eID{name: "devs", group: true, collapsed: 442}
	ops := eID{name: "ops", group: true, collapsed: 5}

	// REAL harvest-stamping (condition 3): NavOrder = harvest sequence.
	h := newNavWidgetHarvester()
	dashGVR, obsGVR, setGVR := eWidgetGVR("flexes"), eWidgetGVR("obs"), eWidgetGVR("cards")
	eHarvestWidget(h, "dashboard-flex", dashGVR) // NavOrder 0 — first-nav
	eHarvestWidget(h, "obs-panel", obsGVR)       // NavOrder 1
	eHarvestWidget(h, "settings-card", setGVR)   // NavOrder 2
	widgets := h.snapshot()
	if len(widgets) != 3 {
		t.Fatalf("harvest setup: want 3 widgets, got %d", len(widgets))
	}

	cheapRA, fanoutRA := eRA("cheap-ra"), eRA("fanout-ra")
	ras := []templatesv1.ObjectReference{fanoutRA, cheapRA} // hostile input order (fanout first)

	rec := &eSeedRecorder{}

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(gvr schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		switch gvr {
		case dashGVR, obsGVR, setGVR:
			return eIdentityTargets(gvr, devs, ops)
		case eCheapRAGVR:
			return eIdentityTargets(eCheapRAGVR, devs) // 1 target
		case eFanoutRAGVR:
			return eIdentityTargets(eFanoutRAGVR, devs, ops, eID{name: "filler", collapsed: 1}) // 3 targets
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevTGVR := restActionTargetGVRFn
	restActionTargetGVRFn = func(_ context.Context, ref templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		if ref.Name == "cheap-ra" {
			return eCheapRAGVR, true
		}
		return eFanoutRAGVR, true
	}
	t.Cleanup(func() { restActionTargetGVRFn = prevTGVR })

	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		rec.record("widget", e.W.GetName(), eIdentityLabel(ctx))
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	prevR := seedOneRestactionFn
	seedOneRestactionFn = func(ctx context.Context, _ redact.Label, ref templatesv1.ObjectReference, _ string, _ seedScopeMode) error {
		rec.record("restaction", ref.Name, eIdentityLabel(ctx))
		return nil
	}
	t.Cleanup(func() { seedOneRestactionFn = prevR })

	if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}

	assertF3bR2Sequence(t, rec.snapshot())
}

// assertF3bR2Sequence verifies NavOrder-major total order / rank tie-break /
// RA-tail / RA-tiebreak on the recorded event sequence.
func assertF3bR2Sequence(t *testing.T, ev []eSeedEvent) {
	t.Helper()

	// (nav) NavOrder-MAJOR: recover each widget's NavOrder from its harvest name
	// (dashboard-flex=0, obs-panel=1, settings-card=2) and assert the WIDGET seed
	// stream is non-decreasing in NavOrder — i.e. every cohort's NavOrder-N widget
	// seeds before ANY cohort's NavOrder-(N+1) widget. This is the load-bearing
	// F3b-r2 property and is the exact INVERSE of the old rank-major invariant
	// (which required all devs before any ops; under NavOrder-major, ops'
	// dashboard-flex seeds before devs' obs-panel — see the (rank) block).
	navOrderOf := map[string]int{"dashboard-flex": 0, "obs-panel": 1, "settings-card": 2}
	lastNav := -1
	var widgetSeq []eSeedEvent
	for _, e := range ev {
		if e.class != "widget" {
			continue
		}
		widgetSeq = append(widgetSeq, e)
		n, ok := navOrderOf[e.label]
		if !ok {
			t.Fatalf("F3b-r2 setup: unexpected widget %q; events=%+v", e.label, ev)
		}
		if n < lastNav {
			t.Fatalf("F3b-r2 (nav) NavOrder-MAJOR VIOLATED: widget %q (NavOrder %d) seeded AFTER a NavOrder-%d widget — "+
				"the flat pass must seed ALL cohorts' NavOrder-N widgets before ANY cohort's NavOrder-(N+1) widget; widget stream=%+v",
				e.label, n, lastNav, widgetSeq)
		}
		lastNav = n
	}
	if len(widgetSeq) != 6 {
		t.Fatalf("F3b-r2 setup: expected 6 widget seeds (3 widgets × 2 cohorts), got %d; events=%+v", len(widgetSeq), ev)
	}

	// (rank) within a NavOrder tie, cohorts interleave by rank (devs rank 0 before
	// ops rank 1). At NavOrder 0 the two dashboard-flex seeds are {devs, ops} in
	// that order; assert devs precedes ops for the SAME widget label.
	for _, label := range []string{"dashboard-flex", "obs-panel", "settings-card"} {
		devsIdx, opsIdx := -1, -1
		for i, e := range widgetSeq {
			if e.label != label {
				continue
			}
			if e.identity == "group:devs" {
				devsIdx = i
			} else if e.identity == "group:ops" {
				opsIdx = i
			}
		}
		if devsIdx < 0 || opsIdx < 0 {
			t.Fatalf("F3b-r2 (rank) setup: widget %q missing a cohort seed (devs=%d ops=%d); widget stream=%+v", label, devsIdx, opsIdx, widgetSeq)
		}
		if devsIdx > opsIdx {
			t.Fatalf("F3b-r2 (rank) tie-break VIOLATED: for widget %q the rank-1 ops seed (idx %d) preceded the rank-0 devs seed (idx %d) — "+
				"at equal NavOrder the cohortRankIndex tie-break must seed the larger cohort (devs) first; widget stream=%+v", label, opsIdx, devsIdx, widgetSeq)
		}
	}

	// (tail) EVERY widget seeds before EVERY restaction (the RA content tail is
	// excluded from the nav-widget readiness class).
	lastWidget, firstRA := -1, len(ev)
	for i, e := range ev {
		if e.class == "widget" {
			lastWidget = i
		} else if e.class == "restaction" && i < firstRA {
			firstRA = i
		}
	}
	if firstRA == len(ev) {
		t.Fatalf("F3b-r2 (tail): expected restaction seeds; events=%+v", ev)
	}
	if lastWidget > firstRA {
		t.Fatalf("F3b-r2 (tail) VIOLATED: a restaction at idx %d seeded BEFORE the last widget at idx %d — "+
			"RAs must seed AFTER the whole widget list; events=%+v", firstRA, lastWidget, ev)
	}

	// (tail) RA ascending-len tiebreak WITHIN the rank-0 (devs) tail: cheap-ra
	// (1 target) before fanout-ra (3 targets).
	var devsRAs []string
	for _, e := range ev {
		if e.class == "restaction" && e.identity == "group:devs" {
			devsRAs = append(devsRAs, e.label)
		}
	}
	seenCheap := false
	for _, ra := range devsRAs {
		if ra == "fanout-ra" && !seenCheap {
			t.Fatalf("F3b-r2 (tail) RA ascending-len tiebreak VIOLATED: fanout-ra (3 targets) seeded before cheap-ra (1 target); devs RA order=%v", devsRAs)
		}
		if ra == "cheap-ra" {
			seenCheap = true
		}
	}
	if !seenCheap {
		t.Fatalf("F3b-r2 (tail): cheap-ra was not seeded under devs; RA order=%v events=%+v", devsRAs, ev)
	}
}

// ── C-r2-1 (LOAD-BEARING): NavOrder total order, RootIndex INVERTED ─────────
// The design's headline falsifier: build widgetSeeds NavOrder 0..M-1 across K>=2
// cohorts, with the LOW-NavOrder widgets carrying a NON-zero (HIGH) RootIndex and
// the HIGH-NavOrder widget carrying RootIndex 0 — an INVERSION vs NavOrder. Assert
// the seed order follows NavOrder ASC regardless of RootIndex; EVERY cohort's
// low-NavOrder widget seeds before ANY cohort's high-NavOrder widget.
//
// RED (captured to /tmp/f3b-r2-falsifiers/ by source-revert of the flat-sort +
// firstNavReachable-rank-tier deletion): the F3 RootIndex-keyed order would sort
// the RootIndex==0 (HIGH-NavOrder) widget's cohort FIRST (firstNavReachable rank
// tier), seeding a RootIndex==0 high-NavOrder widget before the RootIndex!=0
// low-NavOrder one → the NavOrder-ASC assert diverges. This RED specifically
// proves the RootIndex branch is GONE, not dormant: if any code still consulted
// RootIndex==0 for ordering, this inverted fixture would seed high-NavOrder first.
//
// K=3 cohorts × M=3 widgets (>1×>1, feedback_falsifier_shape_must_discriminate).
func TestF3bR2_C_r2_1_NavOrderTotalOrder_RootIndexInverted(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	devs := eID{name: "devs", group: true, collapsed: 442}
	ops := eID{name: "ops", group: true, collapsed: 50}
	sre := eID{name: "sre", group: true, collapsed: 5}

	// REAL harvest-stamping gives NavOrder 0,1,2 in harvest order.
	h := newNavWidgetHarvester()
	wLow, wMid, wHigh := eWidgetGVR("low"), eWidgetGVR("mid"), eWidgetGVR("high")
	eHarvestWidget(h, "w-navorder-0", wLow)  // NavOrder 0
	eHarvestWidget(h, "w-navorder-1", wMid)  // NavOrder 1
	eHarvestWidget(h, "w-navorder-2", wHigh) // NavOrder 2
	widgets := h.snapshot()
	if len(widgets) != 3 {
		t.Fatalf("harvest setup: want 3 widgets, got %d", len(widgets))
	}
	// INVERT RootIndex vs NavOrder: the LOWEST-NavOrder widget gets the HIGHEST
	// RootIndex, the HIGHEST-NavOrder widget gets RootIndex 0. A sort that keyed on
	// RootIndex==0 would seed w-navorder-2 FIRST; a NavOrder sort seeds
	// w-navorder-0 first. (widgets are snapshot copies; NavOrder is the real
	// harvest stamp, only RootIndex is overridden to build the inversion.)
	for i := range widgets {
		switch widgets[i].NavOrder {
		case 0:
			widgets[i].RootIndex = 2 // low NavOrder, HIGH RootIndex
		case 1:
			widgets[i].RootIndex = 1
		case 2:
			widgets[i].RootIndex = 0 // high NavOrder, RootIndex 0 (the "dashboard" bait)
		}
	}

	rec := &eSeedRecorder{}
	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(gvr schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		switch gvr {
		case wLow, wMid, wHigh:
			return eIdentityTargets(gvr, devs, ops, sre)
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		rec.record("widget", e.W.GetName(), eIdentityLabel(ctx))
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	// Map widget name → NavOrder for the assert.
	navOf := map[string]int{"w-navorder-0": 0, "w-navorder-1": 1, "w-navorder-2": 2}

	// (1) The seed stream is non-decreasing in NavOrder (RootIndex ignored).
	lastNav := -1
	for _, e := range ev {
		n := navOf[e.label]
		if n < lastNav {
			t.Fatalf("C-r2-1 VIOLATED: widget %q (NavOrder %d, RootIndex-inverted) seeded AFTER a NavOrder-%d widget — "+
				"the sort must follow NavOrder ASC, NOT RootIndex (a RootIndex==0 sort would seed w-navorder-2 first); events=%+v",
				e.label, n, lastNav, ev)
		}
		lastNav = n
	}

	// (2) Every cohort's low-NavOrder widget seeds before ANY cohort's
	// high-NavOrder widget. The LAST NavOrder-0 seed index < the FIRST NavOrder-2
	// seed index.
	lastNav0, firstNav2 := -1, len(ev)
	for i, e := range ev {
		switch navOf[e.label] {
		case 0:
			lastNav0 = i
		case 2:
			if i < firstNav2 {
				firstNav2 = i
			}
		}
	}
	if lastNav0 < 0 || firstNav2 == len(ev) {
		t.Fatalf("C-r2-1 setup: expected both NavOrder-0 and NavOrder-2 seeds; events=%+v", ev)
	}
	if lastNav0 >= firstNav2 {
		t.Fatalf("C-r2-1 VIOLATED: a NavOrder-2 widget (RootIndex 0) seeded at idx %d BEFORE the last NavOrder-0 "+
			"widget (RootIndex 2) at idx %d — the RootIndex==0 'dashboard' bait was seeded first, so RootIndex is "+
			"STILL consulted for ordering (the branch is dormant, not gone); events=%+v", firstNav2, lastNav0, ev)
	}

	// (3) Set completeness: 3 widgets × 3 cohorts = 9 seeds (PURE ORDERING — no
	// unit dropped by the RootIndex inversion).
	if len(ev) != 9 {
		t.Fatalf("C-r2-1 set: expected 9 (widget × cohort) seeds, got %d; events=%+v", len(ev), ev)
	}
}
