// prewarm_seed_rank_hygiene_test.go — FIX-3 rank hygiene falsifier set
// (docs/fix3-rank-hygiene-design-2026-07-07.md §8 R3-C1..C8, §9 F1..F6 + the
// PM design-gate conditions COND-1..COND-4).
//
// FIX-3 changed the identity rank KEY from first-seen CollapsedBindings (a
// map-iteration-order-dependent value) to (widgetMax DESC, allMax DESC,
// identityKey ASC), both counts MAX-FOLDED over every observation. These arms
// prove the four load-bearing properties through the REAL seedScopeYielding
// path (rank-major loop) — no shadow rank function, no prod mutation seam:
//
//   F1 / COND-4 (determinism, MAP-ORDER variance): N>=20 in-process runs, each
//     rebuilding rankOf FRESH inside a fresh seedScopeYielding call, so Go map
//     iteration over rankOf (ranked := range rankOf, before the sort) naturally
//     varies across runs. The fixture includes a GENUINE TIE (two identities
//     equal on widgetMax AND allMax) so the ONLY thing keeping order stable is
//     the identityKey ASC tie-break — the load-bearing determinism guarantee.
//     Assert byte-identical ranked order every run. Mutation (drop the
//     identityKey tie-break → equal-maxima identities keep Go map order) RED is
//     captured empirically to /tmp/r3/ (source revert + rerun): the tied pair
//     flips across runs. This pins map-order variance through the REAL rankOf
//     map, not a slice-shuffle proxy.
//   F2 / COND-2-adjacent (pollution): boot600 shape (M: RA-only,
//     CollapsedBindings 1344; U1: widget 5; U2: widget 2) → ranked [U1, U2, M].
//     Mutation (drop the widgetMax primary key → allMax-only) RED captured to
//     /tmp/r3/ (M first).
//   F3 (tie-break): two identities with equal (widgetMax, allMax) → order by
//     identityKey ASC, stable.
//   F6 / COND-1 (set-equality, TUPLE-MULTISET): the seeded (unit x identity)
//     tuple-multiset is identical pre/post FIX-3 on the same fixture. The RED
//     arm swaps ONE tuple (a seed moved to the wrong unit) and shows the assert
//     fails — discriminating a SWAP, not only a DROP.
//
// The keepwarm-repair (F4/COND-3) and multi-root segKey (F5/COND-2) arms live
// in prewarm_first_nav_latch_segment_test.go (they need the latch/keepwarm
// seams already wired there). This file cross-references them.
//
// Hermetic, -race, seams only (seedOneWidgetFn / seedOneRestactionFn /
// enumeratePrewarmTargetsForGVRFn / restActionTargetGVRFn). Serializes on
// engineLatchTestMu (shares the seed seams + the customerInFlight atomic with
// the sibling prewarm tests). Does NOT touch ./internal/rbac/... .

package dispatchers

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// rankObsEvent records one seed in call order with the seeding identity and the
// unit (widget name or RA name) it seeded — the (unit x identity) tuple COND-1
// asserts the multiset of.
type rankObsEvent struct {
	class    string // "widget" | "restaction"
	unit     string // ns/name of the widget or RA
	identity string // "user:<u>" | "group:<g>" | "anon"
}

// installRankObsSeams wires the seed seams to record rankObsEvents and the
// enumerator/target-GVR seams from the given fixture closures.
func installRankObsSeams(t *testing.T, ev *[]rankObsEvent,
	enumerate func(schema.GroupVersionResource) []cache.PrewarmTarget,
	raTargetGVR func(templatesv1.ObjectReference) (schema.GroupVersionResource, bool)) {
	t.Helper()

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(gvr schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		return enumerate(gvr)
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevTGVR := restActionTargetGVRFn
	restActionTargetGVRFn = func(_ context.Context, ref templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return raTargetGVR(ref)
	}
	t.Cleanup(func() { restActionTargetGVRFn = prevTGVR })

	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		*ev = append(*ev, rankObsEvent{class: "widget", unit: e.W.GetNamespace() + "/" + e.W.GetName(), identity: eIdentityLabel(ctx)})
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	prevR := seedOneRestactionFn
	seedOneRestactionFn = func(ctx context.Context, _ redact.Label, ref templatesv1.ObjectReference, _ string, _ seedScopeMode) error {
		*ev = append(*ev, rankObsEvent{class: "restaction", unit: ref.Namespace + "/" + ref.Name, identity: eIdentityLabel(ctx)})
		return nil
	}
	t.Cleanup(func() { seedOneRestactionFn = prevR })
}

// rankedOrderFromEvents recovers the ranked identity order from a rank-major
// seed stream: the order in which each identity is FIRST seeded == its rank
// (the loop seeds rank-major, so rank r's targets all precede rank r+1's). This
// reads the order off the REAL prod loop, not a shadow rank function.
func rankedOrderFromEvents(ev []rankObsEvent) []string {
	seen := map[string]bool{}
	var order []string
	for _, e := range ev {
		if !seen[e.identity] {
			seen[e.identity] = true
			order = append(order, e.identity)
		}
	}
	return order
}

// ── F1 / COND-4: DETERMINISM through the REAL rankOf map, with a GENUINE TIE ──
// The fixture puts MANY identities at IDENTICAL (widgetMax, allMax) so the only
// thing keeping ranked order stable is the identityKey ASC tie-break. rankOf is
// rebuilt fresh inside each seedScopeYielding call, so `ranked := range rankOf`
// iterates in a DIFFERENT Go map order each run. Over N>=20 runs the ranked
// order must be byte-identical. Mutation (drop the identityKey tie-break) RED is
// captured empirically to /tmp/r3/ (the tied identities flip across runs).
func TestFix3Rank_Determinism_MapAndInputOrderInvariant(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	// 8 singleton widget cohorts, ALL widgetMax=1, allMax=1 → a full 8-way tie.
	// Names chosen so identityKey ASC order is unambiguous (g0..g7).
	h := newNavWidgetHarvester()
	w := latchWidgetGVR("flexes")
	harvestWidgetAtRoot(h, "dash", w, 0)
	widgets := h.snapshot()

	tied := make([]eID, 0, 8)
	for i := 0; i < 8; i++ {
		tied = append(tied, eID{name: string(rune('0' + i)), group: true, collapsed: 1})
	}
	// Prepend a group-prefixed name so identityKey (join of groups) sorts g0<g1<..
	for i := range tied {
		tied[i].name = "g" + tied[i].name
	}

	enumerate := func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
		if gvr == w {
			// Hostile enumeration order (reversed) — the comparator must still
			// produce g0..g7 regardless of input or map order.
			rev := make([]eID, len(tied))
			for i := range tied {
				rev[len(tied)-1-i] = tied[i]
			}
			return eIdentityTargets(gvr, rev...)
		}
		return nil
	}
	raTargetGVR := func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return schema.GroupVersionResource{}, false
	}

	want := []string{"group:g0", "group:g1", "group:g2", "group:g3", "group:g4", "group:g5", "group:g6", "group:g7"}

	const runs = 24
	for r := 0; r < runs; r++ {
		var ev []rankObsEvent
		installRankObsSeams(t, &ev, enumerate, raTargetGVR)

		if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
			t.Fatalf("run %d: seedScopeYielding returned %v; want nil", r, err)
		}
		order := rankedOrderFromEvents(ev)
		if strings.Join(order, ",") != strings.Join(want, ",") {
			t.Fatalf("F1/COND-4 DETERMINISM VIOLATED: run %d ranked order = %v differs from the identityKey-ASC order %v — the tied identities leaked Go map order (the identityKey tie-break is the load-bearing determinism guarantee)", r, order, want)
		}
	}
}

// ── F2: POLLUTION — the boot600 shape ranks the login cohorts above the whale.
// M is RA-only with CollapsedBindings 1344; U1/U2 are widget cohorts (5/2).
// FIX-3 ranked = [U1, U2, M] because M's widgetMax is 0. Pre-FIX-3 (first-seen /
// allMax-primary) M would take rank 0. Mutation RED (drop widgetMax primary) is
// captured to /tmp/r3/ empirically.
func TestFix3Rank_Pollution_Boot600_LoginCohortsOverWhale(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	h := newNavWidgetHarvester()
	dashA := latchWidgetGVR("flexes")
	dashB := latchWidgetGVR("estategraphs")
	harvestWidgetAtRoot(h, "dash-a", dashA, 0)
	harvestWidgetAtRoot(h, "dash-b", dashB, 0)
	widgets := h.snapshot()

	fanoutRA := latchRA("machine-fanout-ra")
	ras := []templatesv1.ObjectReference{fanoutRA}

	u1 := eID{name: "u1", group: true, collapsed: 5}
	u2 := eID{name: "u2", group: true, collapsed: 2}
	m := eID{name: "machine-sa", collapsed: 1344}

	var ev []rankObsEvent
	installRankObsSeams(t, &ev,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case dashA:
				return eIdentityTargets(gvr, u1)
			case dashB:
				return eIdentityTargets(gvr, u2)
			case eFanoutRAGVR:
				return eIdentityTargets(eFanoutRAGVR, m) // M appears ONLY here.
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return eFanoutRAGVR, true
		})

	if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	order := rankedOrderFromEvents(ev)
	want := []string{"group:u1", "group:u2", "user:machine-sa"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("F2 POLLUTION: ranked order = %v; want %v — the RA-only whale (M, CollapsedBindings 1344, widgetMax 0) must rank BELOW the login-cohort widgets (U1 wMax 5, U2 wMax 2). Pre-FIX-3 M took rank 0 → the prime seed slot warmed a page-less machine SA", order, want)
	}
	// Discriminator: M is strictly LAST — the pollution is fully drained, not
	// merely demoted by one.
	if order[len(order)-1] != "user:machine-sa" {
		t.Fatalf("F2: M (widgetMax 0) is not the LAST-ranked identity; order=%v", order)
	}
}

// ── F3: TIE-BREAK — equal (widgetMax, allMax) → identityKey ASC, stable. ─────
func TestFix3Rank_TieBreak_IdentityKeyAscending(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	h := newNavWidgetHarvester()
	w := latchWidgetGVR("flexes")
	harvestWidgetAtRoot(h, "dash", w, 0)
	widgets := h.snapshot()

	// Three singleton cohorts, all widgetMax=1, allMax=1 → genuine tie. Provide
	// in a hostile (reverse-sorted) enumeration order; the comparator must sort
	// them identityKey ASC (group:zed > group:mid > group:abe lexically → abe,
	// mid, zed). identityKey = Username\x1f join(Groups) so "group" identities
	// key on the group name.
	abe := eID{name: "abe", group: true, collapsed: 1}
	mid := eID{name: "mid", group: true, collapsed: 1}
	zed := eID{name: "zed", group: true, collapsed: 1}

	var ev []rankObsEvent
	installRankObsSeams(t, &ev,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			if gvr == w {
				return eIdentityTargets(gvr, zed, mid, abe) // hostile order
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	order := rankedOrderFromEvents(ev)
	want := []string{"abe", "mid", "zed"} // identity labels are "group:<g>"; check suffix order
	var got []string
	for _, id := range order {
		got = append(got, strings.TrimPrefix(id, "group:"))
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("F3 TIE-BREAK: ranked order = %v; want identityKey ASC %v (equal widgetMax/allMax must fall to the deterministic key tie-break, no starvation)", got, want)
	}
}

// ── F6 / COND-1: SET-EQUALITY (TUPLE-MULTISET), swap-RED discriminating. ─────
// The seeded (unit x identity) tuple-multiset is unchanged pre/post FIX-3 —
// FIX-3 is PURE ORDERING (FIX-E invariant). We assert the multiset the real
// FIX-3 run produces equals the expected set, then SWAP one tuple (move a seed
// to the wrong unit) and show the multiset-equality assert goes RED — proving
// the assert catches a SWAP, not only a DROP.
func TestFix3Rank_SetEquality_TupleMultiset_SwapIsRed(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	h := newNavWidgetHarvester()
	wA := latchWidgetGVR("wa")
	wB := latchWidgetGVR("wb")
	harvestWidgetAtRoot(h, "wa", wA, 0)
	harvestWidgetAtRoot(h, "wb", wB, 0)
	widgets := h.snapshot()

	fanoutRA := latchRA("fan")
	ras := []templatesv1.ObjectReference{fanoutRA}

	devs := eID{name: "devs", group: true, collapsed: 9}
	ops := eID{name: "ops", group: true, collapsed: 2}
	m := eID{name: "sa", collapsed: 50} // RA-only tail

	var ev []rankObsEvent
	installRankObsSeams(t, &ev,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case wA:
				return eIdentityTargets(gvr, devs, ops)
			case wB:
				return eIdentityTargets(gvr, devs)
			case eFanoutRAGVR:
				return eIdentityTargets(eFanoutRAGVR, devs, m)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return eFanoutRAGVR, true
		})

	if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}

	// The expected (unit x identity) tuple-multiset — widget-less M keeps its RA
	// seed at the tail (PURE ORDERING: nothing dropped).
	want := tupleMultiset([]rankObsEvent{
		{class: "widget", unit: "krateo-system/wa", identity: "group:devs"},
		{class: "widget", unit: "krateo-system/wa", identity: "group:ops"},
		{class: "widget", unit: "krateo-system/wb", identity: "group:devs"},
		{class: "restaction", unit: "krateo-system/fan", identity: "group:devs"},
		{class: "restaction", unit: "krateo-system/fan", identity: "user:sa"},
	})
	got := tupleMultiset(ev)
	if !multisetEqual(got, want) {
		t.Fatalf("F6/COND-1 SET-EQUALITY: seeded (unit x identity) tuple-multiset differs from the pre/post-FIX-3 invariant set.\n got=%v\nwant=%v\nFIX-3 must be PURE ORDERING — no seed added/dropped (a widget-less identity still seeds its RA at the tail)", got, want)
	}

	// SWAP-RED: move ONE seed to the wrong unit (devs's wb seed → wa). The
	// multiset now has two (wa,devs) and zero (wb,devs). Assert the equality
	// check DISCRIMINATES this swap (not just a drop).
	swapped := append([]rankObsEvent(nil), ev...)
	for i := range swapped {
		if swapped[i].class == "widget" && swapped[i].unit == "krateo-system/wb" && swapped[i].identity == "group:devs" {
			swapped[i].unit = "krateo-system/wa" // moved to the wrong unit
			break
		}
	}
	if multisetEqual(tupleMultiset(swapped), want) {
		t.Fatalf("F6/COND-1 SWAP NOT DISCRIMINATED: moving a seed from wb to wa left the tuple-multiset equal to the expected set — the assert only catches DROPs, not SWAPs; the COND-1 shape is degenerate")
	}
}

// tupleMultiset renders a stable string-keyed count map of the events.
func tupleMultiset(ev []rankObsEvent) map[string]int {
	m := map[string]int{}
	for _, e := range ev {
		m[e.class+"\x1f"+e.unit+"\x1f"+e.identity]++
	}
	return m
}

func multisetEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	// stable iteration not needed for equality; sort only used in error render
	return true
}

var _ = sort.Strings
