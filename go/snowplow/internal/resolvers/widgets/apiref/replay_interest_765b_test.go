// replay_interest_765b_test.go — #765 B (reason 2 of the #765 trace): a widget
// that does not resolve its RESTAction itself receives the producer's LIST
// edges by REPLAY, and the replay now carries the producer edge's interest
// state. Design: #765 issuecomment-6285000306, ruled sound by architect-623
// (-6285013799, -6285026365).
//
// Drives the REAL apiref.Resolve (SeedResolveMemo wiring + raFullListServe) over
// a real watcher, the real DepTracker and store. The producing resolve is a stub
// that does what restactions.Resolve does for a list edge: a narrowable record
// under its L1 key, then — BEFORE it returns — a commit (narrowed), a widen
// (all) or nothing (pending). The commit-before-return half of the soundness
// argument is pinned against the REAL resolver by restactions/api P8a.
//
//	P1  memo siblings and the 4a-hit producer narrow to the raKey's read set;
//	C2  the memo snapshot is taken after the producer returned (siblings carry
//	    the settled state);
//	P2  an All producer (an `input` chain) leaves every receiver all; a `now`
//	    chain is a separate case that follows the pending `now` ruling;
//	P3  a pending producer edge replays plain, and is counted as pending;
//	P4  a full-list hit carries the raKey edge's state (counted per site);
//	P5  a receiver already sticky all stays all;
//	P6  the forward match set (what an event marks) is identical with the
//	    shadow on and off;
//	P7  an open capture on a receiver still sees the replayed edge ([C1-A]);
//	P8b V2 widens the raKey edge and is Put; a widget slicing V2 is all;
//	P9  the would_change audit covers a receiver widget: an avoid-only refresh
//	    of a memo sibling is sampled, and a forced byte difference is a
//	    would_change.
package apiref

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/cache/jqinterest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const raName765b = "ra-765b"
const backingObj765b = "comp-765b"

// analyze765b analyses one stage filter over a backing LIST stage "s".
func analyze765b(filter string) jqinterest.Interest {
	return jqinterest.Analyze(jqinterest.Chain{
		RA: jqinterest.RESTAction{Stages: []jqinterest.Stage{{
			Name: "s", Path: "/apis/apps.krateo.io/v1/namespaces/" + edge3BackingNS + "/compositions", Filter: &filter,
		}}},
		Tracked: "s",
	}, nil)
}

func backingDK765b() cache.DepKey {
	return cache.DepKey{GVR: edge3BackingGVR(), Namespace: edge3BackingNS, Name: "*"}
}

// stub765b is the producing resolve: a narrowable list record under the ctx's
// L1 key, settled (per *mode) BEFORE it returns, then the real slice jq.
func stub765b(t *testing.T, mode *string, in jqinterest.Interest) func(context.Context, int, int) (map[string]any, error) {
	return func(rctx context.Context, perPage, page int) (map[string]any, error) {
		if l1 := cache.L1KeyFromContext(rctx); l1 != "" {
			d := cache.Deps()
			dk := backingDK765b()
			d.RecordListNarrowable(context.Background(), l1, dk.GVR, dk.Namespace)
			switch *mode {
			case "narrow":
				if in.All {
					d.WidenListInterest(l1, dk, cache.CommitWhyOfAnalysis(in.Why))
				} else {
					d.CommitListInterest(l1, dk, in.Reads)
				}
			case "all":
				d.WidenListInterest(l1, dk, cache.CommitWhyStageError)
			case "pending":
			}
		}
		items := make([]any, 12)
		for i := range items {
			items[i] = map[string]any{"metadata": map[string]any{"name": "panel-" + itoa3(i), "creationTimestamp": tsName(i)}}
		}
		dict := map[string]any{"compositionspanels": items}
		if perPage > 0 && page > 0 {
			dict["slice"] = map[string]any{"perPage": float64(perPage), "page": float64(page), "offset": float64((page - 1) * perPage)}
		}
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: raSliceJQ, Data: dict})
		if err != nil {
			return nil, err
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

type env765b struct {
	dyn     *dynamicfake.FakeDynamicClient
	base    context.Context
	raKey   string
	raIn    cache.ResolvedKeyInputs
	store   *cache.ResolvedCacheStore
	resolve func(context.Context, int, int) (map[string]any, error)
	mode    *string
}

func backingObject765b() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps.krateo.io/v1", "kind": "Composition",
		"metadata": map[string]any{"name": backingObj765b, "namespace": edge3BackingNS, "annotations": map[string]any{"note": "n1"}},
	}}
}

// setup765b builds the watcher, pre-warms the raKey cell through the stub in
// mode, and records the sliceable verdict (the 4a fast path is then a HIT).
func setup765b(t *testing.T, mode string, in jqinterest.Interest) *env765b {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	gRA := gvr()
	seed := append(f6BuildFixture(), edge3RAUnstructured(edge3BackingNS, raName765b), runtime.Object(backingObject765b()))
	dyn := edge3NewWatcher(t, seed...)
	if added, syncCh := cache.Global().EnsureResourceType(gRA); added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gRA, 3*time.Second) {
		t.Fatalf("restactions GVR never became servable")
	}
	e := &env765b{dyn: dyn, store: cache.ResolvedCache(), mode: &mode}
	cache.Deps().SetStore(e.store)
	e.resolve = stub765b(t, e.mode, in)
	e.base = f6CtxWithUser(t, "admin", []string{"system:masters"})
	e.raIn = f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource, edge3BackingNS, raName765b, "C:crb-a-f6-uid", nil)
	e.raKey = cache.ComputeKey(e.raIn)
	full, err := e.resolve(cache.WithL1KeyContext(e.base, e.raKey), 0, 0)
	if err != nil {
		t.Fatalf("raKey pre-warm: %v", err)
	}
	e.store.PutRAFullList(cache.PutGateVerdict(cache.PutGateSinks{}, cache.CacheEntryClassRAFullList), e.raKey, e.raIn, full)
	cache.RecordSliceability(e.raKey, seedFullListShape(gRA, edge3BackingNS, raName765b, ra(raSliceJQ)), true)
	cache.Deps().Record(context.Background(), e.raKey, gRA, edge3BackingNS, raName765b)
	return e
}

func opts765b() ResolveOptions {
	return ResolveOptions{
		ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: raName765b, Namespace: edge3BackingNS},
			Resource:   "restactions",
			APIVersion: "templates.krateo.io/v1",
		},
		PerPage: 5, Page: 1,
	}
}

// seed resolves each widget key through the REAL apiref.Resolve sharing memo:
// the first is the memo MISS (a 4a fast-path HIT: a full_list replay), the
// rest are memo HITs (memo replays).
func (e *env765b) seed(t *testing.T, memo *cache.SeedResolveMemo, keys ...string) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, k := range keys {
		ctx := cache.WithL1KeyContext(e.base, k)
		if memo != nil {
			ctx = cache.WithSeedResolveMemo(ctx, memo)
		}
		body, err := Resolve(ctx, opts765b())
		if err != nil {
			t.Fatalf("Resolve(%s): %v", k, err)
		}
		out[k] = body
	}
	return out
}

func state765b(key string) string { return cache.ListInterestStateForTest(key, backingDK765b()) }

func replays765b() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range cache.ListNarrowReplaySnapshot() {
		out[c.Site+"/"+c.Outcome] = c.Count
	}
	return out
}

func delta765b(a, b map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range b {
		if d := v - a[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

var widgets765b = []string{"L1_w765b_a", "L1_w765b_b", "L1_w765b_c"}

func TestReplay765b_P1_C2_P4_SiblingsNarrowToTheProducersReadSet(t *testing.T) {
	in := analyze765b(`[.s.items[] | {name: .metadata.name}]`)
	if in.All {
		t.Fatalf("setup: the fixture chain must narrow, got %s", in)
	}
	e := setup765b(t, "narrow", in)
	want := state765b(e.raKey)
	if !strings.HasPrefix(want, "narrowed:") {
		t.Fatalf("setup: the raKey edge must be narrowed, got %q", want)
	}
	memo := cache.NewSeedResolveMemo()
	r0 := replays765b()
	hit0 := cache.RAFullListServeSnapshot().Hit
	e.seed(t, memo, widgets765b...)
	if got := cache.RAFullListServeSnapshot().Hit - hit0; got != 1 {
		t.Fatalf("[prove-path] want exactly one 4a HIT (the memo MISS), got %d", got)
	}
	if hits, misses := memo.Stats(); hits != 2 || misses != 1 {
		t.Fatalf("[prove-path] want 1 memo miss + 2 hits, got misses=%d hits=%d", misses, hits)
	}
	for _, w := range widgets765b {
		if got := state765b(w); got != want {
			t.Fatalf("P1/C2 RED: widget %s carries %q, want the producer's %q (a memo sibling or a full-list "+
				"slice must narrow exactly when the producer did)", w, got, want)
		}
	}
	d := delta765b(r0, replays765b())
	if d["full_list/narrowed"] != 1 || d["memo/narrowed"] != 2 || len(d) != 2 {
		t.Fatalf("P4: want full_list/narrowed=1 and memo/narrowed=2 replays, got %v", d)
	}
}

func TestReplay765b_P2_AnAllProducerLeavesEveryReceiverAll(t *testing.T) {
	for _, tc := range []struct{ name, filter string }{
		// `input` is ⊤ whatever the pending `now` ruling decides.
		{"input", `[.s.items[] | {name: .metadata.name, x: input}]`},
		// `now`: today ⊤ as well. This case FOLLOWS the pending ruling (option 3
		// would make it narrow with a clock trigger) — update it with that work.
		{"now_pending_ruling", `[.s.items[] | {name: .metadata.name, t: now}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := analyze765b(tc.filter)
			if !in.All {
				t.Fatalf("setup: %q must be All, got %s", tc.filter, in)
			}
			e := setup765b(t, "narrow", in)
			if got := state765b(e.raKey); got != "all" {
				t.Fatalf("setup: the raKey edge must be all, got %q", got)
			}
			r0 := replays765b()
			e.seed(t, cache.NewSeedResolveMemo(), widgets765b...)
			for _, w := range widgets765b {
				if got := state765b(w); got != "all" {
					t.Fatalf("P2 RED (%s): widget %s is %q although its producer is all", tc.name, w, got)
				}
			}
			if d := delta765b(r0, replays765b()); d["full_list/all"] != 1 || d["memo/all"] != 2 {
				t.Fatalf("P2: want the plain replays counted as all per site, got %v", d)
			}
		})
	}
}

func TestReplay765b_P3_APendingProducerReplaysPlainAndIsCounted(t *testing.T) {
	e := setup765b(t, "pending", jqinterest.Interest{})
	if got := state765b(e.raKey); got != "pending" {
		t.Fatalf("setup: the raKey edge must be pending, got %q", got)
	}
	r0 := replays765b()
	e.seed(t, cache.NewSeedResolveMemo(), widgets765b...)
	for _, w := range widgets765b {
		if got := state765b(w); got != "all" {
			t.Fatalf("P3 RED: widget %s is %q although its producer edge was pending", w, got)
		}
	}
	if d := delta765b(r0, replays765b()); d["full_list/pending"] != 1 {
		t.Fatalf("P3: the lossy pending fall-back must be counted (full_list/pending=1), got %v", d)
	}
}

func TestReplay765b_P5_AStickyAllReceiverStaysAll(t *testing.T) {
	e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
	sticky := widgets765b[2]
	dk := backingDK765b()
	cache.Deps().RecordList(context.Background(), sticky, dk.GVR, dk.Namespace) // a plain record first
	e.seed(t, cache.NewSeedResolveMemo(), widgets765b...)
	if got := state765b(sticky); got != "all" {
		t.Fatalf("P5 RED: a receiver that was already sticky all became %q: a replay must never narrow it", got)
	}
	if got := state765b(widgets765b[1]); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("P5 setup: the other sibling must still narrow, got %q", got)
	}
}

// P5b (architect-623) — the receiver's edge is a NARROWABLE edge that its OWN
// failed resolve widened (an *edgeInterest at all, not P5's plain struct{}).
// A narrowed replay from a clean producer must leave it all: the widening is
// sticky on both edge representations.
func TestReplay765b_P5b_AReceiverWidenedByItsOwnFailedResolveStaysAll(t *testing.T) {
	e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
	widened := widgets765b[2]
	dk := backingDK765b()
	// Its own resolve: a narrowable edge-3 record, then a stage error widens it.
	cache.Deps().RecordListNarrowable(context.Background(), widened, dk.GVR, dk.Namespace)
	cache.Deps().WidenListInterest(widened, dk, cache.CommitWhyStageError)
	if got := state765b(widened); got != "all" {
		t.Fatalf("P5b setup: the receiver's own failed resolve must leave its edge all, got %q", got)
	}
	e.seed(t, cache.NewSeedResolveMemo(), widgets765b...)
	if got := state765b(widened); got != "all" {
		t.Fatalf("P5b RED: a receiver widened by its own failed resolve became %q under a narrowed replay: "+
			"the widening must stay sticky", got)
	}
	if got := state765b(widgets765b[1]); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("P5b setup: the other sibling must still narrow, got %q", got)
	}
}

func TestReplay765b_P6_ForwardMatchesAreIdenticalShadowOnAndOff(t *testing.T) {
	run := func(shadow bool) []string {
		restore := cache.SetListNarrowShadowForTest(shadow)
		defer restore()
		e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
		e.seed(t, cache.NewSeedResolveMemo(), widgets765b...)
		var keys []string
		for k := range cache.Deps().CollectMatchesForTest(edge3BackingGVR(), edge3BackingNS, "any-object") {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return keys
	}
	on, off := run(true), run(false)
	if strings.Join(on, ",") != strings.Join(off, ",") {
		t.Fatalf("P6 RED: carrying states changed WHAT AN EVENT MARKS:\n on:  %v\n off: %v", on, off)
	}
	if len(on) < len(widgets765b)+1 {
		t.Fatalf("P6 setup: want the raKey and every widget matched, got %v", on)
	}
}

func TestReplay765b_P7_ACaptureOnAReceiverSeesTheReplayedEdge(t *testing.T) {
	e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
	memo := cache.NewSeedResolveMemo()
	e.seed(t, memo, widgets765b[0]) // the producer (memo MISS)
	sib := widgets765b[1]
	h := cache.Deps().BeginCapture(sib)
	e.seed(t, memo, sib) // a memo HIT: replay under sib
	got := cache.Deps().EndCapture(sib, h)
	for _, dk := range got {
		if dk == backingDK765b() {
			return
		}
	}
	t.Fatalf("P7 RED: a capture open on the receiver did not see the replayed LIST edge ([C1-A]): %v", got)
}

func TestReplay765b_P8b_AWidgetSlicingADirtyV2IsAll(t *testing.T) {
	e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
	e.seed(t, nil, widgets765b[0]) // V1: a 4a HIT replays narrowed
	if got := state765b(widgets765b[0]); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("P8b setup: V1 slice must narrow, got %q", got)
	}
	// V2: the raKey is re-resolved with a stage error (its commit widens the
	// edge BEFORE the resolve returns) and then Put.
	*e.mode = "all"
	full, err := e.resolve(cache.WithL1KeyContext(e.base, e.raKey), 0, 0)
	if err != nil {
		t.Fatalf("V2 resolve: %v", err)
	}
	e.store.PutRAFullList(cache.PutGateVerdict(cache.PutGateSinks{}, cache.CacheEntryClassRAFullList), e.raKey, e.raIn, full)
	if got := state765b(e.raKey); got != "all" {
		t.Fatalf("P8b setup: V2 must leave the raKey edge all, got %q", got)
	}
	// A widget slicing V2.
	late := "L1_w765b_late"
	e.seed(t, nil, late)
	if got := state765b(late); got != "all" {
		t.Fatalf("P8b RED: a widget slicing the dirty V2 carries %q: the replay must read the raKey edge's "+
			"LIVE state, which V2's commit already widened", got)
	}
	// The V1 widget re-slices V2: its earlier narrowed state must widen (a plain
	// replay is a widening record, never a no-op on a narrowed receiver).
	e.seed(t, nil, widgets765b[0])
	if got := state765b(widgets765b[0]); got != "all" {
		t.Fatalf("P8b RED: the widget narrowed at V1 still carries %q after slicing the dirty V2", got)
	}
}

func TestReplay765b_P9_TheAuditCoversAReceiverWidget(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	e := setup765b(t, "narrow", analyze765b(`[.s.items[] | {name: .metadata.name}]`))
	memo := cache.NewSeedResolveMemo()
	bodies := e.seed(t, memo, widgets765b[0], widgets765b[1]) // [1] is a memo sibling
	sib := widgets765b[1]
	if got := state765b(sib); !strings.HasPrefix(got, "narrowed:") {
		t.Fatalf("P9 setup: the sibling must be narrowed, got %q", got)
	}
	sibIn := edge3WidgetInputs("w765b-sib")
	sibBody, _ := json.Marshal(bodies[sib])
	e.store.Put(sib, &cache.ResolvedEntry{RawJSON: sibBody, Inputs: sibIn})

	// The refresher terminal, as resolve_populate.go does it; `force` makes the
	// rebuild differ.
	var force atomic.Bool
	var calls sync.Map
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, key string, in cache.ResolvedKeyInputs) error {
		if key != sib {
			return nil
		}
		v, _ := calls.LoadOrStore(key, new(atomic.Int64))
		defer v.(*atomic.Int64).Add(1)
		ctx = cache.WithL1KeyContext(ctx, key)
		gen0 := e.store.CaptureGen(key)
		enc := sibBody
		if force.Load() {
			enc = []byte(`{"forced":"difference"}`)
		}
		prior, had := e.store.GetNoTouch(key)
		stored := e.store.ReplaceIfGenRefresh(ctx, key, &cache.ResolvedEntry{RawJSON: enc, Inputs: &in}, gen0)
		cache.NoteRefreshWrite(ctx, key, stored)
		cache.NoteNarrowAuditWrite(ctx, key, prior, had, enc, stored)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache.StartRefresher(ctx)
	if !edge3WaitServable(t, edge3BackingGVR(), 3*time.Second) {
		t.Fatalf("backing GVR never became servable")
	}
	refreshed := func() int64 {
		v, _ := calls.LoadOrStore(sib, new(atomic.Int64))
		return v.(*atomic.Int64).Load()
	}
	annotate := func(v string) {
		t.Helper()
		obj, err := e.dyn.Resource(edge3BackingGVR()).Namespace(edge3BackingNS).Get(context.Background(), backingObj765b, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get backing: %v", err)
		}
		_ = unstructured.SetNestedField(obj.Object, v, "metadata", "annotations", "note")
		if _, err := e.dyn.Resource(edge3BackingGVR()).Namespace(edge3BackingNS).Update(context.Background(), obj, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update backing: %v", err)
		}
	}
	waitRefresh := func(n int64) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for refreshed() < n && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if refreshed() < n {
			t.Fatalf("the sibling was refreshed %d times, want %d", refreshed(), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	s0, a0 := cache.ListNarrowAuditSamples(), cache.ListNarrowAuditSnapshot()

	annotate("n2") // an unread field: the sibling is a would-avoid
	waitRefresh(1)
	s1, a1 := cache.ListNarrowAuditSamples(), cache.ListNarrowAuditSnapshot()
	if s1-s0 < 1 || a1[cache.AuditVerdictIdentical]-a0[cache.AuditVerdictIdentical] < 1 {
		t.Fatalf("P9 RED: the receiver widget's avoid-only refresh was not audited (samples +%d, identical +%d)",
			s1-s0, a1[cache.AuditVerdictIdentical]-a0[cache.AuditVerdictIdentical])
	}
	force.Store(true)
	annotate("n3")
	waitRefresh(2)
	if a2 := cache.ListNarrowAuditSnapshot(); a2[cache.AuditVerdictChanged]-a1[cache.AuditVerdictChanged] != 1 {
		t.Fatalf("P9 RED: a forced byte difference on a receiver widget's avoid-only refresh must be ONE "+
			"would_change, got %d", a2[cache.AuditVerdictChanged]-a1[cache.AuditVerdictChanged])
	}
}
