// list_narrow_shadow_765_test.go — #603 PR-2 / #765 option B SHADOW arms.
//
// Every arm drives the REAL informer handlers (a real synced informer over a
// fake dynamic client: updates go through the informer, which hands the
// handler the old AND the new object), the real dep-event worker, the real
// DepTracker and, where the audit is under test, the real refresher.
//
//	S0  shadow on ≡ off: the same events give the same enqueue sequence, the
//	    same dirty-mark counters and the same served bodies.
//	E1  would_avoid counts an annotation-only UPDATE on a cell whose read set
//	    is {metadata.name, spec.phase} — and the cell is STILL marked.
//	E1b an UPDATE of a read field is kept{read_field}.
//	S1  would_change fires: a cell whose interest is deliberately too narrow
//	    (the "drop one path from R" seam) gets an UPDATE of the dropped path:
//	    avoided, the refresh rebuilds different bytes, audit{changed} = 1. The
//	    correctly narrowed sibling stays out of the audit (it was kept), and an
//	    annotation-only UPDATE audits identical for both.
//	S2  undecidable ⇒ kept, never avoided: plain edge, pending edge, a plain
//	    re-record (replay), a widen, a reconcile submit, a missing mask, an
//	    undecodable object, a late interest, ADD, DELETE, a degraded probe.
//	S3  a refresh that reaches no accepted Put restores its taint, so a later
//	    avoid-only refresh is never compared against bytes older than a kept
//	    mark (no false would_change).
//	S4  A→B→A on a read field, coalesced or not, is never a would-avoid: the
//	    compare runs per transition in the handler, against the pre-image.

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache/jqinterest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const narrowNS = "krateo-system"

// narrowReads analyses one stage filter over the tracked stage "s".
func narrowReads(t *testing.T, filter string) []jqinterest.Read {
	t.Helper()
	in := jqinterest.Analyze(jqinterest.Chain{
		RA:      jqinterest.RESTAction{Stages: []jqinterest.Stage{{Name: "s", Path: "/apis/g/v1/namespaces/ns/flexes", Filter: &filter}}},
		Tracked: "s",
	}, nil)
	if in.All || in.ReadsEverything() {
		t.Fatalf("setup: %q must narrow, got %s", filter, in)
	}
	return in.Reads
}

const (
	filterNamePhase = `[.s.items[] | {name: .metadata.name, phase: .spec.phase}]`
	filterNameOnly  = `[.s.items[] | {name: .metadata.name}]`
)

func narrowObj(gvr schema.GroupVersionResource, name, phase, ann string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "Flex",
		"metadata": map[string]any{
			"namespace":   narrowNS,
			"name":        name,
			"annotations": map[string]any{"note": ann},
		},
		"spec": map[string]any{"phase": phase},
	}}
}

func narrowCreate(t *testing.T, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, u *unstructured.Unstructured) {
	t.Helper()
	if _, err := dyn.Resource(gvr).Namespace(narrowNS).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	waitIndexer(t, rw, gvr, narrowNS, u.GetName(), true)
}

func narrowUpdate(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, name string, mutate func(u *unstructured.Unstructured)) {
	t.Helper()
	u, err := dyn.Resource(gvr).Namespace(narrowNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	mutate(u)
	if _, err := dyn.Resource(gvr).Namespace(narrowNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
}

func setAnn(v string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, v, "metadata", "annotations", "note")
	}
}

func setPhase(v string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(u.Object, v, "spec", "phase")
	}
}

// narrowCounts is the {decision/reason} counter as a map.
func narrowCounts() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range ListNarrowDecisionsSnapshot() {
		out[c.Decision+"/"+c.Reason] = c.Count
	}
	return out
}

func narrowTotal() uint64 {
	var n uint64
	for _, c := range ListNarrowDecisionsSnapshot() {
		n += c.Count
	}
	return n
}

func listDepObjectMarks() uint64 {
	var n uint64
	for _, b := range DirtyMarkAttributionSnapshot() {
		if b.Path == dmPathObject && b.Class == dmClassListDep {
			n += b.Count
		}
	}
	return n
}

// waitNarrowTotal waits until the decision counter reaches want, then lets the
// queue drain.
func waitNarrowTotal(t *testing.T, want uint64) {
	t.Helper()
	deadline := time.Now().Add(harnessWaitBound)
	for time.Now().Before(deadline) {
		if narrowTotal() >= want {
			waitQueueIdle(t, harnessWaitBound)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("decision counter never reached %d (at %d): the event did not reach the worker", want, narrowTotal())
}

func narrowDK(gvr schema.GroupVersionResource) DepKey {
	return DepKey{GVR: gvr, Namespace: narrowNS, Name: listWildcard}
}

// narrowKey records a narrowable list edge for key and commits reads.
func narrowKey(t *testing.T, d *DepTracker, key string, gvr schema.GroupVersionResource, reads []jqinterest.Read) {
	t.Helper()
	d.RecordListNarrowable(context.Background(), key, gvr, narrowNS)
	if reads != nil {
		if v := d.CommitListInterest(key, narrowDK(gvr), reads); v != CommitVerdictNarrowed && ListNarrowShadowOn() {
			t.Fatalf("setup: commit verdict %q", v)
		}
	}
}

// markRecorder is a refresh hook that records every enqueue, in order.
type markRecorder struct {
	mu   sync.Mutex
	keys []string
}

func (m *markRecorder) hook(k string, _ schema.GroupVersionResource) {
	m.mu.Lock()
	m.keys = append(m.keys, k)
	m.mu.Unlock()
}

func (m *markRecorder) take() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.keys
	m.keys = nil
	return out
}

// --- E1 / E1b ------------------------------------------------------------------

func TestListNarrow765_E1_WouldAvoidCountsAndStillMarks(t *testing.T) {
	defer withCleanDepWatch(t)()
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	d := Deps()
	rec := &markRecorder{}
	d.SetRefreshHook(rec.hook)

	const key = "L1_narrow_e1"
	narrowKey(t, d, key, gvr, narrowReads(t, filterNamePhase))
	narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "a", "p1", "n1"))
	waitNarrowTotal(t, 1) // the ADD: kept{membership}
	if got := narrowCounts()["kept/membership"]; got != 1 {
		t.Fatalf("E1: the ADD must be kept{membership}, got %v", narrowCounts())
	}
	rec.take()

	before := narrowCounts()
	narrowUpdate(t, dyn, gvr, "a", setAnn("n2"))
	waitNarrowTotal(t, 2)
	after := narrowCounts()
	if got := after["avoided/unread_field"] - before["avoided/unread_field"]; got != 1 {
		t.Fatalf("E1 RED: an annotation-only UPDATE on a cell reading {metadata.name, spec.phase} must be ONE "+
			"would-avoid, got %d (counts %v)", got, after)
	}
	if marks := rec.take(); !reflect.DeepEqual(marks, []string{key}) {
		t.Fatalf("E1: SHADOW must still mark the cell exactly as today: enqueued %v, want [%s]", marks, key)
	}

	narrowUpdate(t, dyn, gvr, "a", setPhase("p2"))
	waitNarrowTotal(t, 3)
	final := narrowCounts()
	if got := final["kept/read_field"] - after["kept/read_field"]; got != 1 {
		t.Fatalf("E1b RED: an UPDATE of spec.phase (read by the cell) must be kept{read_field}, got %v", final)
	}
	if final["avoided/unread_field"] != after["avoided/unread_field"] {
		t.Fatalf("E1b RED: an UPDATE of a read field was counted as a would-avoid: %v", final)
	}
	if marks := rec.take(); !reflect.DeepEqual(marks, []string{key}) {
		t.Fatalf("E1b: enqueued %v, want [%s]", marks, key)
	}
	// Σ decisions == object-path list_dep dirty marks, by construction.
	if got, want := narrowTotal(), listDepObjectMarks(); got != want {
		t.Fatalf("E1: Σ list_narrow decisions = %d, object-path list_dep marks = %d: they must be equal", got, want)
	}
}

// --- S2: undecidable ⇒ kept ------------------------------------------------------

func TestListNarrow765_S2_UndecidableIsKeptNeverAvoided(t *testing.T) {
	defer withCleanDepWatch(t)()
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	d := Deps()
	rec := &markRecorder{}
	d.SetRefreshHook(rec.hook)
	reads := narrowReads(t, filterNamePhase)

	narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "a", "p1", "n1"))
	waitQueueIdle(t, harnessWaitBound)

	const (
		kPlain    = "L1_s2_plain"
		kPending  = "L1_s2_pending"
		kReplayed = "L1_s2_replayed"
		kWidened  = "L1_s2_widened"
		kOK       = "L1_s2_ok"
	)
	d.RecordList(context.Background(), kPlain, gvr, narrowNS)
	narrowKey(t, d, kPending, gvr, nil)
	narrowKey(t, d, kReplayed, gvr, reads)
	d.ReplayEdges(context.Background(), kReplayed, []DepKey{narrowDK(gvr)})
	narrowKey(t, d, kWidened, gvr, reads)
	d.WidenListInterest(kWidened, narrowDK(gvr), CommitWhyStageError)
	narrowKey(t, d, kOK, gvr, reads)

	// An annotation-only UPDATE: only kOK may be a would-avoid.
	before := narrowTotal()
	narrowUpdate(t, dyn, gvr, "a", setAnn("n2"))
	waitNarrowTotal(t, before+5)
	c := narrowCounts()
	if c["avoided/unread_field"] != 1 {
		t.Fatalf("S2 RED: only the narrowed cell may be a would-avoid, got %v", c)
	}
	if c["kept/interest_all"] != 3 || c["kept/interest_pending"] != 1 {
		t.Fatalf("S2 RED: plain, replayed and widened edges must be kept{interest_all} (3) and the pending "+
			"one kept{interest_pending} (1), got %v", c)
	}
	sortedTake := func() []string { k := rec.take(); sort.Strings(k); return k }
	if marks := sortedTake(); len(marks) != 5 {
		t.Fatalf("S2: every cell must still be marked, got %v", marks)
	}

	keptOnly := func(name string, reason string, fire func()) {
		t.Helper()
		b := narrowCounts()
		n0 := narrowTotal()
		fire()
		waitNarrowTotal(t, n0+5)
		a := narrowCounts()
		if a["avoided/unread_field"] != b["avoided/unread_field"] {
			t.Fatalf("S2 RED (%s): a would-avoid was counted; unknown must fall back to would-mark (%v)", name, a)
		}
		if got := a["kept/"+reason] - b["kept/"+reason]; got != 1 {
			t.Fatalf("S2 RED (%s): want the narrowed cell kept{%s}, got delta %d (%v)", name, reason, got, a)
		}
		if marks := sortedTake(); len(marks) != 5 {
			t.Fatalf("S2 (%s): every cell must still be marked, got %v", name, marks)
		}
	}
	w := depWatchSingleton()
	k := depEventKey{gvr: gvr, namespace: narrowNS, name: "a"}
	keptOnly("reconcile submit", "undecided", func() { w.submitDepEvent(rw, k, dmSourceReconcile) })
	keptOnly("no mask", "no_mask", func() { d.OnObjectEvent(gvr, narrowNS, "a", objExists) })
	keptOnly("late interest", "registry_late", func() {
		d.onObjectEventMasked(gvr, narrowNS, "a", objExists, &eventMask{gen: 0})
	})
	keptOnly("degraded", "degraded", func() {
		d.onObjectEventMasked(gvr, narrowNS, "a", objUnknownDegraded, &eventMask{gen: 1 << 40})
	})
	keptOnly("absent without its DELETE transition", "membership", func() {
		d.onObjectEventMasked(gvr, narrowNS, "a", objAbsent, &eventMask{gen: 1 << 40})
	})
	keptOnly("ADD", "membership", func() { narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "b", "p1", "n1")) })
	keptOnly("DELETE", "membership", func() {
		if err := dyn.Resource(gvr).Namespace(narrowNS).Delete(context.Background(), "b", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
	})
	if m := d.updateMask(gvr, "not-an-object", 42); !m.unknown {
		t.Fatalf("S2 RED: an undecodable transition must give an unknown mask, got %+v", m)
	}
}

// --- S4: A→B→A ------------------------------------------------------------------

func TestListNarrow765_S4_FlipBackOnAReadFieldIsNeverAvoided(t *testing.T) {
	defer withCleanDepWatch(t)()
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	d := Deps()
	rec := &markRecorder{}
	d.SetRefreshHook(rec.hook)
	narrowKey(t, d, "L1_s4", gvr, narrowReads(t, filterNamePhase))
	narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "a", "p1", "n1"))
	waitNarrowTotal(t, 1)

	// Drive the handler directly with both transitions back to back (the
	// queue may coalesce them; the worker then sees only A).
	h := rw.depEventHandlers(gvr)
	a := narrowObj(gvr, "a", "p1", "n1")
	b := narrowObj(gvr, "a", "p2", "n1")
	before := narrowCounts()
	n0 := narrowTotal()
	h.UpdateFunc(a, b)
	h.UpdateFunc(b, a)
	waitNarrowTotal(t, n0+1)
	time.Sleep(50 * time.Millisecond)
	waitQueueIdle(t, harnessWaitBound)
	after := narrowCounts()
	if after["avoided/unread_field"] != before["avoided/unread_field"] {
		t.Fatalf("S4 RED: an A→B→A flip of a READ field was a would-avoid (%v). The compare must run per "+
			"transition against the pre-image, before the queue coalesces.", after)
	}
	if after["kept/read_field"] == before["kept/read_field"] {
		t.Fatalf("S4 RED: the flip was never kept{read_field}: %v", after)
	}
}

// --- S0: shadow on ≡ off ----------------------------------------------------------

type narrowRun765 struct {
	marks   [][]string
	bodies  map[string]string
	stats   DepStats
	attr    []DirtyMarkBucket
	byClass map[string]uint64
}

// runNarrowScenario drives one fixed event script and returns everything an
// observer of marking and serving can see. The "serve" of a marked cell is a
// deterministic rebuild from the informer's current list.
func runNarrowScenario(t *testing.T, shadow bool) narrowRun765 {
	t.Helper()
	defer SetListNarrowShadowForTest(shadow)()
	defer withCleanDepWatch(t)()
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	d := Deps()
	rec := &markRecorder{}
	d.SetRefreshHook(rec.hook)
	keys := []string{"L1_s0_namephase", "L1_s0_nameonly", "L1_s0_plain"}
	narrowKey(t, d, keys[0], gvr, narrowReads(t, filterNamePhase))
	narrowKey(t, d, keys[1], gvr, narrowReads(t, filterNameOnly))
	d.RecordList(context.Background(), keys[2], gvr, narrowNS)

	rebuild := func(key string) string {
		ul, err := dyn.Resource(gvr).Namespace(narrowNS).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var rows []string
		for _, it := range ul.Items {
			ph, _, _ := unstructured.NestedString(it.Object, "spec", "phase")
			an, _, _ := unstructured.NestedString(it.Object, "metadata", "annotations", "note")
			switch key {
			case keys[0]:
				rows = append(rows, it.GetName()+"="+ph)
			case keys[1]:
				rows = append(rows, it.GetName())
			default:
				rows = append(rows, it.GetName()+"="+ph+"/"+an)
			}
		}
		sort.Strings(rows)
		b, _ := json.Marshal(rows)
		return string(b)
	}
	out := narrowRun765{bodies: map[string]string{}}
	step := func(fire func()) {
		t.Helper()
		before := d.Stats().DirtyMarkTotal
		fire()
		deadline := time.Now().Add(harnessWaitBound)
		for d.Stats().DirtyMarkTotal == before && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		waitQueueIdle(t, harnessWaitBound)
		m := rec.take()
		sort.Strings(m)
		out.marks = append(out.marks, m)
		for _, k := range m {
			out.bodies[k] = rebuild(k)
		}
	}
	step(func() { narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "a", "p1", "n1")) })
	step(func() { narrowUpdate(t, dyn, gvr, "a", setAnn("n2")) })
	step(func() { narrowUpdate(t, dyn, gvr, "a", setPhase("p2")) })
	step(func() { narrowCreate(t, rw, dyn, gvr, narrowObj(gvr, "b", "p1", "n1")) })
	step(func() { narrowUpdate(t, dyn, gvr, "b", setAnn("n3")) })
	step(func() {
		if err := dyn.Resource(gvr).Namespace(narrowNS).Delete(context.Background(), "a", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete: %v", err)
		}
	})
	out.stats = d.Stats()
	out.attr = DirtyMarkAttributionSnapshot()
	out.byClass = DirtyMarkByEntryClassSnapshot()
	if shadow && narrowTotal() == 0 {
		t.Fatalf("S0 setup: the shadow ran but classified nothing (a vacuous comparison)")
	}
	if shadow && narrowCounts()["avoided/unread_field"] == 0 {
		t.Fatalf("S0 setup: the script must contain would-avoids for the comparison to mean anything")
	}
	if !shadow && narrowTotal() != 0 {
		t.Fatalf("S0 setup: the shadow is off but classified %d marks", narrowTotal())
	}
	return out
}

func TestListNarrow765_S0_ShadowOnIsByteIdenticalToOff(t *testing.T) {
	on := runNarrowScenario(t, true)
	off := runNarrowScenario(t, false)
	if !reflect.DeepEqual(on.marks, off.marks) {
		t.Fatalf("S0 RED: the shadow changed WHAT IS MARKED.\n on:  %v\n off: %v", on.marks, off.marks)
	}
	if !reflect.DeepEqual(on.bodies, off.bodies) {
		t.Fatalf("S0 RED: the shadow changed WHAT IS SERVED.\n on:  %v\n off: %v", on.bodies, off.bodies)
	}
	strip := func(s DepStats) DepStats { return s }
	if strip(on.stats).DirtyMarkTotal != strip(off.stats).DirtyMarkTotal ||
		on.stats.EvictDeleteTotal != off.stats.EvictDeleteTotal ||
		on.stats.TotalRecords != off.stats.TotalRecords {
		t.Fatalf("S0 RED: dep counters differ: on %+v off %+v", on.stats, off.stats)
	}
	if !reflect.DeepEqual(on.attr, off.attr) || !reflect.DeepEqual(on.byClass, off.byClass) {
		t.Fatalf("S0 RED: dirty-mark attribution differs:\n on:  %v %v\n off: %v %v", on.attr, on.byClass, off.attr, off.byClass)
	}
}

// --- S1 / S3: the audit through the REAL refresher --------------------------------

type auditHarness struct {
	gvr   schema.GroupVersionResource
	rw    *ResourceWatcher
	dyn   dynamic.Interface
	store *ResolvedCacheStore
	d     *DepTracker
	calls sync.Map // key -> *atomic.Int64
	// decline, when set for a key, makes its next refresh return without a Put.
	decline sync.Map
}

func (h *auditHarness) calls_(key string) int64 {
	v, _ := h.calls.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64).Load()
}

// body is the cell's rebuild: {name, phase} of every object. Deterministic.
func (h *auditHarness) body(t *testing.T) []byte {
	ul, err := h.dyn.Resource(h.gvr).Namespace(narrowNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Errorf("list: %v", err)
		return nil
	}
	var rows []string
	for _, it := range ul.Items {
		ph, _, _ := unstructured.NestedString(it.Object, "spec", "phase")
		rows = append(rows, it.GetName()+"="+ph)
	}
	sort.Strings(rows)
	b, _ := json.Marshal(rows)
	return b
}

func newAuditHarness(t *testing.T) *auditHarness {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)
	t.Cleanup(withCleanDepWatch(t))
	h := &auditHarness{gvr: gvrFlexes()}
	h.rw, h.dyn = realWatcher(t, h.gvr)
	h.store = ResolvedCache()
	if h.store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	h.d = Deps()
	h.d.SetStore(h.store)
	// The refresher terminal, exactly as resolve_populate.go does it: read the
	// prior, rebuild, gen-guarded replace, report the write.
	RegisterRefreshFunc("widgets", func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		v, _ := h.calls.LoadOrStore(key, new(atomic.Int64))
		defer v.(*atomic.Int64).Add(1)
		if _, dec := h.decline.LoadAndDelete(key); dec {
			return nil // a declined Put (e.g. a stage-error decline)
		}
		// The resolve-terminal-Put entry (resolve_populate.go): the L1 key and
		// its #375 dep-gen sink.
		ctx = WithL1KeyContext(ctx, key)
		gen0 := h.store.CaptureGen(key)
		encoded := h.body(t)
		prior, had := h.store.GetNoTouch(key)
		stored := h.store.ReplaceIfGenRefresh(ctx, key, &ResolvedEntry{RawJSON: encoded, Inputs: &in}, gen0)
		NoteRefreshWrite(ctx, key, stored)
		NoteNarrowAuditWrite(ctx, key, prior, had, encoded, stored)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	return h
}

// cell puts key resident with the current body and a narrowed list edge.
func (h *auditHarness) cell(t *testing.T, key string, filter string) {
	t.Helper()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Group: h.gvr.Group, Version: h.gvr.Version,
		Resource: h.gvr.Resource, Namespace: narrowNS, Name: key}
	h.store.Put(key, &ResolvedEntry{RawJSON: h.body(t), Inputs: &in})
	narrowKey(t, h.d, key, h.gvr, narrowReads(t, filter))
}

// waitRefreshed waits until key has been refreshed n times in total.
func (h *auditHarness) waitRefreshed(t *testing.T, key string, n int64) {
	t.Helper()
	deadline := time.Now().Add(harnessWaitBound)
	for time.Now().Before(deadline) {
		if h.calls_(key) >= n {
			time.Sleep(20 * time.Millisecond)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s was refreshed %d times, want %d", key, h.calls_(key), n)
}

func TestListNarrow765_S1_WouldChangeFiresWhenASkippedCellReallyChanged(t *testing.T) {
	h := newAuditHarness(t)
	const tooNarrow, ok = "L1_s1_too_narrow", "L1_s1_ok"
	narrowCreate(t, h.rw, h.dyn, h.gvr, narrowObj(h.gvr, "a", "p1", "n1"))
	waitQueueIdle(t, harnessWaitBound)
	// tooNarrow's body shows spec.phase but its interest was cut to
	// {metadata.name}: the "drop one path from R" seam.
	h.cell(t, tooNarrow, filterNameOnly)
	h.cell(t, ok, filterNamePhase)
	a0 := ListNarrowAuditSnapshot()

	narrowUpdate(t, h.dyn, h.gvr, "a", setPhase("p2"))
	h.waitRefreshed(t, tooNarrow, 1)
	h.waitRefreshed(t, ok, 1)
	a1 := ListNarrowAuditSnapshot()
	if got := a1[AuditVerdictChanged] - a0[AuditVerdictChanged]; got != 1 {
		t.Fatalf("S1 RED: the too-narrow cell was a would-avoid and its refresh CHANGED its bytes: "+
			"would_change must be exactly 1, got %d (audit %v)", got, a1)
	}
	if got := a1[AuditVerdictIdentical] - a0[AuditVerdictIdentical]; got != 0 {
		t.Fatalf("S1: the correctly narrowed cell was KEPT (it reads spec.phase), so it is not audited; "+
			"identical moved by %d (audit %v)", got, a1)
	}
	if b, _ := h.store.GetNoTouch(tooNarrow); b == nil || string(b.RawJSON) != `["a=p2"]` {
		t.Fatalf("S1: SHADOW must still have refreshed the would-avoid cell, served %v", b)
	}

	// An annotation-only UPDATE: both cells avoided, both rebuilds identical.
	narrowUpdate(t, h.dyn, h.gvr, "a", setAnn("n2"))
	h.waitRefreshed(t, tooNarrow, 2)
	h.waitRefreshed(t, ok, 2)
	a2 := ListNarrowAuditSnapshot()
	if got := a2[AuditVerdictIdentical] - a1[AuditVerdictIdentical]; got != 2 {
		t.Fatalf("S1: two avoid-only refreshes of unchanged bytes must audit identical twice, got %d (%v)", got, a2)
	}
	if a2[AuditVerdictChanged] != a1[AuditVerdictChanged] {
		t.Fatalf("S1: an identical rebuild was counted as a would_change (%v)", a2)
	}
	if fmt.Sprint(ListNarrowExpvar().(map[string]any)["would_change_total"]) != fmt.Sprint(a2[AuditVerdictChanged]) {
		t.Fatalf("S1: expvar would_change_total diverges from the audit snapshot")
	}
}

func TestListNarrow765_S3_RefreshWithoutAcceptedPutRestoresTheTaint(t *testing.T) {
	h := newAuditHarness(t)
	const key = "L1_s3"
	narrowCreate(t, h.rw, h.dyn, h.gvr, narrowObj(h.gvr, "a", "p1", "n1"))
	waitQueueIdle(t, harnessWaitBound)
	h.cell(t, key, filterNamePhase)
	a0 := ListNarrowAuditSnapshot()

	// A read-field UPDATE (kept: taints the key), whose refresh is DECLINED:
	// the cell keeps its pre-change bytes.
	h.decline.Store(key, true)
	narrowUpdate(t, h.dyn, h.gvr, "a", setPhase("p2"))
	h.waitRefreshed(t, key, 1)
	if b, _ := h.store.GetNoTouch(key); b == nil || string(b.RawJSON) != `["a=p1"]` {
		t.Fatalf("S3 setup: the declined refresh must leave the pre-change bytes, served %v", b)
	}
	// Then an annotation-only UPDATE (avoided). Its refresh rebuilds the
	// phase change the declined one never stored: the bytes change, but the
	// window is NOT avoid-only (the taint was restored), so it is no
	// would_change.
	narrowUpdate(t, h.dyn, h.gvr, "a", setAnn("n2"))
	h.waitRefreshed(t, key, 2)
	a1 := ListNarrowAuditSnapshot()
	if got := a1[AuditVerdictChanged] - a0[AuditVerdictChanged]; got != 0 {
		t.Fatalf("S3 RED: a false would_change (%d). A refresh that reached no accepted Put must put its taint "+
			"back, or the next avoid-only refresh is compared against bytes older than a kept mark (%v)", got, a1)
	}
	if b, _ := h.store.GetNoTouch(key); b == nil || string(b.RawJSON) != `["a=p2"]` {
		t.Fatalf("S3: the second refresh must store the current bytes, served %v", b)
	}
}

// --- G1: the gate readout (#765 follow-up) -----------------------------------
//
// would_avoid counts MARKS; the audit counts REFRESHES. The readout keeps them
// apart, carries the audit's own denominator (avoid-only refreshes) and the
// unaudited coverage share over REFRESHES, never over marks. A narrowed cell
// that is NOT resident takes would-avoid marks but no refresh: it is what
// makes the two units differ here.
func TestListNarrow765_G1_GateReadoutSeparatesMarksFromRefreshes(t *testing.T) {
	h := newAuditHarness(t)
	const resident, ghost = "L1_g1_resident", "L1_g1_ghost"
	narrowCreate(t, h.rw, h.dyn, h.gvr, narrowObj(h.gvr, "a", "p1", "n1"))
	waitQueueIdle(t, harnessWaitBound)
	h.cell(t, resident, filterNamePhase)
	narrowKey(t, h.d, ghost, h.gvr, narrowReads(t, filterNamePhase)) // an edge, no resident cell
	g0 := ListNarrowGateSnapshot()

	// 1) annotation-only UPDATE whose refresh is DECLINED: one avoid-only
	//    refresh, unaudited. 2) another, accepted: identical.
	h.decline.Store(resident, true)
	narrowUpdate(t, h.dyn, h.gvr, "a", setAnn("n2"))
	h.waitRefreshed(t, resident, 1)
	narrowUpdate(t, h.dyn, h.gvr, "a", setAnn("n3"))
	h.waitRefreshed(t, resident, 2)

	g := ListNarrowGateSnapshot()
	if got := g.WouldAvoidMarks - g0.WouldAvoidMarks; got != 4 {
		t.Fatalf("G1: 2 UPDATEs x 2 narrowed cells = 4 would-avoid MARKS, got %d (%+v)", got, g)
	}
	if got := g.AvoidOnlyRefreshes - g0.AvoidOnlyRefreshes; got != 2 {
		t.Fatalf("G1 RED: the resident cell's 2 avoid-only REFRESHES must be the audit denominator "+
			"(the ghost has marks but no refresh), got %d (%+v)", got, g)
	}
	if g.IdenticalAudits-g0.IdenticalAudits != 1 || g.Unaudited-g0.Unaudited != 1 || g.WouldChange != g0.WouldChange {
		t.Fatalf("G1: want 1 identical + 1 unaudited + 0 changed, got %+v (from %+v)", g, g0)
	}
	if g.AvoidOnlyRefreshes != g.IdenticalAudits+g.WouldChange+g.NoPrior+g.Unaudited {
		t.Fatalf("G1 RED: samples (%d) != identical+changed+no_prior+unaudited (%d): every taken sample "+
			"must end in exactly one outcome (%+v)", g.AvoidOnlyRefreshes, g.IdenticalAudits+g.WouldChange+g.NoPrior+g.Unaudited, g)
	}
	if want := float64(g.Unaudited) / float64(g.AvoidOnlyRefreshes); g.UnauditedShare != want {
		t.Fatalf("G1 RED: unaudited_share %v, want unaudited / avoid-only refreshes = %v (never over marks)", g.UnauditedShare, want)
	}
	if _, ok := ListNarrowExpvar().(map[string]any)["gate"].(ListNarrowGate); !ok {
		t.Fatalf("G1: expvar snowplow_deps_list_narrow carries no gate readout")
	}
}
