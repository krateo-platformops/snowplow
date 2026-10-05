// issue406_ra_full_list_layers_test.go — #406 unit arms for the raKey <-> widget layer
// index (ra_full_list_layers.go), over the REAL store + dep tracker: content versions
// move only on a real byte change (or a refill after eviction), a widget Put is judged
// by the versions ITS resolve sliced, and the index is bounded by resident widgets.

package cache

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type layerRig struct {
	t      *testing.T
	c      *ResolvedCacheStore
	raKey  string
	raIn   ResolvedKeyInputs
	mu     sync.Mutex
	marked map[string]int // refresh-hook enqueues per key
	remark map[string]int // "layered" remarks per key
}

func newLayerRig(t *testing.T) *layerRig {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	ResetResolvedCacheForTest()
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	r := &layerRig{t: t, c: ResolvedCache(), marked: map[string]int{}, remark: map[string]int{}}
	Deps().SetStore(r.c)
	r.raIn = RAFullListKeyInputsForTest("templates.krateo.io", "v1", "restactions", "ns", "ra-406", "C:uid", nil)
	r.raKey = ComputeKey(r.raIn)
	Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		r.mu.Lock()
		r.marked[k]++
		r.mu.Unlock()
	})
	t.Cleanup(SetDepGenRemarkObserverForTest(func(k, reason string) {
		if reason == "layered" {
			r.mu.Lock()
			r.remark[k]++
			r.mu.Unlock()
		}
	}))
	return r
}

func (r *layerRig) commitRA(body string) {
	r.t.Helper()
	in := r.raIn
	ctx := WithL1KeyContext(context.Background(), r.raKey)
	if !r.c.PutIfGen(ctx, r.raKey, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in}, r.c.CaptureGen(r.raKey)) {
		r.t.Fatalf("raKey PutIfGen refused")
	}
}

func (r *layerRig) version() uint64 {
	r.t.Helper()
	e, ok := r.c.GetNoTouch(r.raKey)
	if !ok {
		r.t.Fatalf("raKey not resident")
	}
	return e.contentVersion
}

// slice is the widget resolve's fast-path hit: Get raKey, note the slice on ctx.
func (r *layerRig) slice(ctx context.Context) {
	r.t.Helper()
	e, ok := r.c.GetNoTouch(r.raKey)
	if !ok {
		r.t.Fatalf("raKey not resident")
	}
	r.c.NoteRAFullListSlice(ctx, r.raKey, e)
}

func widgetEntry(w string) *ResolvedEntry {
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: w}
	return &ResolvedEntry{RawJSON: []byte(`{"w":1}`), Inputs: &in}
}

func (r *layerRig) putWidget(ctx context.Context, w string) {
	r.t.Helper()
	if !r.c.PutIfGen(ctx, w, widgetEntry(w), r.c.CaptureGen(w)) {
		r.t.Fatalf("widget PutIfGen refused")
	}
}

func (r *layerRig) remarks(k string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.remark[k]
}

// Versions move on a real change only: an exact-bytes re-Put keeps the version.
func TestIssue406_Layers_VersionMovesOnlyOnByteChange(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	v1 := r.version()
	r.commitRA(`{"items":["a"]}`)
	if r.version() != v1 {
		t.Fatalf("identical re-Put must keep the version (%d → %d)", v1, r.version())
	}
	r.commitRA(`{"items":["a","b"]}`)
	v2 := r.version()
	if v2 == v1 {
		t.Fatalf("changed bytes must take a new version")
	}
	r.c.DeleteForTest(r.raKey)
	r.commitRA(`{"items":["a","b"]}`)
	if v3 := r.version(); v3 == v2 || v3 == v1 {
		t.Fatalf("a refill after eviction must never reissue an old version (v1=%d v2=%d v3=%d)", v1, v2, v3)
	}
}

// A resident widget is remarked once per raKey change, never for an identical re-Put.
func TestIssue406_Layers_RemarkOnlyOnChangedContent(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	const w = "w-resident"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.putWidget(ctx, w)
	if got := r.remarks(w); got != 0 {
		t.Fatalf("a widget that sliced the CURRENT raKey must not be remarked by its own Put, got %d", got)
	}
	r.commitRA(`{"items":["a"]}`)
	if got := r.remarks(w); got != 0 {
		t.Fatalf("identical raKey re-Put remarked the widget %d times; want 0", got)
	}
	r.commitRA(`{"items":["a","b"]}`)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 RED: changed raKey commit must remark the widget holding the old slice once, got %d", got)
	}
	if r.marked[w] != 1 {
		t.Fatalf("the remark must reach the refresh hook once, got %d", r.marked[w])
	}
}

// In flight: the widget sliced v1, raKey changed before the widget's Put. The Put
// (gen-guarded, or the boot seed's PutThenRemark) remarks it.
func TestIssue406_Layers_InFlightWidgetRemarkedByOwnPut(t *testing.T) {
	for _, plain := range []bool{false, true} {
		r := newLayerRig(t)
		r.commitRA(`{"items":["old"]}`)
		w := "w-inflight"
		ctx := WithL1KeyContext(context.Background(), w)
		r.slice(ctx)
		r.commitRA(`{"items":["new"]}`)
		if plain {
			r.c.PutThenRemark(ctx, w, widgetEntry(w))
		} else {
			r.putWidget(ctx, w)
		}
		if got := r.remarks(w); got != 1 {
			t.Fatalf("#406 RED (in-flight, plain=%v): want 1 remark, got %d", plain, got)
		}
	}
}

// P1 — raKey removed while the widget is in flight: the Put remarks it.
func TestIssue406_Layers_P1_RAKeyAbsentAtWidgetPut(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["old"]}`)
	const w = "w-p1"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.c.DeleteForTest(r.raKey)
	r.putWidget(ctx, w)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 P1 RED: raKey absent at the widget Put; want 1 remark, got %d", got)
	}
}

// P2 — two changes while in flight: no strike counting, one remark at the Put.
func TestIssue406_Layers_P2_TwoChangesInFlight(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["1"]}`)
	const w = "w-p2"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.commitRA(`{"items":["2"]}`)
	r.commitRA(`{"items":["3"]}`)
	r.putWidget(ctx, w)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 P2 RED: want 1 remark, got %d", got)
	}
}

// P3 — the older of two concurrent resolves Puts last: judged by ITS slice.
func TestIssue406_Layers_P3_OlderResolvePutsLast(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["old"]}`)
	const w = "w-p3"
	ctxA := WithL1KeyContext(context.Background(), w)
	r.slice(ctxA)
	r.commitRA(`{"items":["new"]}`)
	ctxB := WithL1KeyContext(context.Background(), w)
	r.slice(ctxB)
	r.putWidget(ctxB, w)
	if got := r.remarks(w); got != 0 {
		t.Fatalf("B sliced the current version; want 0 remarks, got %d", got)
	}
	r.putWidget(ctxA, w)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 P3 RED: A Put its old slice last; want 1 remark, got %d", got)
	}
	// The index now records A's old version, so the next change remarks the widget too.
	r.commitRA(`{"items":["newer"]}`)
	if got := r.remarks(w); got != 2 {
		t.Fatalf("the index must reflect the LAST writer's slice; want 2 remarks, got %d", got)
	}
}

// Get, then a commit, then the Note: the noted version is already superseded.
// C1 — ONE resolve slices the same raKey at two versions (two apiRefs on one widget,
// or a nested resolve, with a raKey commit in between). The Put is judged against the
// OLDEST noted version: the body embeds that stale slice.
func TestIssue406_Layers_C1_OneResolveSlicesTwoVersions(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["old"]}`)
	const w = "w-c1"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx) // first apiRef: old
	r.commitRA(`{"items":["new"]}`)
	r.slice(ctx) // second apiRef: new (current)
	r.putWidget(ctx, w)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 C1 RED: one resolve sliced raKey at the old AND the current version; the Put must be "+
			"judged by the old one and remark once, got %d", got)
	}
}

// C2 — the identical-bytes compare runs OFF c.mu (a RAFullList body can be tens of MB
// and every customer Get takes c.mu). The prior is re-confirmed under the lock: a commit
// that lands between the compare and the lock voids it, and the Put takes a NEW
// version rather than inheriting one whose content was superseded.
func TestIssue406_Layers_C2_OffLockCompare_ConcurrentCommitVoidsIdentical(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	vA := r.version()
	var vC uint64
	fired := false
	hook := func() {
		if !r.c.mu.TryLock() {
			t.Errorf("#406 C2 RED: the byte compare ran while holding c.mu")
		} else {
			r.c.mu.Unlock()
		}
		if fired {
			return
		}
		fired = true
		r.commitRA(`{"items":["c"]}`) // a concurrent changed commit lands in the window
		vC = r.version()
	}
	raLayerAfterCompareHook.Store(&hook)
	t.Cleanup(func() { raLayerAfterCompareHook.Store(nil) })

	r.commitRA(`{"items":["a"]}`) // bytes equal to the prior it COMPARED against (A)
	if !fired {
		t.Fatalf("setup: the compare window hook did not fire")
	}
	if vB := r.version(); vB == vA || vB == vC {
		t.Fatalf("#406 C2 RED: the Put compared equal to A, but C was committed before it took the lock; "+
			"it must take a NEW version, got vB=%d (vA=%d vC=%d)", vB, vA, vC)
	}
}

// C2 under -race: concurrent identical re-Puts, changed Puts and customer Gets of one
// raKey. The off-lock compare reads only immutable stored entries.
func TestIssue406_Layers_C2_ConcurrentRePutsAndGets_Race(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				in := r.raIn
				body := `{"items":["a"]}`
				if (i+g)%7 == 0 {
					body = `{"items":["b"]}`
				}
				r.c.Put(r.raKey, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in})
				if e, ok := r.c.Get(r.raKey); ok && e.contentVersion == 0 {
					t.Errorf("a stored RAFullList entry must carry a version")
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestIssue406_Layers_CommitBetweenGetAndNote(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["old"]}`)
	const w = "w-race"
	ctx := WithL1KeyContext(context.Background(), w)
	old, _ := r.c.GetNoTouch(r.raKey)
	r.commitRA(`{"items":["new"]}`)
	r.c.NoteRAFullListSlice(ctx, r.raKey, old)
	r.putWidget(ctx, w)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("#406 RED: want 1 remark, got %d", got)
	}
}

// A widget whose body came from a fresh resolve (Forget, then a Put with no slice) is
// not remarked by the raKey commit that precedes its Put.
func TestIssue406_Layers_ForgetBeforeFreshResolve(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	const w = "w-fresh"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.putWidget(ctx, w)
	r.c.ForgetRAFullListConsumer(r.raKey, w)
	r.commitRA(`{"items":["b"]}`)
	r.putWidget(WithL1KeyContext(context.Background(), w), w) // fresh body, no slice
	if got := r.remarks(w); got != 0 {
		t.Fatalf("a forgotten consumer was remarked %d times", got)
	}
	if _, cons := r.c.RAFullListLayerStatsForTest(); cons != 0 {
		t.Fatalf("a Put with no slice must clear the widget's recorded sources, got %d", cons)
	}
}

// C3 — a Put of unknown provenance (ctx-less: the external-TTL carrier, widgets.go)
// replaces the body, so it DROPS the cell's recorded sources: the cell leaves the
// layered remark path, and a later raKey change does not remark it.
func TestIssue406_Layers_UnknownProvenancePutDropsSources(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	const w = "w-external"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.putWidget(ctx, w)
	if _, cons := r.c.RAFullListLayerStatsForTest(); cons != 1 {
		t.Fatalf("setup: want 1 edge, got %d", cons)
	}
	r.c.Put(w, widgetEntry(w)) // provenance unknown
	if recs, cons := r.c.RAFullListLayerStatsForTest(); recs != 0 || cons != 0 {
		t.Fatalf("#406 C3 RED: a ctx-less Put must drop the recorded sources, got %d / %d", recs, cons)
	}
	r.commitRA(`{"items":["b"]}`)
	if got := r.remarks(w); got != 0 {
		t.Fatalf("#406 C3 RED: a cell last written by an unknown-provenance Put was remarked %d times", got)
	}
}

// Bounded by resident widgets.
func TestIssue406_Layers_IndexBoundedByResidentWidgets(t *testing.T) {
	r := newLayerRig(t)
	r.commitRA(`{"items":["a"]}`)
	const w = "w-bound"
	ctx := WithL1KeyContext(context.Background(), w)
	r.slice(ctx)
	r.putWidget(ctx, w)
	if recs, cons := r.c.RAFullListLayerStatsForTest(); recs != 1 || cons != 1 {
		t.Fatalf("want 1 record / 1 edge, got %d / %d", recs, cons)
	}
	// raKey evicted: the widget keeps its edge; the refill (a new version) remarks it.
	r.c.DeleteForTest(r.raKey)
	r.commitRA(`{"items":["a"]}`)
	if got := r.remarks(w); got != 1 {
		t.Fatalf("a refill after eviction must remark the resident widget once, got %d", got)
	}
	r.c.DeleteForTest(w)
	if recs, cons := r.c.RAFullListLayerStatsForTest(); recs != 0 || cons != 0 {
		t.Fatalf("widget removal must drop its edges, got %d / %d", recs, cons)
	}
	// A widget whose Put was refused never enters the index.
	const ghost = "w-ghost"
	gctx := WithL1KeyContext(context.Background(), ghost)
	r.slice(gctx)
	if r.c.PutIfGen(gctx, ghost, widgetEntry(ghost), 12345) {
		t.Fatalf("setup: the PutIfGen with a wrong generation must be refused")
	}
	if recs, cons := r.c.RAFullListLayerStatsForTest(); recs != 0 || cons != 0 {
		t.Fatalf("a refused widget Put must not be indexed, got %d / %d", recs, cons)
	}
}

// STRUCTURAL: every function in this package that stores an entry (calls putCoreLocked)
// must also run raLayerCommitLocked, so a new store write path (e.g. #416's
// ReplaceIfGenRefresh) cannot stamp or skip the content version silently.
func TestIssue406_Layers_EveryStoreWriteRunsTheLayerCommit(t *testing.T) {
	fset := token.NewFileSet()
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	writers := 0
	for _, e := range ents {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "putCoreLocked" {
				continue
			}
			stores, commits := false, false
			ast.Inspect(fn.Body, func(nd ast.Node) bool {
				if call, ok := nd.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "putCoreLocked":
							stores = true
						case "raLayerCommitLocked":
							commits = true
						}
					}
				}
				return true
			})
			if stores {
				writers++
				if !commits {
					t.Errorf("#406: %s (%s) stores an entry without raLayerCommitLocked", fn.Name.Name, n)
				}
			}
		}
	}
	if writers < 3 {
		t.Fatalf("VACUOUS: found %d store write functions, want >= 3", writers)
	}
}
