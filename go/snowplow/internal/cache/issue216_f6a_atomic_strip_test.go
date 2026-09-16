// issue216_f6a_atomic_strip_test.go — 1.12.7 F6a: deleting an entry and
// orphaning its dep edges must not be separately observable.
//
// WHY. Before F6a the strip was the CALLER's job, run after deleteForDep
// returned and outside the store mutex. That leaves a window on the SUCCESS
// branch: the worker deletes the key and returns true, a concurrent resolve
// re-Puts the same key and records fresh edges, and the caller's trailing
// unconditional strip then removes the NEW entry's edges. The result is a
// resident entry with no dep edge — which the audit's submit path skips at the
// hasEdge gate, so no repair path can reach it and only a read can ever
// remove it.
package cache

import (
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// f6aGVR is a coordinate distinct from the other C3/C2 fixtures.
func f6aGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// TestIssue216_F6a_DeletePrimitiveStripsEdgesBeforeItReturns — the
// deterministic half. On return from the delete primitive there must be NO
// state in which the entry is gone and its edges survive.
//
// RED with the strip restored to the caller: deleteForDep returns with the
// edge still present, which is the window a concurrent re-Put lands in.
func TestIssue216_F6a_DeletePrimitiveStripsEdgesBeforeItReturns(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := f6aGVR()
	const key = "L1_f6a-flex"

	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &ResolvedKeyInputs{
		CacheEntryClass: "widgets", Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: "demo-system", Name: "f6a-flex",
	}})
	Deps().Record(key, gvr, "demo-system", "f6a-flex")
	dk := DepKey{GVR: gvr, Namespace: "demo-system", Name: "f6a-flex"}
	if !Deps().hasEdge(key, dk) {
		t.Fatalf("premise: the edge was not recorded")
	}

	if !store.deleteForDep(key) {
		t.Fatalf("premise: deleteForDep did not remove a resident entry")
	}
	// The assertion is on the RETURN, not "eventually": any observable moment
	// with the entry gone and the edge alive is the defect.
	if Deps().hasEdge(key, dk) {
		t.Fatalf("RED (F6a): deleteForDep returned with the entry gone and its dep edge STILL PRESENT. " +
			"A concurrent resolve that re-Puts this key inside that window records fresh edges which the " +
			"caller's trailing strip then removes, leaving a resident entry with no edge — the hasEdge " +
			"gate skips it and no repair path can reach it")
	}
	if _, alive := store.Get(key); alive {
		t.Fatalf("F6a: the entry is still resident after deleteForDep reported it removed")
	}
}

// TestIssue216_F6a_ConcurrentRewriteKeepsItsEdges — the -race half, and the
// one that pins the actual production shape: eviction racing a rewrite. A key
// that is resident when the dust settles MUST still hold its edges.
//
// RED with the caller-side trailing strip restored: the strip lands after the
// re-Put and removes the new entry's edges.
func TestIssue216_F6a_ConcurrentRewriteKeepsItsEdges(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	store := newResolvedCache(1000, 1<<24, time.Hour)
	Deps().SetStore(store)
	gvr := f6aGVR()

	const rounds = 200
	type row struct {
		key string
		dk  DepKey
	}
	rows := make([]row, rounds)
	for i := range rows {
		name := "f6a-" + itoaForTest(i)
		rows[i] = row{key: "L1_" + name, dk: DepKey{GVR: gvr, Namespace: "demo-system", Name: name}}
	}
	put := func(r row) {
		store.Put(r.key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &ResolvedKeyInputs{
			CacheEntryClass: "widgets", Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
			Namespace: r.dk.Namespace, Name: r.dk.Name,
		}})
		Deps().Record(r.key, gvr, r.dk.Namespace, r.dk.Name)
	}
	for _, r := range rows {
		put(r)
	}

	var wg sync.WaitGroup
	for _, r := range rows {
		r := r
		wg.Add(2)
		// The evicting side: the production eviction path.
		go func() {
			defer wg.Done()
			Deps().runEvictionBatch([]string{r.key})
		}()
		// The rewriting side: a resolve that re-Puts the same key and records
		// its edges afresh — exactly what the prewarm seed does.
		go func() {
			defer wg.Done()
			put(r)
		}()
	}
	wg.Wait()

	orphaned := 0
	var sample string
	for _, r := range rows {
		if _, alive := store.Get(r.key); !alive {
			continue // evicted and stayed evicted — nothing to check
		}
		if !Deps().hasEdge(r.key, r.dk) {
			orphaned++
			if sample == "" {
				sample = r.key
			}
		}
	}
	if orphaned > 0 {
		t.Fatalf("RED (F6a): %d of %d keys are RESIDENT with their dep edges stripped (e.g. %s). The "+
			"eviction's trailing strip ran after a concurrent re-Put and removed the NEW entry's edges. "+
			"Such an entry is skipped at the audit's hasEdge gate, so no repair path can reach it and "+
			"only a read can remove it", orphaned, rounds, sample)
	}
}
