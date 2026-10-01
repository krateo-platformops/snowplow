// deps_attribution_test.go — #239 dirty-mark attribution falsifier.
//
// Four arms (freshness-audit brief):
//   A  per-bucket attribution — each object state×class and each type cause×class
//      lands in its OWN {path,cause,class} bucket; each submit source in its own
//      source bucket.
//   B  DISCRIMINATION (K>1×M>1) — a benign fan-out and an amplified scenario
//      produce DIFFERENT distributions, so a real amplifier is separable from
//      benign fan-out (which a scalar dirtyMarkTotal cannot do).
//   C  SUM INVARIANT — Σ over {path,cause,class} == dirtyMarkTotal and
//      unattributed==0, proving the attribution is exhaustive (a 3rd un-routed
//      emit site would break it).
// (Arm D, OTLP-registered, lives in internal/metrics — it needs the SDK
// ManualReader over the real callback.)

package cache

import (
	"context"
	"testing"
	"time"
)

// dmCount reads one {path,cause,class} bucket from a tracker's snapshot.
func dmCount(d *DepTracker, path, cause, class string) uint64 {
	for _, b := range d.dirtyMarkAttributionSnapshot() {
		if b.Path == path && b.Cause == cause && b.Class == class {
			return b.Count
		}
	}
	return 0
}

// dmSumBuckets sums every attribution bucket.
func dmSumBuckets(d *DepTracker) uint64 {
	var s uint64
	for _, b := range d.dirtyMarkAttributionSnapshot() {
		s += b.Count
	}
	return s
}

// threeClassDeps wires a self, an exact-GET and a list dependent on
// (gvr, ns, name) into a fresh tracker+store.
func threeClassDeps(t *testing.T) (*DepTracker, string, string, string) {
	t.Helper()
	d := newTestDepTracker(t, 1_000)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d.SetStore(store)
	gvr := gvrCompositions()
	ns, name := "ns", "obj"
	// self: its own dispatched object IS (gvr, ns, name).
	store.Put("L1self", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, name)})
	d.Record(context.Background(), "L1self", gvr, ns, name)
	// exact_dep: own object is a DIFFERENT object; GET-depends on (gvr,ns,name).
	store.Put("L1exact", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, "owner")})
	d.Record(context.Background(), "L1exact", gvr, ns, name)
	// list_dep: LIST-depends on (gvr, ns, *).
	store.Put("L1list", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, "listowner")})
	d.RecordList(context.Background(), "L1list", gvr, ns)
	return d, "L1self", "L1exact", "L1list"
}

func TestIssue239_A_ObjectPathAttribution(t *testing.T) {
	gvr := gvrCompositions()
	ns, name := "ns", "obj"

	t.Run("add_update marks self+list+exact each in its class", func(t *testing.T) {
		d, _, _, _ := threeClassDeps(t)
		d.OnObjectEvent(gvr, ns, name, objExists)
		for _, class := range []string{dmClassSelf, dmClassListDep, dmClassExactDep} {
			if got := dmCount(d, dmPathObject, dmCauseAddUpdate, class); got != 1 {
				t.Errorf("(object,add_update,%s)=%d, want 1", class, got)
			}
		}
	})

	t.Run("degraded marks all three (nothing evicted)", func(t *testing.T) {
		d, _, _, _ := threeClassDeps(t)
		d.OnObjectEvent(gvr, ns, name, objUnknownDegraded)
		for _, class := range []string{dmClassSelf, dmClassListDep, dmClassExactDep} {
			if got := dmCount(d, dmPathObject, dmCauseDegraded, class); got != 1 {
				t.Errorf("(object,degraded,%s)=%d, want 1", class, got)
			}
		}
	})

	t.Run("delete marks list+exact but NEVER self (self is evicted)", func(t *testing.T) {
		d, _, _, _ := threeClassDeps(t)
		d.OnObjectEvent(gvr, ns, name, objAbsent)
		if got := dmCount(d, dmPathObject, dmCauseDelete, dmClassListDep); got != 1 {
			t.Errorf("(object,delete,list_dep)=%d, want 1", got)
		}
		if got := dmCount(d, dmPathObject, dmCauseDelete, dmClassExactDep); got != 1 {
			t.Errorf("(object,delete,exact_dep)=%d, want 1", got)
		}
		if got := dmCount(d, dmPathObject, dmCauseDelete, dmClassSelf); got != 0 {
			t.Errorf("(object,delete,self)=%d, want 0 — a self entry is EVICTED on absence, never marked", got)
		}
	})
}

func TestIssue239_A_TypePathAttribution(t *testing.T) {
	gvr := gvrCompositions()

	// Fresh tracker with a list-dep and an exact-dep on the GVR (no self class
	// on the type path — a type-scope dep is list or exact).
	newTypeDeps := func(t *testing.T) *DepTracker {
		d := newTestDepTracker(t, 1_000)
		store := newResolvedCache(100, 1<<20, time.Hour)
		d.SetStore(store)
		store.Put("L1exact", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, "ns", "owner")})
		d.Record(context.Background(), "L1exact", gvr, "ns", "obj")
		store.Put("L1list", &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, "ns", "listowner")})
		d.RecordList(context.Background(), "L1list", gvr, "ns")
		return d
	}

	t.Run("crd_add is list-only", func(t *testing.T) {
		d := newTypeDeps(t)
		d.OnResourceTypeAvailable(gvr)
		if got := dmCount(d, dmPathType, dmCauseCRDAdd, dmClassListDep); got != 1 {
			t.Errorf("(type,crd_add,list_dep)=%d, want 1", got)
		}
		if got := dmCount(d, dmPathType, dmCauseCRDAdd, dmClassExactDep); got != 0 {
			t.Errorf("(type,crd_add,exact_dep)=%d, want 0 — crd_add invalidates only stale-negative LISTs", got)
		}
	})

	for _, tc := range []struct {
		name  string
		cause string
		call  func(*DepTracker)
	}{
		{"crd_delete", dmCauseCRDDelete, func(d *DepTracker) { d.OnResourceTypeRemoved(gvr) }},
		{"schema_relist", dmCauseSchemaRelist, func(d *DepTracker) { d.OnResourceTypeSchemaRelisted(gvr) }},
		{"store_repair", dmCauseStoreRepair, func(d *DepTracker) { d.OnResourceTypeStoreRepaired(gvr) }},
	} {
		t.Run(tc.name+" marks both list and exact", func(t *testing.T) {
			d := newTypeDeps(t)
			tc.call(d)
			if got := dmCount(d, dmPathType, tc.cause, dmClassListDep); got != 1 {
				t.Errorf("(type,%s,list_dep)=%d, want 1", tc.cause, got)
			}
			if got := dmCount(d, dmPathType, tc.cause, dmClassExactDep); got != 1 {
				t.Errorf("(type,%s,exact_dep)=%d, want 1", tc.cause, got)
			}
		})
	}
}

func TestIssue239_A_SubmitSourceAttribution(t *testing.T) {
	d := newTestDepTracker(t, 1_000)
	// Each source lands in its OWN bucket (pre-dedup submit axis).
	d.recordSubmitSource(dmSourceWatch)
	d.recordSubmitSource(dmSourceWatch)
	d.recordSubmitSource(dmSourceRelistBridge)
	d.recordSubmitSource(dmSourceReconcile)
	src := d.dirtyMarkSubmitSourceSnapshot()
	if src[dmSourceWatch] != 2 || src[dmSourceRelistBridge] != 1 || src[dmSourceReconcile] != 1 {
		t.Fatalf("submit-source snapshot=%v, want watch=2 relist_bridge=1 reconcile=1", src)
	}
}

// TestIssue239_B_Discrimination — the load-bearing arm: benign fan-out vs an
// amplifier must produce DIFFERENT distributions.
func TestIssue239_B_Discrimination(t *testing.T) {
	gvr := gvrCompositions()
	ns, name := "ns", "obj"

	// (1) The delete-vs-degraded signature: a degraded storm marks the SELF
	// entries that a delete correctly EVICTS — so it marks MORE than the
	// object's absence warrants, and that shows as a self-class bucket the
	// delete path never produces. A scalar dirtyMarkTotal cannot see it.
	benign, _, _, _ := threeClassDeps(t)
	benign.OnObjectEvent(gvr, ns, name, objAbsent) // delete: self evicted
	amp, _, _, _ := threeClassDeps(t)
	amp.OnObjectEvent(gvr, ns, name, objUnknownDegraded) // degraded: self re-marked

	if got := dmCount(benign, dmPathObject, dmCauseDelete, dmClassSelf); got != 0 {
		t.Fatalf("benign delete marked a self entry (%d) — fixture wrong", got)
	}
	if got := dmCount(amp, dmPathObject, dmCauseDegraded, dmClassSelf); got == 0 {
		t.Fatalf("the amplified (degraded) scenario did NOT mark the self entry — the instrument cannot "+
			"show a degrade storm re-marking entries a delete would evict, so it does not discriminate")
	}
	// The distributions DIFFER at the self class — the amplifier signature.
	if dmCount(benign, dmPathObject, dmCauseDelete, dmClassSelf) ==
		dmCount(amp, dmPathObject, dmCauseDegraded, dmClassSelf) {
		t.Fatalf("benign and amplified self-class marks are equal — no discrimination")
	}

	// (2) The fan-out denominator separates equal totals: N dependents on ONE
	// coordinate (benign, high fan-out) vs N events each marking 1 (amplifier,
	// low fan-out) yield the SAME dirtyMarkTotal but DIFFERENT events_total.
	hi := newTestDepTracker(t, 1_000)
	histore := newResolvedCache(100, 1<<20, time.Hour)
	hi.SetStore(histore)
	for i := 0; i < 4; i++ {
		k := "L1_" + itoa(i)
		histore.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, "o"+itoa(i))})
		hi.RecordList(context.Background(), k, gvr, ns)
	}
	hi.OnObjectEvent(gvr, ns, name, objExists) // ONE event, fan-out 4
	if got := hi.dirtyMarkTotal.Load(); got != 4 {
		t.Fatalf("hi total=%d want 4", got)
	}
	if ev := hi.dirtyMarkEventsTotal(); ev != 1 {
		t.Fatalf("hi events=%d want 1 (one high-fan-out event)", ev)
	}

	lo := newTestDepTracker(t, 1_000)
	lostore := newResolvedCache(100, 1<<20, time.Hour)
	lo.SetStore(lostore)
	for i := 0; i < 4; i++ {
		k := "L1_" + itoa(i)
		lostore.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, "o"+itoa(i))})
		lo.Record(context.Background(), k, gvr, ns, "n"+itoa(i)) // exact dep on a distinct coordinate each
	}
	for i := 0; i < 4; i++ {
		lo.OnObjectEvent(gvr, ns, "n"+itoa(i), objExists) // 4 events, fan-out 1 each
	}
	if got := lo.dirtyMarkTotal.Load(); got != 4 {
		t.Fatalf("lo total=%d want 4", got)
	}
	if ev := lo.dirtyMarkEventsTotal(); ev != 4 {
		t.Fatalf("lo events=%d want 4 (four low-fan-out events)", ev)
	}
	// SAME dirtyMarkTotal (4), DIFFERENT events (1 vs 4) → mean fan-out 4 vs 1.
	// The scalar total is blind to this; the events denominator discriminates.
	if hi.dirtyMarkTotal.Load() != lo.dirtyMarkTotal.Load() {
		t.Fatalf("fixture: totals differ (%d vs %d) — the discrimination must be at equal totals",
			hi.dirtyMarkTotal.Load(), lo.dirtyMarkTotal.Load())
	}
	if hi.dirtyMarkEventsTotal() == lo.dirtyMarkEventsTotal() {
		t.Fatalf("events_total equal (%d) at equal dirtyMarkTotal — the fan-out denominator does not "+
			"discriminate benign high-fan-out from a low-fan-out event storm", hi.dirtyMarkEventsTotal())
	}
}

// TestIssue239_C_SumInvariant — Σ buckets == dirtyMarkTotal and no unattributed.
func TestIssue239_C_SumInvariant(t *testing.T) {
	gvr := gvrCompositions()
	ns, name := "ns", "obj"

	d, _, _, _ := threeClassDeps(t)
	// Exercise both emit paths and several causes/classes.
	d.OnObjectEvent(gvr, ns, name, objExists)          // object add_update
	d.OnObjectEvent(gvr, ns, name, objUnknownDegraded) // object degraded
	d.OnResourceTypeRemoved(gvr)                        // type crd_delete
	d.OnResourceTypeSchemaRelisted(gvr)                // type schema_relist
	d.OnObjectEvent(gvr, ns, name, objAbsent)          // object delete (self evicted)

	total := d.dirtyMarkTotal.Load()
	if total == 0 {
		t.Fatal("vacuity: no dirty-marks recorded")
	}
	if sum := dmSumBuckets(d); sum != total {
		t.Fatalf("SUM INVARIANT: Σ buckets=%d but dirtyMarkTotal=%d — attribution is not exhaustive "+
			"(a dirty-mark reached dirtyMarkTotal without a {path,cause,class} bucket)", sum, total)
	}
	if u := d.dirtyMarkUnattributedTotal(); u != 0 {
		t.Fatalf("unattributed=%d, want 0 — the classifier produced a {path,cause,class} combo that was "+
			"not pre-registered", u)
	}
}
