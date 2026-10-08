// deps_dirtymark_probe_test.go — #564 the dirty-mark fan-out self-representation
// PROBE: short-circuit falsifier.
//
// OnObjectEvent's fan-out loop (deps.go) called isSelfRepresentation for EVERY
// resident matched key on EVERY dep event. That probe is store.GetNoTouch →
// getCore, which takes the customer-facing c.mu. Measured on krateo-057
// (differenced over 61s, no assumed denominator): 2,222 dirty marks/s, of which
// the object_event/add_update/list_dep bucket is 99.1% — so ~2,200 acquisitions
// per second of the store mutex computed a value nothing read, because
// dirtyMarkClass returns dmClassListDep BEFORE it reads isSelf.
//
// isSelf has exactly two consumers, and the short-circuit is derived from them:
//
//	needSelf := state == objAbsent || dk.Name != listWildcard
//
//	1. the evict branch reads it only when state == objAbsent
//	2. dirtyMarkClass reads it only when dk.Name != listWildcard
//
// ARM 1 (identity) is the load-bearing arm. It pins the FULL output of the loop
// — the (evicted, marked) return, every {path,cause,class} bucket, residency,
// and the enqueue set — across all EIGHT reachable cells. Row "present +
// list_dep, entry IS the object" is the whole point: it is the ONLY cell where
// the probe's computed value differs (true before, false after), and the arm
// proves the OUTPUTS do not. Run it before and after the diff: identical.
//
// ARM 2 (dmClassSelf reachability) is the anti-dead-detector guard. The obvious
// form of this change, `isSelf := state == objAbsent && isSelfRepresentation(…)`,
// makes dmClassSelf STRUCTURALLY UNREACHABLE (dirtyMarkClass emits it only on
// dk.Name != listWildcard && isSelf, and that form forces isSelf=false whenever
// the object is present). That would convert a working detector into a counter
// whose zero reads as health, inside a change justified by efficiency. This arm
// fails RED under that form and is why the short-circuit is a disjunction.
//
// ARM 3 (saving + its one real cost) counts the probe calls through an EXISTING
// production counter rather than a new one: GetNoTouch is metric-neutral on a
// hit, but on a cell past TTL it performs the lazy evict and bumps
// evictTTLTotal, so an expired fixture makes that counter an exact probe-call
// count. It also records the one thing the short-circuit genuinely changes: the
// skipped probe no longer performs that lazy evict. It is DEFERRED, not lost —
// the same key is enqueued on the next line, and the refresher's dequeue read
// is itself a GetNoTouch (refresher.go), so the evict still happens off the
// customer path and keeps its INTERNAL attribution. The arm asserts the
// deferral's two halves: the cell stays resident AND it was enqueued.

package cache

import (
	"context"
	"sort"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// probeDepClass is how the dependent's edge was recorded, which is what decides
// dk.Name (and so whether dirtyMarkClass ever reads isSelf).
type probeDepClass int

const (
	probeExactDep probeDepClass = iota // Record → dk.Name == the object's name
	probeListDep                       // RecordList → dk.Name == listWildcard
)

// probeFixture wires ONE dependent on (gvr, ns, name) into a fresh
// tracker+store and captures the refresher enqueues.
//
// selfRep controls whether the entry's own resolved output IS the object
// (Inputs == the event coordinate), which is what isSelfRepresentation reads.
// It is deliberately independent of dep: a self-representation entry whose only
// recorded edge is a LIST dep is reachable in production and is the cell the
// short-circuit actually changes.
func probeFixture(t *testing.T, dep probeDepClass, selfRep bool) (*DepTracker, *ResolvedCacheStore, string, *[]string) {
	t.Helper()
	gvr, ns, name := gvrCompositions(), "ns", "obj"
	d := newTestDepTracker(t, 1_000)
	store := newResolvedCache(100, 1<<20, time.Hour)
	d.SetStore(store)

	enqueued := &[]string{}
	d.SetRefreshHook(func(l1Key string, _ schema.GroupVersionResource) {
		*enqueued = append(*enqueued, l1Key)
	})

	const key = "L1dep"
	own := "someotherobject"
	if selfRep {
		own = name // the entry's resolved output IS the event's object
	}
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, ns, own)})
	switch dep {
	case probeExactDep:
		d.Record(context.Background(), key, gvr, ns, name)
	case probeListDep:
		d.RecordList(context.Background(), key, gvr, ns)
	}
	return d, store, key, enqueued
}

// TestDirtyMarkProbe_BehaviourIdentity pins the FULL observable output of the
// fan-out loop for every reachable cell. The short-circuit must not move a
// single number here.
func TestDirtyMarkProbe_BehaviourIdentity(t *testing.T) {
	gvr, ns, name := gvrCompositions(), "ns", "obj"

	for _, tc := range []struct {
		desc  string
		dep   probeDepClass
		self  bool
		state objectState // the event this row fires
		cause string      // the attribution cause that event maps to

		wantEvicted  int
		wantMarked   int
		wantClass    string // "" = no class bucket (the key was evicted, not marked)
		wantResident bool
		wantEnqueued []string
	}{
		// --- object PRESENT (add/update): nothing is ever evicted ------------
		{
			desc: "present + list_dep, entry is a DIFFERENT object",
			dep:  probeListDep, self: false, state: objExists, cause: dmCauseAddUpdate,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassListDep,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},
		{
			// THE ROW THIS CHANGE TURNS ON. isSelfRepresentation would return
			// TRUE here, and after the short-circuit it is never called — but
			// dirtyMarkClass returns dmClassListDep before reading isSelf and
			// the evict branch is gated on objAbsent, so nothing observable
			// depends on it. If this row ever moves, the change is not a no-op.
			desc: "present + list_dep, entry IS the object (probe value differs, output must not)",
			dep:  probeListDep, self: true, state: objExists, cause: dmCauseAddUpdate,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassListDep,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},
		{
			desc: "present + exact_dep, entry is a DIFFERENT object",
			dep:  probeExactDep, self: false, state: objExists, cause: dmCauseAddUpdate,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassExactDep,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},
		{
			// dmClassSelf's live population. ARM 2 is this row under the
			// voided form.
			desc: "present + exact_dep, entry IS the object → self class",
			dep:  probeExactDep, self: true, state: objExists, cause: dmCauseAddUpdate,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassSelf,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},

		// --- object ABSENT (delete): the probe decides evict vs mark ---------
		{
			desc: "absent + list_dep, entry is a DIFFERENT object → marked, NOT evicted",
			dep:  probeListDep, self: false, state: objAbsent, cause: dmCauseDelete,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassListDep,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},
		{
			// The probe MUST still run here (state == objAbsent), or a deleted
			// object's own cell is never evicted. needSelf's first disjunct.
			desc: "absent + list_dep, entry IS the object → EVICTED",
			dep:  probeListDep, self: true, state: objAbsent, cause: dmCauseDelete,
			wantEvicted: 1, wantMarked: 0, wantClass: "",
			wantResident: false, wantEnqueued: nil,
		},
		{
			desc: "absent + exact_dep, entry is a DIFFERENT object → marked",
			dep:  probeExactDep, self: false, state: objAbsent, cause: dmCauseDelete,
			wantEvicted: 0, wantMarked: 1, wantClass: dmClassExactDep,
			wantResident: true, wantEnqueued: []string{"L1dep"},
		},
		{
			desc: "absent + exact_dep, entry IS the object → EVICTED",
			dep:  probeExactDep, self: true, state: objAbsent, cause: dmCauseDelete,
			wantEvicted: 1, wantMarked: 0, wantClass: "",
			wantResident: false, wantEnqueued: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			d, store, key, enqueued := probeFixture(t, tc.dep, tc.self)

			evicted, marked := d.OnObjectEvent(gvr, ns, name, tc.state)

			if evicted != tc.wantEvicted || marked != tc.wantMarked {
				t.Errorf("OnObjectEvent=(evicted=%d, marked=%d), want (%d, %d)",
					evicted, marked, tc.wantEvicted, tc.wantMarked)
			}
			// EVERY class bucket is pinned, not just the expected one, so a
			// mark that silently lands in the wrong class is caught.
			for _, class := range []string{dmClassSelf, dmClassListDep, dmClassExactDep} {
				want := uint64(0)
				if class == tc.wantClass {
					want = 1
				}
				if got := dmCount(d, dmPathObject, tc.cause, class); got != want {
					t.Errorf("(%s,%s,%s)=%d, want %d", dmPathObject, tc.cause, class, got, want)
				}
			}
			// The attribution must stay exhaustive: no mark may escape a bucket.
			if sum, total := dmSumBuckets(d), d.dirtyMarkTotal.Load(); sum != total {
				t.Errorf("Σbuckets=%d != dirtyMarkTotal=%d — a mark escaped attribution", sum, total)
			}
			if got := d.dirtyMarkTotal.Load(); got != uint64(tc.wantMarked) {
				t.Errorf("dirtyMarkTotal=%d, want %d", got, tc.wantMarked)
			}
			// The toEvict/toMark partition, observed on the store and the hook
			// rather than inferred from the counters.
			if got := store.Has(key); got != tc.wantResident {
				t.Errorf("store.Has(%q)=%v, want %v (eviction partition)", key, got, tc.wantResident)
			}
			if got := *enqueued; !sameKeys(got, tc.wantEnqueued) {
				t.Errorf("enqueued=%v, want %v (dirty-mark partition)", got, tc.wantEnqueued)
			}
		})
	}
}

// TestDirtyMarkProbe_SelfClassStaysReachable is the anti-dead-detector guard.
//
// It fails RED under the voided short-circuit
//
//	isSelf := state == objAbsent && d.isSelfRepresentation(store, l1Key, self)
//
// which makes dmClassSelf unreachable for the entire PRESENT population (the
// dominant one): dirtyMarkClass emits dmClassSelf only when
// dk.Name != listWildcard && isSelf, and that form forces isSelf=false whenever
// the object is present. The bucket would then read 0 forever, and 0 is
// indistinguishable from "no self-marks occurred" — the degrade-storm signature
// #239's discrimination arm exists to separate.
//
// The non-vacuity half matters as much as the assertion: it proves the fixture
// CAN produce a self mark, so a 0 here is the defect and not an inert fixture.
func TestDirtyMarkProbe_SelfClassStaysReachable(t *testing.T) {
	gvr, ns, name := gvrCompositions(), "ns", "obj"

	d, _, _, _ := probeFixture(t, probeExactDep, true /* the entry IS the object */)
	if _, marked := d.OnObjectEvent(gvr, ns, name, objExists); marked != 1 {
		t.Fatalf("marked=%d, want 1 — fixture inert, a zero below would be meaningless", marked)
	}
	if got := dmCount(d, dmPathObject, dmCauseAddUpdate, dmClassSelf); got == 0 {
		t.Fatalf("(object_event,add_update,self)=0 — dmClassSelf is UNREACHABLE for a PRESENT " +
			"self-representation entry. The probe short-circuit must be a DISJUNCTION " +
			"(state == objAbsent || dk.Name != listWildcard); a conjunction on objAbsent " +
			"silences this bucket and leaves a counter whose zero reads as health.")
	}
	// Non-vacuity of the discriminator: the SAME coordinate under a delete
	// EVICTS the self cell instead of marking it, so the self bucket is
	// genuinely state-sensitive and not pinned to 1 by construction.
	dDel, _, _, _ := probeFixture(t, probeExactDep, true)
	if evicted, _ := dDel.OnObjectEvent(gvr, ns, name, objAbsent); evicted != 1 {
		t.Fatalf("delete evicted=%d, want 1 — the self cell must be evicted on absence", evicted)
	}
	if got := dmCount(dDel, dmPathObject, dmCauseDelete, dmClassSelf); got != 0 {
		t.Fatalf("(object_event,delete,self)=%d, want 0 — a self cell is evicted, never marked", got)
	}
}

// TestDirtyMarkProbe_SavingAndItsDeferredEvict measures the saving through an
// existing production counter, and records the one behaviour the short-circuit
// genuinely changes.
//
// GetNoTouch is metric-neutral on a hit, so a resident-and-fresh fixture cannot
// count probe calls. On a cell past TTL it is NOT neutral: it performs the lazy
// evict and bumps evictTTLTotal. An all-expired fixture therefore turns
// evictTTLTotal into an exact count of the probes that reached a resident cell.
//
//	BEFORE the short-circuit: 8 list-dep + 1 exact-dep = 9 probes = 9 TTL evicts
//	AFTER:                    only the exact-dep is probed = 1 TTL evict
//
// The exact-dep cell is the CONTROL: it keeps the counter non-vacuous, so the
// list-dep population's drop to zero is a measured saving and not a dead
// instrument.
//
// The deferral, stated honestly: the 8 skipped cells stay RESIDENT past their
// TTL where before they were evicted inside the fan-out. The evict is not lost
// — each was enqueued to the refresher on the next line, and the refresher's
// dequeue read is itself a GetNoTouch (refresher.go), which performs the same
// lazy evict with the same INTERNAL attribution. What changes is only WHEN,
// bounded by the refresher's queue latency. This arm asserts both halves of
// that deferral so a future change cannot quietly break the hand-off.
func TestDirtyMarkProbe_SavingAndItsDeferredEvict(t *testing.T) {
	gvr, name := gvrCompositions(), "obj"
	const (
		nListDeps = 8
		ttl       = time.Second
	)

	d := newTestDepTracker(t, 1_000)
	store := newResolvedCache(100, 1<<20, ttl)
	d.SetStore(store)
	var enqueued []string
	d.SetRefreshHook(func(l1Key string, _ schema.GroupVersionResource) {
		enqueued = append(enqueued, l1Key)
	})

	// Every cell is aged past the TTL at Put time (putPreamble honours a
	// pre-set CreatedAt), so no sleep and no clock injection is needed.
	expired := func(own string) *ResolvedEntry {
		return &ResolvedEntry{
			RawJSON:   []byte(`{}`),
			Inputs:    inputsFor(gvr, "ns", own),
			CreatedAt: time.Now().Add(-2 * ttl),
		}
	}

	var listKeys []string
	for i := 0; i < nListDeps; i++ {
		k := "L1list_" + itoa(i)
		listKeys = append(listKeys, k)
		store.Put(k, expired("listowner"+itoa(i)))
		d.RecordList(context.Background(), k, gvr, "ns")
	}
	const controlKey = "L1exact_control"
	store.Put(controlKey, expired("owner"))
	d.Record(context.Background(), controlKey, gvr, "ns", name)

	if got := store.evictTTLTotal.Load(); got != 0 {
		t.Fatalf("evictTTLTotal=%d before the event, want 0 — the Puts must not have evicted", got)
	}

	_, marked := d.OnObjectEvent(gvr, "ns", name, objExists)

	// The partition is unchanged: every cell is still marked and enqueued.
	if want := nListDeps + 1; marked != want {
		t.Fatalf("marked=%d, want %d — the short-circuit must not change the partition", marked, want)
	}
	if !sameKeys(enqueued, append(append([]string{}, listKeys...), controlKey)) {
		t.Fatalf("enqueued=%v, want all %d cells — the deferred evict relies on the enqueue",
			enqueued, nListDeps+1)
	}

	probes := store.evictTTLTotal.Load()
	t.Logf("probe calls that reached a resident cell (via evictTTLTotal): %d of %d matched keys",
		probes, nListDeps+1)

	// CONTROL / non-vacuity: the exact-dep cell is probed on both forms, so the
	// counter is live and a zero for the list-dep population means something.
	if probes == 0 {
		t.Fatalf("evictTTLTotal=0 — the instrument is dead (not even the exact-dep control " +
			"was probed); the saving measured below would be vacuous")
	}
	if store.Has(controlKey) {
		t.Errorf("the exact-dep control is still resident — it must be probed (and so " +
			"TTL-evicted) on BOTH forms; without it the list-dep zero is uninstrumented")
	}

	// The saving, and the deferral it buys. BEFORE the diff probes == 9 and
	// every list cell is gone; AFTER, probes == 1 and the list cells remain.
	// Both are asserted as one statement so this arm reads as RUN evidence on
	// whichever tree it executes against.
	residentList := 0
	for _, k := range listKeys {
		if store.Has(k) {
			residentList++
		}
	}
	switch {
	case probes == uint64(nListDeps+1) && residentList == 0:
		t.Logf("PRE-SHORT-CIRCUIT tree: all %d matched keys probed; %d list cells TTL-evicted "+
			"inside the fan-out", nListDeps+1, nListDeps)
	case probes == 1 && residentList == nListDeps:
		t.Logf("POST-SHORT-CIRCUIT tree: %d of %d probes elided (%.1f%%); the %d list cells stay "+
			"resident and their lazy TTL evict is DEFERRED to the refresher's own GetNoTouch "+
			"(they were all enqueued above)",
			nListDeps, nListDeps+1, 100*float64(nListDeps)/float64(nListDeps+1), nListDeps)
	default:
		t.Fatalf("unrecognised state: probes=%d, resident list cells=%d/%d. Expected either "+
			"(9,0) pre-short-circuit or (1,8) post. A third outcome means the probe no longer "+
			"tracks the two consumers of isSelf", probes, residentList, nListDeps)
	}
}

// sameKeys compares two key sets order-insensitively (map iteration decides the
// fan-out order, so the assertion must not depend on it).
func sameKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := append([]string{}, a...), append([]string{}, b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}
