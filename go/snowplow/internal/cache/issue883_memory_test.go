//go:build unit || integration

// issue883_memory_test.go — #883 falsifiers (bench run 15 OOM, part 2).
//
//	F3 (c)  N concurrent first readers of a lazy LIST entry run ONE parse.
//	F4 (d)  store, move a dependency, Load → the key is gone.
//	F4 (d)  long arm: the entries kept are exactly the entries whose deps have
//	        not moved — by the test's own model of what each event moves, and
//	        cross-checked against the dep tracker's judgement.
//	F4      memory floor: churned deps nobody looks up again leave the memo; its
//	        entries and bytes stay equal to the live set, round after round.
//
// Every bound below comes from the definition and is counted by a seam the test
// owns (the slow-path hook, the parse fn, the test's own event model) — never
// read back from the counter under test.

package cache

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIssue883_F3_ConcurrentFirstReadersRunOneParse — falsifier (c). N
// goroutines call EnsureItems on a fresh lazy entry; the parse fn blocks until
// all N are past the memo check (counted by the slow-path hook, a seam this test
// owns). Before #883 every one of them parsed: the parse count was N. Now one
// parses and N-1 wait for its published result.
func TestIssue883_F3_ConcurrentFirstReadersRunOneParse(t *testing.T) {
	ResetLazyItemsStatsForTest()
	const n = 32
	e := &ResolvedEntry{RawJSON: []byte(`{"apiVersion":"v1","items":[{}],"kind":"ThingsList"}`)}

	var entered atomic.Int64
	allIn := make(chan struct{})
	hook := func() {
		if entered.Add(1) == n {
			close(allIn)
		}
	}
	ensureItemsSlowPathHook.Store(&hook)
	t.Cleanup(func() { ensureItemsSlowPathHook.Store(nil) })

	var parses atomic.Int64
	parse := func(raw []byte) ([]*unstructured.Unstructured, string, string, bool) {
		parses.Add(1)
		select {
		case <-allIn:
		case <-time.After(30 * time.Second): // hang guard only: fail, never wait forever
			t.Error("not every reader reached EnsureItems' slow path")
		}
		return mkItems("x"), "v1", "ThingsList", true
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[*unstructured.Unstructured]int{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			items, _, _, ok := e.EnsureItems(parse)
			if !ok || len(items) != 1 {
				t.Errorf("reader: ok=%v n=%d", ok, len(items))
				return
			}
			mu.Lock()
			seen[items[0]]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if got := entered.Load(); got != n {
		t.Fatalf("hook counted %d slow-path entries, want %d: the arm did not exercise N concurrent first readers", got, n)
	}
	if got := parses.Load(); got != 1 {
		t.Fatalf("#883 F3 RED: %d concurrent first readers ran %d parses of one entry, want exactly 1", n, got)
	}
	if len(seen) != 1 {
		t.Fatalf("readers saw %d distinct trees, want the one published tree", len(seen))
	}
	if s := LazyItemsStatsSnapshot(); s.Parsed != 1 || s.Served != n-1 {
		t.Fatalf("lazy counters parsed=%d served=%d, want 1 and %d", s.Parsed, s.Served, n-1)
	}
}

var (
	gvr883Hot  = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "benchapps"}
	gvr883RA   = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	gvr883Calm = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1", Resource: "quiet"}
	// gvr883Other: no memo entry depends on it; an event on it moves nothing.
	gvr883Other = schema.GroupVersionResource{Group: "other.krateo.io", Version: "v1", Resource: "unrelated"}
)

// record883 gives dk a tracker bucket under l1, as a producing resolve does.
func record883(t *testing.T, d *DepTracker, l1 string, dk DepKey) {
	t.Helper()
	if dk.Name == listWildcard {
		d.RecordList(t.Context(), l1, dk.GVR, dk.Namespace)
		return
	}
	d.Record(t.Context(), l1, dk.GVR, dk.Namespace, dk.Name)
}

func memoKeys883(m *SeedResolveMemo) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.m))
	for k := range m.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestIssue883_F4_StoreMoveLoad_KeyGone — falsifier (d), short arm. Both
// deletion routes, each on its own memo:
//   - untracked: a Load that finds the entry stale deletes it (before #883 it
//     was refused and kept);
//   - tracked: the dep event itself deletes it, with no Load at all.
func TestIssue883_F4_StoreMoveLoad_KeyGone(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	const ns = "ns-883"
	listDep := DepKey{GVR: gvr883Hot, Namespace: ns, Name: listWildcard}
	record883(t, d, "L1_883_producer", listDep)

	t.Run("load_deletes_stale", func(t *testing.T) {
		m := NewSeedResolveMemo()
		body := memoBody(map[string]any{"v": "OLD"})
		m.Store("k", body, []DepKey{listDep}, DepGenEpochNow())
		if m.Entries() != 1 || m.BodyBytes() != int64(len(body)) {
			t.Fatalf("after Store: entries=%d bytes=%d, want 1 and %d", m.Entries(), m.BodyBytes(), len(body))
		}
		d.OnUpdate(gvr883Hot, ns, "obj-0") // move the dependency
		if _, _, _, ok := m.Load("k"); ok {
			t.Fatal("a moved dep must turn the hit into a miss (#411)")
		}
		if keys := memoKeys883(m); len(keys) != 0 {
			t.Fatalf("#883 F4 RED: Load found the entry stale and KEPT it: %v", keys)
		}
		if m.Entries() != 0 || m.BodyBytes() != 0 {
			t.Fatalf("gauge after the stale Load: entries=%d bytes=%d, want 0/0", m.Entries(), m.BodyBytes())
		}
	})

	t.Run("dep_event_deletes_unread", func(t *testing.T) {
		m := NewSeedResolveMemo()
		defer TrackDepMoves(m)()
		m.Store("k", memoBody(map[string]any{"v": "OLD"}), []DepKey{listDep}, DepGenEpochNow())
		d.OnUpdate(gvr883Hot, ns, "obj-1") // nobody Loads "k" afterwards
		if keys := memoKeys883(m); len(keys) != 0 {
			t.Fatalf("#883 F4 RED: the dep tracker moved the entry's dependency and the entry stayed: %v", keys)
		}
		if m.Entries() != 0 || m.BodyBytes() != 0 {
			t.Fatalf("gauge: entries=%d bytes=%d, want 0/0", m.Entries(), m.BodyBytes())
		}
	})
}

// entry883 is one memo entry of the long arm, with its deps and how the test
// gave (or did not give) each dep a tracker bucket.
type entry883 struct {
	key  string
	deps []DepKey
}

// moves883 is the test's OWN definition of which deps an object event on
// (gvr, ns, name) moves: the coordinate itself, its cluster-scoped name, and the
// LIST of its namespace and of the cluster — plus every dep of the GVR that has
// no bucket, which the tracker judges by the GVR-wide floor. Written from the
// #375 contract, not from coordinateBuckets.
func moves883(dk DepKey, gvr schema.GroupVersionResource, ns, name string, bucketless bool) bool {
	if dk.GVR != gvr {
		return false
	}
	if bucketless {
		return true
	}
	nameHit := dk.Name == name || dk.Name == listWildcard
	nsHit := dk.Namespace == ns || dk.Namespace == ""
	return nameHit && nsHit
}

// TestIssue883_F4_KeptEqualsUnmoved_LongArm — falsifier (d), long arm. 240
// entries over exact, namespace-LIST, cluster-LIST and RA-CR deps, plus two
// populations whose dep loses its tracker bucket — one BEFORE the events (the
// GVR floor then judges it on the next event of the GVR) and one AFTER them (the
// floor is already past its stamp the moment the bucket goes). After the events,
// with no Load at all, the kept keys must equal the test model's unmoved set
// AND the dep tracker's own judgement.
func TestIssue883_F4_KeptEqualsUnmoved_LongArm(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	m := NewSeedResolveMemo()
	defer TrackDepMoves(m)()

	nss := []string{"ns-a", "ns-b", "ns-c", "ns-d"}
	var all []entry883
	bucketless := map[DepKey]bool{} // the test's own record of which deps it un-bucketed
	orphanEarly := map[DepKey]string{}
	orphanLate := map[DepKey]string{}
	stamp := DepGenEpochNow()

	for i := 0; i < 240; i++ {
		ns := nss[i%len(nss)]
		var deps []DepKey
		switch i % 6 {
		case 0: // one exact object
			deps = []DepKey{{GVR: gvr883Hot, Namespace: ns, Name: fmt.Sprintf("obj-%d", i%20)}}
		case 1: // a namespace LIST
			deps = []DepKey{{GVR: gvr883Hot, Namespace: ns, Name: listWildcard}}
		case 2: // an exact object + its RA CR
			deps = []DepKey{
				{GVR: gvr883Hot, Namespace: ns, Name: fmt.Sprintf("obj-%d", i%20)},
				{GVR: gvr883RA, Namespace: "krateo-system", Name: fmt.Sprintf("ra-%d", i%5)},
			}
		case 3: // only a calm GVR nobody touches
			deps = []DepKey{{GVR: gvr883Calm, Namespace: ns, Name: fmt.Sprintf("q-%d", i)}}
		case 4: // a dep whose bucket is pruned BEFORE the events
			dk := DepKey{GVR: gvr883Hot, Namespace: ns, Name: fmt.Sprintf("orphan-early-%d", i)}
			deps = []DepKey{dk}
			orphanEarly[dk] = fmt.Sprintf("L1_883_orphan_early_%d", i)
		case 5: // a dep whose bucket is pruned AFTER the events
			dk := DepKey{GVR: gvr883Hot, Namespace: ns, Name: fmt.Sprintf("orphan-late-%d", i)}
			deps = []DepKey{dk}
			orphanLate[dk] = fmt.Sprintf("L1_883_orphan_late_%d", i)
		}
		if i == 7 { // exactly one cluster-wide LIST: any hot event moves it
			deps = append(deps, DepKey{GVR: gvr883Hot, Namespace: "", Name: listWildcard})
		}
		e := entry883{key: fmt.Sprintf("memo-%03d", i), deps: deps}
		for _, dk := range deps {
			l1 := "L1_883_producer"
			if o, ok := orphanEarly[dk]; ok {
				l1 = o
			}
			if o, ok := orphanLate[dk]; ok {
				l1 = o
			}
			record883(t, d, l1, dk)
		}
		m.Store(e.key, memoBody(map[string]any{"i": float64(i)}), deps, stamp)
		all = append(all, e)
	}
	if got := m.Entries(); got != int64(len(all)) {
		t.Fatalf("stored %d entries, gauge reads %d", len(all), got)
	}

	// Early orphans lose their bucket before any hot event: nothing has moved
	// yet, so they stay — until the hot GVR's next event raises its floor. The
	// prune reaches the memo at the next dep event anywhere (it is logged, never
	// judged under the store lock); an event on a GVR no entry depends on is that
	// event here, so the hot events below must find them floor-judged already.
	for dk, l1 := range orphanEarly {
		d.RemoveL1Key(l1)
		bucketless[dk] = true
	}
	d.OnUpdate(gvr883Other, "ns-other", "x")
	if got := m.Entries(); got != int64(len(all)) {
		t.Fatalf("pruning a bucket with no event since the stamp dropped entries: %d of %d left", got, len(all))
	}

	type ev struct {
		gvr      schema.GroupVersionResource
		ns, name string
	}
	events := []ev{
		{gvr883Hot, "ns-a", "obj-0"},
		{gvr883Hot, "ns-b", "obj-5"},
		{gvr883Hot, "ns-c", "obj-unrelated"},
		{gvr883RA, "krateo-system", "ra-2"},
	}
	moved := map[string]bool{}
	for _, x := range events {
		d.OnUpdate(x.gvr, x.ns, x.name)
		for _, e := range all {
			for _, dk := range e.deps {
				if moves883(dk, x.gvr, x.ns, x.name, bucketless[dk]) {
					moved[e.key] = true
				}
			}
		}
	}
	// Late orphans lose their bucket after the hot GVR's floor moved past their
	// stamp: from that instant the tracker judges them moved, and the next dep
	// event anywhere (again on the unrelated GVR) hands the prune to the memo.
	for dk, l1 := range orphanLate {
		d.RemoveL1Key(l1)
		bucketless[dk] = true
		for _, e := range all {
			for _, edk := range e.deps {
				if edk == dk {
					moved[e.key] = true
				}
			}
		}
	}
	d.OnUpdate(gvr883Other, "ns-other", "y")

	var wantKept []string
	nMoved := 0
	for _, e := range all {
		tracker := seedMemoDepsMovedSince(e.deps, stamp)
		if tracker != moved[e.key] {
			t.Fatalf("model and dep tracker disagree on %s (deps %v): model moved=%v, tracker moved=%v",
				e.key, e.deps, moved[e.key], tracker)
		}
		if moved[e.key] {
			nMoved++
			continue
		}
		wantKept = append(wantKept, e.key)
	}
	sort.Strings(wantKept)
	// Discrimination: the arm must exercise both outcomes in every population.
	if nMoved == 0 || len(wantKept) == 0 {
		t.Fatalf("degenerate arm: moved=%d kept=%d", nMoved, len(wantKept))
	}

	got := memoKeys883(m)
	if fmt.Sprint(got) != fmt.Sprint(wantKept) {
		extra, missing := diff883(got, wantKept)
		t.Fatalf("#883 F4 RED: memo keeps %d entries, want the %d whose deps did not move.\n  kept but moved: %v\n  dropped but unmoved: %v",
			len(got), len(wantKept), extra, missing)
	}
	if m.Entries() != int64(len(wantKept)) {
		t.Fatalf("entries gauge %d, want %d", m.Entries(), len(wantKept))
	}
	var wantBytes int64
	for _, k := range wantKept {
		var i int
		fmt.Sscanf(k, "memo-%d", &i)
		wantBytes += int64(len(memoBody(map[string]any{"i": float64(i)})))
	}
	if m.BodyBytes() != wantBytes {
		t.Fatalf("body_bytes gauge %d, want %d", m.BodyBytes(), wantBytes)
	}
}

func diff883(got, want []string) (extra, missing []string) {
	w := map[string]bool{}
	for _, k := range want {
		w[k] = true
	}
	g := map[string]bool{}
	for _, k := range got {
		g[k] = true
		if !w[k] {
			extra = append(extra, k)
		}
	}
	for _, k := range want {
		if !g[k] {
			missing = append(missing, k)
		}
	}
	return extra, missing
}

// TestIssue883_F4_MemoryFloor_ChurnedDepsDoNotAccumulate — the rising-floor arm.
// Each round stores a batch of entries nobody will ever look up again (a fresh
// identity / coordinate per round, the bench's churn), then the informer churns
// their deps. A stable population on an untouched GVR stays. After every round
// the memo must hold exactly the stable population — entries AND bytes — so its
// size is flat in the number of rounds. Before #883 it grew by the batch size
// every round.
func TestIssue883_F4_MemoryFloor_ChurnedDepsDoNotAccumulate(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	m := NewSeedResolveMemo()
	defer TrackDepMoves(m)()

	const stable, rounds, batch = 25, 40, 30
	var stableBytes int64
	for i := 0; i < stable; i++ {
		dk := DepKey{GVR: gvr883Calm, Namespace: "ns-stable", Name: fmt.Sprintf("s-%d", i)}
		record883(t, d, "L1_883_stable", dk)
		b := memoBody(map[string]any{"stable": float64(i)})
		stableBytes += int64(len(b))
		m.Store(fmt.Sprintf("stable-%d", i), b, []DepKey{dk}, DepGenEpochNow())
	}

	for r := 0; r < rounds; r++ {
		stamp := DepGenEpochNow()
		coords := make([]DepKey, 0, batch)
		for i := 0; i < batch; i++ {
			ns := fmt.Sprintf("ns-%d", i%3)
			dk := DepKey{GVR: gvr883Hot, Namespace: ns, Name: fmt.Sprintf("obj-r%d-%d", r, i)}
			deps := []DepKey{dk, {GVR: gvr883Hot, Namespace: ns, Name: listWildcard}}
			for _, x := range deps {
				record883(t, d, fmt.Sprintf("L1_883_round_%d", r), x)
			}
			// A key per (round, i): a user who is never resolved for again.
			m.Store(fmt.Sprintf("u-r%d-%d", r, i), memoBody(map[string]any{"r": float64(r), "i": float64(i)}), deps, stamp)
			coords = append(coords, dk)
		}
		for _, dk := range coords {
			d.OnUpdate(dk.GVR, dk.Namespace, dk.Name)
		}
		if got := m.Entries(); got != stable {
			t.Fatalf("#883 F4 RED (memory floor): after round %d the memo holds %d entries, want the %d live ones (%d churned bodies retained)",
				r, got, stable, got-stable)
		}
		if got := m.BodyBytes(); got != stableBytes {
			t.Fatalf("#883 F4 RED (memory floor): after round %d body_bytes=%d, want %d", r, got, stableBytes)
		}
	}
	if keys := memoKeys883(m); len(keys) != stable {
		t.Fatalf("memo map holds %d keys, gauge says %d", len(keys), stable)
	}
}
