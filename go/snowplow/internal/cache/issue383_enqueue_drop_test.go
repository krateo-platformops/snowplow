//go:build unit || integration

// issue383_enqueue_drop_test.go — #383: the refresher's dirty-mark hook DROPS the
// queue slot (and the trigger-GVR store) for a key that is NOT resident in L1,
// using #374's side-effect-free Has(); SubmitSliceabilityInvalidate keeps firing
// for every mark (the #91 Lever C split).
//
// Safety rests on #375 (+ #408 for the boot carrier): a resolve in flight across
// the dep change remarks its key at the accepted Put, through the SAME hook, when
// the key is resident — so the dropped mark loses no freshness.
//
// Every arm runs the real refresher (StartRefresher: the production hook closure,
// real workers, real handler dispatch), the real dep tracker (Record / OnUpdate →
// bumpCoordinateGen → fan-out → hook) and the real resolved-L1 singleton.
//
// NEUTERS (recorded in the PR):
//   - arm 1: remarkIfDepsMoved returns immediately (#375 + #408 remark unwired)
//     → both carriers stay STALE → RED;
//   - arm 2: SubmitSliceabilityInvalidate moved inside the residency gate → RED;
//   - arm 3: drop every mark (gate on false) → RED;
//   - arm 4: same neuter as arm 1 (remark unwired) → RED; the seam proves the
//     Put lands inside the check→decision window;
//   - arm 5: residency gate removed (today's main) → enqueue_total == burst → RED;
//   - arm 6: multi-trigger hook not installed AND the trigger-merge hook left ungated
//     → orphan triggers → RED;
//   - arm 7: the multi-trigger hook not installed (remark falls back to merge-per-
//     trigger + hook) → 3 residency decisions for one remark → RED;
//   - arm 8: the counter not published in the residency map → RED.

package cache

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var g383Dep = schema.GroupVersionResource{Group: "dep.example.io", Version: "v1", Resource: "backings"}

// i383Backing is the "apiserver": the value a resolve of a key reads from its dep.
type i383Backing struct {
	mu sync.Mutex
	v  map[string]string
}

func (b *i383Backing) get(name string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v[name]
}

func (b *i383Backing) set(name, val string) {
	b.mu.Lock()
	b.v[name] = val
	b.mu.Unlock()
}

// start383 boots the real refresher with a "widgets" handler that re-resolves a
// key from the backing (Record its dep on the resolve ctx, ReplaceIfGen the
// value). Rate floor 0 (the shared harness default).
func start383(t *testing.T) (*ResolvedCacheStore, *i383Backing, *atomic.Int32) {
	t.Helper()
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	c := ResolvedCache()
	if c == nil {
		t.Fatal("PRECONDITION: resolved cache must be on")
	}
	b := &i383Backing{v: map[string]string{}}
	var reResolves atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		reResolves.Add(1)
		gen0 := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k)
		v := b.get(used.Name)
		Deps().Record(rctx, k, g383Dep, "ns", used.Name)
		c.ReplaceIfGen(rctx, k, &ResolvedEntry{RawJSON: []byte(`"` + v + `"`), Inputs: &used}, gen0)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	return c, b, &reResolves
}

func key383(name string) (string, ResolvedKeyInputs) {
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: name}
	return ComputeKey(in), in
}

func body383(c *ResolvedCacheStore, k string) string {
	e, ok := c.GetNoTouch(k)
	if !ok || e == nil {
		return "<absent>"
	}
	return string(e.RawJSON)
}

// arm 1 — THE load-bearing freshness race. A real in-flight resolve R of a
// NON-resident key K reads its dep (v0); the dep then changes (v1, real OnUpdate)
// while K is still non-resident, so #383 DROPS the dirty-mark at the hook; R then
// Puts v0. With #375 (customer PutIfGen) / #408 (boot PutThenRemark) R's Put
// remarks K through the hook — now resident → enqueued → the refresher
// re-resolves → K converges FRESH (v1). Without the remark K stays STALE (v0).
func TestIssue383_Arm1_DroppedMarkInFlightResolve_ConvergesFreshViaRemark(t *testing.T) {
	for _, carrier := range []string{"customer-PutIfGen-375", "boot-PutThenRemark-408"} {
		t.Run(carrier, func(t *testing.T) {
			c, b, reResolves := start383(t)
			name := "arm1-" + carrier
			k, in := key383(name)
			b.set(name, "v0")

			dropped0 := EnqueueDroppedNonResidentTotal()
			// R: resolve entry (startSeq), read v0, Record the dep.
			rctx := WithL1KeyContext(context.Background(), k)
			gen0 := c.CaptureGen(k)
			read := b.get(name)
			Deps().Record(rctx, k, g383Dep, "ns", name)
			// The dep changes while K is NOT resident: the hook drops the mark.
			b.set(name, "v1")
			Deps().OnUpdate(g383Dep, "ns", name)
			if d := EnqueueDroppedNonResidentTotal() - dropped0; d != 1 {
				t.Fatalf("PRECONDITION: the dirty-mark for non-resident K must be DROPPED at the hook (#383); "+
					"dropped delta=%d", d)
			}
			// R's Put of the pre-change body.
			e := &ResolvedEntry{RawJSON: []byte(`"` + read + `"`), Inputs: &in}
			if carrier == "customer-PutIfGen-375" {
				if !c.PutIfGen(rctx, k, e, gen0) {
					t.Fatal("setup: PutIfGen refused")
				}
			} else {
				c.PutThenRemark(rctx, k, e)
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && body383(c, k) != `"v1"` {
				time.Sleep(5 * time.Millisecond)
			}
			if got := body383(c, k); got != `"v1"` {
				t.Fatalf("#383 arm 1 RED (%s): K holds %s after the dep moved to v1 — the dirty-mark was dropped "+
					"(K non-resident) and nothing re-marked K at its Put. The drop is safe ONLY with the #375/#408 "+
					"put-then-remark wired (re-resolves=%d)", carrier, got, reResolves.Load())
			}
		})
	}
}

// arm 2 — the SSI split. A cell-less stuck-false RAFullList raKey (dep edge, no L1
// cell) gets a backing event: the enqueue is DROPPED (non-resident) but
// SubmitSliceabilityInvalidate STILL fires, so the stuck-false memo clears and
// the next /call re-verifies.
func TestIssue383_Arm2_CellLessStuckFalseRAKey_EnqueueDroppedButSSIStillFires(t *testing.T) {
	c, _, _ := start383(t)
	t.Setenv(envSliceabilityReverifyRateFloorSeconds, "0")
	resetSliceabilityMemoForTest()
	t.Cleanup(resetSliceabilityMemoForTest)
	resetSliceabilityReverifyWorkerForTest()
	t.Cleanup(resetSliceabilityReverifyWorkerForTest)
	sctx, scancel := context.WithCancel(context.Background())
	t.Cleanup(scancel)
	StartSliceabilityReverifier(sctx)

	const raKey, shape = "i383-rakey-stuck-false", "i383-shape"
	RecordSliceability(raKey, shape, false)
	if _, known := SliceabilityLookup(raKey, shape); !known {
		t.Fatal("PRECONDITION: the stuck-false verdict must be memoized")
	}
	Deps().Record(context.Background(), raKey, g383Dep, "ns", "ra-backing")
	if c.Has(raKey) {
		t.Fatal("PRECONDITION: the stuck-false raKey must be cell-less (non-resident)")
	}
	enq0, dropped0 := refresherSingleton().enqueueTotal.Load(), EnqueueDroppedNonResidentTotal()
	ssi0 := SliceabilityReverifyStatsSnapshot().EnqueuedTotal

	Deps().OnUpdate(g383Dep, "ns", "ra-backing")

	if d := EnqueueDroppedNonResidentTotal() - dropped0; d != 1 {
		t.Errorf("#383 arm 2: the non-resident raKey's enqueue must be dropped; dropped delta=%d", d)
	}
	if d := refresherSingleton().enqueueTotal.Load() - enq0; d != 0 {
		t.Errorf("#383 arm 2: no refresher enqueue for a cell-less key; enqueue_total delta=%d", d)
	}
	if d := SliceabilityReverifyStatsSnapshot().EnqueuedTotal - ssi0; d != 1 {
		t.Fatalf("#383 arm 2 RED (#91 Lever C): SubmitSliceabilityInvalidate must STILL fire for the dropped "+
			"cell-less raKey; ssi enqueued delta=%d", d)
	}
	waitFor(t, 3*time.Second, "the stuck-false memo to clear", func() bool {
		_, known := SliceabilityLookup(raKey, shape)
		return !known
	})
}

// arm 3 — a RESIDENT key is never dropped: its dep change is enqueued and the
// refresher re-resolves it to the new value.
func TestIssue383_Arm3_ResidentKey_NeverDropped_Refreshed(t *testing.T) {
	c, b, reResolves := start383(t)
	k, in := key383("arm3")
	b.set("arm3", "v0")
	rctx := WithL1KeyContext(context.Background(), k)
	gen0 := c.CaptureGen(k)
	Deps().Record(rctx, k, g383Dep, "ns", "arm3")
	if !c.PutIfGen(rctx, k, &ResolvedEntry{RawJSON: []byte(`"v0"`), Inputs: &in}, gen0) {
		t.Fatal("setup: PutIfGen refused")
	}
	enq0, dropped0 := refresherSingleton().enqueueTotal.Load(), EnqueueDroppedNonResidentTotal()
	b.set("arm3", "v1")
	Deps().OnUpdate(g383Dep, "ns", "arm3")
	if d := EnqueueDroppedNonResidentTotal() - dropped0; d != 0 {
		t.Fatalf("#383 arm 3 RED: a RESIDENT key's dirty-mark was dropped (dropped delta=%d)", d)
	}
	if d := refresherSingleton().enqueueTotal.Load() - enq0; d != 1 {
		t.Fatalf("#383 arm 3 RED: the resident key must be enqueued exactly once; enqueue_total delta=%d", d)
	}
	waitFor(t, 3*time.Second, "the resident key to refresh to v1", func() bool { return body383(c, k) == `"v1"` })
	if reResolves.Load() < 1 {
		t.Fatal("#383 arm 3: no refresher re-resolve ran")
	}
}

// arm 4 — the check→resident window. The key becomes resident (R's accepted Put
// commits) AFTER the hook's Has() said non-resident and BEFORE the drop takes
// effect. The seam runs R's Put exactly inside that window. Covered by #375: the
// dep bump precedes the fan-out (and so the Has check), so R's put-check sees the
// dep moved and remarks K — now resident → enqueued → FRESH.
func TestIssue383_Arm4_ResidentRightAfterHasCheck_CoveredByRemark(t *testing.T) {
	c, b, _ := start383(t)
	k, in := key383("arm4")
	b.set("arm4", "v0")
	rctx := WithL1KeyContext(context.Background(), k)
	gen0 := c.CaptureGen(k)
	read := b.get("arm4")
	Deps().Record(rctx, k, g383Dep, "ns", "arm4")

	var putInWindow atomic.Bool
	t.Cleanup(SetRefreshHookResidencyCheckedHookForTest(func(key string, resident bool) {
		if key != k || resident || putInWindow.Load() {
			return
		}
		putInWindow.Store(true)
		done := make(chan struct{})
		go func() { // R's Put lands between Has()=false and the drop
			defer close(done)
			c.PutIfGen(rctx, k, &ResolvedEntry{RawJSON: []byte(`"` + read + `"`), Inputs: &in}, gen0)
		}()
		<-done
		if !c.Has(k) {
			t.Errorf("PRECONDITION: K must be resident once R's Put returns")
		}
	}))
	b.set("arm4", "v1")
	Deps().OnUpdate(g383Dep, "ns", "arm4")
	if !putInWindow.Load() {
		t.Fatal("PRECONDITION: the seam must run R's Put inside the check→drop window")
	}
	waitFor(t, 3*time.Second, "K to converge FRESH after a Put inside the check→drop window", func() bool {
		return body383(c, k) == `"v1"`
	})
}

// arm 5 — occupancy. A burst of dirty-marks on N non-resident keys (the 057
// shape: marks for cells that are not resident) must not occupy the queue:
// enqueue_total delta 0, dropped delta N, queue length 0. Resident keys in the
// same burst are still enqueued (the drop discriminates on residency only).
func TestIssue383_Arm5_NonResidentBurst_OccupancyCut(t *testing.T) {
	c, _, _ := start383(t)
	const nonResident, resident = 500, 20
	for i := 0; i < nonResident; i++ {
		Deps().Record(context.Background(), "i383-nr-"+strconv.Itoa(i), g383Dep, "ns", "burst")
	}
	for i := 0; i < resident; i++ {
		k, in := key383("arm5-res-" + strconv.Itoa(i))
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`"r"`), Inputs: &in})
		Deps().Record(context.Background(), k, g383Dep, "ns", "burst")
	}
	r := refresherSingleton()
	enq0, dropped0 := r.enqueueTotal.Load(), EnqueueDroppedNonResidentTotal()
	Deps().OnUpdate(g383Dep, "ns", "burst")
	enq := r.enqueueTotal.Load() - enq0
	dropped := EnqueueDroppedNonResidentTotal() - dropped0
	t.Logf("#383 arm 5: burst of %d marks (%d non-resident, %d resident) → enqueue_total +%d, dropped_non_resident +%d, queue len %d",
		nonResident+resident, nonResident, resident, enq, dropped, r.queue.Len())
	if dropped != nonResident || enq != resident {
		t.Fatalf("#383 arm 5 RED: a non-resident burst must not occupy the queue — want enqueue_total +%d and "+
			"dropped +%d, got enqueue_total +%d dropped +%d", resident, nonResident, enq, dropped)
	}
	if l := r.queue.Len(); l > resident {
		t.Fatalf("#383 arm 5 RED: queue length %d exceeds the %d resident marks", l, resident)
	}
}

// arm 6 — the multi-GVR trigger merge is residency-gated with the enqueue. A
// #375 remark carrying several moved GVRs merges all but the last through the
// merge hook BEFORE the hook call (enqueueRemark). If the key is not resident at
// that point (evicted between its Put and the remark), the hook drops it, and an
// ungated merge would leave the earlier triggers orphaned in triggerGVRByKey
// (consumed only by a dequeue that never comes).
func TestIssue383_Arm6_MultiGVRRemark_NonResident_NoOrphanTriggers(t *testing.T) {
	c, _, _ := start383(t)
	k, in := key383("arm6")
	if c.Has(k) {
		t.Fatal("PRECONDITION: key must be non-resident")
	}
	gA := schema.GroupVersionResource{Group: "a.example.io", Version: "v1", Resource: "as"}
	gB := schema.GroupVersionResource{Group: "b.example.io", Version: "v1", Resource: "bs"}
	r := refresherSingleton()
	StopRefresher() // no dequeue: the trigger set stays observable
	dropped0 := EnqueueDroppedNonResidentTotal()
	Deps().enqueueRemark(k, []schema.GroupVersionResource{gA, gB, g383Dep})
	if d := EnqueueDroppedNonResidentTotal() - dropped0; d != 1 {
		t.Fatalf("PRECONDITION: the remark's single hook call must be dropped (non-resident); dropped delta=%d", d)
	}
	if v, ok := r.triggerGVRByKey.Load(k); ok {
		t.Fatalf("#383 arm 6 RED: a dropped multi-GVR remark left orphan triggers %+v in triggerGVRByKey — the "+
			"merge hook must be residency-gated like the enqueue", v)
	}

	// Control: resident → all three triggers merged and the key enqueued once.
	c.Put(k, &ResolvedEntry{RawJSON: []byte(`"r"`), Inputs: &in})
	enq0 := r.enqueueTotal.Load()
	Deps().enqueueRemark(k, []schema.GroupVersionResource{gA, gB, g383Dep})
	if d := r.enqueueTotal.Load() - enq0; d != 1 {
		t.Fatalf("control: a resident remark must enqueue once; delta=%d", d)
	}
	v, ok := r.triggerGVRByKey.Load(k)
	set, _ := v.(*triggerGVRSet)
	if !ok || set == nil || len(set.gvrs) != 3 {
		t.Fatalf("control: a resident multi-GVR remark must carry all 3 triggers; got %+v", v)
	}
}

// arm 7 — ONE residency decision per remark. A #375 remark with three moved GVRs reaches
// the refresher as one multi-trigger hook call, so the refresher decides residency once
// and merges all three triggers (or drops all three). Before this, enqueueRemark merged
// the first two through the merge hook and then called the single hook: three separate
// decisions, and a key evicted between them was resident for the merges and dropped at
// the enqueue, which orphaned the merged triggers.
func TestIssue383_Arm7_MultiGVRRemark_OneResidencyDecision(t *testing.T) {
	c, _, _ := start383(t)
	k, in := key383("arm7")
	c.Put(k, &ResolvedEntry{RawJSON: []byte(`"r"`), Inputs: &in})
	r := refresherSingleton()
	StopRefresher() // no dequeue: the trigger set stays observable
	var decisions atomic.Int32
	t.Cleanup(SetRefreshHookResidencyCheckedHookForTest(func(key string, _ bool) {
		if key == k {
			decisions.Add(1)
		}
	}))
	gA := schema.GroupVersionResource{Group: "a.example.io", Version: "v1", Resource: "as"}
	gB := schema.GroupVersionResource{Group: "b.example.io", Version: "v1", Resource: "bs"}
	enq0 := r.enqueueTotal.Load()
	Deps().enqueueRemark(k, []schema.GroupVersionResource{gA, gB, g383Dep})
	if n := decisions.Load(); n != 1 {
		t.Fatalf("#383 arm 7 RED: one remark made %d residency decisions, want exactly 1 — a key can then be "+
			"resident for the trigger merges and dropped at the enqueue (orphan triggers)", n)
	}
	if d := r.enqueueTotal.Load() - enq0; d != 1 {
		t.Fatalf("#383 arm 7: a resident remark must enqueue once; delta=%d", d)
	}
	v, _ := r.triggerGVRByKey.Load(k)
	if set, _ := v.(*triggerGVRSet); set == nil || len(set.gvrs) != 3 {
		t.Fatalf("#383 arm 7: a resident remark must carry all 3 triggers; got %+v", v)
	}
}

// arm 8 — the drop counter is PUBLISHED: read through the real expvar handler (the
// /debug/vars route), not the Go accessor, after a real dropped mark.
func TestIssue383_Arm8_DroppedCounterReadableAtDebugVars(t *testing.T) {
	_, _, _ = start383(t)
	RegisterResidencyMetrics374ExpvarForTest()
	read := func() map[string]uint64 {
		t.Helper()
		rec := httptest.NewRecorder()
		expvar.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("/debug/vars returned %d", rec.Code)
		}
		var all map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
			t.Fatalf("decoding /debug/vars: %v", err)
		}
		raw, ok := all["snowplow_refresher_residency_cheapen"]
		if !ok {
			t.Fatal("snowplow_refresher_residency_cheapen is not published at /debug/vars")
		}
		var m map[string]uint64
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("decoding snowplow_refresher_residency_cheapen: %v", err)
		}
		return m
	}
	before := read()
	if _, ok := before["enqueue_dropped_non_resident"]; !ok {
		t.Fatalf("#383 arm 8 RED: /debug/vars snowplow_refresher_residency_cheapen has no "+
			"enqueue_dropped_non_resident key (got %v)", before)
	}
	if _, ok := before["pickup_noop_no_park"]; !ok {
		t.Fatalf("#383 arm 8: the #374 key pickup_noop_no_park must stay published (got %v)", before)
	}
	Deps().Record(context.Background(), "i383-arm8-nonresident", g383Dep, "ns", "arm8")
	Deps().OnUpdate(g383Dep, "ns", "arm8")
	after := read()
	if d := after["enqueue_dropped_non_resident"] - before["enqueue_dropped_non_resident"]; d != 1 {
		t.Fatalf("#383 arm 8: one dropped mark must move /debug/vars enqueue_dropped_non_resident by 1; got %d", d)
	}
}
