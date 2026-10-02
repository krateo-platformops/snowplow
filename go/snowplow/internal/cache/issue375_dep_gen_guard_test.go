// issue375_dep_gen_guard_test.go — #375 dependency-generation guard (option c,
// resolve-START epoch, PUT-THEN-REMARK): the cache-package falsifier arms.
//
// THE DEFECT (#375): dep X changes → recordDirtyMarks enqueues dependent K → the
// refresher dequeues K while it is NOT resident (skipped_no_entry consumes the mark)
// → a resolve R of K that started BEFORE the change Puts K AFTER the dequeue → K is
// stale with no pending mark until #316 (TTL/4). #189 PutIfGen guards K's OWN gen,
// not its DEPS'.
//
// THE GUARD: a resolve captures startSeq = depEventSeq at ENTRY (WithL1KeyContext /
// WithDepGenSink); every recorded dep lands in the per-resolve sink; the handler-only
// bumpCoordinateGen (OnObjectEvent, R1) stamps the changed coordinate's buckets +
// GVR floor BEFORE the dirty-mark fan-out (R2); an ACCEPTED gen-guarded Put remarks
// K ONCE if any recorded dep moved after startSeq.
//
// Every arm drives the REAL boundary: real OnObjectEvent (via OnAdd/OnUpdate/OnDelete,
// the test shims over it) for every churn, real gen-guarded Puts on a real store, the
// real remark funnel (remarkIfDepsMoved → enqueueRemark → refresher hook). Remarks are
// COUNTED through depGenRemarkObserver, a channel separate from the dirty-mark fan-out.
// Each arm's neuter (the mutation that must turn it RED) is named in its comment and
// recorded in the PR.

package cache

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	g375A = schema.GroupVersionResource{Group: "a.example.io", Version: "v1", Resource: "foos"}
	g375B = schema.GroupVersionResource{Group: "b.example.io", Version: "v1", Resource: "bars"}
)

// remarkLog counts PUT-THEN-REMARKs per key via the observer seam.
type remarkLog struct {
	mu      sync.Mutex
	byKey   map[string]int
	reasons map[string]int
}

func (l *remarkLog) count(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.byKey[key]
}

func (l *remarkLog) total() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, v := range l.byKey {
		n += v
	}
	return n
}

func (l *remarkLog) reason(r string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reasons[r]
}

// hookEvent is one call of the refresher hook (dirty-mark OR remark), with whether
// the key was RESIDENT at that instant — a mark that lands on a non-resident key is
// the one the refresher's skipped_no_entry consumes (the #375 race).
type hookEvent struct {
	key      string
	gvr      schema.GroupVersionResource
	resident bool
}

type hookLog struct {
	mu  sync.Mutex
	evs []hookEvent
}

func (h *hookLog) residentMarks(key string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, e := range h.evs {
		if e.key == key && e.resident {
			n++
		}
	}
	return n
}

// setup375 gives each arm a clean Deps() tracker wired to a fresh store, a remark
// log, and (optionally) a refresher-hook recorder.
func setup375(t *testing.T) (*ResolvedCacheStore, *remarkLog, *hookLog) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	resetRefresherForTest()
	resetDepsForTest()
	c := newResolvedCache(256, 1<<22, time.Hour)
	Deps().SetStore(c)
	rl := &remarkLog{byKey: map[string]int{}, reasons: map[string]int{}}
	restoreObs := SetDepGenRemarkObserverForTest(func(k, reason string) {
		rl.mu.Lock()
		rl.byKey[k]++
		rl.reasons[reason]++
		rl.mu.Unlock()
	})
	hl := &hookLog{}
	Deps().SetRefreshHook(func(k string, g schema.GroupVersionResource) {
		_, resident := c.Get(k)
		hl.mu.Lock()
		hl.evs = append(hl.evs, hookEvent{key: k, gvr: g, resident: resident})
		hl.mu.Unlock()
	})
	t.Cleanup(func() {
		restoreObs()
		resetDepsForTest()
		resetRefresherForTest()
	})
	return c, rl, hl
}

func body375(s string) *ResolvedEntry { return &ResolvedEntry{RawJSON: []byte(s)} }

// ---------------------------------------------------------------------------
// arm-1 (cache-level) — WINDOW discriminator: churn INSIDE [data read → Record].
// The resolve reads its dep (data@N), the dep churns (real OnObjectEvent), and only
// THEN does the resolve Record it. Under (c) startSeq was captured at ENTRY, so the
// churn is > startSeq → remark. NEUTER (a) "capture at Record" (recordInternal
// re-captures s.startSeq on append) → the baseline is post-churn → NO remark → RED.
// The load-bearing ms-class variant at apistage (read :605 → Record :661) lives in
// restactions/api/issue375_dep_gen_guard_test.go.
// ---------------------------------------------------------------------------
func TestIssue375_Arm1_ChurnInReadToRecordWindow_Remarks(t *testing.T) {
	c, rl, _ := setup375(t)
	const K = "arm1-K"
	// WARM bucket: another dependent already depends on the coordinate.
	Deps().Record(context.Background(), "arm1-other", g375A, "ns1", "obj")

	ctx := WithL1KeyContext(context.Background(), K) // startSeq @ entry
	gen0 := c.CaptureGen(K)
	// ... the resolve READS obj (data@N) here ...
	Deps().OnUpdate(g375A, "ns1", "obj") // churn → N+1 lands INSIDE [read, Record]
	Deps().Record(ctx, K, g375A, "ns1", "obj")
	if !c.PutIfGen(ctx, K, body375(`{"obj":"N"}`), gen0) {
		t.Fatalf("setup: PutIfGen refused")
	}
	if got := rl.count(K); got != 1 {
		t.Fatalf("#375 arm-1 RED: churn inside [data-read → Record] must PUT-THEN-REMARK K exactly once "+
			"(startSeq is captured at resolve ENTRY, before the read) — got %d remarks. A capture-at-Record "+
			"baseline sees the post-churn seq and accepts the stale body silently.", got)
	}
}

// arm-1b — COLD coordinate (fix ii). No bucket exists for the coordinate when it
// churns (nothing depended on it; empty buckets are pruned), so bumpCoordinateGen has
// no bucket to stamp; the resolve's later Record CREATES the bucket. The bucket must
// inherit the GVR floor, else lastBumpSeq=0 and the stale body is accepted with NO
// remark and NO dirty-mark. RED @38f18496 (no GVR floor); NEUTER: drop the floor
// inheritance in recordInternal + the depMovedSince fallback.
func TestIssue375_Arm1b_ColdCoordinateChurn_RemarksViaGVRFloor(t *testing.T) {
	c, rl, hl := setup375(t)
	const K = "arm1b-K"
	if _, ok := DepBucketLastBumpSeqForTest(g375A, "ns1", "cold"); ok {
		t.Fatalf("setup: coordinate must be COLD (no bucket)")
	}
	ctx := WithL1KeyContext(context.Background(), K)
	gen0 := c.CaptureGen(K)
	Deps().OnUpdate(g375A, "ns1", "cold") // churn on a coordinate nobody depends on yet
	Deps().Record(ctx, K, g375A, "ns1", "cold")
	if !c.PutIfGen(ctx, K, body375(`{}`), gen0) {
		t.Fatalf("setup: PutIfGen refused")
	}
	if got := rl.count(K); got != 1 {
		t.Fatalf("#375 arm-1b RED (cold coordinate): got %d remarks, want 1 — the cold bucket created by "+
			"Record must inherit the GVR's last-bump floor; at lastBumpSeq=0 the stale body is accepted", got)
	}
	if n := len(hl.evs); n != 1 {
		t.Fatalf("#375 arm-1b: the churn found no dependent (cold), so the ONLY hook call must be the "+
			"remark; got %d hook calls", n)
	}
}

// arm-1b anti-amp — a COLD resolve whose GVR saw NO event after startSeq does not
// remark, even while a DIFFERENT GVR churns (the floor is per-GVR, not global).
// NEUTER: a GLOBAL floor (gvrLastBumpSeq returns depEventSeq) → RED.
func TestIssue375_Arm1b_ColdCoordinate_OtherGVRChurn_NoRemark(t *testing.T) {
	c, rl, _ := setup375(t)
	const K = "arm1b-anti-K"
	Deps().OnUpdate(g375A, "ns1", "cold") // A's floor moves BEFORE the resolve starts
	ctx := WithL1KeyContext(context.Background(), K)
	gen0 := c.CaptureGen(K)
	for i := 0; i < 5; i++ {
		Deps().OnUpdate(g375B, "ns1", "other") // churn on a DIFFERENT GVR inside the window
	}
	Deps().Record(ctx, K, g375A, "ns1", "cold")
	if !c.PutIfGen(ctx, K, body375(`{}`), gen0) {
		t.Fatalf("setup: PutIfGen refused")
	}
	if got := rl.count(K); got != 0 {
		t.Fatalf("#375 arm-1b anti-amp RED: a cold resolve whose own GVR saw no event after startSeq "+
			"remarked %d times — churn on an UNRELATED GVR must not amplify", got)
	}
}

// ---------------------------------------------------------------------------
// arm-2 — IDLE, zero amplification. No dep bumped after startSeq → zero remarks,
// across many resolves, including deps that churned BEFORE the resolve started.
// NEUTER: compare `>=` instead of `>` (or treat any recorded dep as moved) → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm2_IdleNoChurnAfterStart_ZeroRemarks(t *testing.T) {
	c, rl, _ := setup375(t)
	for i := 0; i < 50; i++ {
		k := "arm2-K"
		// Churn BEFORE entry — already visible to the resolve's read, not a move.
		Deps().OnUpdate(g375A, "ns1", "obj")
		Deps().OnUpdate(g375A, "ns1", "")
		ctx := WithL1KeyContext(context.Background(), k)
		gen0 := c.CaptureGen(k)
		Deps().Record(ctx, k, g375A, "ns1", "obj")
		Deps().RecordList(ctx, k, g375A, "ns1")
		if !c.PutIfGen(ctx, k, body375(`{}`), gen0) {
			t.Fatalf("setup: PutIfGen refused")
		}
	}
	if got := rl.total(); got != 0 {
		t.Fatalf("#375 arm-2 RED: %d remarks across 50 idle resolves — with no dep event after startSeq "+
			"the guard must be a pure read-compare (the 0.30.185 amplification class)", got)
	}
}

// ---------------------------------------------------------------------------
// arm-3 — a #189-REFUSED Put writes nothing → NO remark, even though a recorded
// dep moved. NEUTER: call remarkIfDepsMoved on the refused branch → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm3_S189RefusedPut_NoRemark(t *testing.T) {
	c, rl, _ := setup375(t)
	const K = "arm3-K"
	c.Put(K, body375(`{"v":"pre"}`))
	gen0 := c.CaptureGen(K)
	ctx := WithL1KeyContext(context.Background(), K)
	Deps().Record(ctx, K, g375A, "ns1", "obj")
	Deps().OnUpdate(g375A, "ns1", "obj") // dep moved
	c.DeleteForTest(K)                   // and K was DELETE-evicted during the resolve
	if c.PutIfGen(ctx, K, body375(`{"v":"stale"}`), gen0) {
		t.Fatalf("setup: #189 must refuse the Put")
	}
	if got := rl.count(K); got != 0 {
		t.Fatalf("#375 arm-3 RED: a #189-refused PutIfGen remarked %d times — a refused Put stored "+
			"nothing; remarking it resurrects nothing but churns the refresher", got)
	}
	// ReplaceIfGen (refresher carrier): absent key → refused → no remark.
	if c.ReplaceIfGen(ctx, K, body375(`{}`), gen0) {
		t.Fatalf("setup: ReplaceIfGen of an absent key must refuse")
	}
	if got := rl.count(K); got != 0 {
		t.Fatalf("#375 arm-3 RED: a refused ReplaceIfGen remarked %d times", got)
	}
}

// ---------------------------------------------------------------------------
// arm-4 / arm-5 — LIST-wildcard member ADD / REMOVE. The resolve recorded ONLY the
// LIST edge (gvr, ns, "*"), never the member; a member add/delete is an event on the
// MEMBER coordinate, which must stamp the list bucket. Both the ns-scoped list and
// the cluster-wide list. NEUTER: drop the listWildcard stamps in bumpCoordinateGen.
// ---------------------------------------------------------------------------
func TestIssue375_Arm4_ListMemberAdd_RemarksViaListBucket(t *testing.T) {
	for _, tc := range []struct{ name, listNS string }{{"ns-list", "ns1"}, {"cluster-list", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			c, rl, _ := setup375(t)
			K := "arm4-" + tc.name
			ctx := WithL1KeyContext(context.Background(), K)
			gen0 := c.CaptureGen(K)
			Deps().RecordList(ctx, K, g375A, tc.listNS)
			Deps().OnAdd(g375A, "ns1", "new-member") // an UNREAD member appears
			if !c.PutIfGen(ctx, K, body375(`{"items":[]}`), gen0) {
				t.Fatalf("setup: PutIfGen refused")
			}
			if got := rl.count(K); got != 1 {
				t.Fatalf("#375 arm-4 RED (%s): a member ADD under a recorded LIST edge must remark once, got %d",
					tc.name, got)
			}
		})
	}
}

func TestIssue375_Arm5_ListMemberRemove_RemarksViaListBucket(t *testing.T) {
	for _, tc := range []struct{ name, listNS string }{{"ns-list", "ns1"}, {"cluster-list", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			c, rl, _ := setup375(t)
			K := "arm5-" + tc.name
			ctx := WithL1KeyContext(context.Background(), K)
			gen0 := c.CaptureGen(K)
			Deps().RecordList(ctx, K, g375A, tc.listNS)
			Deps().OnDelete(g375A, "ns1", "member-x") // a member leaves
			if !c.PutIfGen(ctx, K, body375(`{"items":[{"x":1}]}`), gen0) {
				t.Fatalf("setup: PutIfGen refused")
			}
			if got := rl.count(K); got != 1 {
				t.Fatalf("#375 arm-5 RED (%s): a member DELETE under a recorded LIST edge must remark once, got %d",
					tc.name, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// arm-6 — WORST CASE K>1 resolves × M>1 interleaved mutations. Four concurrent
// (interleaved) resolves over overlapping dep sets, three mutations at fixed points.
// Oracle: a resolve remarks EXACTLY once iff some recorded dep had an event after its
// startSeq and before its Put-check; else zero. r1 sees TWO moved deps → still 1.
// NEUTERS: per-dep remark (drop the once) → r1 counts 2 → RED; (a) capture-at-Record
// → r0/r1 miss → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm6_KResolvesTimesMMutations_ExactOracle(t *testing.T) {
	c, rl, _ := setup375(t)
	type res struct {
		key  string
		ctx  context.Context
		gen0 uint64
	}
	start := func(k string) *res {
		return &res{key: k, ctx: WithL1KeyContext(context.Background(), k), gen0: c.CaptureGen(k)}
	}
	rec := func(r *res, name string) { Deps().Record(r.ctx, r.key, g375A, "ns", name) }
	put := func(r *res) {
		if !c.PutIfGen(r.ctx, r.key, body375(`{}`), r.gen0) {
			t.Fatalf("setup: PutIfGen(%s) refused", r.key)
		}
	}
	r0 := start("arm6-r0")
	rec(r0, "x")
	r1 := start("arm6-r1")
	rec(r1, "x")
	rec(r1, "y")
	Deps().OnUpdate(g375A, "ns", "x") // m1: moves x → r0, r1
	r2 := start("arm6-r2")
	rec(r2, "x") // x moved BEFORE r2 started → not a move for r2
	rec(r2, "z")
	Deps().OnUpdate(g375A, "ns", "y") // m2: moves y → r1 (second moved dep)
	put(r0)
	r3 := start("arm6-r3")
	rec(r3, "w")
	Deps().OnUpdate(g375A, "ns", "z") // m3: moves z → r2
	put(r1)
	put(r2)
	put(r3)

	want := map[string]int{"arm6-r0": 1, "arm6-r1": 1, "arm6-r2": 1, "arm6-r3": 0}
	for k, w := range want {
		if got := rl.count(k); got != w {
			t.Errorf("#375 arm-6 RED: %s remarks=%d want %d (oracle: once iff a recorded dep moved in "+
				"(startSeq, Put]; never per-dep)", k, got, w)
		}
	}
}

// ---------------------------------------------------------------------------
// arm-8 — REFRESHER re-resolve self-remark is BOUNDED and correct. A real refresher
// worker re-resolves K under WithL1KeyContext (the resolve_populate entry shape) and
// ReplaceIfGen's it. The dep churns inside the FIRST re-resolve → that re-resolve
// self-remarks once → the second re-resolve sees no churn → the loop STOPS.
// NEUTER: depMovedSince always true (remark every accepted Put) → the refresher
// re-resolves K forever → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm8_RefresherReResolveSelfRemark_Bounded(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	c := ResolvedCache()
	rl := &remarkLog{byKey: map[string]int{}, reasons: map[string]int{}}
	defer SetDepGenRemarkObserverForTest(func(k, reason string) {
		rl.mu.Lock()
		rl.byKey[k]++
		rl.reasons[reason]++
		rl.mu.Unlock()
	})()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "arm8"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now().Add(-agedBackoff)})

	var calls atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		n := calls.Add(1)
		gen0 := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k) // resolve_populate.go entry shape
		Deps().Record(rctx, k, g375A, "ns", "dep")
		if n == 1 {
			Deps().OnUpdate(g375A, "ns", "dep") // churn INSIDE the first re-resolve
		}
		c.ReplaceIfGen(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen0)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key)

	waitFor(t, 3*time.Second, "the self-remarked second re-resolve", func() bool { return calls.Load() >= 2 })
	time.Sleep(500 * time.Millisecond) // settle: a self-remark loop would keep climbing
	if got := calls.Load(); got != 2 {
		t.Fatalf("#375 arm-8 RED: refresher re-resolves=%d, want exactly 2 (initial + ONE self-remark/"+
			"dirty-mark coalesced); a growing count is an unbounded self-remark loop", got)
	}
	if got := rl.count(key); got != 1 {
		t.Fatalf("#375 arm-8 RED: self-remarks=%d, want 1 (only the re-resolve that saw the churn)", got)
	}
}

// ---------------------------------------------------------------------------
// arm-9 — COVERAGE-ENUM, STRUCTURAL. Reflect over *ResolvedCacheStore at test time:
// EVERY generation-guarded Put method (name contains "IfGen" — PutIfGen,
// ReplaceIfGen, PutRAFullListIfGen, and any future one, e.g. #258's
// ReplaceIfGenReMint) must (1) take a context.Context first (it cannot reach the
// resolve's sink otherwise) and (2) invoke the dep-gen check on ACCEPT — proven
// behaviourally by an accepted call on a NIL-sink ctx bumping unguarded_put_total
// and remarking exactly once. A new gen-guarded Put added unwired goes RED here with
// no edit to this test. NEUTER: remove the remark from any one method → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm9_EveryGenGuardedPutInvokesTheCheck(t *testing.T) {
	c, rl, _ := setup375(t)
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	st := reflect.TypeOf(c)
	var covered []string
	for i := 0; i < st.NumMethod(); i++ {
		m := st.Method(i)
		if !strings.Contains(m.Name, "IfGen") {
			continue
		}
		covered = append(covered, m.Name)
		mt := m.Type // receiver is In(0)
		if mt.NumIn() < 2 || mt.In(1) != ctxType {
			t.Errorf("#375 arm-9 RED: gen-guarded Put %s does not take context.Context first — it cannot "+
				"reach the resolve's dep-gen sink, so it is UNGUARDED", m.Name)
			continue
		}
		key := "arm9-" + m.Name
		c.Put(key, body375(`{"seed":1}`)) // live, so a Replace* variant can accept
		args := []reflect.Value{reflect.ValueOf(c), reflect.ValueOf(context.Background())}
		ok := true
		for j := 2; j < mt.NumIn(); j++ {
			pt := mt.In(j)
			switch {
			case pt.Kind() == reflect.String:
				args = append(args, reflect.ValueOf(key))
			case pt == reflect.TypeOf(&ResolvedEntry{}):
				args = append(args, reflect.ValueOf(body375(`{"v":2}`)))
			case pt.Kind() == reflect.Uint64:
				args = append(args, reflect.ValueOf(c.CaptureGen(key)))
			case pt == reflect.TypeOf(ResolvedKeyInputs{}):
				args = append(args, reflect.ValueOf(ResolvedKeyInputs{CacheEntryClass: "arm9"}))
			case pt == reflect.TypeOf(map[string]any{}):
				args = append(args, reflect.ValueOf(map[string]any{"v": 2}))
			case pt.Kind() == reflect.Bool:
				args = append(args, reflect.ValueOf(false))
			default:
				t.Errorf("#375 arm-9: gen-guarded Put %s has a parameter of type %s this enumeration cannot "+
					"synthesize — extend arm-9 so the new method is DRIVEN, never silently skipped", m.Name, pt)
				ok = false
			}
		}
		if !ok {
			continue
		}
		before := Deps().Stats().UnguardedPutTotal
		remarksBefore := rl.count(key)
		out := m.Func.Call(args)
		if len(out) == 0 || out[0].Kind() != reflect.Bool || !out[0].Bool() {
			t.Errorf("#375 arm-9: %s did not ACCEPT the synthesized live-key Put (out=%v) — cannot prove the "+
				"accept branch invokes the check; extend the synthesis", m.Name, out)
			continue
		}
		if got := Deps().Stats().UnguardedPutTotal - before; got != 1 {
			t.Errorf("#375 arm-9 RED: accepted %s on a NIL-sink ctx moved unguarded_put_total by %d, want 1 — "+
				"the method does not invoke remarkIfDepsMoved on accept (UNWIRED gen-guarded Put)", m.Name, got)
		}
		if got := rl.count(key) - remarksBefore; got != 1 {
			t.Errorf("#375 arm-9 RED: accepted %s on a NIL-sink ctx remarked %d times, want 1", m.Name, got)
		}
	}
	for _, must := range []string{"PutIfGen", "ReplaceIfGen", "PutRAFullListIfGen", "ReplaceIfGenReMint"} {
		found := false
		for _, n := range covered {
			found = found || n == must
		}
		if !found {
			t.Errorf("#375 arm-9: enumeration did not find %s — the reflection filter is broken", must)
		}
	}
	t.Logf("arm-9 enumerated gen-guarded Puts: %v", covered)
}

// ---------------------------------------------------------------------------
// arm-10 — HIGH-CHURN amplification is BOUNDED (the 0.30.185 guard). A churn storm
// (thousands of real OnObjectEvents on 5 deps) runs while 30 customer resolves of the
// SAME key each record all 5 deps (twice each) and Put. Assert: remarks ==
// resolves-that-saw-churn (ONCE per resolve, NOT per dep, NOT per event), and the
// real refresher's re-resolves of that key stay ≤ the rate-floor bound for the
// window, independent of the event count. NEUTER: per-dep remark → remarks = 5×.
// ---------------------------------------------------------------------------
func TestIssue375_Arm10_HighChurnStorm_RemarkOncePerResolve_RateFloorBounded(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "1")
	resetRefresherForTest()
	c := ResolvedCache()
	rl := &remarkLog{byKey: map[string]int{}, reasons: map[string]int{}}
	defer SetDepGenRemarkObserverForTest(func(k, reason string) {
		rl.mu.Lock()
		rl.byKey[k]++
		rl.reasons[reason]++
		rl.mu.Unlock()
	})()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "arm10"}
	key := ComputeKey(in)
	deps := []string{"d0", "d1", "d2", "d3", "d4"}

	var reResolves atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		reResolves.Add(1)
		gen0 := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k)
		for _, d := range deps {
			Deps().Record(rctx, k, g375A, "ns", d)
		}
		c.ReplaceIfGen(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen0)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	stop := make(chan struct{})
	var events atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the production-shape churn storm
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			Deps().OnUpdate(g375A, "ns", deps[i%len(deps)])
			events.Add(1)
			time.Sleep(200 * time.Microsecond)
		}
	}()

	const resolves = 30
	window := time.Now()
	for i := 0; i < resolves; i++ {
		rctx := WithL1KeyContext(context.Background(), key)
		gen0 := c.CaptureGen(key)
		seq0 := DepEventSeqForTest()
		for _, d := range deps { // each dep recorded twice
			Deps().Record(rctx, key, g375A, "ns", d)
			Deps().Record(rctx, key, g375A, "ns", d)
		}
		// Hold the resolve open until the storm has provably moved a recorded dep.
		for DepEventSeqForTest() < seq0+len64(deps)+1 {
			time.Sleep(100 * time.Microsecond)
		}
		if !c.PutIfGen(rctx, key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in}, gen0) {
			t.Fatalf("setup: customer PutIfGen refused")
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	// While customers keep re-Putting K the floor keeps deferring (CreatedAt stays
	// young); once they stop, the deferred mark must still RESOLVE (lossless), once.
	waitFor(t, 3*time.Second, "the deferred re-resolve after the storm", func() bool { return reResolves.Load() >= 1 })
	time.Sleep(1200 * time.Millisecond)
	elapsed := time.Since(window)

	customerRemarks := rl.count(key)
	// Every customer resolve saw churn (held open across a bump of every dep), so each
	// remarks exactly once; refresher re-resolves may add their own (≤ reResolves).
	if customerRemarks < resolves || customerRemarks > resolves+int(reResolves.Load()) {
		t.Fatalf("#375 arm-10 RED: remarks=%d for %d churn-seeing customer resolves (+%d refresher "+
			"re-resolves) over %d dep events — must be ONCE per resolve, never per dep / per event",
			customerRemarks, resolves, reResolves.Load(), events.Load())
	}
	// Rate floor 1s: ≤ one re-resolve per floor window (+1 immediate +1 deferred tail).
	bound := int32(elapsed/time.Second) + 3
	if got := reResolves.Load(); got > bound {
		t.Fatalf("#375 arm-10 RED: %d refresher re-resolves of ONE key in %s under a %d-event storm — "+
			"exceeds the rate-floor bound %d (amplification)", got, elapsed, events.Load(), bound)
	}
	t.Logf("arm-10: %d dep events, %d customer resolves → %d remarks, %d refresher re-resolves in %s (bound %d)",
		events.Load(), resolves, customerRemarks, reResolves.Load(), elapsed, bound)
}

func len64(s []string) uint64 { return uint64(len(s)) }

// ---------------------------------------------------------------------------
// arm-11 — SEQ ATOMICITY under -race. Bumpers (real OnObjectEvent) race resolvers
// (Record + accepted PutIfGen). Invariant: if a bump STARTED after a resolve's
// startSeq capture and COMPLETED before that resolve's Put, the Put remarks — no
// torn/missing lastBumpSeq. Plus -race itself: every seq/bucket/floor access is
// atomic. NEUTER: a non-atomic lastBumpSeq write in bumpCoordinateGen → -race RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm11_ConcurrentBumpAndPutCheck_NoTornSeq(t *testing.T) {
	c, rl, _ := setup375(t)
	var started, completed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for b := 0; b < 4; b++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				started.Add(1)
				Deps().OnUpdate(g375A, "ns", "x")
				completed.Add(1)
			}
		}()
	}
	type outcome struct {
		key        string
		mustRemark bool
	}
	var mu sync.Mutex
	var outs []outcome
	var rw sync.WaitGroup
	for r := 0; r < 8; r++ {
		rw.Add(1)
		go func(r int) {
			defer rw.Done()
			for i := 0; i < 40; i++ {
				k := "arm11-" + string(rune('a'+r)) + "-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
				ctx := WithL1KeyContext(context.Background(), k) // startSeq
				s0 := started.Load()                             // bumps started BEFORE this point ≤ s0
				gen0 := c.CaptureGen(k)
				Deps().Record(ctx, k, g375A, "ns", "x")
				// Give the bumpers a moment (bounded) so most resolves straddle a bump.
				for spin := time.Now(); completed.Load() <= s0 && time.Since(spin) < 2*time.Millisecond; {
					runtime.Gosched()
				}
				c1 := completed.Load() // c1 > s0 ⇒ ≥1 bump started after startSeq and completed
				if !c.PutIfGen(ctx, k, body375(`{}`), gen0) {
					t.Errorf("setup: PutIfGen(%s) refused", k)
					return
				}
				mu.Lock()
				outs = append(outs, outcome{key: k, mustRemark: c1 > s0})
				mu.Unlock()
			}
		}(r)
	}
	rw.Wait()
	close(stop)
	wg.Wait()
	must := 0
	for _, o := range outs {
		if o.mustRemark {
			must++
			if rl.count(o.key) != 1 {
				t.Errorf("#375 arm-11 RED: %s — a bump started after startSeq and completed before the Put, "+
					"yet remarks=%d (torn/missing lastBumpSeq)", o.key, rl.count(o.key))
			}
		} else if rl.count(o.key) > 1 {
			t.Errorf("#375 arm-11: %s remarked %d times (> once)", o.key, rl.count(o.key))
		}
	}
	if must == 0 {
		t.Fatalf("#375 arm-11: no resolve observed a completed post-start bump — the arm exercised nothing")
	}
	t.Logf("arm-11: %d resolves, %d with a provable post-start bump, all remarked", len(outs), must)
}

// ---------------------------------------------------------------------------
// arm-12 — NIL sink vs EMPTY sink (TL final). NIL (drifted resolve-entry, bare ctx)
// at an accepted gen-guarded Put → EXACTLY 1 remark + unguarded_put_total+1 + the
// test-build hook fires (installed as a panic, the documented CI enforcement).
// EMPTY (sink installed, zero deps — a legit no-deps resolve) → 0 remarks, detector
// unchanged, hook silent. NEUTERS: nil-sink no-remark → RED; empty treated as nil → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm12_NilSinkRemarksOnceAndDetects_EmptySinkSilent(t *testing.T) {
	c, rl, _ := setup375(t)
	var hookKeys []string
	restore := SetUnguardedPutHookForTest(func(k string) { hookKeys = append(hookKeys, k) })
	defer restore()

	base := Deps().Stats().UnguardedPutTotal
	if !c.PutIfGen(context.Background(), "arm12-nil", body375(`{}`), 0) {
		t.Fatalf("setup: nil-sink PutIfGen refused")
	}
	if got := rl.count("arm12-nil"); got != 1 {
		t.Fatalf("#375 arm-12 RED: nil-sink accepted Put remarked %d times, want exactly 1 (fail-fresh)", got)
	}
	if rl.reason("nil_sink") != 1 {
		t.Fatalf("#375 arm-12: the remark reason must be nil_sink")
	}
	if got := Deps().Stats().UnguardedPutTotal - base; got != 1 {
		t.Fatalf("#375 arm-12 RED: unguarded_put_total moved by %d, want 1", got)
	}
	if len(hookKeys) != 1 || hookKeys[0] != "arm12-nil" {
		t.Fatalf("#375 arm-12: the test-build unguarded hook must fire once for the drifted key, got %v", hookKeys)
	}

	empty := WithDepGenSink(context.Background()) // installed, zero deps recorded
	Deps().OnUpdate(g375A, "ns", "unrelated")     // churn somewhere — irrelevant to an empty sink
	if !c.PutIfGen(empty, "arm12-empty", body375(`{}`), 0) {
		t.Fatalf("setup: empty-sink PutIfGen refused")
	}
	if got := rl.count("arm12-empty"); got != 0 {
		t.Fatalf("#375 arm-12 RED: EMPTY-sink Put remarked %d times — a legit no-deps resolve must not remark", got)
	}
	if got := Deps().Stats().UnguardedPutTotal - base; got != 1 {
		t.Fatalf("#375 arm-12 RED: EMPTY sink moved the detector (now +%d) — empty is NOT drift", got)
	}

	// The test-build PANIC path: an arm/CI harness installs the hook as a panic, so a
	// drifted entry fails the build rather than paging production.
	defer SetUnguardedPutHookForTest(func(k string) { panic("#375 unguarded gen-guarded Put: " + k) })()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("#375 arm-12: the test-build panic hook did not fire on a nil-sink Put")
			}
		}()
		c.PutIfGen(context.Background(), "arm12-panic", body375(`{}`), 0)
	}()
}

// ---------------------------------------------------------------------------
// R1 — the READ-ONLY collectMatchesWithDep callers (hasEdge, collectMatches /
// CollectMatchesForTest) and the other read surfaces never bump: depEventSeq, the
// bucket lastBumpSeq and the GVR floor are UNCHANGED after a storm of reads; a real
// OnObjectEvent (control) does bump. NEUTER: bump inside collectMatchesWithDep → RED.
// ---------------------------------------------------------------------------
func TestIssue375_R1_ReadOnlyMatchQueriesNeverBump(t *testing.T) {
	_, _, _ = setup375(t)
	d := Deps()
	d.Record(context.Background(), "r1-K", g375A, "ns", "obj")
	d.RecordList(context.Background(), "r1-K", g375A, "ns")
	seq0 := DepEventSeqForTest()
	b0, _ := DepBucketLastBumpSeqForTest(g375A, "ns", "obj")
	l0, _ := DepBucketLastBumpSeqForTest(g375A, "ns", "")
	f0 := d.gvrLastBumpSeq(g375A)
	for i := 0; i < 100; i++ {
		_ = d.hasEdge("r1-K", DepKey{GVR: g375A, Namespace: "ns", Name: "obj"})
		_ = d.collectMatches(g375A, "ns", "obj")
		_ = d.CollectMatchesForTest(g375A, "ns", "other")
		_ = d.EdgesUnder("r1-K")
		d.RangeEdges(func(string, []DepKey) bool { return true })
	}
	b1, _ := DepBucketLastBumpSeqForTest(g375A, "ns", "obj")
	l1, _ := DepBucketLastBumpSeqForTest(g375A, "ns", "")
	if DepEventSeqForTest() != seq0 || b1 != b0 || l1 != l0 || d.gvrLastBumpSeq(g375A) != f0 {
		t.Fatalf("#375 R1 RED: read-only match queries BUMPED (seq %d→%d, bucket %d→%d, list %d→%d, floor %d→%d) "+
			"— a read path bump is a spurious gen move (R1 violation + FP amplification)",
			seq0, DepEventSeqForTest(), b0, b1, l0, l1, f0, d.gvrLastBumpSeq(g375A))
	}
	d.OnUpdate(g375A, "ns", "obj") // control: the real handler DOES bump
	if b2, _ := DepBucketLastBumpSeqForTest(g375A, "ns", "obj"); DepEventSeqForTest() == seq0 || b2 <= b0 {
		t.Fatalf("#375 R1 control: a real OnObjectEvent must bump (seq %d→%d, bucket %d→%d)",
			seq0, DepEventSeqForTest(), b0, b2)
	}
}

// ---------------------------------------------------------------------------
// R2 + torn window — a resolve's accepted Put lands INSIDE the handler's bump, at
// (i) "after_store" (buckets stamped, dirty-mark fan-out not yet run) and (ii)
// "after_add" (seq advanced, buckets NOT yet stamped — the torn [Add→Store] window).
// Requirement: the stale Put is CAUGHT — either the Put-check remarks it, or the
// dirty-mark that follows lands while K is RESIDENT (so the refresher re-resolves it
// instead of skipping it). R2 (Store BEFORE mark) is what makes (i) remark and (ii)
// mark-caught. NEUTER: move the bump AFTER the fan-out (defer it in OnObjectEvent)
// → the mark lands on a non-resident K (consumed by skipped_no_entry) and the Put-
// check sees no stamp → NEITHER → RED.
// ---------------------------------------------------------------------------
func TestIssue375_R2_PutInsideBumpWindow_IsCaught(t *testing.T) {
	for _, stage := range []string{"after_store", "after_add"} {
		t.Run(stage, func(t *testing.T) {
			c, rl, hl := setup375(t)
			const K = "r2-K"
			ctx := WithL1KeyContext(context.Background(), K)
			gen0 := c.CaptureGen(K)
			Deps().Record(ctx, K, g375A, "ns", "obj") // K depends on obj (edge exists)
			var fired atomic.Bool
			defer SetDepBumpHookForTest(func(s string) {
				if s == stage && fired.CompareAndSwap(false, true) {
					if !c.PutIfGen(ctx, K, body375(`{"stale":true}`), gen0) {
						t.Errorf("setup: PutIfGen refused inside the window")
					}
				}
			})()
			Deps().OnUpdate(g375A, "ns", "obj")
			if !fired.Load() {
				t.Fatalf("setup: the %s window never fired", stage)
			}
			remarked := rl.count(K) > 0
			caught := hl.residentMarks(K) > 0
			if !remarked && !caught {
				t.Fatalf("#375 R2 RED (%s): a Put inside the bump window was NEITHER remarked NOR dirty-marked "+
					"while resident — stale K with no pending mark (Store must precede the mark)", stage)
			}
			if stage == "after_store" && !remarked {
				t.Fatalf("#375 R2 RED: a Put after the Store must be REMARKED by its own Put-check (R2)")
			}
			t.Logf("%s: remarked=%v resident-mark=%v", stage, remarked, caught)
		})
	}
}

// ---------------------------------------------------------------------------
// Sink chaining (TL-approved scope note 1). A NESTED WithL1KeyContext (resolve:true
// nested dispatch; apiref's RAFullList fullCtx) chains to the enclosing sink, so a dep
// recorded only by the nested resolve reaches the OUTER Put's check. NEUTER: drop the
// parent link in WithDepGenSink → the outer Put misses the nested dep → RED.
// ---------------------------------------------------------------------------
func TestIssue375_NestedResolveDep_ReachesOuterPutCheck(t *testing.T) {
	c, rl, _ := setup375(t)
	const outer, nested = "nest-outer", "nest-inner"
	octx := WithL1KeyContext(context.Background(), outer)
	ogen := c.CaptureGen(outer)
	nctx := WithL1KeyContext(octx, nested) // nested dispatch
	ngen := c.CaptureGen(nested)
	Deps().Record(nctx, nested, g375A, "ns", "deep") // only the NESTED resolve reads "deep"
	Deps().OnUpdate(g375A, "ns", "deep")
	if !c.PutIfGen(nctx, nested, body375(`{}`), ngen) || !c.PutIfGen(octx, outer, body375(`{}`), ogen) {
		t.Fatalf("setup: Put refused")
	}
	if rl.count(nested) != 1 {
		t.Fatalf("#375 nested: the nested Put must remark once, got %d", rl.count(nested))
	}
	if rl.count(outer) != 1 {
		t.Fatalf("#375 nested RED: the OUTER Put embeds the nested output but remarked %d times — the "+
			"nested sink must chain to the outer sink", rl.count(outer))
	}
}

// (A) child content sink, cache-level: the content cell's OWN coordinate is pre-
// declared at its entry, so the content Put sees its own churn even though the cell
// Records its dep only AFTER the Put; an OUTER-only dep churning does NOT remark the
// content Put (no outer-dep FP); and a Record under the child still reaches the parent.
// NEUTER: WithContentDepGenSink returns ctx unchanged → own-dep RED + FP RED.
func TestIssue375_ContentChildSink_OwnDepRemarks_OuterDepSilent_ParentSees(t *testing.T) {
	c, rl, _ := setup375(t)
	const outer, content = "cs-outer", "cs-content"
	octx := WithL1KeyContext(context.Background(), outer)
	Deps().Record(octx, outer, g375B, "ns", "outer-only") // an outer dep the content never reads

	// Case 1: own coordinate churns in [read, Put].
	cctx := WithContentDepGenSink(octx, g375A, "ns", "") // content LIST cell entry
	cgen := c.CaptureGen(content)
	Deps().OnAdd(g375A, "ns", "member") // churn on the cell's own list
	if !c.PutIfGen(cctx, content, body375(`{}`), cgen) {
		t.Fatalf("setup: content Put refused")
	}
	Deps().RecordList(cctx, content, g375A, "ns") // the cell records AFTER the accepted Put
	if rl.count(content) != 1 {
		t.Fatalf("#375 (A) RED: the content cell's own coordinate churned in [read, Put] but the content Put "+
			"remarked %d times (want 1)", rl.count(content))
	}
	if _, deps, ok := DepGenSinkForTest(octx); !ok || !containsDK(deps, DepKey{GVR: g375A, Namespace: "ns", Name: listWildcard}) {
		t.Fatalf("#375 (A): a Record under the child sink must propagate to the parent (outer) sink; outer deps=%v", deps)
	}

	// Case 2: only an OUTER dep churns → the content Put must not remark.
	const content2 = "cs-content-2"
	cctx2 := WithContentDepGenSink(octx, g375A, "ns", "other")
	cgen2 := c.CaptureGen(content2)
	Deps().OnUpdate(g375B, "ns", "outer-only")
	if !c.PutIfGen(cctx2, content2, body375(`{}`), cgen2) {
		t.Fatalf("setup: content Put refused")
	}
	if rl.count(content2) != 0 {
		t.Fatalf("#375 (A) RED: an OUTER-only dep churned and the content Put remarked %d times — the content "+
			"check must be scoped to the cell's own coordinate (no outer-dep FP)", rl.count(content2))
	}
}

func containsDK(ds []DepKey, dk DepKey) bool {
	for _, d := range ds {
		if d == dk {
			return true
		}
	}
	return false
}

// (B) — the remark CARRIES the moved deps' GVRs as refresh triggers, through the
// dirty-mark hook (tier routing + trigger set), ONE enqueue per Put. NEUTER: enqueue
// the remark with no trigger → RED.
func TestIssue375_RemarkCarriesMovedGVRsAsTriggers(t *testing.T) {
	c, rl, _ := setup375(t)
	merged := map[schema.GroupVersionResource]int{}
	var enq []schema.GroupVersionResource
	Deps().SetRefreshTriggerMergeHook(func(_ string, g schema.GroupVersionResource) { merged[g]++ })
	Deps().SetRefreshHook(func(_ string, g schema.GroupVersionResource) { enq = append(enq, g) })
	const K = "trig-K"
	ctx := WithL1KeyContext(context.Background(), K)
	gen0 := c.CaptureGen(K)
	Deps().Record(ctx, K, g375A, "ns", "a")
	Deps().Record(ctx, K, g375B, "ns", "b")
	enq = nil // drop nothing-yet; the churn below dirty-marks nothing resident
	Deps().OnUpdate(g375A, "ns", "a")
	Deps().OnUpdate(g375B, "ns", "b")
	enq = nil // the two dirty-marks (K not resident) — the remark is what we assert on
	if !c.PutIfGen(ctx, K, body375(`{}`), gen0) {
		t.Fatalf("setup: Put refused")
	}
	if rl.count(K) != 1 || len(enq) != 1 {
		t.Fatalf("#375 (B): want ONE remark via ONE hook enqueue, got remarks=%d enqueues=%d", rl.count(K), len(enq))
	}
	got := map[schema.GroupVersionResource]bool{enq[0]: true}
	for g := range merged {
		got[g] = true
	}
	if !got[g375A] || !got[g375B] {
		t.Fatalf("#375 (B) RED: the remark must carry BOTH moved GVRs as triggers (enqueued %v, merged %v)", enq, merged)
	}
}

// (C) — the refresher's trigger SET: two dirty-marks of one key with DIFFERENT GVRs
// before the dequeue → the re-resolve ctx force-misses BOTH (pre-#375 the second Store
// overwrote the first). Real OnObjectEvents, real refresher dequeue. NEUTER: Store
// (overwrite) instead of merge → RED.
func TestIssue375_TwoMarksDifferentGVRsBeforeDequeue_BothForceMiss(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "twomarks"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in, CreatedAt: time.Now().Add(-agedBackoff)})
	Deps().Record(context.Background(), key, g375A, "ns", "a")
	Deps().Record(context.Background(), key, g375B, "ns", "b")

	gate := make(chan struct{})
	var seen sync.Map
	var calls atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		if calls.Add(1) == 1 {
			<-gate // hold the FIRST (unrelated) dequeue so both marks pile up behind it
			return nil
		}
		seen.Store("A", RefreshTriggerHas(ctx, g375A))
		seen.Store("B", RefreshTriggerHas(ctx, g375B))
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key) // dequeued + held in the handler
	waitFor(t, 2*time.Second, "first dequeue in flight", func() bool { return calls.Load() == 1 })
	Deps().OnUpdate(g375A, "ns", "a") // mark 1 (GVR A) while K is processing → re-queued after Done
	Deps().OnUpdate(g375B, "ns", "b") // mark 2 (GVR B) before that dequeue
	close(gate)
	waitFor(t, 3*time.Second, "the re-dequeue carrying the marks", func() bool { return calls.Load() >= 2 })
	a, _ := seen.Load("A")
	b, _ := seen.Load("B")
	if a != true || b != true {
		t.Fatalf("#375 (C) RED: two dirty-marks (GVR A then GVR B) before the dequeue must force-miss BOTH; "+
			"re-resolve saw A=%v B=%v (a last-write-wins trigger drops the first)", a, b)
	}
}

// (C) race-safety — concurrent same-key merges with DISTINCT GVRs under -race: the
// consumed set holds every GVR merged before the consume; nothing torn, nothing lost
// across a concurrent LoadAndDelete (a merge after the consume lands in a FRESH set).
func TestIssue375_TriggerSetMerge_RaceSafe(t *testing.T) {
	r := &refresher{}
	const key = "merge-K"
	gvrs := make([]schema.GroupVersionResource, 32)
	for i := range gvrs {
		gvrs[i] = schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "r" + string(rune('a'+i%26)) + string(rune('0'+i/26))}
	}
	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup
		for _, g := range gvrs {
			wg.Add(1)
			go func(g schema.GroupVersionResource) { defer wg.Done(); r.mergeTriggerGVR(key, g) }(g)
		}
		// A concurrent consumer, as processNext does.
		var consumed [][]schema.GroupVersionResource
		var cmu sync.Mutex
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, ok := r.triggerGVRByKey.LoadAndDelete(key); ok {
				cmu.Lock()
				consumed = append(consumed, v.(*triggerGVRSet).gvrs)
				cmu.Unlock()
			}
		}()
		wg.Wait()
		if v, ok := r.triggerGVRByKey.LoadAndDelete(key); ok {
			consumed = append(consumed, v.(*triggerGVRSet).gvrs)
		}
		seen := map[schema.GroupVersionResource]int{}
		for _, set := range consumed {
			for _, g := range set {
				seen[g]++
			}
		}
		for _, g := range gvrs {
			if seen[g] != 1 {
				t.Fatalf("#375 (C) race RED (round %d): GVR %v consumed %d times across the merge/consume race, want exactly 1",
					round, g, seen[g])
			}
		}
	}
}
