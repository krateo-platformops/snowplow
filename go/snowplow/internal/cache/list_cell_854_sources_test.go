package cache

// list_cell_854_sources_test.go — #854 (#865 review): the comparator's selector
// blind spot, and the REAL sources of every unanchor signal. F2c / F2d moved the
// signals directly; these arms drive the production code that moves them, and
// each reads the unanchor REASON, so a cell dropped for another reason cannot
// stand in for the source under test.
//
//   F5s  an object that LEFT the selector, its delivery recorded and its apply
//        lost, is confirmed divergent on the second pass (the in-flight check
//        reads the indexer's unfiltered object). Control: the same lost apply on
//        an object that still matches.
//        Mutation → RED: compare the delivery with the filtered listing only.
//   U7   a submit onto a shut-down dep-watch queue (the event is dropped) moves
//        the lost-event signal: the next read unanchors (lost_event).
//        Mutation → RED: the drop is not counted.
//   U8   the production watch-error handler moves the servability epoch: a read
//        AFTER the watch recovered still unanchors (servability_epoch).
//        Mutation → RED: the handler does not bump the epoch.
//   U9   a discovery retraction followed by a re-confirmation, both between two
//        reads: the read unanchors (servability_epoch).
//        Mutation → RED: the retraction does not bump the epoch.
//   U11  informer removal and re-creation between two reads: the read unanchors
//        (servability_epoch). The removal is the one bump: an informer is only
//        ever created where none exists, so a cell can only see a re-creation
//        after a removal.
//        Mutation → RED: the removal does not bump the epoch.
//   U13b a build error (an object the cell's decoder or selector cannot read)
//        unanchors on that read and never serves the generation before it.
//        Mutation → RED: a build error serves the stale generation.
//   F5y  a comparator pass a read makes due is refresher work: with a customer
//        in flight the REAL refresher parks it (yieldToCustomer), and it runs
//        once the customers are gone — never on the reader's goroutine.
//        Mutation → RED: the read runs the pass itself (the pre-#865 goroutine).

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// c854SelFuncs is c854Funcs selecting metadata.labels.sel == "yes".
func c854SelFuncs() ListCellFuncs {
	f := c854Funcs()
	f.Match = func(raw []byte) (bool, bool) {
		var m struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return false, false
		}
		return m.Metadata.Labels["sel"] == "yes", true
	}
	return f
}

// c854LostApply writes object i into the indexer with label sel and a new spec,
// records its delivery (the dep handler ran) and drops it from pending (the
// apply was lost) — the failure class the comparator exists for.
func c854LostApply(t *testing.T, rw *ResourceWatcher, c *ListCell, i int, sel string) {
	t.Helper()
	o := c854Obj(i)
	o.SetLabels(map[string]string{"sel": sel})
	o.Object["spec"] = map[string]any{"i": int64(1000 + i)}
	bo, err := newBytesObject(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := rw.c854Indexer(t).Update(bo); err != nil {
		t.Fatal(err)
	}
	noteDeliveredForTest(c854GVR, bo, false)
	c.mu.Lock()
	c.pending = map[listObjKey]struct{}{}
	c.mu.Unlock()
}

func TestListCell854_F5s_ObjectThatLeftTheSelectorIsConfirmedDivergent(t *testing.T) {
	t.Run("left_selector", func(t *testing.T) {
		rw, c, _ := c854AnchorWith(t, 5, c854SelFuncs(), map[string]string{"sel": "yes"})
		c854LostApply(t, rw, c, 2, "no")
		n1, n2 := c.ComparePass(), c.ComparePass()
		if n1 != 0 || n2 == 0 || c.Anchored() {
			t.Fatalf("F5s RED: an object that left the selector (delivered, apply lost) — passes %d,%d, anchored=%v; want 0, >0, false: "+
				"the cell keeps serving an item a full fill no longer holds", n1, n2, c.Anchored())
		}
		if got := ListCellStatsSnapshot().Divergences["key_mismatch"]; got == 0 {
			t.Fatalf("F5s: divergence reason key_mismatch not counted (%v)", ListCellStatsSnapshot().Divergences)
		}
	})
	t.Run("control_still_matches", func(t *testing.T) {
		rw, c, _ := c854AnchorWith(t, 5, c854SelFuncs(), map[string]string{"sel": "yes"})
		c854LostApply(t, rw, c, 2, "yes")
		if n1, n2 := c.ComparePass(), c.ComparePass(); n1 != 0 || n2 == 0 {
			t.Fatalf("F5s control: a changed, still-matching object with a lost apply — passes %d,%d, want 0, >0", n1, n2)
		}
	})
}

// c854UnanchorReason waits for nothing: it reads the delta of one unanchor
// reason around fn.
func c854UnanchorDelta(reason string, fn func()) uint64 {
	before := ListCellStatsSnapshot().Unanchors[reason]
	fn()
	return ListCellStatsSnapshot().Unanchors[reason] - before
}

// c854WaitServable polls until the GVR is servable again.
func c854WaitServable(t *testing.T, rw *ResourceWatcher) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !rw.IsServable(c854GVR); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("PRECONDITION: the GVR never became servable again")
		}
	}
}

func TestListCell854_U7_ASubmitOnAShutDownQueueUnanchors(t *testing.T) {
	_, c, dyn := c854AnchorWith(t, 5, c854Funcs(), nil)
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
		t.Fatal("PRECONDITION: the cell does not serve")
	}
	// One real event first, so the bridge's queue exists (a bridge stopped before
	// its first event has no queue at all — the test-only q()==nil branch).
	update := func(i, v int) {
		t.Helper()
		o := c854Obj(i)
		o.Object["spec"] = map[string]any{"i": int64(v)}
		if _, err := dyn.Resource(c854GVR).Namespace("c854").Update(context.Background(), o, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	seq0 := DepGenEpochNow()
	update(0, 50)
	for end := time.Now().Add(10 * time.Second); DepGenEpochNow() == seq0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("PRECONDITION: the first write never reached the dep hook")
		}
	}
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
		t.Fatal("PRECONDITION: the cell does not serve after a delivered change")
	}
	// Production never shuts the dep-watch queue down (stopWorker is reachable
	// only from the test reset); this is the one way to reach the drop. The
	// event itself is a real write through the informer and the real submit.
	depWatchSingleton().stopWorker()
	t.Cleanup(resetDepWatchForTest)
	sig0 := depWatchLostEventSignal()
	update(1, 77)
	for end := time.Now().Add(10 * time.Second); depWatchLostEventSignal() == sig0 && time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
	}
	var ok bool
	if d := c854UnanchorDelta("lost_event", func() { _, ok = c.Snapshot(DepGenEpochNow()) }); ok || d != 1 {
		t.Fatalf("U7 RED: an event dropped on a shut-down queue — the cell still serves (ok=%v, lost_event unanchors +%d): "+
			"its change never reaches pending, so the cell would serve the old object", ok, d)
	}
}

func TestListCell854_U8_TheWatchErrorHandlerUnanchorsEvenAfterRecovery(t *testing.T) {
	rw, c, _ := c854AnchorWith(t, 5, c854Funcs(), nil)
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
		t.Fatal("PRECONDITION: the cell does not serve")
	}
	rw.InvokeWatchErrorHandlerForTest(c854GVR, errors.New("watch channel closed"))
	rw.MarkWatchRecovered(c854GVR)
	c854WaitServable(t, rw)
	var ok bool
	if d := c854UnanchorDelta("servability_epoch", func() { _, ok = c.Snapshot(DepGenEpochNow()) }); ok || d != 1 {
		t.Fatalf("U8 RED: a watch broke and recovered between two reads — the cell still serves (ok=%v, servability_epoch +%d): "+
			"an event lost while the watch was down is never applied", ok, d)
	}
}

// c854Discovery serves (or omits) c854GVR's resource in a successful discovery.
type c854Discovery struct {
	mu     sync.Mutex
	served bool
}

func (d *c854Discovery) set(served bool) { d.mu.Lock(); d.served = served; d.mu.Unlock() }
func (d *c854Discovery) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	l := &metav1.APIResourceList{GroupVersion: gv}
	if d.served {
		l.APIResources = []metav1.APIResource{{Name: c854GVR.Resource, Namespaced: true, Kind: "Thing"}}
	}
	return l, nil
}

func TestListCell854_U9_ARetractionThenReconfirmationUnanchors(t *testing.T) {
	rw, c, _ := c854AnchorWith(t, 5, c854Funcs(), nil)
	disco := &c854Discovery{served: true}
	rw.SetDiscoveryClient(disco)
	rw.RefreshDiscovery(context.Background())
	c854WaitServable(t, rw)
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
		t.Fatal("PRECONDITION: a first confirmation must not drop the cell (it is not an exit from servable)")
	}
	disco.set(false)
	rw.RefreshDiscovery(context.Background())
	if rw.IsServable(c854GVR) {
		t.Fatal("PRECONDITION: a successful discovery without the resource did not retract it")
	}
	disco.set(true)
	rw.RefreshDiscovery(context.Background())
	c854WaitServable(t, rw)
	var ok bool
	if d := c854UnanchorDelta("servability_epoch", func() { _, ok = c.Snapshot(DepGenEpochNow()) }); ok || d != 1 {
		t.Fatalf("U9 RED: the type was retracted and confirmed again between two reads — the cell still serves (ok=%v, servability_epoch +%d)", ok, d)
	}
}

func TestListCell854_U11_InformerRemovalAndRecreationUnanchors(t *testing.T) {
	rw, c, _ := c854AnchorWith(t, 5, c854Funcs(), nil)
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
		t.Fatal("PRECONDITION: the cell does not serve")
	}
	rw.RemoveResourceType(c854GVR)
	_, syncCh := rw.EnsureResourceType(c854GVR)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatal("PRECONDITION: the re-created informer did not sync")
	}
	c854WaitServable(t, rw)
	var ok bool
	if d := c854UnanchorDelta("servability_epoch", func() { _, ok = c.Snapshot(DepGenEpochNow()) }); ok || d != 1 {
		t.Fatalf("U11 RED: the informer was removed and re-created between two reads — the cell still serves (ok=%v, servability_epoch +%d): "+
			"a delete during the gap is never applied", ok, d)
	}
}

func TestListCell854_U13b_ABuildErrorNeverServesTheStaleGeneration(t *testing.T) {
	poisoned := func(raw []byte) bool {
		var m struct {
			Spec struct {
				I int64 `json:"i"`
			} `json:"spec"`
		}
		return json.Unmarshal(raw, &m) == nil && m.Spec.I < 0
	}
	cases := []struct {
		name, reason string
		funcs        func() ListCellFuncs
	}{
		{"decode", "build_decode", func() ListCellFuncs {
			f := c854Funcs()
			dec := f.Decode
			f.Decode = func(raw []byte) (*unstructured.Unstructured, error) {
				if poisoned(raw) {
					return nil, errors.New("undecodable")
				}
				return dec(raw)
			}
			return f
		}},
		{"unreadable_item", "build_unreadable_item", func() ListCellFuncs {
			f := c854Funcs()
			f.Match = func(raw []byte) (bool, bool) { return true, !poisoned(raw) }
			return f
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, dyn := c854AnchorWith(t, 5, tc.funcs(), nil)
			if _, ok := c.Snapshot(DepGenEpochNow()); !ok {
				t.Fatal("PRECONDITION: the cell does not serve")
			}
			seq0 := DepGenEpochNow()
			o := c854Obj(3)
			o.Object["spec"] = map[string]any{"i": int64(-1)}
			if _, err := dyn.Resource(c854GVR).Namespace("c854").Update(context.Background(), o, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for end := time.Now().Add(10 * time.Second); DepGenEpochNow() == seq0; time.Sleep(5 * time.Millisecond) {
				if time.Now().After(end) {
					t.Fatal("PRECONDITION: the write never reached the dep hook")
				}
			}
			var ok bool
			if d := c854UnanchorDelta(tc.reason, func() { _, ok = c.Snapshot(DepGenEpochNow()) }); ok || d != 1 {
				t.Fatalf("U13b RED (%s): the build failed and the read was still served (ok=%v, %s +%d) — "+
					"the generation before the change is stale", tc.name, ok, tc.reason, d)
			}
			if c.Anchored() {
				t.Fatalf("U13b (%s): the cell is still anchored after a build error", tc.name)
			}
		})
	}
}

func TestListCell854_F5y_AComparatorPassIsRefresherWorkThatYieldsToCustomers(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0) // floor 0: the rate floor is not under test
	defer cleanup()
	_, c, _ := c854AnchorWith(t, 5, c854Funcs(), nil)
	in := &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Namespace: "c854"}
	ResolvedCache().put(c.key, NewListCellEntry(c, in))
	var customers atomic.Bool
	customers.Store(true)
	SetCustomerInflightHook(customers.Load)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	p0 := ListCellStatsSnapshot().Passes
	if _, ok := c.Snapshot(DepGenEpochNow()); !ok { // the first read makes a pass due
		t.Fatal("PRECONDITION: the cell does not serve")
	}
	if !c.passDue.Load() {
		t.Fatal("PRECONDITION: the first read made no pass due")
	}
	time.Sleep(300 * time.Millisecond) // 12 yield re-checks with a customer in flight
	if got := ListCellStatsSnapshot().Passes - p0; got != 0 {
		t.Fatalf("F5y RED: %d comparator pass(es) ran while a customer was in flight — the pass is not behind the refresher's customer yield", got)
	}
	customers.Store(false)
	for end := time.Now().Add(5 * time.Second); ListCellStatsSnapshot().Passes == p0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("F5y RED: the due pass never ran after the customers left — reads no longer schedule passes")
		}
	}
	if c.passDue.Load() {
		t.Fatal("F5y: the pass ran but is still marked due")
	}
}
