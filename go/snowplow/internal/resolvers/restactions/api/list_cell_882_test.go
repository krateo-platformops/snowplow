// list_cell_882_test.go — #882: the incremental LIST content cell under churn,
// through the production reader apistageContentServe on a real informer.
//
//	F1a  (falsifier a) an anchored cell is not removed by the store TTL or the
//	     max entry age: with both at 1 s and 5 s of informer events, the list
//	     is still served from the cell — zero new list dispatches — and its
//	     body equals a full fill of the informer now. Control on the same store:
//	     a plain GET-by-name cell IS expired by the 1 s TTL (re-dispatched), so
//	     the bounds are live and the cell is the exemption.
//	F2c  (falsifier c) anchoring under churn: edits (updates, a create, a
//	     delete) land between the fill's informer listing and its anchor, on
//	     keys the cell has recorded as pending or still in flight. The anchor
//	     takes them from the informer instead of aborting: anchor_raced stays
//	     0, the cell is anchored, one list dispatch in total, and the cell
//	     equals a full fill with every edit in it.
//
// Every count the assertions bound is the test's own (the dispatch seam);
// snowplow's anchor_raced is read only as the named falsifier signal.
package api

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// list882CountDispatches wraps the real dispatch and counts LIST and other
// (GET-by-name) dispatches separately: the test's own seam.
func list882CountDispatches(t *testing.T, onList func()) (lists, others *atomic.Int64) {
	t.Helper()
	lists, others = new(atomic.Int64), new(atomic.Int64)
	listPath := fill710ListCall().Path
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		if call.Path == listPath {
			lists.Add(1)
			if onList != nil {
				onList()
			}
		} else {
			others.Add(1)
		}
		return raw, ok
	}
	return lists, others
}

func list882GetCall(name string) httpcall.RequestOptions {
	return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps/" + name,
		Verb: ptr.To(http.MethodGet),
	}}
}

// list882ServedItems is the items of a served LIST value (the gated envelope).
func list882ServedItems(t *testing.T, v any) []any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("served value is %T, not an envelope map", v)
	}
	items, _ := m["items"].([]any)
	return items
}

func TestListCell882_F1a_IncrementalCellIsNotExpiredByTTLOrMaxAge(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "1")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "1")
	const n = 20
	dyn := newInc854Watcher(t, n)
	store := cache.ResolvedCache()
	lists, gets := list882CountDispatches(t, nil)
	call := fill710ListCall()

	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, call, false); !ok || !served {
		t.Fatal("PRECONDITION: the first LIST read was not served")
	}
	e, ok := store.GetNoTouch(fill710ListKey())
	if !ok || e.ListCell() == nil || !e.ListCell().Anchored() {
		t.Fatal("PRECONDITION: the LIST content cell is not an anchored incremental cell")
	}
	getName := fmt.Sprintf("fsa-%05d", n-1)
	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, list882GetCall(getName), false); !ok || !served {
		t.Fatal("PRECONDITION: the control GET-by-name read was not served")
	}
	if lists.Load() != 1 || gets.Load() != 1 {
		t.Fatalf("PRECONDITION: dispatches list=%d get=%d, want 1/1", lists.Load(), gets.Load())
	}

	// 5 s of informer events (5× the TTL and the max entry age), with the
	// read-independent reaper running every second as on the summary tick.
	end := time.Now().Add(5 * time.Second)
	nextReap := time.Now().Add(time.Second)
	var events int
	for k := 0; time.Now().Before(end); k++ {
		before := cache.DepGenEpochNow()
		if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(k%(n-1), 1+k%5), metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update %d: %v", k, err)
		}
		inc854Settle(t, r1Ns, before)
		events++
		if time.Now().After(nextReap) {
			store.ReapPastMaxEntryAgeForTest()
			nextReap = time.Now().Add(time.Second)
		}
		time.Sleep(50 * time.Millisecond)
	}
	store.ReapPastMaxEntryAgeForTest()

	v, served, ok, _ := apistageContentServe(r1Ctx(), store, call, false)
	if !ok || !served {
		t.Fatal("F1a: the LIST read after 5 s was not served")
	}
	if got := lists.Load(); got != 1 {
		t.Fatalf("F1a RED (#882 F1): %d LIST dispatches after %d events over 5 s with TTL = max entry age = 1 s, want 1 — "+
			"the incremental cell was expired and re-filled", got, events)
	}
	inc854Same(t, "F1a after 5 s", store, call)
	_, full := inc854Full(t, call)
	got := list882ServedItems(t, v)
	if len(got) != len(full) {
		t.Fatalf("F1a: served %d items, a full fill has %d", len(got), len(full))
	}
	for i := range full {
		if !reflect.DeepEqual(got[i], full[i].Object) {
			t.Fatalf("F1a: served item %d differs from a full fill now", i)
		}
	}

	// Control: the bounds are live on this store — the plain GET cell expired.
	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, list882GetCall(getName), false); !ok || !served {
		t.Fatal("control: the GET-by-name read was not served")
	}
	if got := gets.Load(); got != 2 {
		t.Fatalf("control: %d GET dispatches, want 2 — the 1 s TTL did not expire a plain cell, so F1a proves nothing", got)
	}
	t.Logf("F1a: %d events over 5 s; LIST dispatches=%d (cell kept), GET dispatches=%d (plain cell expired)", events, lists.Load(), gets.Load())
}

func TestListCell882_F2c_AnchorUnderChurnTakesEditedKeysAndNeverRaces(t *testing.T) {
	const n = 30
	dyn := newInc854Watcher(t, n)
	store := cache.ResolvedCache()
	w := &inc854World{t: t, dyn: dyn, exist: map[[2]string]bool{}}
	var once sync.Once
	var edits int
	lists, _ := list882CountDispatches(t, func() {
		once.Do(func() {
			// The fill has listed the informer. Edits land before its anchor:
			// updates, a create and a delete, each taken by the dep hook
			// (pending), then one more left unsettled (pending or in flight).
			for i := 0; i < 10; i++ {
				w.apply(r1Ns, fmt.Sprintf("fsa-%05d", i), "update", 10+i, "")
				edits++
			}
			w.apply(r1Ns, "fsa-new-882", "create", 4, "")
			w.apply(r1Ns, fmt.Sprintf("fsa-%05d", n-1), "delete", 0, "")
			if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), inc854Obj(r1Ns, "fsa-00011", 7, ""), metav1.UpdateOptions{}); err != nil {
				t.Errorf("unsettled update: %v", err)
			}
			edits += 3
		})
	})
	raced0 := cache.ListCellStatsSnapshot().Unanchors["anchor_raced"]

	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatal("the fill was not served")
	}
	if got := cache.ListCellStatsSnapshot().Unanchors["anchor_raced"] - raced0; got != 0 {
		t.Fatalf("F2c RED (#882 F2): anchor_raced moved by %d with %d edits on recorded keys during the fill — "+
			"an edit during the fill aborted the anchor instead of becoming pending", got, edits)
	}
	e, ok := store.GetNoTouch(fill710ListKey())
	if !ok || e.ListCell() == nil || !e.ListCell().Anchored() {
		t.Fatalf("F2c RED (#882 F2): the fill stored no anchored incremental cell under churn (unanchors %v)",
			cache.ListCellStatsSnapshot().Unanchors)
	}
	// Let the unsettled edit reach the hook, then the cell must equal a full
	// fill of the informer now — every edit in it — with no second fill.
	if !fill710WaitFor(10*time.Second, func() bool {
		v, ok := e.ListCell().Snapshot(cache.DepGenEpochNow())
		return ok && inc854Managed(v, "fsa-00011") == 7
	}) {
		t.Fatal("F2c: the cell never showed the edit left unsettled at the anchor")
	}
	inc854Same(t, "F2c", store, fill710ListCall())
	v, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
	if !ok || !served {
		t.Fatal("F2c: the read after the anchor was not served")
	}
	if got := len(list882ServedItems(t, v)); got != n {
		t.Fatalf("F2c: served %d items, want %d (one created, one deleted)", got, n)
	}
	if got := lists.Load(); got != 1 {
		t.Fatalf("F2c: %d LIST dispatches, want 1", got)
	}
}

// list882Managed is len(status.managed) of the named item in a served LIST
// value, -1 when the item is absent.
func list882Managed(t *testing.T, v any, name string) int {
	t.Helper()
	for _, it := range list882ServedItems(t, v) {
		obj, _ := it.(map[string]any)
		md, _ := obj["metadata"].(map[string]any)
		if md["name"] != name {
			continue
		}
		st, _ := obj["status"].(map[string]any)
		m, _ := st["managed"].([]any)
		return len(m)
	}
	return -1
}

// list882HoldFirstList holds the first LIST dispatch AFTER its informer
// listing until release is closed; listed is closed when it is held. Every
// LIST dispatch is counted (the test's own seam).
func list882HoldFirstList(t *testing.T) (listed, release chan struct{}, lists *atomic.Int64) {
	t.Helper()
	listed, release, lists = make(chan struct{}), make(chan struct{}), new(atomic.Int64)
	listPath := fill710ListCall().Path
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		if call.Path == listPath && lists.Add(1) == 1 {
			close(listed)
			<-release
		}
		return raw, ok
	}
	return listed, release, lists
}

func list882Wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("PRECONDITION: %s", what)
	}
}

// F2m — a reader that joined a flight AFTER its list moved, when that flight
// ends with no incremental cell (the anchor returned nil and nothing was
// stored), must not be served the flight's body: it re-reads (errJoinedMovedFlight)
// and gets every edit up to its own entry.
func TestListCell882_F2m_MovedJoinerOfAFlightWithNoCellReReads(t *testing.T) {
	dyn := newInc854Watcher(t, 10)
	store := cache.ResolvedCache()
	key := fill710ListKey()
	inc854Anchor(t, store, fill710ListCall())
	// Unanchor it: the next read takes the full fill (reanchor), with the entry
	// still resident — its generation is what that fill's Put is checked against.
	cache.Deps().OnResourceTypeRemoved(r1FsaGVR) // a type-level event (#870 deleted the store-repair one)
	listed, release, lists := list882HoldFirstList(t)

	leader := stamp765Serve(r1Ctx(), store)
	list882Wait(t, listed, "the leader never listed the informer")
	stamp765Change(t, dyn, 9) // the list moves after the flight began and after its listing
	joiner := stamp765Serve(r1Ctx(), store)
	if !fill710WaitFor(30*time.Second, func() bool { return contentFillJoinWaiting.Load() >= 1 || lists.Load() > 1 }) {
		t.Fatal("PRECONDITION: the second reader neither joined nor filled")
	}
	if lists.Load() != 1 {
		t.Fatal("PRECONDITION: the second reader led its own fill instead of joining the flight")
	}
	// The flight anchors no cell (a type-level event drops the cell it is
	// building: Anchor returns nil) and stores nothing (a DELETE-eviction of the
	// key: the #189 PutIfGen is refused).
	cache.Deps().OnResourceTypeRemoved(r1FsaGVR) // a type-level event (#870 deleted the store-repair one)
	store.DeleteForTest(key)
	close(release)

	lr, jr := <-leader, <-joiner
	if !lr.ok || !lr.served || !jr.ok || !jr.served {
		t.Fatalf("not served: leader ok=%v served=%v, joiner ok=%v served=%v", lr.ok, lr.served, jr.ok, jr.served)
	}
	if got := list882Managed(t, jr.v, stamp765Item); got != 9 {
		t.Fatalf("F2m RED (#882 F2): the moved joiner was served managed=%d, want 9 — it got the body of a flight whose read "+
			"predates an edit it saw before it entered, with no cell to bring it up to its own seq", got)
	}
	if got := lists.Load(); got != 2 {
		t.Fatalf("F2m: %d LIST dispatches, want 2 (the flight's, then the moved joiner's re-read)", got)
	}
}

// F1u — once a cell is no longer incremental (unanchored), the store's age
// bounds apply to its entry again, from its original clocks.
func TestListCell882_F1u_AnUnanchoredCellGetsItsAgeBoundsBack(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "1")
	newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	key := fill710ListKey()
	c := inc854Anchor(t, store, fill710ListCall())
	cache.Deps().OnResourceTypeRemoved(r1FsaGVR) // a type-level event (#870 deleted the store-repair one)
	if c.Anchored() {
		t.Fatal("PRECONDITION: the type-level event did not unanchor the cell")
	}
	if !fill710WaitFor(5*time.Second, func() bool {
		_, live := store.CellPutSeq(key)
		return !live
	}) {
		t.Fatal("F1u RED (#882 F1): an UNANCHORED list cell outlived the 1 s TTL — the age-bound exemption " +
			"follows the entry's shape, not the cell being incremental")
	}
	if _, ok := store.GetNoTouch(key); ok {
		t.Fatal("F1u RED (#882 F1): the expired, unanchored entry was not evicted on read")
	}
}

// F2r — a reader on its RETRY attempt (it may not snapshot-join a moved flight
// itself) that LEADS a fill still makes that flight incremental: its leader
// anchors, so a later reader whose list moved joins it rather than leading a
// second fill.
func TestListCell882_F2r_AFlightLedOnARetryAdmitsMovedReaders(t *testing.T) {
	dyn := newInc854Watcher(t, 10)
	store := cache.ResolvedCache()
	listed, release, lists := list882HoldFirstList(t)

	leader := make(chan fill710Served, 1)
	go func() {
		v, served, ok, _ := apistageContentServeAttempt(r1Ctx(), store, fill710ListCall(), false, false) // a retry attempt
		leader <- fill710Served{v, served, ok}
	}()
	list882Wait(t, listed, "the retrying leader never listed the informer")
	stamp765Change(t, dyn, 9)
	joiner := stamp765Serve(r1Ctx(), store)
	fill710WaitFor(30*time.Second, func() bool { return contentFillJoinWaiting.Load() >= 1 || lists.Load() > 1 })
	close(release)
	lr, jr := <-leader, <-joiner
	if !lr.ok || !lr.served || !jr.ok || !jr.served {
		t.Fatalf("not served: leader ok=%v served=%v, joiner ok=%v served=%v", lr.ok, lr.served, jr.ok, jr.served)
	}
	if got := lists.Load(); got != 1 {
		t.Fatalf("F2r RED (#882 F2): %d LIST dispatches, want 1 — the flight a retrying reader led was not incremental, "+
			"so a reader whose list moved led a second fill", got)
	}
	if got := list882Managed(t, jr.v, stamp765Item); got != 9 {
		t.Fatalf("F2r: the moved joiner was served managed=%d, want 9 (the cell brought up to its own seq)", got)
	}
}
