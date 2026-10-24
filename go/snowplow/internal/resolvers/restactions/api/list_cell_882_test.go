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
