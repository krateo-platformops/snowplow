// list_cell_854_race_test.go — #854: the orderings the incremental list cell's
// correctness rests on, forced deterministically with test hooks.
//
//	F3a  MC1: a reader that gets a generation whose stamp covers event e also
//	     gets e's data. The dep worker is paused right AFTER e's seq bump (the
//	     torn window); a reader needing seq(e) must still see e — because the
//	     hook recorded e's key in pending BEFORE the bump.
//	F5b  C1: a key whose event is mid-processing in the dep worker (after
//	     takeMask, before the hook) mismatches the indexer on every pass; it is
//	     IN FLIGHT, never a divergence. Then a 10,000-event storm with the
//	     worker repeatedly stalled and comparator passes interleaved:
//	     divergence_total stays 0, and the cell ends equal to a full fill.
package api

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestListCell854_F3a_StampNeverRunsAheadOfTheData(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())

	var armed atomic.Bool
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(cache.SetDepBumpHookForTest(func(stage string) {
		if stage == "after_add" && armed.Load() {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
	}))
	armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(1, 6), metav1.UpdateOptions{})
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("PRECONDITION: the update never reached the seq bump")
	}
	// The dep worker sits between e's seq bump and the rest of its hook. A
	// reader needing the current seq (>= seq(e)) must get e.
	v, ok := c.Snapshot(cache.DepGenEpochNow())
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("PRECONDITION: the cell unanchored")
	}
	got := -1
	for _, it := range v.Items() {
		md, _ := it.Object["metadata"].(map[string]any)
		if md["name"] == stamp765Item {
			st, _ := it.Object["status"].(map[string]any)
			m, _ := st["managed"].([]any)
			got = len(m)
		}
	}
	if got != 6 {
		t.Fatalf("F3a RED (MC1): a generation stamped at or after seq(e) lacks e (managed=%d, want 6) — the cell reported itself fresh while stale", got)
	}
}

// inc854StallWorker stalls the dep worker after takeMask for keys whose name
// stall() selects, until the returned release is called.
func inc854StallWorker(t *testing.T, stall func(name string) bool) (stalled <-chan string, release func()) {
	t.Helper()
	ch := make(chan string, 64)
	gate := make(chan struct{})
	var rel sync.Once
	t.Cleanup(cache.SetDepWorkerTakenHookForTest(func(gvr schema.GroupVersionResource, ns, name string) {
		if gvr == r1FsaGVR && stall(name) {
			select {
			case ch <- name:
			default:
			}
			<-gate
		}
	}))
	release = func() { rel.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return ch, release
}

func TestListCell854_F5b_KeyMidProcessingIsInFlightNotDiverged(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())
	stalled, release := inc854StallWorker(t, func(name string) bool { return name == stamp765Item })
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(1, 7), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("PRECONDITION: the worker never took the key")
	}
	// The indexer holds the change; the cell does not; the event is in the
	// worker. Two passes see the same mismatch with the same identities.
	for i := 0; i < 2; i++ {
		if n := c.ComparePass(); n != 0 {
			release()
			t.Fatalf("F5b RED (C1): pass %d counted %d divergence(s) for a key still in the dep worker — a backlog would unanchor the cell and trigger a full fill", i+1, n)
		}
	}
	release()
	inc854Settle(t, r1Ns, 0)
	if !fill710WaitFor(10*time.Second, func() bool {
		v, ok := c.Snapshot(cache.DepGenEpochNow())
		return ok && inc854Managed(v, stamp765Item) == 7
	}) {
		t.Fatal("F5b: the released event never reached the cell")
	}
	if n := c.ComparePass(); n != 0 {
		t.Fatalf("F5b: %d divergences after the event was applied", n)
	}
}

func inc854Managed(v cache.ListView, name string) int {
	for _, it := range v.Items() {
		md, _ := it.Object["metadata"].(map[string]any)
		if md["name"] == name {
			st, _ := it.Object["status"].(map[string]any)
			m, _ := st["managed"].([]any)
			return len(m)
		}
	}
	return -1
}

func TestListCell854_F5b_StormWithStalledWorkerHasZeroDivergence(t *testing.T) {
	const objects, events = 50, 10000
	dyn := newInc854Watcher(t, objects)
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())
	// Stall the worker on one in eleven events, briefly, at random keys.
	var n atomic.Int64
	t.Cleanup(cache.SetDepWorkerTakenHookForTest(func(gvr schema.GroupVersionResource, ns, name string) {
		if gvr == r1FsaGVR && n.Add(1)%11 == 0 {
			time.Sleep(200 * time.Microsecond)
		}
	}))
	t.Cleanup(cache.SetListCellDivergenceHookForTest(func(d string) { t.Logf("divergence: %s", d) }))
	submitted0 := cache.DepWatchStatsSnapshot().EventsSubmitted
	// Comparator passes run CONTINUOUSLY during the storm, so they meet keys
	// queued, mid-processing (stalled) and pending — every in-flight state.
	stop := make(chan struct{})
	var passWG sync.WaitGroup
	passWG.Add(1)
	go func() {
		defer passWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.ComparePass()
			}
		}
	}()
	res := dyn.Resource(r1FsaGVR).Namespace(r1Ns)
	for i := 0; i < events; i++ {
		if _, err := res.Update(context.Background(), fill710FSA(i%objects, 1+i%9), metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if i%50 == 49 {
			// The fake watcher's channel holds 100 events: let the informer
			// deliver every write to the dep handler (also the one window the
			// comparator cannot observe, the client-go listener buffer).
			want := submitted0 + uint64(i+1)
			if !fill710WaitFor(10*time.Second, func() bool { return cache.DepWatchStatsSnapshot().EventsSubmitted >= want }) {
				t.Fatalf("the informer never delivered write %d", i)
			}
		}
		if i%250 == 249 {
			if _, ok := c.Snapshot(cache.DepGenEpochNow()); !ok {
				t.Fatalf("F5b storm RED: the cell unanchored at event %d (unanchors %v, divergences %v)", i,
					cache.ListCellStatsSnapshot().Unanchors, cache.ListCellStatsSnapshot().Divergences)
			}
		}
	}
	close(stop)
	passWG.Wait()
	if got := cache.ListCellStatsSnapshot().DivergenceTotal; got != 0 {
		t.Fatalf("F5b storm RED: divergence_total=%d after %d events, want 0 (%v)", got, events, cache.ListCellStatsSnapshot().Divergences)
	}
	want := submitted0 + events
	if !fill710WaitFor(20*time.Second, func() bool { return cache.DepWatchStatsSnapshot().EventsSubmitted >= want }) {
		t.Fatal("the storm never fully reached the dep handler")
	}
	if !fill710WaitFor(20*time.Second, func() bool {
		v, ok := c.Snapshot(cache.DepGenEpochNow())
		raw, _ := inc854Full(t, fill710ListCall())
		return ok && string(v.Raw()) == string(raw)
	}) {
		t.Fatal("F5b storm: the cell never converged to a full fill")
	}
	inc854Same(t, fmt.Sprintf("after %d events", events), store, fill710ListCall())
	st := cache.ListCellStatsSnapshot()
	t.Logf("F5b storm: %d events, passes=%d suspects=%d divergence=%d generations=%d decodes=%d",
		events, st.Passes, st.Suspects, st.DivergenceTotal, st.Generations, st.Decodes)
}
