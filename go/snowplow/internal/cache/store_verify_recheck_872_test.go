package cache

// store_verify_recheck_872_test.go — #872 and #875: the forced pass re-reads a
// divergence only if the store may have moved past the LIST, and then counts
// it only if it still holds against the object's CURRENT state.
//
// All pass arms run on the silent-watch fixture: a real ResourceWatcher whose
// watch delivers nothing unless the arm writes to it, so the store holds
// exactly what it was seeded with plus what the arm delivers. The "apiserver"
// the forced pass talks to answers LIST from a fixed snapshot and GET from the
// cluster's current state. An arm that needs the store to move past the LIST
// delivers watch events WHILE the forced LIST is in flight, which is the only
// way that happens in production.
//
//   - STORE MOVED DURING THE LIST (#872). The delivery makes the store newer
//     than the LIST in every class; the re-check must clear it: nothing
//     published, nothing repaired, one GET.
//   - A TRUE LOSS NOTHING WAS DELIVERED FOR (#875). The store is behind and no
//     delivery touched the key during the pass: counted directly, NO GET,
//     repaired. The wholly divergent store is this arm at scale.
//   - A TRUE LOSS ON A DELIVERED KEY. Delivered, but still behind afterwards:
//     re-read (one GET) and still counted and repaired.
//   - A RELIST DELETION. The one store change the transform does not see; the
//     presence-at-open check must cover it.
//   - A FAILED RE-CHECK. Inconclusive: nothing published or repaired, the GVR
//     not marked verified, the skip counted under its own reason.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/metadata"
	clientcache "k8s.io/client-go/tools/cache"
)

// r872Client answers LIST from list and GET from inner (the cluster's current
// state), or fails every GET with getErr. duringList, when set, runs inside the
// forced LIST before it answers: whatever it delivers reaches the store while
// the LIST is in flight.
type r872Client struct {
	inner      metadata.Interface
	list       []*metav1.PartialObjectMetadata
	duringList func()
	getErr     error
	gets       atomic.Int64
	// #918 — answers, when it holds a name, is the object's current state the
	// GET answers with instead of inner's; onGet, when set, runs once the GET
	// has taken its answer and before it returns: what it delivers lands after
	// the read.
	answers map[string]r872Object
	onGet   func(name string)

	mu     sync.Mutex
	got    []string
	listRV string
}

func (c *r872Client) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return &r872Getter{Getter: c.inner.Resource(gvr), c: c}
}

// gotten returns the names GET, sorted.
func (c *r872Client) gotten() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.got...)
	sort.Strings(out)
	return out
}

type r872Getter struct {
	metadata.Getter
	c *r872Client
}

func (g *r872Getter) Namespace(ns string) metadata.ResourceInterface {
	return &r872Resource{ResourceInterface: g.Getter.Namespace(ns), c: g.c}
}

type r872Resource struct {
	metadata.ResourceInterface
	c *r872Client
}

func (r *r872Resource) List(_ context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	r.c.mu.Lock()
	r.c.listRV = opts.ResourceVersion
	r.c.mu.Unlock()
	if r.c.duringList != nil {
		r.c.duringList()
	}
	out := &metav1.PartialObjectMetadataList{}
	for _, o := range r.c.list {
		out.Items = append(out.Items, *o)
	}
	return out, nil
}

func (r *r872Resource) Get(ctx context.Context, name string, opts metav1.GetOptions, sub ...string) (*metav1.PartialObjectMetadata, error) {
	r.c.gets.Add(1)
	r.c.mu.Lock()
	r.c.got = append(r.c.got, name)
	r.c.mu.Unlock()
	if r.c.getErr != nil {
		return nil, r.c.getErr
	}
	r.c.mu.Lock()
	o, answered := r.c.answers[name]
	r.c.mu.Unlock()
	var obj *metav1.PartialObjectMetadata
	var err error
	if answered {
		obj = partialMeta("demo", o.name, o.rv, o.uid)
	} else {
		obj, err = r.ResourceInterface.Get(ctx, name, opts, sub...)
	}
	if r.c.onGet != nil {
		r.c.onGet(name)
	}
	return obj, err
}

// answer sets the object's current state as the GET answers it.
func (c *r872Client) answer(o r872Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.answers == nil {
		c.answers = map[string]r872Object{}
	}
	c.answers[o.name] = o
}

// r872Object is one object as "name, rv, uid".
type r872Object struct{ name, rv, uid string }

func (o r872Object) obj() *unstructured.Unstructured { return panelObj("demo", o.name, o.rv, o.uid) }

func r872Store(objs []r872Object) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.obj())
	}
	return out
}

func r872Meta(objs []r872Object) []*metav1.PartialObjectMetadata {
	out := make([]*metav1.PartialObjectMetadata, 0, len(objs))
	for _, o := range objs {
		out = append(out, partialMeta("demo", o.name, o.rv, o.uid))
	}
	return out
}

// r872Setup seeds the store with store, the cluster's current state with
// current, the forced LIST's answer with list, and returns the watcher and the
// client.
//
// It also delivers one ordinary, published update to a sentinel object that
// every side agrees on, so the latest delivery before the pass is a published
// one, as it is in steady state. Without it the latest object the transform
// saw is the last item of the initial sync, which a pass records as possibly
// in flight (store_verify_delivery.go step 3), and every arm's exact GET count
// would depend on which object the fake happened to list last. That rule has
// its own arm (TestDeliveryLog875_ADeliveryInFlightAtOpenIsRecorded).
func r872Setup(t *testing.T, store, current, list []r872Object, getErr error) (*ResourceWatcher, *r872Client) {
	t.Helper()
	c3Setup(t)
	resetVerificationForArm(t)
	zOld, zNew := r872Object{"panel-zz", "99", "uid-zz"}, r872Object{"panel-zz", "100", "uid-zz"}
	store = append(append([]r872Object(nil), store...), zOld)
	current = append(append([]r872Object(nil), current...), zNew)
	list = append(append([]r872Object(nil), list...), zNew)
	rw, dyn := silentWatchCluster(t, r872Store(store)...)
	rec := attachMetaClient(t, rw, r872Meta(current)...)
	c := &r872Client{inner: rec.inner, list: r872Meta(list), getErr: getErr}
	rw.SetMetadataClient(c)
	// The cluster too, so a relist sees the sentinel as the watch delivered it.
	if _, err := dyn.Resource(repairGVR).Namespace("demo").Update(context.Background(), zNew.obj(), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating the sentinel in the fake cluster: %v", err)
	}
	r872Deliver(t, rw, modified(zNew))
	return rw, c
}

// r872Event is one watch event an arm delivers.
type r872Event struct {
	typ watch.EventType
	obj r872Object
}

func modified(o r872Object) r872Event { return r872Event{watch.Modified, o} }
func added(o r872Object) r872Event    { return r872Event{watch.Added, o} }
func deleted(o r872Object) r872Event  { return r872Event{watch.Deleted, o} }

// r872Deliver writes events to the informer's watch and waits until the store
// shows the last of them for every key — so they are in the store, as they
// would be, by the time the LIST answers.
func r872Deliver(t *testing.T, rw *ResourceWatcher, events ...r872Event) {
	t.Helper()
	w := silentWatchLatest.Load()
	if w == nil {
		t.Fatalf("precondition: repairGVR's informer holds no watch to deliver through")
	}
	want := map[string]r872Event{}
	for _, e := range events {
		w.Action(e.typ, e.obj.obj())
		want[e.obj.name] = e
	}
	deadline := time.Now().Add(verifyBound)
	for name, e := range want {
		for {
			rv, found := indexerRV(t, rw, "demo", name)
			if e.typ == watch.Deleted && !found || e.typ != watch.Deleted && found && rv == e.obj.rv {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("precondition: the store never applied %s %s (rv=%q found=%v)", e.typ, name, rv, found)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func r872Published() uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	return lu + ld + la + um
}

func r872ClassCount(class string) uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	switch class {
	case "update":
		return lu
	case "delete":
		return ld
	case "add":
		return la
	case "uid":
		return um
	}
	panic(class)
}

var (
	a1 = r872Object{"panel-a", "1", "uid-a"}
	a2 = r872Object{"panel-a", "2", "uid-a"}
	a3 = r872Object{"panel-a", "3", "uid-a"}
	ax = r872Object{"panel-a", "7", "uid-x"} // panel-a deleted and recreated
	b1 = r872Object{"panel-b", "1", "uid-b"}
	// panel-b as its DELETE event carries it: the deletion's own resourceVersion.
	b1gone = r872Object{"panel-b", "3", "uid-b"}
	// panel-a as its DELETE event carries it, before ax was created.
	a1gone = r872Object{"panel-a", "6", "uid-a"}
	// panel-b recreated after the store saw it deleted.
	bnew = r872Object{"panel-b", "5", "uid-b2"}
)

// TestForcedPass872_StoreMovedDuringTheList_IsNotALoss — the #872 artifact, per
// class, produced the way production produces it: the watch delivers while the
// forced LIST is in flight, so the store is newer than the LIST. RED before
// #872: the confirm loop cannot clear a store that is AHEAD of the LIST, and a
// repair is queued for a store that lost nothing.
func TestForcedPass872_StoreMovedDuringTheList_IsNotALoss(t *testing.T) {
	for _, c := range []struct {
		name                 string
		store, current, list []r872Object
		deliver              []r872Event
		class                string
	}{
		// Updated during the LIST.
		{"update", []r872Object{a1}, []r872Object{a2}, []r872Object{a1}, []r872Event{modified(a2)}, "update"},
		// Deleted during the LIST: the store lacks a key the LIST still has.
		{"add", []r872Object{a2, b1}, []r872Object{a2}, []r872Object{a2, b1}, []r872Event{deleted(b1gone)}, "add"},
		// Created during the LIST: the store has a key the LIST lacks.
		{"delete", []r872Object{a2}, []r872Object{a2, b1}, []r872Object{a2}, []r872Event{added(b1)}, "delete"},
		// Deleted and recreated during the LIST: the store has the new uid.
		{"uid", []r872Object{a1}, []r872Object{ax}, []r872Object{a1}, []r872Event{deleted(a1gone), added(ax)}, "uid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rw, cli := r872Setup(t, c.store, c.current, c.list, nil)
			cli.duringList = func() { r872Deliver(t, rw, c.deliver...) }
			candidates0 := storeDivergenceCandidates.Load()
			forcedVerify(context.Background(), repairGVR)

			// Non-vacuity: the LIST really disagreed with the store.
			if storeDivergenceCandidates.Load() == candidates0 {
				t.Fatalf("precondition: the stale LIST produced no candidate, so this arm tests nothing")
			}
			if got := r872Published(); got != 0 {
				t.Fatalf("#872 RED: a store that moved past the LIST published %d forced divergences (class %s); it lost nothing", got, c.class)
			}
			if d := storeRepairQueueDepth(); d != 0 {
				t.Fatalf("#872 RED: %d repair(s) queued for a store that lost nothing", d)
			}
			if got := storeRecheckClearedTotal.Load(); got != 1 {
				t.Errorf("store_divergence_recheck_cleared_total = %d, want 1", got)
			}
			if got := storeRecheckUndeliveredTotal.Load(); got != 0 {
				t.Errorf("#875 RED: a key delivered during the pass was counted without a re-check (%d)", got)
			}
			// Bound: one GET per re-checked candidate when the first read clears it.
			if g := cli.gets.Load(); g != 1 {
				t.Errorf("re-check issued %d GETs for 1 candidate, want 1", g)
			}
			if row, ok := verificationRowFor(repairGVR); !ok || !row.verified {
				t.Errorf("a completed pass that found nothing must mark the GVR verified (row=%+v ok=%v)", row, ok)
			}
		})
	}
}

// TestForcedPass872_TrueLoss_NothingDelivered_IsRepairedWithoutAGET — a real
// divergence, per class, on a key nothing was delivered for during the pass:
// the silent watch dropped the event, so the store is behind a cluster the
// LIST agrees with. It cannot be "store ahead", so it is counted as it stands,
// with no GET (#875), and repaired.
func TestForcedPass872_TrueLoss_NothingDelivered_IsRepairedWithoutAGET(t *testing.T) {
	for _, c := range []struct {
		name           string
		store, current []r872Object
		class          string
	}{
		{"update", []r872Object{a1}, []r872Object{a2}, "update"},
		{"add", []r872Object{a2}, []r872Object{a2, b1}, "add"},
		{"delete", []r872Object{a2, b1}, []r872Object{a2}, "delete"},
		{"uid", []r872Object{a1}, []r872Object{ax}, "uid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, cli := r872Setup(t, c.store, c.current, c.current, nil)
			forcedVerify(context.Background(), repairGVR)
			if got := r872ClassCount(c.class); got != 1 {
				lu, ld, la, um := divergenceCounts(verifySiteForced)
				t.Fatalf("a TRUE %s loss published %d (update/delete/add/uid = %d/%d/%d/%d), want exactly 1 of its class",
					c.class, got, lu, ld, la, um)
			}
			if got := r872Published(); got != 1 {
				t.Errorf("published %d divergences in total, want 1", got)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Fatalf("store_repairs_pending = %d after a TRUE loss, want 1", d)
			}
			if g := cli.gets.Load(); g != 0 {
				t.Errorf("#875 RED: %d GETs for a candidate nothing was delivered for, want 0", g)
			}
			if got := storeRecheckUndeliveredTotal.Load(); got != 1 {
				t.Errorf("store_divergence_recheck_undelivered_total = %d, want 1", got)
			}
		})
	}
}

// TestForcedPass875_DeliveredButStillLost_IsDetectedAndRepaired — the re-check
// must not hide a real divergence on a key that WAS delivered during the pass,
// per class: the delivery moved the store, but it is still behind the cluster
// afterwards (the next event was dropped).
func TestForcedPass875_DeliveredButStillLost_IsDetectedAndRepaired(t *testing.T) {
	for _, c := range []struct {
		name           string
		store, current []r872Object
		deliver        []r872Event
		class          string
	}{
		{"update", []r872Object{a1}, []r872Object{a3}, []r872Event{modified(a2)}, "update"},
		{"add", []r872Object{a2, b1}, []r872Object{a2, bnew}, []r872Event{deleted(b1gone)}, "add"},
		{"delete", []r872Object{a2}, []r872Object{a2}, []r872Event{added(b1)}, "delete"},
		{"uid", []r872Object{a1}, []r872Object{ax}, []r872Event{modified(a2)}, "uid"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rw, cli := r872Setup(t, c.store, c.current, c.current, nil)
			cli.duringList = func() { r872Deliver(t, rw, c.deliver...) }
			forcedVerify(context.Background(), repairGVR)
			if got := r872ClassCount(c.class); got != 1 {
				lu, ld, la, um := divergenceCounts(verifySiteForced)
				t.Fatalf("a TRUE %s loss on a delivered key published %d (update/delete/add/uid = %d/%d/%d/%d), want exactly 1 of its class",
					c.class, got, lu, ld, la, um)
			}
			if got := r872Published(); got != 1 {
				t.Errorf("published %d divergences in total, want 1", got)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Fatalf("store_repairs_pending = %d after a TRUE loss, want 1 — the re-check must not stop the repair", d)
			}
			// #918 — a candidate is read once: a second read could only
			// give the drain more time (store_verify_recheck.go).
			if g := cli.gets.Load(); g != 1 {
				t.Errorf("re-check issued %d GETs for 1 divergent delivered candidate, want 1", g)
			}
			if got := storeRecheckClearedTotal.Load(); got != 0 {
				t.Errorf("store_divergence_recheck_cleared_total = %d for a true loss, want 0", got)
			}
		})
	}
}

// TestForcedPass875_WhollyDivergentStore_GoesStraightToRepair — the #875 FAIL,
// as the reviewer probed it: every stored object is a TRUE loss (#237's broken
// store) and nothing is churning. Before #875 the re-check issued two quorum
// GETs per object (20 for 10, 400 for 200) before the one repair, and any one
// failing kept the repair from being queued. Now: no GET, one repair.
func TestForcedPass875_WhollyDivergentStore_GoesStraightToRepair(t *testing.T) {
	for _, n := range []int{10, 200} {
		t.Run(fmt.Sprintf("%d_objects", n), func(t *testing.T) {
			var store, current []r872Object
			for i := 0; i < n; i++ {
				name := fmt.Sprintf("panel-%03d", i)
				store = append(store, r872Object{name, "1", "uid-" + name})
				current = append(current, r872Object{name, "2", "uid-" + name})
			}
			_, cli := r872Setup(t, store, current, current, nil)
			forcedVerify(context.Background(), repairGVR)

			if got := r872ClassCount("update"); got != uint64(n) {
				t.Fatalf("published %d lost updates for %d stale objects, want %d", got, n, n)
			}
			if g := cli.gets.Load(); g != 0 {
				t.Errorf("#875 RED: a wholly divergent store issued %d quorum GETs for %d objects nothing was delivered for, want 0", g, n)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Errorf("store_repairs_pending = %d, want 1", d)
			}
			if got := storeRecheckUndeliveredTotal.Load(); got != uint64(n) {
				t.Errorf("store_divergence_recheck_undelivered_total = %d, want %d", got, n)
			}
		})
	}
}

// TestForcedPass875_Mixed_OnlyDeliveredKeysAreReread — three objects moved
// during the LIST (delivered, store ahead) and three truly lost (nothing
// delivered, store behind). Exactly the three delivered keys are re-read, each
// once; the three lost ones are counted without a GET and repaired.
func TestForcedPass875_Mixed_OnlyDeliveredKeysAreReread(t *testing.T) {
	var store, current, list []r872Object
	var moves []r872Event
	var movedNames []string
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("panel-%d", i)
		uid := "uid-" + name
		store = append(store, r872Object{name, "1", uid})
		current = append(current, r872Object{name, "2", uid})
		if i < 3 {
			// Moved: the LIST still has rv 1; the store reaches rv 2 while
			// the LIST is in flight.
			list = append(list, r872Object{name, "1", uid})
			moves = append(moves, modified(r872Object{name, "2", uid}))
			movedNames = append(movedNames, name)
		} else {
			// Lost: the LIST and the cluster are at rv 2, the store at rv 1.
			list = append(list, r872Object{name, "2", uid})
		}
	}
	rw, cli := r872Setup(t, store, current, list, nil)
	cli.duringList = func() { r872Deliver(t, rw, moves...) }
	forcedVerify(context.Background(), repairGVR)

	if got := fmt.Sprint(cli.gotten()); got != fmt.Sprint(movedNames) {
		t.Errorf("#875 RED: re-read %s, want exactly the delivered keys %v", got, movedNames)
	}
	if got := r872ClassCount("update"); got != 3 {
		t.Errorf("published %d lost updates, want the 3 truly lost", got)
	}
	if got := storeRecheckClearedTotal.Load(); got != 3 {
		t.Errorf("store_divergence_recheck_cleared_total = %d, want the 3 that moved", got)
	}
	if got := storeRecheckUndeliveredTotal.Load(); got != 3 {
		t.Errorf("store_divergence_recheck_undelivered_total = %d, want the 3 truly lost", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Errorf("store_repairs_pending = %d, want 1", d)
	}
}

// TestForcedPass875_ListCarriesThePositionReadAtOpen — the record starts when
// the pass opens, so the LIST's lower bound must be the position read then: a
// delivery published after the last confirm pass copied lastSyncRV, but before
// the pass opened, is in neither the record nor a LIST at that older position.
// The sentinel delivery has moved the published position to 100 since the
// fixture's confirm pass recorded 4242.
func TestForcedPass875_ListCarriesThePositionReadAtOpen(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a1}, []r872Object{a1}, nil)
	rw.mu.RLock()
	confirmed := rw.lastSyncRV[repairGVR]
	rw.mu.RUnlock()
	if confirmed == "100" {
		t.Fatalf("precondition: the confirm-time position already equals the published one, so this arm tests nothing")
	}
	forcedVerify(context.Background(), repairGVR)
	if cli.listRV != "100" {
		t.Errorf("#875 RED: the forced LIST carried resourceVersion %q, want the position published when the pass opened (100), not the confirm-time %q",
			cli.listRV, confirmed)
	}
}

// TestForcedPass875_RelistDeletionDuringThePass_IsNotALoss — the one store
// change the transform does not see. The watch expires (410) while the LIST is
// in flight, the reflector relists, and the relist DELETES panel-b, which the
// cluster no longer has, with a synthesized tombstone that skips the
// transform. The LIST still has panel-b, so it is a lost-ADD candidate with no
// delivery recorded for its key; it was in the store when the pass opened,
// and the presence check is what makes it re-read and cleared.
func TestForcedPass875_RelistDeletionDuringThePass_IsNotALoss(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a2, b1}, []r872Object{a2}, []r872Object{a2, b1}, nil)
	dyn := rw.dyn
	cli.duringList = func() {
		if err := dyn.Resource(repairGVR).Namespace("demo").Delete(context.Background(), b1.name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("deleting panel-b from the cluster: %v", err)
		}
		silentWatchLatest.Load().Error(&metav1.Status{
			Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired,
			Message: "too old resource version",
		})
		deadline := time.Now().Add(verifyBound)
		for {
			if _, found := indexerRV(t, rw, "demo", b1.name); !found {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("precondition: the relist never removed panel-b from the store")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	forcedVerify(context.Background(), repairGVR)

	if got := r872Published(); got != 0 {
		t.Fatalf("#875 RED: a relist deletion during the pass published %d forced divergence(s); the store lost nothing", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#875 RED: %d repair(s) queued for a store a relist had just made current", d)
	}
	if g := cli.gets.Load(); g != 1 {
		t.Errorf("re-check issued %d GETs, want 1 (panel-b, gone from the store during the pass)", g)
	}
}

// TestForcedPass872_ObjectMovedAfterItsReRead_IsNotALoss — the object moves
// again after its re-read, and the watch delivers the move before the pass
// compares: the store is ahead of the read. #875 read a second time to catch
// this; under the #918 rule the delivery after the read settles it with one
// read (the many-object form is TestForcedPass918_*).
func TestForcedPass872_ObjectMovedAfterItsReRead_IsNotALoss(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a2}, []r872Object{a1}, nil)
	cli.duringList = func() { r872Deliver(t, rw, modified(a2)) }
	cli.answer(a2)
	cli.onGet = func(string) {
		cli.answer(a3)
		r872Deliver(t, rw, modified(a3))
	}
	forcedVerify(context.Background(), repairGVR)
	if got := r872Published(); got != 0 {
		t.Fatalf("#872 RED: an object that moved after its re-read was published as %d divergence(s)", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#872 RED: %d repair(s) queued for an object that only moved", d)
	}
	if g := cli.gets.Load(); g != 1 {
		t.Errorf("re-check issued %d GETs, want 1", g)
	}
}

// TestForcedPass872_FailedRecheck_IsInconclusive — a re-check read that fails
// must neither repair (a transient apiserver error would become the very false
// repair #872 removes) nor be recorded as a clean verification (a real loss
// would then hide for a whole maxVerificationAge). The candidate is a delivered
// key, so it is one the pass must re-read.
func TestForcedPass872_FailedRecheck_IsInconclusive(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a2}, []r872Object{a1},
		errors.New("etcdserver: request timed out"))
	cli.duringList = func() { r872Deliver(t, rw, modified(a2)) }
	before := VerifySkippedByReasonSnapshot()[verifySkipRecheckError]
	forcedVerify(context.Background(), repairGVR)

	if cli.gets.Load() == 0 {
		t.Fatalf("precondition: the re-check never ran, so this arm tests nothing")
	}
	if got := r872Published(); got != 0 {
		t.Errorf("#872 RED: an unconfirmed candidate was published as %d divergence(s) after the re-check failed", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Errorf("#872 RED: %d repair(s) queued on a re-check error; an error must not become a repair", d)
	}
	if got := VerifySkippedByReasonSnapshot()[verifySkipRecheckError] - before; got != 1 {
		t.Errorf("store_verification_skipped{recheck_error} advanced by %d, want 1", got)
	}
	if row, ok := verificationRowFor(repairGVR); !ok || row.verified {
		t.Errorf("an inconclusive pass must leave the GVR unverified so the deadline returns to it (row=%+v ok=%v)", row, ok)
	}
}

// --- the delivery log itself ------------------------------------------------

// r872Position is a settable published position.
type r872Position struct {
	mu sync.Mutex
	rv string
}

func (p *r872Position) get() string   { p.mu.Lock(); defer p.mu.Unlock(); return p.rv }
func (p *r872Position) set(rv string) { p.mu.Lock(); p.rv = rv; p.mu.Unlock() }

// TestDeliveryLog875_RecordsWhileAPassIsOpen — a delivery before the pass
// opens is not in its record (except the latest one, which may be in flight);
// one after is; nothing is recorded after close.
func TestDeliveryLog875_RecordsWhileAPassIsOpen(t *testing.T) {
	pos := &r872Position{rv: "10"}
	dl := newDeliveryLog(pos.get)
	tf := dl.transform(nil)
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})

	_, _ = tf(a1.obj())                                  // delivered and published before the pass
	_, _ = tf(r872Object{"panel-c", "4", "uid-c"}.obj()) // the latest before the pass
	win := dl.openPass(idx)
	if win.position != "10" {
		t.Fatalf("the pass opened at %q, want the published position 10", win.position)
	}
	_, _ = tf(b1.obj())
	keys := win.close()
	if _, in := keys["demo/panel-a"]; in {
		t.Errorf("a delivery followed by another before the pass opened is in its record: %v", keys)
	}
	if _, in := keys["demo/panel-b"]; !in {
		t.Errorf("a delivery while the pass was open is missing from its record: %v", keys)
	}
	_, _ = tf(ax.obj())
	if again := win.close(); len(again) != 2 {
		t.Errorf("close is not idempotent, or recording continued after it: %v", again)
	}
}

// TestDeliveryLog875_ADeliveryInFlightAtOpenIsRecorded — a delivery whose
// transform has run but whose position the reflector has not yet published is
// in flight: its store write may already be visible while the position the
// LIST is no older than is still the one before it. It is always the latest
// object the transform saw, so opening a pass records it.
func TestDeliveryLog875_ADeliveryInFlightAtOpenIsRecorded(t *testing.T) {
	pos := &r872Position{rv: "10"}
	dl := newDeliveryLog(pos.get)
	tf := dl.transform(nil)
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	_, _ = tf(a2.obj()) // transformed; the reflector has not published rv 2 yet

	win := dl.openPass(idx)
	keys := win.close()
	if win.position != "10" {
		t.Fatalf("precondition: the pass should read the still-unpublished position 10, read %q", win.position)
	}
	if _, in := keys["demo/panel-a"]; !in {
		t.Errorf("#875 RED: a delivery in flight when the pass opened is missing from its record: %v", keys)
	}

	// Once the reflector publishes it, the position IS its resourceVersion:
	// the LIST includes it and it needs no re-read.
	pos.set("2")
	if keys := dl.openPass(idx).close(); len(keys) != 0 {
		t.Errorf("a published delivery is recorded as in flight: %v", keys)
	}
}

// TestDeliveryLog875_TransformNeverBlocksOnInformerLocks — the transform runs
// under the FIFO's lock, and HasSynced holds the informer's startedLock while
// it takes that same lock. A transform that read the informer's position (it
// takes startedLock) deadlocked against any concurrent HasSynced: the first
// draft of #875 did, and the kind churn arm hung for 20 minutes. Here the watch
// delivers a stream of updates while other goroutines call HasSynced and
// LastSyncResourceVersion as fast as they can; every update must reach the
// store.
func TestDeliveryLog875_TransformNeverBlocksOnInformerLocks(t *testing.T) {
	_, _ = r872Setup(t, []r872Object{a1}, []r872Object{a1}, []r872Object{a1}, nil)
	rw := Global()
	rw.mu.RLock()
	inf := rw.informers[repairGVR].Informer()
	rw.mu.RUnlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = inf.HasSynced()
				_ = inf.LastSyncResourceVersion()
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()

	w := silentWatchLatest.Load()
	done := make(chan struct{})
	const updates = 300
	go func() {
		defer close(done)
		for i := 2; i <= updates+1; i++ {
			w.Modify(r872Object{"panel-a", fmt.Sprint(i), "uid-a"}.obj())
		}
	}()
	select {
	case <-done:
	case <-time.After(verifyBound):
		t.Fatalf("#875 RED: the reflector stopped taking events while HasSynced ran concurrently — the transform is blocked on an informer lock")
	}
	want := fmt.Sprint(updates + 1)
	for deadline := time.Now().Add(verifyBound); ; time.Sleep(5 * time.Millisecond) {
		if rv, _ := indexerRV(t, rw, "demo", "panel-a"); rv == want {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("#875 RED: the store stopped at rv %s of %s while HasSynced ran concurrently", rv, want)
		}
	}
}

// TestDeliveryLog875_ALostAddPresentAtOpenCountsAsDelivered — the relist
// deletion rule in isolation: only a lost ADD, and only for a key the store
// held when the pass opened.
func TestDeliveryLog875_ALostAddPresentAtOpenCountsAsDelivered(t *testing.T) {
	pos := &r872Position{rv: "10"}
	dl := newDeliveryLog(pos.get)
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	if err := idx.Add(b1.obj()); err != nil {
		t.Fatal(err)
	}
	win := dl.openPass(idx)
	win.close()
	if !win.moved("demo/panel-b", classLostAdd) {
		t.Errorf("a lost ADD for a key the store held at open must be re-read")
	}
	if win.moved("demo/panel-b", classLostUpdate) {
		t.Errorf("presence at open says nothing about any class but a lost ADD")
	}
	if win.moved("demo/panel-c", classLostAdd) {
		t.Errorf("a lost ADD for a key the store never held is a real loss, not a move")
	}
}

// --- the walker's turn order ------------------------------------------------

// TestNextOverdueGVR875_AnAttemptSendsAGVRToTheBack — a GVR whose passes keep
// ending inconclusive keeps its old epoch; ordered by epoch alone it would be
// re-picked every interval while every other overdue GVR waited. After an
// attempt it goes to the back, and both are still picked in turn.
func TestNextOverdueGVR875_AnAttemptSendsAGVRToTheBack(t *testing.T) {
	resetVerificationForArm(t)
	failing := schema.GroupVersionResource{Group: "a.krateo.io", Version: "v1", Resource: "failings"}
	waiting := schema.GroupVersionResource{Group: "b.krateo.io", Version: "v1", Resource: "waitings"}
	old := time.Now().Add(-3 * maxVerificationAge())
	storeVerify.mu.Lock()
	storeVerify.gvrs[failing] = &gvrVerification{epoch: old}
	storeVerify.gvrs[waiting] = &gvrVerification{epoch: old.Add(time.Minute)}
	storeVerify.mu.Unlock()

	pick := func() schema.GroupVersionResource {
		t.Helper()
		g, _, ok := nextOverdueGVR()
		if !ok {
			t.Fatal("nothing overdue")
		}
		return g
	}
	if g := pick(); g != failing {
		t.Fatalf("first pick %v, want the oldest epoch %v", g, failing)
	}
	noteVerifyAttempt(failing) // its pass ends inconclusive: epoch unchanged
	if g := pick(); g != waiting {
		t.Fatalf("#875 RED: after an inconclusive attempt the walker re-picked %v; %v has waited longer", g, waiting)
	}
	noteVerifyAttempt(waiting)
	if g := pick(); g != failing {
		t.Errorf("after both were attempted the turn did not come back to %v (got %v)", failing, g)
	}
	if row, _ := verificationRowFor(failing); !row.epoch.Equal(old) {
		t.Errorf("an attempt moved the epoch (%v -> %v); the age must stay true", old, row.epoch)
	}
}

// --- a GVR with no delivery record ----------------------------------------

// factory875GVR's group is never navigation-discovered, so its informer comes
// from the shared dynamic factory: the #237 / #244 class.
var factory875GVR = schema.GroupVersionResource{Group: "factory875.krateo.io", Version: "v1", Resource: "panels"}

func factory875Obj(name, rv, uid string) *unstructured.Unstructured {
	u := panelObj("demo", name, rv, uid)
	u.SetAPIVersion(factory875GVR.GroupVersion().String())
	return u
}

func factory875Meta(name, rv, uid string) *metav1.PartialObjectMetadata {
	m := partialMeta("demo", name, rv, uid)
	m.APIVersion = factory875GVR.GroupVersion().String()
	return m
}

// TestForcedPass875_NoDeliveryRecord_StillVerifiesAndCountsDirectly — residual
// B. A shared-factory GVR that is removed and registered again gets its
// ALREADY-STARTED informer back from the factory, SetTransform refuses it, and
// the GVR has no delivery record. Skipping the pass there would leave exactly
// the #237 broken-store class unverified and starve #244's factory-divergence
// age of its only input. The pass must run and count every candidate as it
// stands: published, repair attempted, the age signal set, no GET.
func TestForcedPass875_NoDeliveryRecord_StillVerifiesAndCountsDirectly(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	ctx := context.Background()
	rw, _ := silentWatchClusterFor(t, factory875GVR, false, factory875Obj("panel-a", "1", "uid-a"))

	// The re-registration that loses the transform, as production does it.
	rw.RemoveResourceType(factory875GVR)
	_, synced := rw.EnsureResourceType(factory875GVR)
	select {
	case <-synced:
	case <-time.After(verifyBound):
		t.Fatalf("precondition: the re-registered informer never reported synced")
	}
	rw.RefreshDiscovery(ctx)
	rw.mu.RLock()
	_, recorded := rw.deliveries[factory875GVR]
	rw.mu.RUnlock()
	if recorded {
		t.Fatalf("precondition: the re-registered GVR has a delivery record, so this arm does not reach the no-record path")
	}
	if !rw.IsServable(factory875GVR) {
		t.Fatalf("precondition: the re-registered GVR is not servable, so the pass would skip for that reason instead")
	}
	if row, ok := verificationRowFor(factory875GVR); !ok || row.ownsInformer {
		t.Fatalf("precondition: want a registered factory-built GVR (row=%+v ok=%v)", row, ok)
	}

	// A TRUE loss: the store holds rv 1, the cluster and the LIST rv 2.
	rec := attachMetaClient(t, rw, factory875Meta("panel-a", "2", "uid-a"))
	cli := &r872Client{inner: rec.inner, list: []*metav1.PartialObjectMetadata{factory875Meta("panel-a", "2", "uid-a")}}
	rw.SetMetadataClient(cli)
	skipped0, unsupported0 := storeVerifySkippedTotal.Load(), storeRepairUnsupportedTotal.Load()

	forcedVerify(ctx, factory875GVR)

	if got := storeVerifySkippedTotal.Load() - skipped0; got != 0 {
		t.Fatalf("#875 B RED: the pass was skipped (%d) for a GVR with no delivery record: %v", got, VerifySkippedByReasonSnapshot())
	}
	if lu, _, _, _ := divergenceCounts(verifySiteForced); lu != 1 {
		t.Errorf("published %d lost updates for a TRUE loss, want 1", lu)
	}
	if got := storeRecheckUnavailableTotal.Load(); got != 1 {
		t.Errorf("store_divergence_recheck_unavailable_total = %d, want 1", got)
	}
	if g := cli.gets.Load(); g != 0 {
		t.Errorf("%d GETs for a GVR with no delivery record, want 0", g)
	}
	// The repair is attempted; for a factory-built informer the verb refuses
	// it (#244) and the passive self-heal is the path, timed by the age below.
	if got := storeRepairUnsupportedTotal.Load() - unsupported0; got != 1 {
		t.Errorf("repair attempts refused as unsupported advanced by %d, want 1", got)
	}
	if factory244Since(factory875GVR).IsZero() {
		t.Errorf("#875 B RED: #244's factory-divergence age got no input: factoryDivergentSince is unset after a divergent pass")
	}
	if row, _ := verificationRowFor(factory875GVR); !row.verified {
		t.Errorf("a completed pass must mark the GVR verified")
	}
}
