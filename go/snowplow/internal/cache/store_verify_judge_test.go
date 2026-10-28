package cache

// store_verify_judge_test.go — #872, #875, #878, #918: the lost-update rule
// (store_verify_judge.go) and the records it reads (store_verify_delivery.go).
//
// Two kinds of arm:
//
//   - RULE arms judge one key against a plain indexer and a delivery log the
//     arm drives by hand, so every input of the rule — the store, the record,
//     the applied position — is set to an exact value.
//   - PASS arms run forcedVerify on the silent-watch fixture: a real
//     ResourceWatcher whose watch delivers only what the arm writes to it, so
//     the store moves only when the arm delivers, and the forced LIST answers
//     with exactly what the arm says the apiserver holds.
//
// RESOURCE VERSIONS. The fixture's initial LIST is at clusterListRV (4242), so
// every object the arms seed carries a resourceVersion below it and every watch
// event one above it, as a real apiserver's would: the rule orders them.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/metadata"
	clientcache "k8s.io/client-go/tools/cache"
)

// --- objects --------------------------------------------------------------

// j9Object is one object as "name, rv, uid", in namespace demo.
type j9Object struct{ name, rv, uid string }

func (o j9Object) key() string { return "demo/" + o.name }

func (o j9Object) id() objIdentity { return objIdentity{rv: o.rv, uid: o.uid} }

func (o j9Object) next(t *testing.T) j9Object {
	t.Helper()
	n, err := strconv.Atoi(o.rv)
	if err != nil {
		t.Fatalf("rv %q: %v", o.rv, err)
	}
	return j9Object{o.name, strconv.Itoa(n + 1), o.uid}
}

func (o j9Object) obj(gvr schema.GroupVersionResource) *unstructured.Unstructured {
	u := panelObj("demo", o.name, o.rv, o.uid)
	u.SetAPIVersion(gvr.GroupVersion().String())
	return u
}

func (o j9Object) meta(gvr schema.GroupVersionResource) metav1.PartialObjectMetadata {
	return metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: gvr.GroupVersion().String(), Kind: "Panel"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: o.name, ResourceVersion: o.rv, UID: k8stypes.UID(o.uid)},
	}
}

var (
	// Seeded (below the initial LIST's 4242).
	ja1 = j9Object{"panel-a", "4001", "uid-a"}
	jb1 = j9Object{"panel-b", "4001", "uid-b"}
	// Watch events (above it).
	ja2    = j9Object{"panel-a", "5002", "uid-a"}
	ja3    = j9Object{"panel-a", "5003", "uid-a"}
	jax    = j9Object{"panel-a", "5007", "uid-x"} // panel-a deleted and recreated
	ja1del = j9Object{"panel-a", "5006", "uid-a"} // panel-a's DELETE event, before jax
	jb1del = j9Object{"panel-b", "5003", "uid-b"} // panel-b's DELETE event
	jb2    = j9Object{"panel-b", "5004", "uid-b"}
	// The clock: an object every arm may touch to move the store's applied
	// position without touching the keys under test.
	jclock = j9Object{"panel-zz", "4099", "uid-zz"}
)

// --- the fixture ----------------------------------------------------------

// j9Client is the apiserver the forced pass talks to: LIST answers with list
// at listRV, after running duringList (what the watch delivers while the LIST
// is in flight). A GET answers from current and then runs onGet; it is
// counted, because the rule never re-reads (#918) — the arms that drive churn
// from it do so to reproduce #875's re-check, which did.
type j9Client struct {
	inner      metadata.Interface
	gvr        schema.GroupVersionResource
	duringList func()
	onGet      func(name string)
	gets       atomic.Int64

	mu      sync.Mutex
	list    []j9Object
	listRV  string
	asked   string
	current map[string]j9Object
	listErr error
}

// setCurrent records o as the apiserver's current state for a GET.
func (c *j9Client) setCurrent(o j9Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		c.current = map[string]j9Object{}
	}
	c.current[o.name] = o
}

func (c *j9Client) answer(listRV string, objs ...j9Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list, c.listRV = objs, listRV
}

func (c *j9Client) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return &j9Getter{Getter: c.inner.Resource(gvr), c: c}
}

type j9Getter struct {
	metadata.Getter
	c *j9Client
}

func (g *j9Getter) Namespace(ns string) metadata.ResourceInterface {
	return &j9Resource{ResourceInterface: g.Getter.Namespace(ns), c: g.c}
}

type j9Resource struct {
	metadata.ResourceInterface
	c *j9Client
}

func (r *j9Resource) List(_ context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	r.c.mu.Lock()
	r.c.asked = opts.ResourceVersion
	r.c.mu.Unlock()
	if r.c.duringList != nil {
		r.c.duringList()
	}
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if r.c.listErr != nil {
		return nil, r.c.listErr
	}
	out := &metav1.PartialObjectMetadataList{}
	out.ResourceVersion = r.c.listRV
	for _, o := range r.c.list {
		out.Items = append(out.Items, o.meta(r.c.gvr))
	}
	return out, nil
}

func (r *j9Resource) Get(_ context.Context, name string, _ metav1.GetOptions, _ ...string) (*metav1.PartialObjectMetadata, error) {
	r.c.gets.Add(1)
	r.c.mu.Lock()
	o, ok := r.c.current[name]
	r.c.mu.Unlock()
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: r.c.gvr.Group, Resource: r.c.gvr.Resource}, name)
	}
	m := o.meta(r.c.gvr)
	if r.c.onGet != nil {
		r.c.onGet(name)
	}
	return &m, nil
}

// j9Setup builds a watcher for gvr (owned: a standalone informer the GVR owns;
// otherwise one from the shared dynamic factory) whose store holds store plus
// the clock object, and a forced-pass client.
func j9Setup(t *testing.T, gvr schema.GroupVersionResource, owned bool, store ...j9Object) (*ResourceWatcher, *j9Client) {
	t.Helper()
	c3Setup(t)
	resetVerificationForArm(t)
	objs := []*unstructured.Unstructured{jclock.obj(gvr)}
	for _, o := range store {
		objs = append(objs, o.obj(gvr))
	}
	rw, _ := silentWatchClusterFor(t, gvr, owned, objs...)
	rec := attachMetaClient(t, rw)
	c := &j9Client{inner: rec.inner, gvr: gvr}
	rw.SetMetadataClient(c)
	rw.mu.RLock()
	_, recorded := rw.deliveries[gvr]
	rw.mu.RUnlock()
	if !recorded {
		t.Fatalf("precondition: %s has no delivery record", gvr)
	}
	return rw, c
}

// j9Event is one watch event an arm delivers.
type j9Event struct {
	typ watch.EventType
	obj j9Object
}

func jmod(o j9Object) j9Event { return j9Event{watch.Modified, o} }
func jadd(o j9Object) j9Event { return j9Event{watch.Added, o} }
func jdel(o j9Object) j9Event { return j9Event{watch.Deleted, o} }

// j9Deliver writes events to gvr's watch and waits until the store has
// applied the last of them for every key.
func j9Deliver(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource, events ...j9Event) {
	t.Helper()
	w := silentWatchLatest.Load()
	if w == nil {
		t.Fatalf("precondition: %s's informer holds no watch to deliver through", gvr)
	}
	want := map[string]j9Event{}
	for _, e := range events {
		w.Action(e.typ, e.obj.obj(gvr))
		want[e.obj.name] = e
	}
	for name, e := range want {
		j9WaitFor(t, fmt.Sprintf("the store applying %s %s", e.typ, name), func() bool {
			st := rw.StoreObjectState(gvr, "demo", name)
			if e.typ == watch.Deleted {
				return !st.Found
			}
			return st.Found && st.ResourceVersion == e.obj.rv
		})
	}
}

// j9Bookmark writes a bookmark at rv to gvr's watch and waits until the store
// has applied it (client-go v0.36.5 AtomicFIFO: a bookmark reaches the store
// in order and advances its applied position, with no handler callback).
func j9Bookmark(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource, rv string) {
	t.Helper()
	b := &unstructured.Unstructured{}
	b.SetAPIVersion(gvr.GroupVersion().String())
	b.SetKind("Panel")
	b.SetResourceVersion(rv)
	silentWatchLatest.Load().Action(watch.Bookmark, b)
	j9WaitFor(t, "the store applying the bookmark at "+rv, func() bool { return j9Position(t, rw, gvr) == rv })
}

// j9Log is gvr's delivery record.
func j9Log(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) *deliveryLog {
	t.Helper()
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.deliveries[gvr]
}

// j9Position is gvr's store's applied position.
func j9Position(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) string {
	t.Helper()
	rw.mu.RLock()
	gi := rw.informers[gvr]
	rw.mu.RUnlock()
	return gi.Informer().GetIndexer().LastStoreSyncResourceVersion()
}

// j9WaitFor waits for cond, an event the arm caused, to become true.
func j9WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(verifyBound); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("precondition: never observed %s", what)
		}
	}
}

func j9Published() uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	return lu + ld + la + um
}

func j9Class(class string) uint64 {
	lu, ld, la, um := divergenceCounts(verifySiteForced)
	return map[string]uint64{"update": lu, "delete": ld, "add": la, "uid": um}[class]
}

// --- PASS arms --------------------------------------------------------------

// TestForcedPass918_ObjectsThatChangeAfterTheirRead_AreNotLosses — THE #918
// FALSIFIER. Every object moves past the forced LIST while it is in flight,
// and moves again during the pass (from the GET hook #875's re-check offered:
// each object changes again after its read, as in bench run 16, where a 60-76 s
// sweep outlasted the objects' 20 s update interval). The store is AHEAD of
// every read. Nothing was lost.
//
// RED before #918 (measured on ce8f8d31): every key was published as a lost
// update and a repair queued. Now: no GET at all, every key cleared as
// store-ahead.
func TestForcedPass918_ObjectsThatChangeAfterTheirRead_AreNotLosses(t *testing.T) {
	const n = 6
	var seeded, listed []j9Object
	var moves []j9Event
	for i := 0; i < n; i++ {
		o := j9Object{fmt.Sprintf("panel-%d", i), "4001", fmt.Sprintf("uid-%d", i)}
		seeded = append(seeded, o)
		listed = append(listed, j9Object{o.name, "5001", o.uid}) // the LIST's read
		moves = append(moves, jmod(j9Object{o.name, strconv.Itoa(5100 + i), o.uid}))
	}
	rw, cli := j9Setup(t, repairGVR, true, seeded...)
	cli.answer("5050", append(listed, jclock)...)
	cli.duringList = func() { j9Deliver(t, rw, repairGVR, moves...) }
	// #875's re-check read each object, and each changed again right after
	// its read: churn driven from the GET, so it happens exactly where that
	// re-check would look. The rule issues no GET, so here it never fires.
	for _, m := range moves {
		cli.setCurrent(m.obj)
	}
	cli.onGet = func(name string) {
		cli.mu.Lock()
		cur := cli.current[name]
		cli.mu.Unlock()
		next := cur.next(t)
		cli.setCurrent(next)
		j9Deliver(t, rw, repairGVR, jmod(next))
	}
	candidates0 := storeDivergenceCandidates.Load()
	forcedVerify(context.Background(), repairGVR)

	if got := storeDivergenceCandidates.Load() - candidates0; got != n {
		t.Fatalf("precondition: %d candidates, want the %d keys that moved past the LIST", got, n)
	}
	if got := j9Published(); got != 0 {
		t.Fatalf("#918 RED: %d of %d objects the store had moved past the read were published as lost updates", got, n)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#918 RED: %d repair(s) queued for a store that lost nothing (the #870 window)", d)
	}
	if got := storeAheadTotal.Load(); got != n {
		t.Errorf("store_divergence_store_ahead_total = %d, want %d", got, n)
	}
	if g := cli.gets.Load(); g != 0 {
		t.Errorf("#918 RED: the pass issued %d GETs; the rule judges against the LIST's own revisions", g)
	}
	if row, ok := verificationRowFor(repairGVR); !ok || !row.verified {
		t.Errorf("a completed pass that found nothing must mark the GVR verified (row=%+v ok=%v)", row, ok)
	}
}

// TestForcedPass918_StoreAheadOfTheList_PerClass — #872's artifact in every
// class: the watch delivers while the forced LIST is in flight, so the store
// is newer than the LIST. Cleared, by ordering, with no GET.
func TestForcedPass918_StoreAheadOfTheList_PerClass(t *testing.T) {
	for _, c := range []struct {
		name    string
		store   []j9Object
		list    []j9Object
		deliver []j9Event
	}{
		{"update", []j9Object{ja1}, []j9Object{ja1}, []j9Event{jmod(ja2)}},
		// Deleted during the LIST: the store lacks a key the LIST still has.
		{"add", []j9Object{ja1, jb1}, []j9Object{ja1, jb1}, []j9Event{jdel(jb1del)}},
		// Created during the LIST: the store has a key the LIST lacks.
		{"delete", []j9Object{ja1}, []j9Object{ja1}, []j9Event{jadd(jb2)}},
		// Deleted and recreated during the LIST.
		{"uid", []j9Object{ja1}, []j9Object{ja1}, []j9Event{jdel(ja1del), jadd(jax)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rw, cli := j9Setup(t, repairGVR, true, c.store...)
			cli.answer("5001", append(c.list, jclock)...)
			cli.duringList = func() {
				for _, e := range c.deliver {
					j9Deliver(t, rw, repairGVR, e)
				}
			}
			candidates0 := storeDivergenceCandidates.Load()
			forcedVerify(context.Background(), repairGVR)
			if storeDivergenceCandidates.Load() == candidates0 {
				t.Fatalf("precondition: the LIST produced no candidate, so this arm tests nothing")
			}
			if got := j9Published(); got != 0 {
				t.Fatalf("#872 RED: a store that moved past the LIST published %d divergences (class %s)", got, c.name)
			}
			if d := storeRepairQueueDepth(); d != 0 {
				t.Fatalf("#872 RED: %d repair(s) queued for a store that lost nothing", d)
			}
			if got := storeAheadTotal.Load(); got != 1 {
				t.Errorf("store_divergence_store_ahead_total = %d, want 1", got)
			}
		})
	}
}

// TestForcedPass918_RealLoss_PerClass — the real-loss arm. The apiserver holds
// a change the store never received (the silent watch dropped it), and the
// store's applied position has passed it (a later event on another key). Every
// class is published and repaired.
func TestForcedPass918_RealLoss_PerClass(t *testing.T) {
	later := j9Object{jclock.name, "5100", jclock.uid}
	for _, c := range []struct {
		name  string
		store []j9Object
		list  []j9Object
	}{
		{"update", []j9Object{ja1}, []j9Object{ja2}},
		{"add", []j9Object{ja1}, []j9Object{ja1, jb2}},
		{"delete", []j9Object{ja1, jb1}, []j9Object{ja1}},
		{"uid", []j9Object{ja1}, []j9Object{jax}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rw, cli := j9Setup(t, repairGVR, true, c.store...)
			j9Deliver(t, rw, repairGVR, jmod(later))
			cli.answer("5100", append(c.list, later)...)
			forcedVerify(context.Background(), repairGVR)
			if got := j9Class(c.name); got != 1 {
				lu, ld, la, um := divergenceCounts(verifySiteForced)
				t.Fatalf("a real %s loss published %d (update/delete/add/uid = %d/%d/%d/%d), want 1 of its class", c.name, got, lu, ld, la, um)
			}
			if got := j9Published(); got != 1 {
				t.Errorf("published %d divergences in total, want 1", got)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Fatalf("store_repairs_pending = %d after a real loss, want 1", d)
			}
			if g := cli.gets.Load(); g != 0 {
				t.Errorf("%d GETs, want 0", g)
			}
		})
	}
}

// TestForcedPass918_RealLossAmongChurn — the falsifier's churn and one real
// loss together: only the real one is published.
func TestForcedPass918_RealLossAmongChurn(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1, jb1)
	lostB := j9Object{jb1.name, "5003", jb1.uid}  // the apiserver's panel-b; never delivered
	movedA := j9Object{ja1.name, "5004", ja1.uid} // panel-a, delivered past the LIST
	later := j9Object{jclock.name, "5100", jclock.uid}
	cli.answer("5050", ja2, lostB, jclock)
	cli.duringList = func() { j9Deliver(t, rw, repairGVR, jmod(movedA), jmod(later)) }
	forcedVerify(context.Background(), repairGVR)
	if got := j9Class("update"); got != 1 {
		t.Fatalf("published %d lost updates, want exactly the real one (panel-b)", got)
	}
	if got := storeAheadTotal.Load(); got < 1 {
		t.Errorf("store_divergence_store_ahead_total = %d, want panel-a cleared as ahead", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Fatalf("store_repairs_pending = %d, want 1", d)
	}
}

// TestForcedPass918_WhollyDivergentStore_GoesStraightToRepair — #237's broken
// store at scale: every object is a real loss and nothing is churning. One
// pass, no GET, one repair.
func TestForcedPass918_WhollyDivergentStore_GoesStraightToRepair(t *testing.T) {
	for _, n := range []int{10, 200} {
		t.Run(fmt.Sprintf("%d_objects", n), func(t *testing.T) {
			var seeded, listed []j9Object
			for i := 0; i < n; i++ {
				o := j9Object{fmt.Sprintf("panel-%03d", i), "4001", fmt.Sprintf("uid-%03d", i)}
				seeded = append(seeded, o)
				listed = append(listed, j9Object{o.name, "4100", o.uid})
			}
			rw, cli := j9Setup(t, repairGVR, true, seeded...)
			// The store's position (4242, its initial LIST) is past every lost
			// change (4100).
			if pos := j9Position(t, rw, repairGVR); pos != clusterListRV {
				t.Fatalf("precondition: the store's applied position is %q, want %s", pos, clusterListRV)
			}
			cli.answer("4242", append(listed, jclock)...)
			forcedVerify(context.Background(), repairGVR)
			if got := j9Class("update"); got != uint64(n) {
				t.Fatalf("published %d lost updates for %d stale objects, want %d", got, n, n)
			}
			if g := cli.gets.Load(); g != 0 {
				t.Errorf("%d GETs for a wholly divergent store, want 0", g)
			}
			if d := storeRepairQueueDepth(); d != 1 {
				t.Errorf("store_repairs_pending = %d, want 1", d)
			}
		})
	}
}

// TestForcedPass918_878_ReceivedNotYetApplied_IsHeldUntilApplied — #878, the
// store BEHIND the read. The reflector has received the read's state and the
// store has not applied it (the transform ran; the object was never queued
// here). Not a loss: the key is held PENDING until the apply lands, not
// cleared, so a delivery the queue then drops is still judged (#923 review
// L1). Here the apply lands — the watch delivers it for real — and the key
// resolves as received.
//
// RED before #918: the store alone was compared and the key published.
func TestForcedPass918_878_ReceivedNotYetApplied_IsHeldUntilApplied(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	cli.duringList = func() { j9Transformed(t, rw, repairGVR, ja2) }
	forcedVerify(context.Background(), repairGVR)
	if rv := rw.StoreObjectState(repairGVR, "demo", ja1.name).ResourceVersion; rv != ja1.rv {
		t.Fatalf("precondition: the store applied the delivery (rv %q), so it is not behind the read", rv)
	}
	if got := j9Published(); got != 0 {
		t.Fatalf("#878 RED: a store that had received the read's state was published as %d lost update(s)", got)
	}
	if got := storeReceivedTotal.Load(); got != 1 {
		t.Errorf("store_divergence_received_total = %d, want 1", got)
	}
	if got := j9Log(t, rw, repairGVR).pendingCount(); got != 1 {
		t.Fatalf("L1 RED: a received, unapplied key is not held pending (%d pending)", got)
	}
	j9Deliver(t, rw, repairGVR, jmod(ja2))
	j9WaitFor(t, "the held key resolving", func() bool { return storePendingKeys.Load() == 0 })
	if got := storePendingResolvedReceivedTotal.Load(); got != 1 {
		t.Errorf("store_divergence_pending_resolved_received_total = %d, want 1", got)
	}
	if got := j9Published(); got != 0 {
		t.Errorf("the applied delivery was published as %d lost update(s)", got)
	}
}

// j9Transformed runs gvr's installed delivery record on o, as the reflector's
// transform does, WITHOUT queueing it: a delivery received and not applied.
func j9Transformed(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource, o j9Object) {
	t.Helper()
	rw.mu.RLock()
	dl := rw.deliveries[gvr]
	rw.mu.RUnlock()
	if _, err := dl.transform(nil)(o.obj(gvr)); err != nil {
		t.Fatalf("transform: %v", err)
	}
}

// TestForcedPass918_PendingKey_ClearedByTheNextAppliedEvent — the store is
// behind the read and its applied position has not reached it: PENDING, not
// published. The next applied event of the GVR re-judges it: here the very
// delivery the watch was late with. Resolved as received, never published.
// The same arm runs on a shared-factory GVR: the rule is the same for every
// informer.
func TestForcedPass918_PendingKey_ClearedByTheNextAppliedEvent(t *testing.T) {
	for _, origin := range []struct {
		name  string
		gvr   schema.GroupVersionResource
		owned bool
	}{{"owned", repairGVR, true}, {"factory", factory875GVR, false}} {
		t.Run(origin.name, func(t *testing.T) {
			rw, cli := j9Setup(t, origin.gvr, origin.owned, ja1)
			cli.answer("5002", ja2, jclock)
			forcedVerify(context.Background(), origin.gvr)
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("precondition: %d pending keys, want panel-a (the store's position %s has not reached the read 5002)",
					got, j9Position(t, rw, origin.gvr))
			}
			dl := j9Log(t, rw, origin.gvr)
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("precondition: the key resolved before the watch caught up (%d pending)", got)
			}
			if got := j9Published(); got != 0 {
				t.Fatalf("a pending key was published as %d lost update(s)", got)
			}
			if row, _ := verificationRowFor(origin.gvr); !row.hasPending {
				t.Errorf("a pass that left a key pending must flag its GVR for the walker")
			}
			// The watch catches up: the read's own state is delivered.
			j9Deliver(t, rw, origin.gvr, jmod(ja2))
			j9WaitFor(t, "the pending key resolving", func() bool { return storePendingKeys.Load() == 0 })
			if got := storePendingResolvedReceivedTotal.Load(); got != 1 {
				t.Errorf("store_divergence_pending_resolved_received_total = %d, want 1", got)
			}
			if got := storePendingResolvedLostTotal.Load() + j9Published(); got != 0 {
				t.Fatalf("a key the watch caught up with was published as lost (%d)", got)
			}
			// H2 — the flag goes with the keys: the walker must not re-LIST a
			// GVR that has nothing left to decide.
			if row, _ := verificationRowFor(origin.gvr); row.hasPending {
				t.Errorf("H2 RED: the GVR is still flagged after its last pending key resolved")
			}
		})
	}
}

// TestForcedPass918_PendingKey_ResolvedLostByAnAppliedEventPastIt — pending,
// then an applied event on ANOTHER key moves the store's position past the
// read while the key's own change never arrives: the change was lost. The
// resolver publishes it and queues the repair, after the pass, on both origins.
func TestForcedPass918_PendingKey_ResolvedLostByAnAppliedEventPastIt(t *testing.T) {
	for _, origin := range []struct {
		name  string
		gvr   schema.GroupVersionResource
		owned bool
	}{{"owned", repairGVR, true}, {"factory", factory875GVR, false}} {
		t.Run(origin.name, func(t *testing.T) {
			rw, cli := j9Setup(t, origin.gvr, origin.owned, ja1)
			cli.answer("5002", ja2, jclock)
			forcedVerify(context.Background(), origin.gvr)
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("precondition: %d pending keys, want 1", got)
			}
			dl := j9Log(t, rw, origin.gvr)
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
			j9Deliver(t, rw, origin.gvr, jmod(j9Object{jclock.name, "5100", jclock.uid}))
			j9WaitFor(t, "the pending key resolving", func() bool { return storePendingKeys.Load() == 0 })
			if got := storePendingResolvedLostTotal.Load(); got != 1 {
				t.Fatalf("store_divergence_pending_resolved_lost_total = %d, want 1", got)
			}
			if got := j9Class("update"); got != 1 {
				t.Errorf("published %d lost updates, want 1", got)
			}
			// Owned GVRs queue the repair; a factory-built one is refused by
			// the repair verb (#244) and self-heals passively.
			if origin.owned {
				if d := storeRepairQueueDepth(); d != 1 {
					t.Errorf("store_repairs_pending = %d, want 1", d)
				}
			}
		})
	}
}

// TestForcedPass918_PendingKey_QuietGVR_ResolvedByTheNextPass — a BOOKMARK
// moves the store's position past the read, and nothing else happens on the
// GVR. client-go v0.36.5 gives a bookmark no callback
// (thread_safe_store.go:447-458, controller.go:279), so nothing re-judges the
// key until the GVR's next forced pass — which the walker takes next, and
// which judges the key against its OWN read before taking its own: lost.
func TestForcedPass918_PendingKey_QuietGVR_ResolvedByTheNextPass(t *testing.T) {
	for _, origin := range []struct {
		name  string
		gvr   schema.GroupVersionResource
		owned bool
	}{{"owned", repairGVR, true}, {"factory", factory875GVR, false}} {
		t.Run(origin.name, func(t *testing.T) {
			rw, cli := j9Setup(t, origin.gvr, origin.owned, ja1)
			cli.answer("5002", ja2, jclock)
			forcedVerify(context.Background(), origin.gvr)
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("precondition: %d pending keys, want 1", got)
			}
			if got := PendingByGVRSnapshot()[origin.gvr.String()]; got != 1 {
				t.Errorf("L3: the per-GVR breakdown shows %d pending on %s, want 1", got, origin.gvr)
			}
			// The resolver judges once when the key is held; let it, so the
			// bookmark below is the only thing that changes afterwards.
			dl := j9Log(t, rw, origin.gvr)
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
			j9Bookmark(t, rw, origin.gvr, "5100")
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("the bookmark resolved the pending key (%d left); client-go v0.36.5 gives it no callback, so this arm no longer describes the code", got)
			}
			if got, _, ok := nextOverdueGVR(); !ok || got != origin.gvr {
				t.Fatalf("the walker would take %v next, want %v (a pending key on it)", got, origin.gvr)
			}
			cli.answer("5150", ja2, jclock) // the cluster moved on past the bookmark
			forcedVerify(context.Background(), origin.gvr)
			if got := j9Class("update"); got != 1 {
				t.Fatalf("#918 RED: the next pass published %d lost updates, want 1", got)
			}
			if got := storePendingResolvedLostTotal.Load(); got != 1 {
				t.Errorf("store_divergence_pending_resolved_lost_total = %d, want 1 (decided against its own read)", got)
			}
			if got := storePendingKeys.Load(); got != 0 {
				t.Errorf("%d keys still pending after the deciding pass", got)
			}
		})
	}
}

// TestForcedPass918_LostDeleteOnAQuietGVR_PublishedByTheNextPass — #923 review
// H1. A real lost DELETE: the store still holds panel-b, which the apiserver
// deleted, so every LIST lacks it and it is held to the LIST's own
// resourceVersion — cluster-wide, moved on by every other resource — while
// this GVR's position moves only on its own events and bookmarks. Each LIST
// is taken after the cluster moved past the latest bookmark. The pending key
// must be judged against its OWN read at the next pass: published at pass 2.
//
// RED before the fix: the next pass discarded the pending key unjudged and
// held it again against its own, newer LIST, on every pass.
func TestForcedPass918_LostDeleteOnAQuietGVR_PublishedByTheNextPass(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1, jb1)
	dl := j9Log(t, rw, repairGVR)
	rounds := []struct{ bookmark, listRV string }{
		{"", "5002"},     // pass 1: the LIST is at 5002, the store at 4242
		{"5100", "5150"}, // a bookmark at 5100; the cluster moved on, LIST at 5150
		{"5200", "5250"},
		{"5300", "5350"},
	}
	for i, r := range rounds {
		if r.bookmark != "" {
			j9Bookmark(t, rw, repairGVR, r.bookmark)
		}
		cli.answer(r.listRV, ja1, jclock) // panel-b is gone upstream
		sweeps0 := dl.sweeps.Load()
		forcedVerify(context.Background(), repairGVR)
		if dl.pendingCount() > 0 {
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() > sweeps0 })
		}
		got := j9Class("delete")
		switch {
		case i == 0 && got != 0:
			t.Fatalf("pass 1 published %d lost deletes while the store had not reached the read", got)
		case i == 1 && got != 1:
			t.Fatalf("H1 RED: pass %d published %d lost deletes, want 1 — the key pending since pass 1 was not judged against its own read", i+1, got)
		case i > 1 && got < 1:
			t.Fatalf("pass %d: the lost delete is no longer published", i+1)
		}
		// Later passes publish it again while the repair (queued, not run by
		// this fixture) has not fixed the store: the divergence is still real.
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Errorf("store_repairs_pending = %d, want 1", d)
	}
}

// TestForcedPass918_PendingKeyAge_SurvivesTheSupersedingPass — #923 review L3:
// a key that stays pending pass after pass keeps the time it FIRST became
// pending, so store_divergence_pending_oldest_age_seconds grows instead of
// resetting at every pass.
func TestForcedPass918_PendingKeyAge_SurvivesTheSupersedingPass(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	dl := j9Log(t, rw, repairGVR)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	first := dl.oldestPending()
	if first.IsZero() {
		t.Fatalf("precondition: nothing pending after pass 1")
	}
	cli.answer("5003", ja3, jclock) // the store still has not caught up
	forcedVerify(context.Background(), repairGVR)
	if got := dl.pendingCount(); got != 1 {
		t.Fatalf("precondition: %d pending after pass 2, want the same key again", got)
	}
	if again := dl.oldestPending(); !again.Equal(first) {
		t.Errorf("L3 RED: the superseding pass reset the key's pending time (%v -> %v)", first, again)
	}
	if storePendingSupersededTotal.Load() != 1 {
		t.Errorf("store_divergence_pending_superseded_total = %d, want 1", storePendingSupersededTotal.Load())
	}
	if pendingOldestAgeSeconds() <= 0 {
		t.Errorf("store_divergence_pending_oldest_age_seconds is %v with a key pending", pendingOldestAgeSeconds())
	}
}

// TestForcedPass918_ReceivedPendingKey_KeptWhenTheNextPassCannotList — #923
// review N3. A key is pending, its read's state is received and still queued
// when the next pass begins, and that pass's LIST fails. judgePending must
// keep it pending, for the resolver to settle when the apply lands: the pass
// that would have superseded it never took its read.
func TestForcedPass918_ReceivedPendingKey_KeptWhenTheNextPassCannotList(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	if dl.pendingCount() != 1 {
		t.Fatalf("precondition: nothing pending")
	}
	j9Transformed(t, rw, repairGVR, ja2) // received, still queued
	cli.mu.Lock()
	cli.listErr = errors.New("the apiserver is unavailable")
	cli.mu.Unlock()
	forcedVerify(context.Background(), repairGVR)
	if got := dl.pendingCount(); got != 1 {
		t.Fatalf("N3 RED: a received, still-queued pending key was settled by a pass that could not LIST (%d pending)", got)
	}
	if got := j9Published(); got != 0 {
		t.Errorf("published %d divergence(s) for a received key", got)
	}
}

// TestForcedPass918_PositionUnavailable_GVRIsUnverifiable — #923 review N1:
// with client-go's AtomicFIFO off the store reports no position, so the
// forced pass cannot decide a loss. With a REAL lost update on the GVR, the
// pass must not run, must not mark the GVR verified, and the GVR must read as
// unverifiable with reason position_unavailable — never "looked, found
// nothing" — and it is not flagged.
func TestForcedPass918_PositionUnavailable_GVRIsUnverifiable(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.AtomicFIFO, false)
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	if pos := j9Position(t, rw, repairGVR); pos != "" {
		t.Fatalf("precondition: the store reports position %q with AtomicFIFO off", pos)
	}
	j9Deliver(t, rw, repairGVR, jmod(j9Object{jclock.name, "5100", jclock.uid}))
	cli.answer("5100", ja2, j9Object{jclock.name, "5100", jclock.uid}) // panel-a's 5002 was lost
	skipped0 := VerifySkippedByReasonSnapshot()[verifySkipPositionUnavailable]
	forcedVerify(context.Background(), repairGVR)
	if got := VerifySkippedByReasonSnapshot()[verifySkipPositionUnavailable] - skipped0; got != 1 {
		t.Errorf("store_verification_skipped{position_unavailable} advanced by %d, want 1", got)
	}
	if row, _ := verificationRowFor(repairGVR); row.verified || row.hasPending {
		t.Fatalf("N1 RED: with no store position the GVR reads verified=%v flagged=%v; want neither", row.verified, row.hasPending)
	}
	if _, unverifiable, _, _, byReason := storeVerificationGauges(); unverifiable < 1 || byReason[unverifiablePositionUnavailable] < 1 {
		t.Errorf("N1 RED: store_gvrs_unverifiable = %d, by reason %v; want the GVR counted under position_unavailable", unverifiable, byReason)
	}
	if got := j9Published(); got != 0 {
		t.Errorf("published %d divergence(s) with no store position", got)
	}
}

// TestForcedPass918_SkippedPass_KeepsTheKeysAndTheFlag — #923 review L4: a
// pass that cannot take its read keeps what is pending, and the flag with it.
func TestForcedPass918_SkippedPass_KeepsTheKeysAndTheFlag(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	if dl.pendingCount() != 1 {
		t.Fatalf("precondition: nothing pending")
	}
	cli.mu.Lock()
	cli.listErr = errors.New("the apiserver is unavailable")
	cli.mu.Unlock()
	forcedVerify(context.Background(), repairGVR)
	if got := dl.pendingCount(); got != 1 {
		t.Errorf("L4 RED: a pass that could not LIST dropped the pending key (%d left)", got)
	}
	if row, _ := verificationRowFor(repairGVR); !row.hasPending {
		t.Errorf("L4 RED: a pass that could not LIST cleared the flag while the key stays pending")
	}
}

// TestForcedPass918_ListCarriesThePublishedPosition — the forced LIST is no
// older than the reflector's position read when the pass opens.
func TestForcedPass918_ListCarriesThePublishedPosition(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	later := j9Object{jclock.name, "5100", jclock.uid}
	j9Deliver(t, rw, repairGVR, jmod(later))
	cli.answer("5100", ja1, later)
	forcedVerify(context.Background(), repairGVR)
	cli.mu.Lock()
	asked := cli.asked
	cli.mu.Unlock()
	if asked != "5100" {
		t.Errorf("the forced LIST carried resourceVersion %q, want the published position 5100", asked)
	}
}

// TestForcedPass918_RelistTombstone_HeldAndDecidedByTheNextPass — a relist
// during the pass deletes panel-b with a synthesized tombstone, which says
// nothing of when. The LIST still has panel-b: held pending, not published,
// and the GVR flagged. The NEXT pass decides it (#923 review L6): its read
// comes after the relist, so the tombstone is no longer "after the read".
func TestForcedPass918_RelistTombstone_HeldAndDecidedByTheNextPass(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1, jb1)
	dyn := rw.dyn
	cli.answer("5001", ja1, jb1, jclock)
	cli.duringList = func() {
		if err := dyn.Resource(repairGVR).Namespace("demo").Delete(context.Background(), jb1.name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("deleting panel-b from the cluster: %v", err)
		}
		silentWatchLatest.Load().Error(&metav1.Status{
			Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old resource version",
		})
		j9WaitFor(t, "the relist removing panel-b", func() bool { return !rw.StoreObjectState(repairGVR, "demo", jb1.name).Found })
	}
	forcedVerify(context.Background(), repairGVR)
	if got := j9Published(); got != 0 {
		t.Fatalf("a relist deletion during the pass published %d divergence(s)", got)
	}
	if got := j9Log(t, rw, repairGVR).pendingCount(); got != 1 {
		t.Fatalf("the tombstoned key is not held pending (%d pending)", got)
	}
	if row, _ := verificationRowFor(repairGVR); !row.hasPending {
		t.Errorf("a held tombstone must flag the GVR for the walker")
	}
	// The next pass: the apiserver no longer has panel-b either.
	cli.duringList = nil
	cli.answer(j9Position(t, rw, repairGVR), ja1, jclock)
	forcedVerify(context.Background(), repairGVR)
	if got := j9Log(t, rw, repairGVR).pendingCount(); got != 0 {
		t.Errorf("L6 RED: the tombstoned key is still pending after the next pass (%d)", got)
	}
	if got := j9Published(); got != 0 {
		t.Errorf("the next pass published %d divergence(s) for a key that agrees", got)
	}
	if row, _ := verificationRowFor(repairGVR); row.hasPending {
		t.Errorf("the flag outlived the tombstoned key")
	}
}

// TestForcedPass918_NoDeliveryLog_StillVerifies — #875 residual B: a
// re-registered shared-factory GVR gets its informer back from the factory
// STOPPED (RemoveResourceType stopped it, #244), and a stopped informer takes
// neither a transform nor a handler, so the GVR has no delivery log at all.
// The pass still runs and the rule still orders the store against the LIST: a
// real loss is published and the repair attempted, and #244's
// factory-divergence age gets its input. What it cannot know — deliveries
// received and not applied, applied deletes, a key to hold pending — is
// #940.
func TestForcedPass918_NoDeliveryLog_StillVerifies(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	ctx := context.Background()
	rw, _ := silentWatchClusterFor(t, factory875GVR, false, ja1.obj(factory875GVR), jb1.obj(factory875GVR))
	rw.mu.RLock()
	first := rw.informers[factory875GVR].Informer()
	rw.mu.RUnlock()
	rw.RemoveResourceType(factory875GVR)
	j9WaitFor(t, "the removed factory informer stopping", first.IsStopped)
	_, synced := rw.EnsureResourceType(factory875GVR)
	select {
	case <-synced:
	case <-time.After(verifyBound):
		t.Fatalf("precondition: the re-registered informer never reported synced")
	}
	rw.RefreshDiscovery(ctx)
	rw.mu.RLock()
	dl := rw.deliveries[factory875GVR]
	rw.mu.RUnlock()
	if dl != nil {
		t.Fatalf("precondition: the re-registered GVR has a delivery log (recording=%v)", dl.recording)
	}
	rec := attachMetaClient(t, rw)
	cli := &j9Client{inner: rec.inner, gvr: factory875GVR}
	cli.answer("4242", j9Object{ja1.name, "4100", ja1.uid}, jb1) // a real loss
	rw.SetMetadataClient(cli)
	unsupported0 := storeRepairUnsupportedTotal.Load()
	forcedVerify(ctx, factory875GVR)
	if lu, _, _, _ := divergenceCounts(verifySiteForced); lu != 1 {
		t.Fatalf("published %d lost updates for a real loss, want 1", lu)
	}
	if got := storeUnrecordedTotal.Load(); got != 1 {
		t.Errorf("store_divergence_unrecorded_total = %d, want 1", got)
	}
	if got := storeRepairUnsupportedTotal.Load() - unsupported0; got != 1 {
		t.Errorf("repair attempts refused as unsupported advanced by %d, want 1", got)
	}
	if factory244Since(factory875GVR).IsZero() {
		t.Errorf("#244's factory-divergence age got no input")
	}
}

// --- RULE arms ----------------------------------------------------------------

// j9Rule builds a store holding objs, a delivery log over it, and the store's
// applied position set to pos ("" for unknown).
func j9Rule(t *testing.T, pos string, objs ...j9Object) (clientcache.Indexer, *deliveryLog) {
	t.Helper()
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	for _, o := range objs {
		if err := idx.Add(o.obj(repairGVR)); err != nil {
			t.Fatal(err)
		}
	}
	idx.Bookmark(pos)
	if got := idx.LastStoreSyncResourceVersion(); got != pos {
		t.Fatalf("precondition: the store's applied position is %q, want %q (is AtomicFIFO on?)", got, pos)
	}
	dl := newDeliveryLog(repairGVR)
	dl.indexer, dl.applied = idx, idx.LastStoreSyncResourceVersion
	return idx, dl
}

func j9Present(o j9Object) readState   { return readState{present: true, id: o.id(), rev: o.rv} }
func j9Absent(listRV string) readState { return readState{rev: listRV} }

// TestJudge918_TheRule — every branch of the rule, on exact inputs.
func TestJudge918_TheRule(t *testing.T) {
	cases := []struct {
		name     string
		pos      string
		store    []j9Object
		received []j9Object
		delete   *appliedDelete
		read     readState
		want     outcome
		class    divergenceClass
		reverify bool
	}{
		{name: "agrees", pos: "5000", store: []j9Object{ja2}, read: j9Present(ja2), want: outcomeAgree},
		{name: "store ahead", pos: "5003", store: []j9Object{ja3}, read: j9Present(ja2), want: outcomeStoreAhead},
		{name: "recreated after the read", pos: "5007", store: []j9Object{jax}, read: j9Present(ja2), want: outcomeStoreAhead},
		{name: "behind, position past the read: lost", pos: "5003", store: []j9Object{ja1}, read: j9Present(ja2), want: outcomeLost, class: classLostUpdate},
		{name: "behind, position at the read: lost", pos: "5002", store: []j9Object{ja1}, read: j9Present(ja2), want: outcomeLost, class: classLostUpdate},
		{name: "behind, position before the read: pending", pos: "5001", store: []j9Object{ja1}, read: j9Present(ja2), want: outcomePending},
		{name: "behind, position unknown: pending, never lost", pos: "", store: []j9Object{ja1}, read: j9Present(ja2), want: outcomePending},
		{name: "received, still queued: received", pos: "5001", store: []j9Object{ja1}, received: []j9Object{ja2}, read: j9Present(ja2), want: outcomeReceived},
		{name: "received newer, still queued: received", pos: "5001", store: []j9Object{ja1}, received: []j9Object{ja3}, read: j9Present(ja2), want: outcomeReceived},
		{name: "received older, still queued: pending", pos: "5001", store: []j9Object{ja1}, received: []j9Object{{ja1.name, "5001", ja1.uid}}, read: j9Present(ja2), want: outcomePending},
		{name: "received but never applied, position past it: lost", pos: "5009", store: []j9Object{ja1}, received: []j9Object{ja2}, read: j9Present(ja2), want: outcomeLost, class: classLostUpdate},
		{name: "uid differs, behind: uid mismatch", pos: "5009", store: []j9Object{ja1}, read: j9Present(jax), want: outcomeLost, class: classUIDMismatch},
		{name: "absent in the read, store older: lost delete", pos: "5009", store: []j9Object{ja1}, read: j9Absent("5009"), want: outcomeLost, class: classLostDelete},
		{name: "absent in the read, store created after it", pos: "5009", store: []j9Object{ja3}, read: j9Absent("5002"), want: outcomeStoreAhead},
		{name: "absent in the read, absent in the store", pos: "5009", read: j9Absent("5002"), want: outcomeAgree},
		{name: "absent in the read, delete received and queued", pos: "5001", store: []j9Object{ja1}, received: []j9Object{ja1del}, read: j9Absent("5006"), want: outcomeReceived},
		{name: "present in the read, store deleted it after", pos: "5009", delete: &appliedDelete{rv: "5006"}, read: j9Present(ja2), want: outcomeStoreAhead},
		{name: "present in the read, store deleted it before: lost add", pos: "5009", delete: &appliedDelete{rv: "5001"}, read: j9Present(ja2), want: outcomeLost, class: classLostAdd},
		{name: "present in the read, never in the store: lost add", pos: "5009", read: j9Present(ja2), want: outcomeLost, class: classLostAdd},
		{name: "relist tombstone after the read: undecided, held", pos: "5009", delete: &appliedDelete{tombstone: true, gen: 1}, read: readState{present: true, id: ja2.id(), rev: ja2.rv, gen: 1}, want: outcomeUndecided, reverify: true},
		{name: "relist tombstone before the read: decided (L6)", pos: "5009", delete: &appliedDelete{tombstone: true, gen: 1}, read: readState{present: true, id: ja2.id(), rev: ja2.rv, gen: 2}, want: outcomeLost, class: classLostAdd},
		{name: "delete applied, handler not yet run: store ahead (H3)", pos: "5006", received: []j9Object{ja1del}, read: j9Present(ja2), want: outcomeStoreAhead},
		{name: "older delivery applied, store lacks the key: lost add", pos: "5006", received: []j9Object{{ja1.name, "5001", ja1.uid}}, read: j9Present(ja2), want: outcomeLost, class: classLostAdd},
		{name: "non-comparable versions: undecided", pos: "5009", store: []j9Object{{ja1.name, "abc", ja1.uid}}, read: j9Present(j9Object{ja1.name, "xyz", ja1.uid}), want: outcomeUndecided},
		{name: "non-comparable position: undecided", pos: "p-9", store: []j9Object{ja1}, read: j9Present(ja2), want: outcomeUndecided},
		{name: "non-comparable but equal: agrees", pos: "p-9", store: []j9Object{{ja1.name, "abc", ja1.uid}}, read: j9Present(j9Object{ja1.name, "abc", ja1.uid}), want: outcomeAgree},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			idx, dl := j9Rule(t, c.pos, c.store...)
			tf := dl.transform(nil)
			for _, r := range c.received {
				if _, err := tf(r.obj(repairGVR)); err != nil {
					t.Fatal(err)
				}
			}
			if c.delete != nil {
				dl.deletes[ja1.key()] = *c.delete
			}
			v := judgeKey(idx, dl, positionOf(idx.LastStoreSyncResourceVersion()), ja1.key(), c.read)
			if v.outcome != c.want || (c.want == outcomeLost && v.class != c.class) || v.reverify != c.reverify {
				t.Errorf("verdict %+v, want outcome %d class %d reverify %v", v, c.want, c.class, c.reverify)
			}
		})
	}
}

// TestJudge918_EmptyPositionNeverPublishes — a forced pass over a store whose
// applied position is unknown publishes nothing: every key behind its read is
// pending, never lost.
func TestJudge918_EmptyPositionNeverPublishes(t *testing.T) {
	resetVerificationForArm(t)
	idx, dl := j9Rule(t, "", ja1, jb1)
	defer dl.close()
	upstream := map[string]objIdentity{ja1.key(): ja2.id(), jb1.key(): {rv: "5003", uid: jb1.uid}}
	rep := judgeForcedPass(repairGVR, idx, dl, upstream, "5009", 0, diffStore(idx, upstream), nil)
	if rep.total() != 0 {
		t.Fatalf("an unknown store position published %d lost update(s)", rep.total())
	}
	if got := dl.pendingCount(); got != 2 {
		t.Errorf("%d keys pending, want 2", got)
	}
}

// TestJudge918_PositionUnavailableIsLoud — #923 review L2: with AtomicFIFO
// off the store reports no position and the forced pass can never publish;
// the store_position_unavailable gauge says so.
func TestJudge918_PositionUnavailableIsLoud(t *testing.T) {
	if got := positionUnavailable(); got != 0 {
		t.Fatalf("precondition: store_position_unavailable = %d with client-go's default gates", got)
	}
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.AtomicFIFO, false)
	if got := positionUnavailable(); got != 1 {
		t.Errorf("L2 RED: store_position_unavailable = %d with AtomicFIFO off, want 1", got)
	}
}

// TestJudge918_SnapshotPath — the snapshot path's rule: the reflector is
// blocked inside the read, so a store behind it is counted; a delivery the
// store had received before the read began, and not applied yet, is not. A
// delivery recorded AFTER the read began is the snapshot itself (a watch-list
// transforms its initial events into the reflector's temporary store) and
// settles nothing.
func TestJudge918_SnapshotPath(t *testing.T) {
	upstream := map[string]objIdentity{ja1.key(): ja2.id()}
	t.Run("received before the read", func(t *testing.T) {
		resetVerificationForArm(t)
		idx, dl := j9Rule(t, "5001", ja1)
		_, _ = dl.transform(nil)(ja2.obj(repairGVR))
		mark := dl.mark()
		if got := compareStore(idx, dl, upstream, "5002", mark).total(); got != 0 {
			t.Errorf("#878 RED: a delivery received before the read was counted as %d divergence(s)", got)
		}
	})
	t.Run("recorded by the read itself", func(t *testing.T) {
		resetVerificationForArm(t)
		idx, dl := j9Rule(t, "5001", ja1)
		mark := dl.mark()
		_, _ = dl.transform(nil)(ja2.obj(repairGVR)) // the watch-list's initial event
		if got := compareStore(idx, dl, upstream, "5002", mark).total(); got != 1 {
			t.Errorf("the snapshot's own initial events settled the divergence (%d counted, want 1)", got)
		}
	})
	t.Run("store ahead of the snapshot", func(t *testing.T) {
		resetVerificationForArm(t)
		idx, dl := j9Rule(t, "5003", ja3)
		if got := compareStore(idx, dl, upstream, "5002", dl.mark()).total(); got != 0 {
			t.Errorf("a store ahead of the snapshot was counted as %d divergence(s)", got)
		}
	})
}

// --- the records ----------------------------------------------------------------

// TestDeliveryLog918_RecordsAfterTheStrip — F1 of the #923 review. The
// transform records a delivery only once the strip has returned it: a strip
// error (never queued) or a nil object (queued, never applied) is not
// "received". The reviewer's case: the store holds rv 1, the strip fails on
// rv 2, the apiserver holds rv 2 — a real loss, which the record must not
// settle.
func TestDeliveryLog918_RecordsAfterTheStrip(t *testing.T) {
	for _, c := range []struct {
		name  string
		strip clientcache.TransformFunc
	}{
		{"nil object", func(any) (any, error) { return nil, nil }},
		{"typed nil object", func(any) (any, error) { return (*unstructured.Unstructured)(nil), nil }},
		{"error", func(any) (any, error) { return nil, errors.New("strip failed") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			idx, dl := j9Rule(t, "5001", ja1)
			_, _ = dl.transform(c.strip)(ja2.obj(repairGVR))
			if _, ok := dl.receivedFor(ja1.key()); ok {
				t.Fatalf("F1 RED: a delivery the strip dropped is recorded as received")
			}
			// Later deliveries move the store past it; the change is lost.
			idx.Bookmark("5009")
			if v := judgeKey(idx, dl, positionOf("5009"), ja1.key(), j9Present(ja2)); v.outcome != outcomeLost {
				t.Errorf("F1 RED: the real loss judged %+v, want lost", v)
			}
		})
	}
	t.Run("a stripped object is recorded", func(t *testing.T) {
		_, dl := j9Rule(t, "5001", ja1)
		_, _ = dl.transform(func(o any) (any, error) { return o, nil })(ja2.obj(repairGVR))
		if r, ok := dl.receivedFor(ja1.key()); !ok || r.id != ja2.id() {
			t.Errorf("a delivery the strip returned is not recorded (%+v, %v)", r, ok)
		}
	})
}

// TestDeliveryLog918_HandlerSeesAppliedDeliveries — the handler drops a
// received entry only when that very delivery is applied, and records deletes
// with their resourceVersion, and tombstones.
func TestDeliveryLog918_HandlerSeesAppliedDeliveries(t *testing.T) {
	_, dl := j9Rule(t, "5001", ja1)
	tf, h := dl.transform(nil), dl.handler()
	_, _ = tf(ja2.obj(repairGVR))
	_, _ = tf(ja3.obj(repairGVR))
	h.OnUpdate(ja1.obj(repairGVR), ja2.obj(repairGVR)) // the older one applied
	if r, ok := dl.receivedFor(ja1.key()); !ok || r.id != ja3.id() {
		t.Fatalf("an older delivery's apply dropped the newer received entry (%+v, %v)", r, ok)
	}
	h.OnUpdate(ja2.obj(repairGVR), ja3.obj(repairGVR))
	if _, ok := dl.receivedFor(ja1.key()); ok {
		t.Errorf("the latest delivery's apply left its received entry")
	}
	h.OnDelete(jb1del.obj(repairGVR))
	if d, ok := dl.deleteFor(jb1.key()); !ok || d.rv != jb1del.rv || d.tombstone {
		t.Errorf("an applied delete recorded as %+v (%v), want rv %s", d, ok, jb1del.rv)
	}
	h.OnDelete(clientcache.DeletedFinalStateUnknown{Key: ja1.key(), Obj: ja3.obj(repairGVR)})
	if d, ok := dl.deleteFor(ja1.key()); !ok || !d.tombstone {
		t.Errorf("a relist tombstone recorded as %+v (%v)", d, ok)
	}
	dl.beginPass()
	if _, ok := dl.deleteFor(jb1.key()); !ok {
		t.Errorf("a delete applied before the latest pass opened is forgotten by it")
	}
	dl.beginPass()
	if _, ok := dl.deleteFor(jb1.key()); ok {
		t.Errorf("a delete two passes old is still held")
	}
}

// TestDeliveryLog918_AppliedEntriesReleasedByTheNextPass — #923 review L8 / N2:
// a relist re-delivers every object and applies an unchanged one without
// telling any handler, so its received entry is never removed by the handler.
// The next pass releases every entry the store's position has passed for a key
// the store holds — and KEEPS one for a key the store lacks: that may be a
// DELETE the store applied before the handler recorded it, which only the
// entry tells apart from a lost add (H3), so the rule must still read it.
func TestDeliveryLog918_AppliedEntriesReleasedByTheNextPass(t *testing.T) {
	idx, dl := j9Rule(t, "5006", ja2)
	tf := dl.transform(nil)
	_, _ = tf(ja2.obj(repairGVR))    // an unchanged re-delivery: the store holds it
	_, _ = tf(jb1del.obj(repairGVR)) // panel-b's delete, applied, handler not yet run
	dl.beginPass()
	if _, ok := dl.receivedFor(ja2.key()); ok {
		t.Errorf("L8 RED: a received entry the store has applied is held past the next pass")
	}
	if _, ok := dl.receivedFor(jb1.key()); !ok {
		t.Fatalf("H3 RED: the next pass released the entry of a key the store lacks")
	}
	if v := judgeKey(idx, dl, positionOf("5006"), jb1.key(), j9Present(jb1)); v.outcome != outcomeStoreAhead {
		t.Errorf("H3 RED: a delete applied before the handler, after the pass opened, judged %+v, want store ahead", v)
	}
}

// TestDeliveryLog918_TransformNeverBlocksOnInformerLocks — the transform runs
// under the queue's lock and must take no informer lock (#875). Here the watch
// delivers a stream of updates while other goroutines call the informer's
// accessors, LastStoreSyncResourceVersion included, as fast as they can; every
// update must reach the store.
func TestDeliveryLog918_TransformNeverBlocksOnInformerLocks(t *testing.T) {
	rw, _ := j9Setup(t, repairGVR, true, ja1)
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
				_ = inf.GetIndexer().LastStoreSyncResourceVersion()
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()
	const updates = 300
	w := silentWatchLatest.Load()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= updates; i++ {
			w.Modify(j9Object{ja1.name, strconv.Itoa(5000 + i), ja1.uid}.obj(repairGVR))
		}
	}()
	select {
	case <-done:
	case <-time.After(verifyBound):
		t.Fatalf("#875 RED: the reflector stopped taking events while the accessors ran — the transform is blocked on an informer lock")
	}
	want := strconv.Itoa(5000 + updates)
	j9WaitFor(t, "the last update in the store", func() bool {
		return rw.StoreObjectState(repairGVR, "demo", ja1.name).ResourceVersion == want
	})
}

// TestNextOverdueGVR918_FlaggedAndOverdueAlternate — owner ruling 2026-10-27
// (#923 review H2): a GVR with pending keys is taken next, but the turn after
// goes to the most overdue GVR, and so on. Two GVRs stay flagged across
// passes while a third is overdue: the overdue one is served within one turn.
func TestNextOverdueGVR918_FlaggedAndOverdueAlternate(t *testing.T) {
	resetVerificationForArm(t)
	f1 := schema.GroupVersionResource{Group: "a.krateo.io", Version: "v1", Resource: "flaggedones"}
	f2 := schema.GroupVersionResource{Group: "b.krateo.io", Version: "v1", Resource: "flaggedtwos"}
	over := schema.GroupVersionResource{Group: "c.krateo.io", Version: "v1", Resource: "overdues"}
	now := time.Now()
	storeVerify.mu.Lock()
	storeVerify.gvrs[f1] = &gvrVerification{epoch: now}
	storeVerify.gvrs[f2] = &gvrVerification{epoch: now}
	storeVerify.gvrs[over] = &gvrVerification{epoch: now.Add(-3 * maxVerificationAge())}
	storeVerify.mu.Unlock()
	setPendingFlag(f1, true)
	setPendingFlag(f2, true)

	var turns []schema.GroupVersionResource
	for i := 0; i < 4; i++ {
		g, _, ok := nextOverdueGVR()
		if !ok {
			t.Fatalf("turn %d: nothing picked", i+1)
		}
		turns = append(turns, g)
		noteVerifyAttempt(g) // the pass runs; f1 and f2 stay flagged
	}
	if turns[0] != f1 && turns[0] != f2 {
		t.Errorf("turn 1 took %v, want a flagged GVR", turns[0])
	}
	if turns[1] != over {
		t.Errorf("H2 RED: turn 2 took %v, want the overdue %v after a flagged turn (turns: %v)", turns[1], over, turns)
	}
	if turns[2] != f1 && turns[2] != f2 {
		t.Errorf("turn 3 took %v, want a flagged GVR again", turns[2])
	}
	seen := map[schema.GroupVersionResource]bool{}
	for _, g := range turns {
		seen[g] = true
	}
	if !seen[f1] || !seen[f2] {
		t.Errorf("both flagged GVRs must get turns: %v", turns)
	}
}

// TestNextOverdueGVR918_FlagGoesWithTheKeys — the flag is the pending set's:
// cleared the moment it empties, by whatever empties it.
func TestNextOverdueGVR918_FlagGoesWithTheKeys(t *testing.T) {
	resetVerificationForArm(t)
	gvr := schema.GroupVersionResource{Group: "a.krateo.io", Version: "v1", Resource: "flaggeds"}
	over := schema.GroupVersionResource{Group: "b.krateo.io", Version: "v1", Resource: "overdues"}
	storeVerify.mu.Lock()
	storeVerify.gvrs[gvr] = &gvrVerification{epoch: time.Now()}
	storeVerify.gvrs[over] = &gvrVerification{epoch: time.Now().Add(-3 * maxVerificationAge())}
	storeVerify.mu.Unlock()
	setPendingFlag(gvr, true)
	if g, _, _ := nextOverdueGVR(); g != gvr {
		t.Fatalf("precondition: picked %v, want the flagged %v", g, gvr)
	}
	setPendingFlag(gvr, false)
	if g, _, _ := nextOverdueGVR(); g != over {
		t.Errorf("an unflagged GVR inside its deadline was picked (%v)", g)
	}
}

// TestNextOverdueGVR875_AnAttemptSendsAGVRToTheBack — a GVR whose passes keep
// ending inconclusive keeps its old epoch; after an attempt it goes to the
// back, and both are still picked in turn.
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
	noteVerifyAttempt(failing)
	if g := pick(); g != waiting {
		t.Fatalf("#875 RED: after an inconclusive attempt the walker re-picked %v", g)
	}
	noteVerifyAttempt(waiting)
	if g := pick(); g != failing {
		t.Errorf("after both were attempted the turn did not come back to %v (got %v)", failing, g)
	}
}

// factory875GVR's group is never navigation-discovered, so its informer comes
// from the shared dynamic factory: the #237 / #244 class.
var factory875GVR = schema.GroupVersionResource{Group: "factory875.krateo.io", Version: "v1", Resource: "panels"}

func factory875Obj(name, rv, uid string) *unstructured.Unstructured {
	u := panelObj("demo", name, rv, uid)
	u.SetAPIVersion(factory875GVR.GroupVersion().String())
	return u
}
