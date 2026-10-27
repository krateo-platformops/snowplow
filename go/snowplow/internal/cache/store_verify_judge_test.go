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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
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
// is in flight). A GET is counted and refused: the rule never re-reads (#918).
type j9Client struct {
	inner      metadata.Interface
	gvr        schema.GroupVersionResource
	duringList func()
	gets       atomic.Int64

	mu     sync.Mutex
	list   []j9Object
	listRV string
	asked  string
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
	out := &metav1.PartialObjectMetadataList{}
	out.ResourceVersion = r.c.listRV
	for _, o := range r.c.list {
		out.Items = append(out.Items, o.meta(r.c.gvr))
	}
	return out, nil
}

func (r *j9Resource) Get(context.Context, string, metav1.GetOptions, ...string) (*metav1.PartialObjectMetadata, error) {
	r.c.gets.Add(1)
	return nil, errors.New("the lost-update rule never re-reads an object (#918)")
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

// TestForcedPass918_878_ReceivedNotYetApplied_IsNotALoss — #878, the store
// BEHIND the read. The reflector has received the read's state and the store
// has not applied it. Recorded as received (the transform ran), not applied
// (never queued here), its rv past the store's applied position.
//
// RED before #918: the store alone was compared and the key published.
func TestForcedPass918_878_ReceivedNotYetApplied_IsNotALoss(t *testing.T) {
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
			if row, _ := verificationRowFor(origin.gvr); !row.reverifyFirst {
				t.Errorf("a pass that left a key pending must put its GVR at the front of the walker")
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
// key until the GVR's next forced pass — which the walker takes first. That
// pass decides it: lost.
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
			// The resolver judges once when the key is held; let it, so the
			// bookmark below is the only thing that changes afterwards.
			dl := j9Log(t, rw, origin.gvr)
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
			j9Bookmark(t, rw, origin.gvr, "5100")
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("the bookmark resolved the pending key (%d left); client-go v0.36.5 gives it no callback, so this arm no longer describes the code", got)
			}
			if got, _, ok := nextOverdueGVR(); !ok || got != origin.gvr {
				t.Fatalf("#918 RED: the walker would take %v next, want %v (a pending key on it)", got, origin.gvr)
			}
			cli.answer("5100", ja2, jclock)
			forcedVerify(context.Background(), origin.gvr)
			if got := storePendingSupersededTotal.Load(); got != 1 {
				t.Errorf("store_divergence_pending_superseded_total = %d, want 1", got)
			}
			if got := j9Class("update"); got != 1 {
				t.Fatalf("#918 RED: the next pass published %d lost updates, want 1", got)
			}
			if got := storePendingKeys.Load(); got != 0 {
				t.Errorf("%d keys still pending after the deciding pass", got)
			}
		})
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

// TestForcedPass918_RelistTombstone_IsUndecidedAndReverified — a relist during
// the pass deletes panel-b with a synthesized tombstone, which says nothing of
// when. The LIST still has panel-b: undecided, not published, and the GVR goes
// to the front of the walker so a pass over the relisted store decides it.
func TestForcedPass918_RelistTombstone_IsUndecidedAndReverified(t *testing.T) {
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
	if got := storeUndecidedTotal.Load(); got < 1 {
		t.Errorf("store_divergence_undecided_total = %d, want the tombstoned key", got)
	}
	if row, _ := verificationRowFor(repairGVR); !row.reverifyFirst {
		t.Errorf("an undecided tombstone must put the GVR at the front of the walker")
	}
}

// TestForcedPass918_NoDeliveryRecord_StillVerifies — #875 residual B: a
// re-registered shared-factory GVR has no record. The rule still orders the
// store against the LIST: a real loss is published and the repair attempted,
// and #244's factory-divergence age gets its input.
func TestForcedPass918_NoDeliveryRecord_StillVerifies(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	ctx := context.Background()
	rw, _ := silentWatchClusterFor(t, factory875GVR, false, ja1.obj(factory875GVR))
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
		t.Fatalf("precondition: the re-registered GVR has a delivery record")
	}
	rec := attachMetaClient(t, rw)
	cli := &j9Client{inner: rec.inner, gvr: factory875GVR}
	cli.answer("4242", j9Object{ja1.name, "4100", ja1.uid}) // changed before the store's position
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
		{name: "relist tombstone: undecided, reverify", pos: "5009", delete: &appliedDelete{tombstone: true}, read: j9Present(ja2), want: outcomeUndecided, reverify: true},
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
	rep, reverify := judgeForcedPass(repairGVR, idx, dl, upstream, "5009", diffStore(idx, upstream))
	if rep.total() != 0 {
		t.Fatalf("an unknown store position published %d lost update(s)", rep.total())
	}
	if got := dl.pendingCount(); got != 2 {
		t.Errorf("%d keys pending, want 2", got)
	}
	if !reverify {
		t.Errorf("pending keys must put the GVR at the front of the walker")
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

// TestNextOverdueGVR918_ReverifyFirstGoesAhead — a GVR flagged by a pass that
// left keys pending is taken before every overdue one, and the flag clears
// when its pass starts.
func TestNextOverdueGVR918_ReverifyFirstGoesAhead(t *testing.T) {
	resetVerificationForArm(t)
	overdue := schema.GroupVersionResource{Group: "a.krateo.io", Version: "v1", Resource: "overdues"}
	fresh := schema.GroupVersionResource{Group: "b.krateo.io", Version: "v1", Resource: "freshes"}
	storeVerify.mu.Lock()
	storeVerify.gvrs[overdue] = &gvrVerification{epoch: time.Now().Add(-3 * maxVerificationAge())}
	storeVerify.gvrs[fresh] = &gvrVerification{epoch: time.Now()}
	storeVerify.mu.Unlock()
	if g, _, _ := nextOverdueGVR(); g != overdue {
		t.Fatalf("precondition: first pick %v, want %v", g, overdue)
	}
	markReverifyFirst(fresh)
	if g, _, ok := nextOverdueGVR(); !ok || g != fresh {
		t.Fatalf("#918 RED: picked %v, want the flagged %v ahead of the overdue one", g, fresh)
	}
	noteVerifyAttempt(fresh)
	if g, _, _ := nextOverdueGVR(); g != overdue {
		t.Errorf("the flag did not clear when the pass started (picked %v)", g)
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
