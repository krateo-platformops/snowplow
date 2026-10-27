package cache

// store_swap_adapter_arms_test.go — #870 delivery-order arms on the REAL
// handler adapter (store_instance.go), driven in a listener's exact order.
//
// A client-go processorListener calls its handler sequentially, one
// notification at a time, and every store write is visible before its
// notification is delivered. These arms play that contract by hand: the store
// is written, then the adapter receives the listener's notifications in the
// order the arm states, including the swapDelivery the swap enqueues. That makes
// each interleaving the design names deterministic (v5 §2.7, v5.1 C1): the
// same code runs on a real listener in production (the real-informer arms in
// store_swap_arms_test.go).

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

type stubSharedInformer struct {
	clientcache.SharedIndexInformer
	idx clientcache.Indexer
}

func (s stubSharedInformer) GetIndexer() clientcache.Indexer { return s.idx }

type stubGenericInformer struct{ inf stubSharedInformer }

func (g stubGenericInformer) Informer() clientcache.SharedIndexInformer { return g.inf }
func (g stubGenericInformer) Lister() clientcache.GenericLister         { return nil }

var _ informers.GenericInformer = stubGenericInformer{}

// adapterRig is one instance's adapter with a recording consumer and the
// (GVR, handler) acted record it shares with earlier instances.
type adapterRig struct {
	t     *testing.T
	gvr   schema.GroupVersionResource
	idx   clientcache.Indexer
	inst  *storeInstance
	a     *handlerAdapter
	rec   *swapRecorder
	acted *actedRecord
}

func newAdapterRig(t *testing.T, acted *actedRecord, muted bool) *adapterRig {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	inst := newStoreInstance(gvr, stubGenericInformer{inf: stubSharedInformer{idx: idx}}, shapeStandaloneDynamic, make(chan struct{}))
	inst.muted.Store(muted)
	rec := &swapRecorder{}
	if acted == nil {
		acted = newActedRecord()
	}
	a := inst.adapt("ext/1", rec.handler(), acted)
	return &adapterRig{t: t, gvr: gvr, idx: idx, inst: inst, a: a, rec: rec, acted: acted}
}

// obj is v of key "swap/k" at resourceVersion rv.
func (r *adapterRig) obj(rv, v string) any { return swapObj(r.gvr, "k", rv, v) }

// write stores o (the store write precedes its notification).
func (r *adapterRig) write(o any) {
	if err := r.idx.Update(o); err != nil {
		r.t.Fatal(err)
	}
}

func (r *adapterRig) del(o any) {
	if err := r.idx.Delete(o); err != nil {
		r.t.Fatal(err)
	}
}

// the listener's notifications.
func (r *adapterRig) nAdd(o any)           { r.a.OnAdd(o, false) }
func (r *adapterRig) nUpdate(old, new any) { r.a.OnUpdate(old, new) }
func (r *adapterRig) nDelete(o any)        { r.a.OnDelete(o) }
func (r *adapterRig) nSwap()               { r.a.OnUpdate(swapDelivery{key: "swap/k"}, swapDelivery{key: "swap/k"}) }

func (r *adapterRig) seq() []string {
	var out []string
	for _, e := range r.rec.all() {
		if e.kind == "delete" {
			out = append(out, "delete")
			continue
		}
		out = append(out, e.kind+":"+e.rv+":"+e.v)
	}
	return out
}

func eqSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// liveActedOn makes the consumer have acted on o through an EARLIER instance.
func liveActedOn(t *testing.T, o any) *actedRecord {
	acted := newActedRecord()
	acted.set("swap/k", o, true)
	return acted
}

// TestStoreSwap_K11b_OrderIsNonDecreasing — N2. The swapDelivery for k is
// queued AHEAD of two real writes (v4, v5) whose store writes landed before it
// is processed. The consumer sees v5 ONCE: v4 is suppressed and v5's own
// notification is swallowed. RED under X17 (deliver the object captured when
// the notification was enqueued) and X25 (no suppress-until-S).
func TestStoreSwap_K11b_OrderIsNonDecreasing(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	v2 := swapObj(gvr, "k", "2", "v2")
	r := newAdapterRig(t, liveActedOn(t, v2), true)
	v3 := r.obj("3", "v3")
	r.write(v3)
	r.nAdd(v3) // the new copy's initial list, muted: seen = v3
	r.inst.muted.Store(false)
	// M = {k} (acted v2 ≠ v3). Before the listener reaches the swapDelivery,
	// two real writes land: v4 then v5, their notifications queued BEHIND it.
	v4, v5 := r.obj("4", "v4"), r.obj("5", "v5")
	r.write(v4)
	r.write(v5)
	r.nSwap()
	r.nUpdate(v3, v4)
	r.nUpdate(v4, v5)
	if got, want := r.seq(), []string{"update:5:v5"}; !eqSeq(got, want) {
		t.Fatalf("K11b RED (X17/X25): the consumer's sequence for k is %v, want %v — v5 once, v4 suppressed, v5's own notification swallowed", got, want)
	}
	if n, _ := r.a.suppressingForTest(); n != 0 {
		t.Fatalf("K11b: suppression did not end on S's own notification (%d keys still suppressed)", n)
	}
	// And then life goes on: a later write is delivered.
	v6 := r.obj("6", "v6")
	r.write(v6)
	r.nUpdate(v5, v6)
	if got := r.seq(); len(got) != 2 || got[1] != "update:6:v6" {
		t.Fatalf("K11b: the write after suppression ended was not delivered: %v", got)
	}
}

// TestStoreSwap_K11c0_NoSuppressionWhenNothingPending — at the swap delivery the
// store holds a same-version REPLACE of what the listener already delivered
// (seen = S by presence and version; client-go sends no notification for a
// same-version replace), then a real write: the consumer receives S then the
// write. RED when suppression starts although seen = S.
func TestStoreSwap_K11c0_NoSuppressionWhenNothingPending(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "2", "v2")), true)
	a3 := r.obj("3", "v3")
	r.write(a3)
	r.nAdd(a3) // seen = rv 3
	r.inst.muted.Store(false)
	b3 := r.obj("3", "v3") // same version, a new object (a relist's replace): no notification
	r.write(b3)
	r.nSwap()
	v4 := r.obj("4", "v4")
	r.write(v4)
	r.nUpdate(b3, v4)
	if got, want := r.seq(), []string{"update:3:v3", "update:4:v4"}; !eqSeq(got, want) {
		t.Fatalf("K11c-0 RED: sequence %v, want %v — nothing was pending, so nothing may be suppressed", got, want)
	}
}

// TestStoreSwap_K11c1_PendingPredecessorEndsSuppression — S's same-version
// predecessor P is still queued; S itself arrived by a same-version replace and
// will never be notified. Suppression must end on P (version equality), or it
// stalls and swallows every later write. RED under X26 (match by identity).
func TestStoreSwap_K11c1_PendingPredecessorEndsSuppression(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "2", "v2")), true)
	v6 := r.obj("6", "v6")
	r.write(v6)
	r.nAdd(v6) // seen = rv 6
	r.inst.muted.Store(false)
	p7 := r.obj("7", "v7")
	r.write(p7) // P: its notification is queued behind the swap delivery
	s7 := r.obj("7", "v7")
	r.write(s7) // S: a same-version replace — no notification, ever
	r.nSwap()
	r.nUpdate(v6, p7) // P's notification: same version as S → ends suppression
	v8 := r.obj("8", "v8")
	r.write(v8)
	r.nUpdate(s7, v8)
	if got, want := r.seq(), []string{"update:7:v7", "update:8:v8"}; !eqSeq(got, want) {
		t.Fatalf("K11c-1 RED (X26): sequence %v, want %v — suppression must match S's version, not its identity", got, want)
	}
}

// TestStoreSwap_K11c2_DeleteAfterSIsRead — the key is deleted after the swap
// delivery read S: suppression ends on S's notification and the consumer ends
// ABSENT.
func TestStoreSwap_K11c2_DeleteAfterSIsRead(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "2", "v2")), true)
	v3 := r.obj("3", "v3")
	r.write(v3)
	r.nAdd(v3)
	r.inst.muted.Store(false)
	v5 := r.obj("5", "v5")
	r.write(v5)
	r.nSwap() // reads S = v5
	r.del(v5) // deleted after S was read
	r.nUpdate(v3, v5)
	r.nDelete(v5)
	if got, want := r.seq(), []string{"update:5:v5", "delete"}; !eqSeq(got, want) {
		t.Fatalf("K11c-2 RED: sequence %v, want %v — the consumer must end absent", got, want)
	}
}

// TestStoreSwap_K11c3_SuppressionDiesWithTheInstance — the instance stops while
// suppressing: its suppression state (and seen) are freed. RED under X34.
func TestStoreSwap_K11c3_SuppressionDiesWithTheInstance(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "2", "v2")), true)
	v3 := r.obj("3", "v3")
	r.write(v3)
	r.nAdd(v3)
	r.inst.muted.Store(false)
	r.write(r.obj("5", "v5"))
	r.nSwap()
	if n, _ := r.a.suppressingForTest(); n != 1 {
		t.Fatalf("precondition: suppressing %d keys, want 1", n)
	}
	r.inst.freeAdapters() // what Run's return does (runInformer)
	if n, freed := r.a.suppressingForTest(); n != 0 || !freed {
		t.Fatalf("K11c-3 RED (X34): a stopped instance still holds suppression state (%d keys, freed=%v)", n, freed)
	}
	if _, ok, live := r.a.seenObject("swap/k"); ok || live {
		t.Fatal("K11c-3 RED (X34): a stopped instance's seen record survives")
	}
}

// TestStoreSwap_K11c4_AbsentSConvergesThroughTheStatedTransient — P2. S is
// absent and the pending notifications are delete → recreate (r2) → delete. The
// FIRST delete ends suppression; the consumer passes through the stated
// transient (OnDelete, OnAdd(r2), OnDelete) and converges on absent; a later
// recreate (r3) is delivered. RED under X39 (absent-S suppression waits for a
// later delete instead of the first: it stalls and swallows r3).
func TestStoreSwap_K11c4_AbsentSConvergesThroughTheStatedTransient(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	v1 := swapObj(gvr, "k", "1", "v1")
	r := newAdapterRig(t, liveActedOn(t, v1), true)
	r.write(v1)
	r.nAdd(v1) // seen = v1
	r.inst.muted.Store(false)
	r2 := r.obj("2", "r2")
	r.del(v1)
	r.write(r2)
	r.del(r2) // the store is absent when the swap delivery reads S
	r.nSwap()
	r.nDelete(v1)
	r.nAdd(r2)
	r.nDelete(r2)
	r3 := r.obj("3", "r3")
	r.write(r3)
	r.nAdd(r3)
	if got, want := r.seq(), []string{"delete", "add:2:r2", "delete", "add:3:r3"}; !eqSeq(got, want) {
		t.Fatalf("K11c-4 RED (X39): sequence %v, want %v — the stated transient, then convergence and a live recreate", got, want)
	}
}

// TestStoreSwap_K11d_CaughtUpListenerStillDelivers — P1. The new copy's
// listener already delivered S while muted (seen = S); the consumer still gets
// exactly ONE event for the key, because delivery is decided by what the
// consumer ACTED on, not by what the listener saw. RED under X32.
func TestStoreSwap_K11d_CaughtUpListenerStillDelivers(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "2", "v2")), true)
	s := r.obj("3", "v3")
	r.write(s)
	r.nAdd(s) // muted: seen = S, the consumer did nothing
	if len(r.rec.all()) != 0 {
		t.Fatal("precondition: a muted delivery reached the consumer")
	}
	r.inst.muted.Store(false)
	r.nSwap()
	if got, want := r.seq(), []string{"update:3:v3"}; !eqSeq(got, want) {
		t.Fatalf("K11d RED (X32): sequence %v, want %v — exactly one event per key of M", got, want)
	}
	if n, _ := r.a.suppressingForTest(); n != 0 {
		t.Fatal("K11d: seen = S, so nothing may be suppressed")
	}
}

// TestStoreSwap_K11f_WriteAfterUnmuteIsDeliveredOnce — C1. One write lands
// between the unmute and M (its notification queued AHEAD of the swap
// delivery), another between M and the swap delivery: the consumer receives
// each content EXACTLY once. RED under X40 (always deliver S).
func TestStoreSwap_K11f_WriteAfterUnmuteIsDeliveredOnce(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	r := newAdapterRig(t, liveActedOn(t, swapObj(gvr, "k", "1", "v1")), true)
	v1 := r.obj("1", "v1")
	r.write(v1)
	r.nAdd(v1)
	r.inst.muted.Store(false)
	// Between the unmute and M: v6.
	v6 := r.obj("6", "v6")
	r.write(v6)
	// M is computed here: acted = v1, the store v6 → k ∈ M.
	// Between M and the swap delivery: v7. Both notifications precede it.
	v7 := r.obj("7", "v7")
	r.write(v7)
	r.nUpdate(v1, v6)
	r.nUpdate(v6, v7)
	r.nSwap() // S = v7 = acted → C1: nothing to deliver
	if got, want := r.seq(), []string{"update:6:v6", "update:7:v7"}; !eqSeq(got, want) {
		t.Fatalf("K11f RED (X40): sequence %v, want %v — each content exactly once", got, want)
	}
}
