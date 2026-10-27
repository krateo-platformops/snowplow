package cache

// store_verify_recheck_918_test.go — #918 (and #878): a forced-pass candidate
// is a lost update only if NOTHING was delivered for its key after the read it
// is held to began, AND the store still disagrees with that read.
//
// Run 16 (#918): the re-check read 379 candidates at the metadata client's
// 5 reads/s, a 60-76 s sweep, and compared the store only after it. The churned
// objects change every ~20 s, so most had changed — and been delivered — after
// their read: the store was AHEAD of the read, and 264 "lost updates" queued a
// full repair of the 50K composition store (the #870 window). Nothing was lost.
//
// #878 is the other direction: the store BEHIND the read, because the
// delivery that carries the read's state had reached the reflector but not yet
// the store when the pass compared.
//
// All arms run on the silent-watch fixture (store_verify_recheck_872_test.go):
// the store moves only when an arm delivers. An arm that needs a delivery
// AFTER a read delivers it from the GET itself (r872Client.onGet), which runs
// once the GET has taken its answer — so the delivery follows the read.
//
// The #878 arms need a delivery the reflector has made but the store has not
// yet applied. That state is the transform having run (the reflector delivers
// through it, then queues the object for the store) with the store write still
// pending; r918Transformed produces it by running the GVR's own installed
// delivery record on the object, and never queues it.

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	clientcache "k8s.io/client-go/tools/cache"
)

// r918Transformed runs repairGVR's delivery record on o, as the reflector's
// transform does, WITHOUT the store applying it: a delivery the store lags.
func r918Transformed(t *testing.T, rw *ResourceWatcher, o r872Object) {
	t.Helper()
	rw.mu.RLock()
	dl := rw.deliveries[repairGVR]
	rw.mu.RUnlock()
	if dl == nil {
		t.Fatalf("precondition: repairGVR has no delivery record")
	}
	if _, err := dl.transform(nil)(o.obj()); err != nil {
		t.Fatalf("transform: %v", err)
	}
}

// r918Next is o one version later.
func r918Next(t *testing.T, o r872Object) r872Object {
	t.Helper()
	n, err := strconv.Atoi(o.rv)
	if err != nil {
		t.Fatalf("rv %q: %v", o.rv, err)
	}
	return r872Object{o.name, strconv.Itoa(n + 1), o.uid}
}

// r918Churn builds n objects the forced LIST sees at rv 1 and the store
// reaches rv 2 while the LIST is in flight, so every one is a candidate the
// pass must re-read: the run-16 shape, where the 50K LIST took long enough
// for the churn to move hundreds of keys past it.
func r918Churn(n int) (store, list []r872Object, moves []r872Event, moved []r872Object) {
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("panel-%d", i)
		uid := "uid-" + name
		store = append(store, r872Object{name, "1", uid})
		list = append(list, r872Object{name, "1", uid})
		o := r872Object{name, "2", uid}
		moves = append(moves, modified(o))
		moved = append(moved, o)
	}
	return store, list, moves, moved
}

// TestForcedPass918_ObjectsThatChangeAfterTheirReRead_AreNotLosses — THE #918
// FALSIFIER. The re-check's sweep is longer than the objects' update interval:
// every object it reads changes again after its read, and the watch delivers
// that change before the pass compares. The store is then AHEAD of the read
// for every key. No update was lost.
//
// RED before #918: the pass compared the store with each read only after the
// sweep, saw "store != read" and counted a lost update — read twice, the
// object had moved again after the second read too — so every key was
// published and a repair queued.
func TestForcedPass918_ObjectsThatChangeAfterTheirReRead_AreNotLosses(t *testing.T) {
	const n = 6
	store, list, moves, moved := r918Churn(n)
	rw, cli := r872Setup(t, store, moved, list, nil)
	cli.duringList = func() { r872Deliver(t, rw, moves...) }
	current := map[string]r872Object{}
	for _, o := range moved {
		current[o.name] = o
		cli.answer(o)
	}
	// The churn: an object read is changed right after its read, and the
	// watch delivers the change. The next read (if any) sees the new state.
	cli.onGet = func(name string) {
		next := r918Next(t, current[name])
		current[name] = next
		cli.answer(next)
		r872Deliver(t, rw, modified(next))
	}
	candidates0 := storeDivergenceCandidates.Load()
	forcedVerify(context.Background(), repairGVR)

	if got := storeDivergenceCandidates.Load() - candidates0; got != n {
		t.Fatalf("precondition: %d candidates, want the %d keys that moved during the LIST", got, n)
	}
	if cli.gets.Load() == 0 {
		t.Fatalf("precondition: the re-check never read, so no read was followed by a delivery")
	}
	if got := r872Published(); got != 0 {
		t.Fatalf("#918 RED: %d of %d objects that changed after their re-read were published as lost updates; the store had received every change", got, n)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#918 RED: %d repair(s) queued for a store that lost nothing (the #870 window)", d)
	}
	if got := storeRecheckDeliveredAfterTotal.Load(); got != n {
		t.Errorf("store_divergence_recheck_delivered_after_read_total = %d, want %d: every key was settled by its delivery after the read", got, n)
	}
	// One read per candidate: no second read, no retry.
	if g := cli.gets.Load(); g != n {
		t.Errorf("re-check issued %d GETs for %d candidates, want %d", g, n, n)
	}
	if row, ok := verificationRowFor(repairGVR); !ok || !row.verified {
		t.Errorf("a completed pass that found nothing must mark the GVR verified (row=%+v ok=%v)", row, ok)
	}
}

// TestForcedPass918_RealLossAmongChurn_IsStillDetectedAndRepaired — the
// real-loss arm. The same churning sweep, plus one key the store is genuinely
// stale for: it was delivered during the LIST (so it is re-read), its current
// state was never delivered, and nothing is delivered for it after its read.
// Exactly that key is published, as a lost update, and the repair is queued.
func TestForcedPass918_RealLossAmongChurn_IsStillDetectedAndRepaired(t *testing.T) {
	const n = 6
	store, list, moves, moved := r918Churn(n)
	lostName := moved[n-1].name
	rw, cli := r872Setup(t, store, moved, list, nil)
	cli.duringList = func() { r872Deliver(t, rw, moves...) }
	current := map[string]r872Object{}
	for _, o := range moved {
		current[o.name] = o
		cli.answer(o)
	}
	// The lost key: the apiserver is at rv 3, the store stays at rv 2.
	current[lostName] = r918Next(t, moved[n-1])
	cli.answer(current[lostName])
	cli.onGet = func(name string) {
		if name == lostName {
			return // nothing delivered after its read: the change is lost
		}
		next := r918Next(t, current[name])
		current[name] = next
		cli.answer(next)
		r872Deliver(t, rw, modified(next))
	}
	forcedVerify(context.Background(), repairGVR)

	if got := r872ClassCount("update"); got != 1 {
		lu, ld, la, um := divergenceCounts(verifySiteForced)
		t.Fatalf("published %d lost updates (update/delete/add/uid = %d/%d/%d/%d), want exactly the 1 real loss", got, lu, ld, la, um)
	}
	if got := r872Published(); got != 1 {
		t.Errorf("published %d divergences in total, want 1", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Fatalf("store_repairs_pending = %d after a real loss, want 1: the rule must not hide it", d)
	}
	if rv, _ := indexerRV(t, rw, "demo", lostName); rv != "2" {
		t.Fatalf("precondition: the lost key's store rv is %q, want the stale 2", rv)
	}
	if got := storeRecheckDeliveredAfterTotal.Load(); got != n-1 {
		t.Errorf("store_divergence_recheck_delivered_after_read_total = %d, want the %d churned keys", got, n-1)
	}
}

// TestForcedPass918_878_StoreBehindTheRead_DeliveredBeforeItButNotYetApplied —
// #878, the store BEHIND the read. The reflector had already delivered the
// object's current state (rv 2) when the pass re-read it, but the store has not
// applied that delivery and does not within the pass's drain wait (CPU load).
// The read answers rv 2 and the store still holds rv 1: no update was lost —
// the store has RECEIVED the read's state, it is just behind in applying it.
//
// RED before #918: the pass compared only the store, read twice, and published
// a lost update.
func TestForcedPass918_878_StoreBehindTheRead_DeliveredBeforeItButNotYetApplied(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a2}, []r872Object{a2}, nil)
	// While the LIST is in flight the reflector delivers rv 2; the store does
	// not apply it.
	cli.duringList = func() { r918Transformed(t, rw, a2) }
	forcedVerify(context.Background(), repairGVR)

	if cli.gets.Load() == 0 {
		t.Fatalf("precondition: the key was not re-read, so this arm does not reach the re-check")
	}
	if rv, _ := indexerRV(t, rw, "demo", a1.name); rv != "1" {
		t.Fatalf("precondition: the store applied the delivery (rv %q), so it is not behind the read", rv)
	}
	if got := r872Published(); got != 0 {
		t.Fatalf("#878 RED: a store that had received the read's state but not yet applied it was published as %d lost update(s)", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#878 RED: %d repair(s) queued for a store that lost nothing", d)
	}
	if got := storeRecheckReceivedReadStateTotal.Load(); got != 1 {
		t.Errorf("store_divergence_recheck_read_state_received_total = %d, want 1", got)
	}
}

// TestForcedPass918_878_StoreBehindTheRead_DeliveryArrivesAfterTheRead — #878,
// the late delivery. The store is behind the read (rv 2 against the read's
// rv 3); the delivery of rv 3 reaches the reflector only AFTER the read, and
// the store does not apply it within the pass's drain wait. Nothing was lost.
//
// RED before #918: both reads found the store behind, and it was published.
func TestForcedPass918_878_StoreBehindTheRead_DeliveryArrivesAfterTheRead(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a3}, []r872Object{a1}, nil)
	cli.duringList = func() { r872Deliver(t, rw, modified(a2)) }
	cli.answer(a3)
	cli.onGet = func(string) { r918Transformed(t, rw, a3) }
	forcedVerify(context.Background(), repairGVR)

	if cli.gets.Load() == 0 {
		t.Fatalf("precondition: the key was not re-read, so this arm does not reach the re-check")
	}
	if rv, _ := indexerRV(t, rw, "demo", a1.name); rv != "2" {
		t.Fatalf("precondition: the store is at rv %q, want 2 (behind the read's 3)", rv)
	}
	if got := r872Published(); got != 0 {
		t.Fatalf("#878 RED: a key delivered after its read was published as %d lost update(s) because the store had not yet applied it", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#878 RED: %d repair(s) queued for a store that lost nothing", d)
	}
	if got := storeRecheckDeliveredAfterTotal.Load(); got != 1 {
		t.Errorf("store_divergence_recheck_delivered_after_read_total = %d, want 1", got)
	}
}

// TestForcedPass918_878_RealLossBehindTheRead_IsStillDetected — the #878
// direction must not hide a loss: the store is behind the read, the read's
// state was never delivered (before or after the read), and nothing else was
// delivered for the key after it. Counted and repaired, whatever was
// delivered for the key BEFORE the read.
func TestForcedPass918_878_RealLossBehindTheRead_IsStillDetected(t *testing.T) {
	rw, cli := r872Setup(t, []r872Object{a1}, []r872Object{a3}, []r872Object{a1}, nil)
	cli.duringList = func() { r872Deliver(t, rw, modified(a2)) }
	cli.answer(a3)
	forcedVerify(context.Background(), repairGVR)

	if got := r872ClassCount("update"); got != 1 {
		t.Fatalf("published %d lost updates for a store behind a read whose state was never delivered, want 1", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Fatalf("store_repairs_pending = %d, want 1", d)
	}
	if g := cli.gets.Load(); g != 1 {
		t.Errorf("re-check issued %d GETs, want 1", g)
	}
}

// TestForcedPass918_ListHeldCandidate_DeliveredDuringTheSweep_IsNotALoss — the
// rule applies to every candidate, not only the re-read ones. panel-b is behind
// the LIST and nothing was delivered for it while the LIST was in flight, so it
// is held to the LIST, with no GET. While the re-check reads panel-a, the
// watch delivers panel-b's next state: delivered after the read panel-b is
// held to, so the store has received the object's state after it, and it is
// not a lost update.
//
// RED before #918: the record was closed before the re-check, and panel-b was
// counted as it stood at the LIST.
func TestForcedPass918_ListHeldCandidate_DeliveredDuringTheSweep_IsNotALoss(t *testing.T) {
	b2 := r872Object{"panel-b", "2", "uid-b"}
	b3 := r872Object{"panel-b", "3", "uid-b"}
	rw, cli := r872Setup(t, []r872Object{a1, b1}, []r872Object{a2, b3}, []r872Object{a1, b2}, nil)
	cli.duringList = func() { r872Deliver(t, rw, modified(a2)) }
	cli.onGet = func(name string) {
		if name == a1.name {
			r872Deliver(t, rw, modified(b3))
		}
	}
	forcedVerify(context.Background(), repairGVR)

	if got := fmt.Sprint(cli.gotten()); got != "[panel-a]" {
		t.Fatalf("precondition: re-read %s, want only the key that moved during the LIST", got)
	}
	if got := r872Published(); got != 0 {
		t.Fatalf("#918 RED: a LIST-held key delivered after the LIST began was published as %d lost update(s)", got)
	}
	if d := storeRepairQueueDepth(); d != 0 {
		t.Fatalf("#918 RED: %d repair(s) queued for a store that lost nothing", d)
	}
	if got := storeRecheckUndeliveredTotal.Load(); got != 0 {
		t.Errorf("store_divergence_recheck_undelivered_total = %d, want 0", got)
	}
}

// --- the delivery positions ---------------------------------------------------

// TestDeliveryLog918_PositionsOrderDeliveriesAgainstReads — the mark a read
// takes and the number a delivery gets: a delivery before the mark is not
// "after the read", one after it is, and the record keeps each key's latest
// delivery with the identity it carried. The in-flight delivery recorded at
// open keeps the number it was delivered under, so it is never "after" a read
// the pass issues.
func TestDeliveryLog918_PositionsOrderDeliveriesAgainstReads(t *testing.T) {
	pos := &r872Position{rv: "10"}
	dl := newDeliveryLog(pos.get)
	tf := dl.transform(nil)
	idx := clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})

	_, _ = tf(a1.obj()) // the latest before the pass: in flight at open
	win := dl.openPass(idx)
	if d, ok := win.latestFor("demo/panel-a"); !ok || d.seq > win.openedAt {
		t.Fatalf("the in-flight delivery is recorded as %+v (ok=%v), want at or below openedAt %d", d, ok, win.openedAt)
	}
	_, _ = tf(b1.obj())
	m := win.mark()
	if d, _ := win.latestFor("demo/panel-b"); d.seq > m {
		t.Errorf("a delivery before the mark is numbered %d, above the mark %d", d.seq, m)
	}
	_, _ = tf(a2.obj())
	d, ok := win.latestFor("demo/panel-a")
	if !ok || d.seq <= m {
		t.Errorf("a delivery after the mark is numbered %d (ok=%v), want above the mark %d", d.seq, ok, m)
	}
	if d.id != (objIdentity{rv: a2.rv, uid: a2.uid}) {
		t.Errorf("the record keeps %+v for panel-a, want its latest delivery's identity", d.id)
	}
	win.close()
	_, _ = tf(a3.obj())
	if d, _ := win.latestFor("demo/panel-a"); d.id.rv != a2.rv {
		t.Errorf("a delivery after close changed the record: %+v", d)
	}
}
