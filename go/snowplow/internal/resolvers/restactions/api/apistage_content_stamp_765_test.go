// apistage_content_stamp_765_test.go — #765 option A, the content stamp, through
// the production reader apistageContentServe on a real informer.
//
// #606 run 8: every refresher re-resolve whose trigger set held a content cell's
// GVR discarded the shared cell and re-filled it, so N dependents refreshed after
// one change cost N re-fills of the same list. The fix re-fills only when the
// cell's read may be older than a trigger the refresh consumed.
//
// Arms. This file compiles against main (7c966ba2) so the RED-on-main run is
// the same code:
//   - A1 ONE CHANGE, N DEPENDENTS (sequential): one real UPDATE, then N refresher
//     reads with the GVR as trigger. Exactly ONE dispatch; every read is served
//     the post-UPDATE body. main: N dispatches.
//   - A2 CHURN (run 8's shape): a refresher fill is in flight; K changes land one
//     by one, each followed by one refresher reader. At most ONE more dispatch
//     (the pending flight), and every reader is served a body at least as new
//     as the change before it entered. main: K more dispatches.
//   - B1 LOSSLESS, change after the fill: the cell was filled, a change lands,
//     a refresh must re-dispatch and serve the post-change body.
//   - B2 LOSSLESS, change during an in-flight fill: a refresher reader entering
//     after the change must not be served the in-flight (older) read.
//   - B3 LOSSLESS, change while a pending flight is queued: a reader joining the
//     pending flight after a second change must be served that change too.
//   - B4 LOSSLESS through the REAL refresher: a real UPDATE dirty-marks the cell;
//     the refresher's dequeue stamps the refresh's need; the re-resolve must
//     re-fill and serve the post-UPDATE body.
//   - B5 USERS NEVER QUEUE (#710): a customer-triggered refresh (#740 K5 / #547
//     demand revalidation) refused by an in-flight fill fills at once — it never
//     waits behind it on a pending flight — and is served the post-change body.
//
// B1–B4 are GREEN on main by construction (main always re-fills); they are the
// arms the #765 mutations turn RED.
package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
)

// stamp765Item is the fixture item whose .status.managed length each change sets.
const stamp765Item = "fsa-00001"

// stamp765RefreshCtx is a refresher re-resolve of a cell that reads the fixture
// LIST, triggered by the fixture GVR (as processNext stamps it), in the background.
func stamp765RefreshCtx() context.Context {
	return cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(r1Ctx()), r1FsaGVR)
}

// stamp765Change writes managed=m on the fixture item and waits until the event
// has moved the LIST coordinate (the bump runs after the informer indexed it).
func stamp765Change(t *testing.T, dyn dynamic.Interface, m int) {
	t.Helper()
	before := cache.DepGenEpochNow()
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(1, m), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update managed=%d: %v", m, err)
	}
	if !fill710WaitFor(10*time.Second, func() bool {
		return cache.ContentCoordinateMovedSince(r1FsaGVR, r1Ns, "", before)
	}) {
		t.Fatalf("PRECONDITION: the UPDATE managed=%d never moved the list coordinate", m)
	}
}

func stamp765Serve(ctx context.Context, store *cache.ResolvedCacheStore) chan fill710Served {
	ch := make(chan fill710Served, 1)
	go func() {
		v, served, ok, _ := apistageContentServe(ctx, store, fill710ListCall(), false)
		ch <- fill710Served{v, served, ok}
	}()
	return ch
}

func stamp765Recv(t *testing.T, name string, ch chan fill710Served) fill710Served {
	t.Helper()
	select {
	case r := <-ch:
		if !r.ok || !r.served {
			t.Fatalf("%s not served: ok=%v served=%v", name, r.ok, r.served)
		}
		return r
	case <-time.After(20 * time.Second):
		t.Fatalf("%s never returned", name)
	}
	return fill710Served{}
}

// stamp765Prime fills the LIST cell with a customer read (managed=r1StaleManaged).
func stamp765Prime(t *testing.T, store *cache.ResolvedCacheStore) {
	t.Helper()
	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatalf("PRECONDITION: the priming read was not served (ok=%v served=%v)", ok, served)
	}
	if _, ok := store.GetNoTouch(fill710ListKey()); !ok {
		t.Fatalf("PRECONDITION: the priming read stored no content cell")
	}
}

// stamp765HoldDispatches wraps the real dispatch: the first `hold` dispatches READ
// the informer, report on read[i] and then wait on gate[i]; later ones pass.
func stamp765HoldDispatches(t *testing.T, hold int) (calls *atomic.Int64, read []chan struct{}, release []func()) {
	t.Helper()
	calls = new(atomic.Int64)
	read = make([]chan struct{}, hold)
	gates := make([]chan struct{}, hold)
	release = make([]func(), hold)
	for i := range gates {
		read[i] = make(chan struct{})
		gates[i] = make(chan struct{})
		release[i] = sync.OnceFunc(func() { close(gates[i]) })
		t.Cleanup(release[i])
	}
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		if n := int(calls.Add(1)); n <= hold {
			close(read[n-1])
			<-gates[n-1]
		}
		return raw, ok
	}
	return calls, read, release
}

// stamp765ManagedOf is fill710ManagedOf for a non-test goroutine: -1 when the
// item is missing or the value is not a LIST envelope.
func stamp765ManagedOf(v any) int {
	m, _ := v.(map[string]any)
	items, _ := m["items"].([]any)
	for _, it := range items {
		obj, _ := it.(map[string]any)
		md, _ := obj["metadata"].(map[string]any)
		if md["name"] == stamp765Item {
			st, _ := obj["status"].(map[string]any)
			managed, _ := st["managed"].([]any)
			return len(managed)
		}
	}
	return -1
}

func stamp765WaitClosed(t *testing.T, what string, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("PRECONDITION: %s", what)
	}
}

func stamp765Refused() uint64 {
	s := ContentFillStatsSnapshot()
	var n uint64
	for _, v := range s.JoinRefused {
		n += v
	}
	return n
}

// TestContentStamp765_A1_OneChangeNDependentsOneRefill is A1.
func TestContentStamp765_A1_OneChangeNDependentsOneRefill(t *testing.T) {
	const n = 16
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)

	var calls atomic.Int64
	fill710SpyDispatch(t, nil, &calls, nil)
	stamp765Change(t, dyn, r1FreshManaged)

	for i := 0; i < n; i++ {
		r := stamp765Recv(t, "refresh", stamp765Serve(stamp765RefreshCtx(), store))
		if got := fill710ManagedOf(t, r.v, stamp765Item); got != r1FreshManaged {
			t.Fatalf("LOSSY: refresh %d after the change was served managed=%d, want the post-change %d", i, got, r1FreshManaged)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("A1 RED (#765): %d refresher re-resolves after ONE change dispatched %d fills of the shared list, want exactly 1 "+
			"(the first re-fills; the rest read a cell already newer than the change)", n, got)
	}
	if s := ContentFillStatsSnapshot(); s.Total["refresh_forced"] != 1 {
		t.Fatalf("fill_total = %v, want refresh_forced=1", s.Total)
	}
}

// TestContentStamp765_A2_ChurnCoalescesToOnePendingFill is A2.
func TestContentStamp765_A2_ChurnCoalescesToOnePendingFill(t *testing.T) {
	const k = 6
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)

	calls, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(stamp765RefreshCtx(), store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])

	readers := make([]chan fill710Served, k)
	for i := 0; i < k; i++ {
		stamp765Change(t, dyn, 3+i)
		readers[i] = stamp765Serve(stamp765RefreshCtx(), store)
		// The reader has decided once it returned (its own fill) or was refused by
		// the in-flight fill (it then waits behind it).
		if !fill710WaitFor(10*time.Second, func() bool { return len(readers[i]) == 1 || stamp765Refused() == uint64(i+1) }) {
			t.Fatalf("PRECONDITION: reader %d neither returned nor was refused by the in-flight fill (refused=%d)", i, stamp765Refused())
		}
	}
	release[0]()
	if got := fill710ManagedOf(t, stamp765Recv(t, "first refresh", lead).v, stamp765Item); got != 2 {
		t.Fatalf("first refresh served managed=%d, want 2 (what it read)", got)
	}
	for i, ch := range readers {
		if got := fill710ManagedOf(t, stamp765Recv(t, "churn reader", ch).v, stamp765Item); got < 3+i {
			t.Fatalf("LOSSY: churn reader %d entered after managed=%d and was served managed=%d", i, 3+i, got)
		}
	}
	if got := calls.Load(); got > 2 {
		t.Fatalf("A2 RED (#765): %d refresher readers, each after one more change, dispatched %d fills of the shared list, "+
			"want at most 2 (the in-flight fill plus ONE pending fill they all share)", k, got)
	}
}

// TestContentStamp765_B1_ChangeAfterFillForcesRefill is B1.
func TestContentStamp765_B1_ChangeAfterFillForcesRefill(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)
	// A refresh with no change since the fill: nothing to re-read.
	stamp765Recv(t, "refresh before the change", stamp765Serve(stamp765RefreshCtx(), store))

	var calls atomic.Int64
	fill710SpyDispatch(t, nil, &calls, nil)
	stamp765Change(t, dyn, r1FreshManaged)
	r := stamp765Recv(t, "refresh after the change", stamp765Serve(stamp765RefreshCtx(), store))
	if got := fill710ManagedOf(t, r.v, stamp765Item); got != r1FreshManaged {
		t.Fatalf("B1 RED (LOSSY): a refresh after the change was built from the older cell: managed=%d, want %d", got, r1FreshManaged)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("B1: dispatches = %d, want 1 (a change after the fill forces exactly one re-fill)", got)
	}
}

// TestContentStamp765_B2_ChangeDuringInflightFillIsNotJoined is B2.
func TestContentStamp765_B2_ChangeDuringInflightFillIsNotJoined(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)

	_, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(stamp765RefreshCtx(), store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])

	stamp765Change(t, dyn, 3)
	late := stamp765Serve(stamp765RefreshCtx(), store)
	if !fill710WaitFor(10*time.Second, func() bool { return stamp765Refused() == 1 || len(late) == 1 }) {
		t.Fatalf("B2 RED: the reader entering after the change neither was refused by the older in-flight fill nor returned")
	}
	release[0]()
	stamp765Recv(t, "first refresh", lead)
	if got := fill710ManagedOf(t, stamp765Recv(t, "late refresh", late).v, stamp765Item); got != 3 {
		t.Fatalf("B2 RED (LOSSY): a refresh entering after managed=3 was served managed=%d from the fill that read before it", got)
	}
}

// TestContentStamp765_B3_PendingJoinerSeesChangeAfterPendingWasQueued is B3.
func TestContentStamp765_B3_PendingJoinerSeesChangeAfterPendingWasQueued(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)

	_, read, release := stamp765HoldDispatches(t, 2)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(stamp765RefreshCtx(), store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])

	stamp765Change(t, dyn, 3)
	r1 := stamp765Serve(stamp765RefreshCtx(), store)
	if !fill710WaitFor(10*time.Second, func() bool { return stamp765Refused() == 1 }) {
		t.Fatalf("PRECONDITION: the second reader was not refused by the in-flight fill")
	}
	// Give a fill that does not wait for the in-flight one the chance to read now.
	select {
	case <-read[1]:
	case <-time.After(300 * time.Millisecond):
	}

	stamp765Change(t, dyn, 4)
	r2 := stamp765Serve(stamp765RefreshCtx(), store)
	if !fill710WaitFor(10*time.Second, func() bool { return stamp765Refused() >= 2 || len(r2) == 1 }) {
		t.Fatalf("PRECONDITION: the third reader never decided")
	}
	release[0]()
	stamp765Recv(t, "first refresh", lead)
	release[1]()
	if got := fill710ManagedOf(t, stamp765Recv(t, "second refresh", r1).v, stamp765Item); got < 3 {
		t.Fatalf("B3 RED (LOSSY): the second refresh entered after managed=3 and was served managed=%d", got)
	}
	if got := fill710ManagedOf(t, stamp765Recv(t, "third refresh", r2).v, stamp765Item); got != 4 {
		t.Fatalf("B3 RED (LOSSY): the third refresh entered after managed=4 and was served managed=%d — "+
			"a queued fill read before the change its joiner must reflect", got)
	}
}

// TestContentStamp765_B4_RealRefresherDequeueRefillsAfterChange is B4.
func TestContentStamp765_B4_RealRefresherDequeueRefillsAfterChange(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	cache.ResetRefresherForTest()
	stamp765Prime(t, store)

	var mu sync.Mutex
	var got []int
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(ctx context.Context, _ string, _ cache.ResolvedKeyInputs) error {
		if !cache.RefreshTriggerHas(ctx, r1FsaGVR) {
			return nil
		}
		rctx := xcontext.BuildContext(cache.WithBackgroundResolve(ctx), xcontext.WithUserInfo(jwtutil.UserInfo{Username: r1User}))
		v, served, ok, _ := apistageContentServe(rctx, store, fill710ListCall(), false)
		if ok && served {
			m := stamp765ManagedOf(v)
			mu.Lock()
			got = append(got, m)
			mu.Unlock()
		}
		return nil
	})
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	// Join the dep-event workers before the watcher cleanup resets the cache under them.
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest(); cache.ResetDepsForTest() })

	stamp765Change(t, dyn, r1FreshManaged)
	if !fill710WaitFor(20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) > 0 }) {
		t.Fatalf("PRECONDITION: the refresher never re-resolved the dirty-marked content cell")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, m := range got {
		if m != r1FreshManaged {
			t.Fatalf("B4 RED (LOSSY): refresher re-resolve %d after the change was built from the older cell: managed=%d, want %d", i, m, r1FreshManaged)
		}
	}
}

// TestContentStamp765_B5_CustomerTriggeredRefreshNeverWaitsOnPending is B5.
func TestContentStamp765_B5_CustomerTriggeredRefreshNeverWaitsOnPending(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)

	_, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(stamp765RefreshCtx(), store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])

	stamp765Change(t, dyn, 3)
	cust := stamp765Serve(cache.WithCustomerTriggeredRefresh(stamp765RefreshCtx()), store)
	select {
	case r := <-cust:
		if !r.ok || !r.served {
			t.Fatalf("customer-triggered refresh not served: ok=%v served=%v", r.ok, r.served)
		}
		if got := fill710ManagedOf(t, r.v, stamp765Item); got != 3 {
			t.Fatalf("B5 RED (LOSSY): customer-triggered refresh after managed=3 was served managed=%d", got)
		}
	case <-time.After(5 * time.Second):
		release[0]()
		<-cust
		t.Fatalf("B5 RED (#710 users never queue): a customer-triggered refresh WAITED behind the in-flight background fill")
	}
	release[0]()
	stamp765Recv(t, "first refresh", lead)
}
