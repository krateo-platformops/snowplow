// apistage_fill_flight_710_test.go — #710 PR-1: the single-flight content fill,
// through the production reader apistageContentServe on a real informer.
//
//   - F-SF1 COALESCING (the PR-0 herd arm, expectations flipped): one LIST cell
//     made known-stale for real, 64 concurrent readers. The precondition is 64
//     BLOCKED readers — one held in the dispatch, 63 waiting on its flight.
//     Then: exactly 1 dispatch, all 64 served and each narrowed by its own
//     identity, fill_duplicate_total 0, and one parse — every reader's items
//     are the SAME decoded maps (the shared immutable trees JQ reads). On main
//     (PR-0) the same setup gives 64 dispatches and 63 duplicates.
//   - F-SF2 FRESHNESS (epoch at entry): a reader whose cell coordinate moved
//     AFTER an in-flight fill started does not join it — it leads its own fill
//     and is served the post-change body — while a reader that arrived before
//     the change does join. Mutation "join regardless of epoch" serves the
//     pre-change body.
//   - F-SF3 CANCELLATION: the leader's request is cancelled mid-fill; every
//     joiner is still served. A joiner whose own ctx ends returns at once and
//     leaves the others waiting. Mutation "fill under the leader's own ctx"
//     fails every joiner.
package api

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fill710NobodyUser holds no grant on the fixture: its serve is narrowed to
// zero items by its own gate.
const fill710NobodyUser = "fill710-nobody"

func fill710CtxAs(user string) context.Context {
	return xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: user}))
}

// fill710Items returns the served LIST envelope's items.
func fill710Items(t *testing.T, v any) []any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("served value is %T, want the decoded LIST envelope", v)
	}
	items, _ := m["items"].([]any)
	return items
}

// fill710ManagedOf returns .status.managed's length of the named item.
func fill710ManagedOf(t *testing.T, v any, name string) int {
	t.Helper()
	for _, it := range fill710Items(t, v) {
		obj, _ := it.(map[string]any)
		md, _ := obj["metadata"].(map[string]any)
		if md["name"] == name {
			st, _ := obj["status"].(map[string]any)
			managed, _ := st["managed"].([]any)
			return len(managed)
		}
	}
	t.Fatalf("item %s not served", name)
	return -1
}

// fill710WaitFor polls cond for up to d.
func fill710WaitFor(d time.Duration, cond func() bool) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

type fill710Served struct {
	v      any
	served bool
	ok     bool
}

// TestContentFill710_SF1_KnownStaleHerd_64Readers_OneFlight is F-SF1.
func TestContentFill710_SF1_KnownStaleHerd_64Readers_OneFlight(t *testing.T) {
	const (
		readers = 64
		items   = 50
	)
	dyn := newFill710Watcher(t, items)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)

	gate := make(chan struct{})
	var arrived atomic.Int64
	envLen := fill710SpyDispatch(t, gate, &arrived, nil)
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)

	// Every other reader holds no grant: one shared flight, per-reader narrowing.
	users := make([]string, readers)
	for i := range users {
		users[i] = r1User
		if i%2 == 1 {
			users[i] = fill710NobodyUser
		}
	}
	out := make([]fill710Served, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, served, ok := apistageContentServe(fill710CtxAs(users[i]), store, fill710ListCall(), false)
			out[i] = fill710Served{v, served, ok}
		}(i)
	}
	// The precondition: 64 blocked readers — one in the dispatch, 63 on its flight.
	if !fill710WaitFor(20*time.Second, func() bool {
		return arrived.Load() == 1 && ContentFillStatsSnapshot().JoinWaiting == readers-1
	}) {
		s := ContentFillStatsSnapshot()
		release()
		wg.Wait()
		t.Fatalf("F-SF1 RED: want 1 reader in the dispatch and %d waiting on its flight, got %d dispatching and %d waiting "+
			"(fills=%v duplicates=%v) — concurrent readers of one content key are not coalesced",
			readers-1, arrived.Load(), s.JoinWaiting, s.Total, s.Duplicate)
	}
	release()
	wg.Wait()

	if got := arrived.Load(); got != 1 {
		t.Fatalf("F-SF1: %d dispatches, want exactly 1", got)
	}
	var shared uintptr
	for i, r := range out {
		if !r.ok || !r.served {
			t.Fatalf("reader %d (%s) not served: ok=%v served=%v", i, users[i], r.ok, r.served)
		}
		got := fill710Items(t, r.v)
		if users[i] == fill710NobodyUser {
			if len(got) != 0 {
				t.Fatalf("reader %d holds no grant but was served %d items — the joiner was not narrowed by its own identity", i, len(got))
			}
			continue
		}
		if len(got) != items {
			t.Fatalf("reader %d served %d items, want %d", i, len(got), items)
		}
		// One parse: every reader's items alias the same decoded maps.
		p := reflect.ValueOf(got[0]).Pointer()
		if shared == 0 {
			shared = p
		} else if p != shared {
			t.Fatalf("reader %d's items are a different decode than reader 0's — more than one parse", i)
		}
	}
	s := ContentFillStatsSnapshot()
	if s.Total["known_stale"] != 1 || s.Duplicate["known_stale"] != 0 || s.Joined["known_stale"] != readers-1 {
		t.Fatalf("fills=%v duplicates=%v joined=%v, want 1 fill, 0 duplicates, %d joined", s.Total, s.Duplicate, s.Joined, readers-1)
	}
	if s.Duration.Count != 1 || s.InflightPeak != 1 || s.InflightBytesPeak != envLen.Load() {
		t.Fatalf("parse/duration count %d, inflight peak %d, bytes peak %d — want one fill of %d bytes",
			s.Duration.Count, s.InflightPeak, s.InflightBytesPeak, envLen.Load())
	}
	fill710RequireIdle(t, s)
	if s.JoinWaiting != 0 || ContentFlightsInFlightForTest() != 0 {
		t.Fatalf("flight not retired: join_waiting=%d flights=%d", s.JoinWaiting, ContentFlightsInFlightForTest())
	}
	t.Logf("#710 PR-1: %d readers → dispatches=%d fills=%v joined=%v duplicates=%v", readers, arrived.Load(), s.Total, s.Joined, s.Duplicate)
}

// TestContentFill710_SF2_ReaderWithNewerEpochDoesNotJoinOlderFlight is F-SF2.
func TestContentFill710_SF2_ReaderWithNewerEpochDoesNotJoinOlderFlight(t *testing.T) {
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)

	// The first dispatch READS the informer, then holds: the leader's body is
	// fixed before the change below. Later dispatches pass straight through.
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	leaderRead := make(chan struct{})
	var calls atomic.Int64
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		if calls.Add(1) == 1 {
			close(leaderRead)
			<-gate
		}
		return raw, ok
	}

	serve := func() chan fill710Served {
		ch := make(chan fill710Served, 1)
		go func() {
			v, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
			ch <- fill710Served{v, served, ok}
		}()
		return ch
	}
	leader := serve()
	select {
	case <-leaderRead:
	case <-time.After(10 * time.Second):
		t.Fatalf("PRECONDITION: the leader never dispatched")
	}
	// A reader arriving BEFORE the change joins the flight (positive control).
	early := serve()
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == 1 }) {
		t.Fatalf("PRECONDITION: a reader arriving before any change did not join the in-flight fill")
	}

	// The change: a real UPDATE after the flight started. Wait until it has moved
	// the cell's coordinate past the flight's epoch.
	afterLeader := cache.DepGenEpochNow()
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(1, r1FreshManaged), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if !fill710WaitFor(10*time.Second, func() bool {
		return cache.ContentCoordinateMovedSince(r1FsaGVR, r1Ns, "", afterLeader)
	}) {
		t.Fatalf("PRECONDITION: the UPDATE never moved the list coordinate")
	}

	// A reader arriving AFTER the change must not join the older flight.
	late := serve()
	select {
	case r := <-late:
		if !r.ok || !r.served {
			t.Fatalf("late reader not served: ok=%v served=%v", r.ok, r.served)
		}
		if got := fill710ManagedOf(t, r.v, "fsa-00001"); got != r1FreshManaged {
			t.Fatalf("F-SF2 RED: the late reader was served %d managed for fsa-00001, want the post-UPDATE %d", got, r1FreshManaged)
		}
	case <-time.After(10 * time.Second):
		release()
		r := <-late
		t.Fatalf("F-SF2 RED: a reader whose coordinate moved after the flight started JOINED it (blocked on it; "+
			"released, it got %d managed for fsa-00001, the pre-UPDATE body) — a body older than its own fill would produce",
			fill710ManagedOf(t, r.v, "fsa-00001"))
	}
	s := ContentFillStatsSnapshot()
	if refused := s.JoinRefused["known_stale"] + s.JoinRefused["absent"]; refused != 1 {
		t.Fatalf("join_refused = %v, want 1", s.JoinRefused)
	}

	release()
	for name, ch := range map[string]chan fill710Served{"leader": leader, "early joiner": early} {
		r := <-ch
		if !r.ok || !r.served {
			t.Fatalf("%s not served: ok=%v served=%v", name, r.ok, r.served)
		}
		// Both read before the change: the pre-UPDATE body is exactly their own fill's.
		if got := fill710ManagedOf(t, r.v, "fsa-00001"); got != r1StaleManaged {
			t.Fatalf("%s served %d managed for fsa-00001, want the pre-UPDATE %d it read", name, got, r1StaleManaged)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("dispatches = %d, want 2 (the leader's and the late reader's own)", got)
	}
}

// TestContentFill710_SF3_CancelledLeaderDoesNotFailJoiners is F-SF3.
func TestContentFill710_SF3_CancelledLeaderDoesNotFailJoiners(t *testing.T) {
	const joiners = 10
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)

	// The dispatch is held, then — like a real dispatch whose ctx was cancelled —
	// fails when its ctx has ended.
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	var arrived atomic.Int64
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		arrived.Add(1)
		<-gate
		if ctx.Err() != nil {
			return nil, false
		}
		return orig(ctx, call)
	}

	leaderCtx, cancelLeader := context.WithCancel(r1Ctx())
	defer cancelLeader()
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		apistageContentServe(leaderCtx, store, fill710ListCall(), false)
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return arrived.Load() == 1 }) {
		t.Fatalf("PRECONDITION: the leader never dispatched")
	}

	out := make([]fill710Served, joiners)
	var wg sync.WaitGroup
	for i := 0; i < joiners; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
			out[i] = fill710Served{v, served, ok}
		}(i)
	}
	// One more joiner whose own request goes away while it waits.
	quitterCtx, quit := context.WithCancel(r1Ctx())
	quitter := make(chan fill710Served, 1)
	go func() {
		v, served, ok := apistageContentServe(quitterCtx, store, fill710ListCall(), false)
		quitter <- fill710Served{v, served, ok}
	}()
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == joiners+1 }) {
		t.Fatalf("PRECONDITION: %d joiners waiting, want %d", ContentFillStatsSnapshot().JoinWaiting, joiners+1)
	}
	quit()
	select {
	case r := <-quitter:
		if r.ok {
			t.Fatalf("a joiner whose ctx ended was reported content-servable")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a joiner whose ctx ended is still waiting on the flight")
	}
	if !fill710WaitFor(5*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == joiners }) {
		t.Fatalf("the departing joiner took others with it: %d waiting, want %d", ContentFillStatsSnapshot().JoinWaiting, joiners)
	}

	// The leader's request goes away mid-fill, then the dispatch completes.
	cancelLeader()
	release()
	wg.Wait()
	<-leaderDone
	for i, r := range out {
		if !r.ok || !r.served {
			t.Fatalf("F-SF3 RED: joiner %d failed (ok=%v served=%v) because its leader's request was cancelled", i, r.ok, r.served)
		}
		if got := len(fill710Items(t, r.v)); got != 5 {
			t.Fatalf("joiner %d served %d items, want 5", i, got)
		}
	}
	if got := arrived.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
	// The detached fill also stored the cell: the next reader is a HIT.
	if _, ok := store.GetNoTouch(fill710ListKey()); !ok {
		t.Fatalf("the cancelled leader's fill did not store the cell")
	}
}
