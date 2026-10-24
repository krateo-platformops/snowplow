//go:build integration

// apistage_fill_herd_710_integration_test.go — #710 PR-0: the arrival burst at
// reduced scale, kind-free. One namespace holds 5,000 objects; their LIST cell
// is made known-stale for real (#547 drive, fill710MakeKnownStale); then 200
// readers released together each make a first load of it — the shape design
// §1 predicts for the 50K OOM (every arriving reader re-fills the same list).
//
// No dispatch barrier here: the readers arrive as fast as goroutines start,
// so the counts are what the code does under a real burst, not a staged one.
//
// PR-0 measured main here (-race): 200 fills, 199 duplicates, 113 envelopes in
// flight at once (102 MB raw), HeapInuse 25 → 1,089 MiB. PR-1 (single-flight)
// asserts the herd is gone: no duplicate fill (no event moves the coordinate
// during the burst, so nobody may refuse a join), one envelope in flight at a
// time, and every reader joined or hit. More than one fill is allowed only for
// a reader that missed the cell just before the flight's Put and found no
// flight just after it — a sequential refill, never a concurrent one.
package api

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestContentFill710_Integration_5KItems200Readers_KnownStaleBurst(t *testing.T) {
	const (
		items   = 5000
		readers = 200
	)
	dyn := newFill710Watcher(t, items)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)
	envLen := fill710SpyDispatch(t, nil, nil, nil)

	// Heap high-water while the burst runs (HeapInuse, sampled every 1 ms).
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peakHeap atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peakHeap.Load() {
				peakHeap.Store(m.HeapInuse)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	var notServed atomic.Int64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
				notServed.Add(1)
			}
		}()
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	close(stop)
	<-sampled

	if n := notServed.Load(); n != 0 {
		t.Fatalf("%d of %d readers were not served", n, readers)
	}
	s := ContentFillStatsSnapshot()
	fill710RequireIdle(t, s)
	fills := s.Total["known_stale"] + s.Total["absent"]
	dups := s.Duplicate["known_stale"] + s.Duplicate["absent"]
	env := envLen.Load()
	joined := s.Joined["known_stale"] + s.Joined["absent"]
	t.Logf("#710 burst (PR-1): %d items, envelope %d B; %d readers in %s → fills=%v joined=%v duplicates=%v "+
		"inflight_peak=%d inflight_bytes_peak=%d (%.1f envelopes); HeapInuse %d → peak %d MiB",
		items, env, readers, elapsed, s.Total, s.Joined, s.Duplicate,
		s.InflightPeak, s.InflightBytesPeak, float64(s.InflightBytesPeak)/float64(max(env, 1)),
		base.HeapInuse>>20, peakHeap.Load()>>20)

	if s.Total["known_stale"] == 0 {
		t.Fatalf("no reader read the known-stale cell through: fills=%v", s.Total)
	}
	if dups != 0 || s.InflightPeak != 1 || s.InflightBytesPeak != env {
		t.Fatalf("herd RED: %d readers of one known-stale list → fills=%d duplicates=%d inflight_peak=%d inflight_bytes_peak=%d "+
			"— concurrent fills of one content key were not coalesced (PR-0 main: 200/199/113 envelopes)",
			readers, fills, dups, s.InflightPeak, s.InflightBytesPeak)
	}
	if joined == 0 {
		t.Fatalf("no reader joined the flight (fills=%d) — the burst did not overlap the fill", fills)
	}
}

// herd882Obj is fill710FSA with status.rev: the edit sequence a writer stamps
// on the object it owns.
func herd882Obj(name string, rev int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": name},
		"status":     map[string]any{"managed": r1ManagedList(r1StaleManaged), "rev": rev},
	}}
}

// herd882Revs reads status.rev of the named items out of a served envelope
// (-1 for a named item the body does not hold).
func herd882Revs(v any, names []string) map[string]int64 {
	out := map[string]int64{}
	for _, n := range names {
		out[n] = -1
	}
	m, _ := v.(map[string]any)
	items, _ := m["items"].([]any)
	for _, it := range items {
		obj, _ := it.(map[string]any)
		md, _ := obj["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		if _, want := out[name]; !want {
			continue
		}
		st, _ := obj["status"].(map[string]any)
		switch r := st["rev"].(type) {
		case float64:
			out[name] = int64(r)
		case int64:
			out[name] = r
		default:
			out[name] = 0
		}
	}
	return out
}

// TestContentFill882_Integration_5KItems32Readers_EditsFlowing — #882 F2,
// falsifier (b): the #710 burst with edits flowing. 32 readers of a MISSING
// 5,000-object list while writers keep editing it (aggregate target: one edit
// per millisecond) must cost exactly ONE dispatch and ONE envelope parse, and
// every reader's body must contain every edit up to its own entry.
//
// The shape is staged so every joiner's list has MOVED since the flight began
// (the case the #710 epoch rule refused, each refused reader leading its own
// fill — the run-15 herd) AND since the flight's own informer listing: reader 0
// leads, its dispatch lists the informer and is then held, edits are
// acknowledged, the 31 others enter, more edits land, then the fill goes on to
// parse and anchor. A joiner served the flight's own body would miss edits it
// saw acknowledged before it entered; it must get the cell brought up to its
// own seq.
//
// Ground truth, owned by the test: each writer owns one object (given its own
// dep coordinate by a GET-by-name content cell) and edits it sequentially; an
// edit is ACKNOWLEDGED once that object's coordinate moved past the seq read
// before the edit — the dep hook has taken it, so its seq is at most the seq
// when the ack is read. A reader snapshots the acks before it enters; its entry
// seq is later, so its body must hold status.rev >= the snapshot for every
// writer. Dispatches and parses are counted by the test's own seams
// (dispatchViaInformerFn, parseListEnvelopeHookForTest), never by snowplow's
// fill metrics.
func TestContentFill882_Integration_5KItems32Readers_EditsFlowing(t *testing.T) {
	const (
		items   = 5000
		readers = 32
		writers = 8
	)
	dyn := newFill710Watcher(t, items)
	t.Cleanup(cache.SetListCellAnchoringForTest(true)) // the incremental path
	store := cache.ResolvedCache()
	fill710Reset(t)
	listPath := fill710ListCall().Path

	names := make([]string, writers)
	for w := range names {
		names[w] = fmt.Sprintf("fsa-%05d", w*600+7)
		getCall := fill710ListCall()
		getCall.Path = listPath + "/" + names[w]
		if _, served, ok, _ := apistageContentServe(r1Ctx(), store, getCall, false); !ok || !served {
			t.Fatalf("PRECONDITION: the GET-by-name read of writer %d's object was not served", w)
		}
	}

	// The test's own seams.
	var listDispatches, listParses atomic.Int64
	var flightSeq atomic.Uint64
	dispatchStarted := make(chan struct{})
	release := make(chan struct{})
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		if call.Path == listPath && listDispatches.Add(1) == 1 {
			// The leader has LISTED the informer: everything acknowledged from
			// here on is newer than the flight's own body.
			flightSeq.Store(cache.DepGenEpochNow())
			close(dispatchStarted)
			<-release
		}
		return raw, ok
	}
	parseHook := func(gvr schema.GroupVersionResource) {
		if gvr == r1FsaGVR {
			listParses.Add(1)
		}
	}
	parseListEnvelopeHookForTest.Store(&parseHook)
	t.Cleanup(func() { parseListEnvelopeHookForTest.Store(nil) })

	// Writers: each edits its own object every `writers` ms, sequentially.
	acked := make([]atomic.Int64, writers)
	var editsTotal atomic.Int64
	stopW := make(chan struct{})
	var wwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wwg.Add(1)
		go func(w int) {
			defer wwg.Done()
			tk := time.NewTicker(time.Duration(writers) * time.Millisecond)
			defer tk.Stop()
			res := dyn.Resource(r1FsaGVR).Namespace(r1Ns)
			for rev := int64(1); ; rev++ {
				select {
				case <-stopW:
					return
				case <-tk.C:
				}
				before := cache.DepGenEpochNow()
				if _, err := res.Update(context.Background(), herd882Obj(names[w], rev), metav1.UpdateOptions{}); err != nil {
					t.Errorf("writer %d rev %d: %v", w, rev, err)
					return
				}
				if !fill710WaitFor(10*time.Second, func() bool {
					return cache.ContentCoordinateMovedSince(r1FsaGVR, r1Ns, names[w], before)
				}) {
					t.Errorf("writer %d rev %d: the edit never reached the dep hook", w, rev)
					return
				}
				acked[w].Store(rev)
				editsTotal.Add(1)
			}
		}(w)
	}
	stopWriters := sync.OnceFunc(func() { close(stopW); wwg.Wait() })
	t.Cleanup(stopWriters)
	snapshotAcks := func() []int64 {
		out := make([]int64, writers)
		for w := range out {
			out[w] = acked[w].Load()
		}
		return out
	}
	waitAcksPast := func(base []int64, by int64, what string) {
		t.Helper()
		if !fill710WaitFor(30*time.Second, func() bool {
			for w := range base {
				if acked[w].Load() < base[w]+by {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("PRECONDITION: the writers stalled (%s)", what)
		}
	}
	waitAcksPast(snapshotAcks(), 2, "warm-up")

	type served struct {
		ok   bool
		acks []int64
		revs map[string]int64
	}
	results := make([]served, readers)
	var wg sync.WaitGroup
	read := func(i int) {
		defer wg.Done()
		acks := snapshotAcks() // read BEFORE the reader enters: its entry seq is later
		v, sv, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false)
		results[i] = served{ok: ok && sv, acks: acks, revs: herd882Revs(v, names)}
	}
	t0 := time.Now()
	e0 := editsTotal.Load()
	wg.Add(1)
	go read(0)
	select {
	case <-dispatchStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("PRECONDITION: the leader never dispatched")
	}
	waitAcksPast(snapshotAcks(), 2, "after the flight began")
	if !cache.ContentCoordinateMovedSince(r1FsaGVR, r1Ns, "", flightSeq.Load()) {
		t.Fatal("PRECONDITION: the list did not move after the flight began")
	}
	for i := 1; i < readers; i++ {
		wg.Add(1)
		go read(i)
	}
	// Hold the fill until the 31 are waiting in it — or until one of them led
	// its own fill (the #710 epoch rule refusing a moved list: the RED shape).
	fill710WaitFor(30*time.Second, func() bool {
		return contentFillJoinWaiting.Load() >= readers-1 || listDispatches.Load() > 1
	})
	waitAcksPast(snapshotAcks(), 2, "after the readers entered")
	close(release)
	wg.Wait()
	elapsed := time.Since(t0)
	edits := editsTotal.Load() - e0
	stopWriters()

	for i, r := range results {
		if !r.ok {
			t.Fatalf("reader %d was not served", i)
		}
	}
	d, p := listDispatches.Load(), listParses.Load()
	t.Logf("#882 herd with edits flowing: %d readers of a missing %d-item list; %d edits acknowledged in %s (%.2f/ms); list dispatches=%d parses=%d",
		readers, items, edits, elapsed, float64(edits)/float64(max(elapsed.Milliseconds(), 1)), d, p)
	if d != 1 || p != 1 {
		t.Fatalf("F2 (b) RED (#882): %d readers of one missing list with edits flowing → %d dispatches, %d envelope parses, want exactly 1 and 1 "+
			"— a reader whose list moved led its own fill", readers, d, p)
	}
	for i, r := range results {
		for w, name := range names {
			if got := r.revs[name]; got < r.acks[w] {
				t.Fatalf("F2 (b) RED (#882): reader %d was served rev %d of %s, but rev %d was acknowledged before it entered "+
					"— a body older than the reader's own entry seq", i, got, name, r.acks[w])
			}
		}
	}
}
