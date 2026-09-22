// reflector_path_attribution_test.go — #237 A3.
//
// Every arm here drives a REAL ResourceWatcher over a REAL dynamic client
// against an httptest apiserver stub, with the PRODUCTION transport wrapper
// installed on the rest.Config exactly as main.go installs it. Nothing calls
// the classifier directly with a hand-built request: the wire shapes asserted
// are the ones a real reflector actually put on the wire, which is the only
// way this can be evidence about production rather than about the test.
//
// The load-bearing arm is TestReflectorPath_EtcdDelegatedListIsNonZeroToday.
// reflector_list_etcd_delegated_total is the ONLY production proof the C1
// follow-up will have, so establishing that it is non-zero on TODAY's binary —
// before any fix — is the whole point of shipping A3 first. A counter that
// reads 0 both before and after a fix proves nothing about the fix.

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

var reflectorPathGVR = schema.GroupVersionResource{
	Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders",
}

// apiserverStub records every request line it is asked to serve and answers
// LISTs and WATCHes well enough to drive a real reflector.
type apiserverStub struct {
	mu       sync.Mutex
	requests []string // "<method> <path>?<rawquery>", in arrival order

	// failWatchOnce, when set for a path, makes the FIRST watch of that path
	// answer 500. client-go treats that as a non-retriable watch failure, so
	// ListAndWatch returns and the reflector re-enters it — producing the
	// second real invocation (and with it the relist at a non-zero RV) rather
	// than a hand-installed crossed state.
	failedWatch map[string]bool

	// watchList makes every watch answer with an initial-events bookmark, the
	// shape an apiserver honouring SendInitialEvents returns.
	watchList bool
}

func (s *apiserverStub) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	line := r.Method + " " + r.URL.Path
	if r.URL.RawQuery != "" {
		line += "?" + r.URL.RawQuery
	}
	s.requests = append(s.requests, line)
}

func (s *apiserverStub) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// listKindFor derives a plausible List kind from the last path segment. The
// reflector only needs apiVersion/kind/metadata.resourceVersion/items.
func reflectorPathListKindFor(p string) (apiVersion, kind string) {
	segs := strings.Split(strings.Trim(p, "/"), "/")
	res := segs[len(segs)-1]
	switch segs[0] {
	case "api":
		apiVersion = segs[1]
	default:
		apiVersion = segs[1] + "/" + segs[2]
	}
	singular := strings.TrimSuffix(res, "s")
	if singular != "" {
		singular = strings.ToUpper(singular[:1]) + singular[1:]
	}
	return apiVersion, singular + "List"
}

func (s *apiserverStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.record(r)
	q := r.URL.Query()
	apiVersion, kind := reflectorPathListKindFor(r.URL.Path)

	if q.Get("watch") == "true" {
		s.mu.Lock()
		fail := !s.failedWatch[r.URL.Path]
		if fail {
			s.failedWatch[r.URL.Path] = true
		}
		wl := s.watchList
		s.mu.Unlock()
		if fail && !wl {
			// A non-retriable failure: ListAndWatch returns, the reflector
			// backs off and re-enters it with a LIST at the RV it last synced.
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if wl && q.Get("sendInitialEvents") == "true" {
			// The synthetic end-of-initial-events bookmark: what an apiserver
			// honouring SendInitialEvents sends to close the initial set. The
			// reflector Replaces the store on it, so WaitForCacheSync returns
			// instead of blocking to the go-test timeout.
			bookmark := map[string]any{
				"type": "BOOKMARK",
				"object": map[string]any{
					"apiVersion": apiVersion,
					"kind":       strings.TrimSuffix(kind, "List"),
					"metadata": map[string]any{
						"resourceVersion": "100",
						"annotations":     map[string]any{metav1.InitialEventsAnnotationKey: "true"},
					},
				},
			}
			_ = json.NewEncoder(w).Encode(bookmark)
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done() // hold the watch open; the test cancels it
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"resourceVersion": "100"},
		"items":      []any{},
	})
}

// startReflectorPathWatcher stands up the stub, installs the PRODUCTION
// wrapper on the rest.Config the way main.go does, and returns a watcher whose
// informers talk to the stub.
func startReflectorPathWatcher(t *testing.T, watchList bool) (*ResourceWatcher, *apiserverStub) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	stub := &apiserverStub{failedWatch: map[string]bool{}, watchList: watchList}
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	rc := &rest.Config{Host: srv.URL}
	// EXACTLY the main.go line under test.
	rc.WrapTransport = transport.Wrappers(rc.WrapTransport, ReflectorPathWrapper())

	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		t.Fatalf("dynamic.NewForConfig: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		cancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); cancel() })
	return rw, stub
}

// waitFor polls cond until it holds or the deadline passes. Used instead of a
// fixed sleep because the second reflector invocation arrives after client-go's
// own backoff, whose timing is not ours to assume.
func reflectorPathWaitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

func recordedDump(stub *apiserverStub) string {
	return "recorded wire requests:\n  " + strings.Join(stub.recorded(), "\n  ")
}

// etcdDelegatedLine returns the recorded LIST for resource that carries a limit
// together with a non-zero resourceVersion and no continue — the wire shape
// client-go's own rule says is delegated to etcd — or "" if none was recorded
// yet.
//
// The test WAITS on this rather than on the counter. The watcher registers
// several GVRs and every one of them relists, so a counter reaching 1 says
// SOME GVR got there; asserting a resource-specific wire shape immediately
// afterwards is a race, which is exactly how this arm first failed under the
// full serial suite while passing on its own.
func etcdDelegatedLine(stub *apiserverStub, resource string) string {
	for _, line := range stub.recorded() {
		if !strings.Contains(line, resource) || strings.Contains(line, "watch=true") {
			continue
		}
		if strings.Contains(line, "limit=") && strings.Contains(line, "resourceVersion=") &&
			!strings.Contains(line, "resourceVersion=0") && !strings.Contains(line, "continue=") {
			return line
		}
	}
	return ""
}

// TestReflectorPath_EtcdDelegatedListIsNonZeroToday is the arm C1 depends on.
//
// It drives TWO real reflector invocations for the GVR: an initial sync (LIST
// at resourceVersion=0) and, after the stub fails the watch once, a real
// re-entry into ListAndWatch whose LIST carries the resourceVersion the
// reflector last synced. Today's listOptionsTweak sets Limit unconditionally
// and runs INSIDE the ListFunc — after the pager has already decided — so that
// second request goes out as `resourceVersion=100&limit=500`, which by
// client-go's own documented rule is delegated to etcd and skips the watch
// cache.
//
// RED BEFORE A3: the counter did not exist, and no expvar family named the
// reflector path (captured before this file: the whole-repo grep for
// watchlist/initial_events/bookmark/reflector_ over non-test .go files returned
// nothing). The assertion is deliberately on a RECORDED WIRE SHAPE as well as
// on the counter, so a green here cannot be produced by a mis-wired counter.
func TestReflectorPath_EtcdDelegatedListIsNonZeroToday(t *testing.T) {
	rw, stub := startReflectorPathWatcher(t, false)

	_, syncCh := rw.EnsureResourceType(reflectorPathGVR)
	select {
	case <-syncCh:
	case <-time.After(20 * time.Second):
		t.Fatalf("informer never synced.\n%s", recordedDump(stub))
	}

	// Wait for the WIRE SHAPE for this GVR, not for the counter: the watcher
	// registers several GVRs and any of them reaching the bucket first would
	// satisfy a counter wait while this GVR's relist is still in flight.
	reflectorPathWaitFor(t, "pageheaders' second real reflector invocation (a relist at a non-zero resourceVersion)",
		30*time.Second, func() bool {
			return etcdDelegatedLine(stub, "pageheaders") != ""
		})
	found := etcdDelegatedLine(stub, "pageheaders")

	got := ReflectorPathStatsSnapshot()
	if got.ListEtcdDelegatedTotal == 0 {
		t.Fatalf("a LIST carrying limit + a non-zero resourceVersion went out on the wire (%s) "+
			"but reflector_list_etcd_delegated_total is 0 — the classifier is not counting the "+
			"shape it exists to count. That counter is C1's only production proof, so a zero "+
			"here before the fix would make the fix unfalsifiable.\n%s", found, recordedDump(stub))
	}
	t.Logf("etcd-delegated wire shape on today's binary: %s", found)

	// Denominators: a zero in either makes every bucket unreadable.
	if got.TransportRequestsTotal == 0 {
		t.Fatalf("reflector_transport_requests_total = 0 while requests were demonstrably "+
			"served — the wrapper is not installed.\n%s", recordedDump(stub))
	}
	if got.CollectionRequestsTotal == 0 {
		t.Fatalf("reflector_collection_requests_total = 0 while %d requests were delegated — "+
			"the wrapper sees traffic but recognises no LIST, which is a broken classifier "+
			"masquerading as a quiet reflector.\n%s", got.TransportRequestsTotal, recordedDump(stub))
	}
	if got.WatchPlainTotal == 0 {
		t.Fatalf("reflector_watch_plain_total = 0; the reflector demonstrably watched.\n%s",
			recordedDump(stub))
	}
	if got.ListCacheEligibleTotal == 0 {
		t.Fatalf("reflector_list_cache_eligible_total = 0; the INITIAL list is at "+
			"resourceVersion=0 and must land in this bucket.\n%s", recordedDump(stub))
	}
	// Under the package's gate default (WatchListClient=false) the watch-list
	// branch cannot have run. Asserting it is zero HERE is what stops this arm
	// from being read as coverage of the production path — that is the next
	// arm's job.
	if got.WatchListEstablishedTotal != 0 {
		t.Fatalf("reflector_watchlist_established_total = %d under WatchListClient=false; "+
			"the classifier is putting plain watches in the watch-list bucket.\n%s",
			got.WatchListEstablishedTotal, recordedDump(stub))
	}
}

// TestReflectorPath_WatchListBranchRunsUnderTheGate covers the branch
// PRODUCTION takes. internal/cache's TestMain forces WatchListClient=false for
// the whole package (the package's fakes never emit an initial-events bookmark,
// so a watch-list reflector would block until the go-test timeout), which means
// every other test in this package — including the arm above — drives
// list()+watch() while production drives watchList()+watch(). An instrument
// proven only on the fallback path would be exactly the coverage hole that let
// a store-staleness class ship undetected.
//
// SetFeatureDuringTest goes through the feature gate's Set method, which
// Enabled consults AHEAD of the env map, so this opts in without touching
// featuregate_watchlist_test.go's TestMain (no existing guard is edited or
// removed). It refuses a concurrent override from another test, so this arm
// must not call t.Parallel().
func TestReflectorPath_WatchListBranchRunsUnderTheGate(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	rw, stub := startReflectorPathWatcher(t, true)
	_, syncCh := rw.EnsureResourceType(reflectorPathGVR)
	select {
	case <-syncCh:
	case <-time.After(20 * time.Second):
		t.Fatalf("informer never synced under watch-list semantics.\n%s", recordedDump(stub))
	}

	got := ReflectorPathStatsSnapshot()
	if got.WatchListEstablishedTotal == 0 {
		t.Fatalf("reflector_watchlist_established_total = 0 with WatchListClient=true and a "+
			"stub that honours sendInitialEvents — the watch-list branch of the classifier "+
			"never executed, so nothing here is evidence about the production path.\n%s",
			recordedDump(stub))
	}
	// And prove it was the REAL wire shape, not a bucket default.
	var found string
	for _, line := range stub.recorded() {
		if strings.Contains(line, "watch=true") && strings.Contains(line, "sendInitialEvents=true") {
			found = line
			break
		}
	}
	if found == "" {
		t.Fatalf("no recorded request carries watch=true&sendInitialEvents=true.\n%s",
			recordedDump(stub))
	}
	t.Logf("watch-list wire shape: %s", found)

	if p := ReflectorPathsSnapshot()[reflectorPathGVR.String()]; p != reflectorPathWatchList {
		t.Fatalf("per-GVR path for %s = %q, want %q — the per-GVR map is what answers "+
			"\"which path is THIS GVR taking\", the question #237 recorded as unanswerable "+
			"from outside the process", reflectorPathGVR, p, reflectorPathWatchList)
	}
}

// capturedRecord is one emitted log line.
type capturedRecord struct{ level, text string }

// reflectorPathLogCapture installs a slog handler AT THE LEVEL THE CHART SHIPS
// (warn) for the duration of a test. Asserting through a warn-gated handler is
// stronger than inspecting a Record's level: a line emitted at info would be
// dropped here, which is exactly what happens on the live pod.
type reflectorPathLogCapture struct {
	mu  sync.Mutex
	buf strings.Builder
}

func newReflectorPathLogCapture(t *testing.T) *reflectorPathLogCapture {
	t.Helper()
	c := &reflectorPathLogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(c, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

func (c *reflectorPathLogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// records returns the captured cache.reflector.path lines, in order.
func (c *reflectorPathLogCapture) records() []capturedRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []capturedRecord
	for _, line := range strings.Split(c.buf.String(), "\n") {
		if !strings.Contains(line, "cache.reflector.path") {
			continue
		}
		var rec struct {
			Level string `json:"level"`
		}
		_ = json.Unmarshal([]byte(line), &rec)
		out = append(out, capturedRecord{level: rec.Level, text: line})
	}
	return out
}

// TestReflectorPath_TransitionWarnsOnceAndOnChange pins the log contract: one
// WARN per GVR at first observation, silence while the path is stable, and a
// WARN naming both sides on a flip. The chart ships LOG_LEVEL=warn (the live
// pod's 20h log is 217 WARN + 30 ERROR and ZERO INFO), so an INFO line would be
// invisible exactly when needed — that is why this is warn-on-transition and
// not info-per-request, and why the level is asserted rather than assumed.
func TestReflectorPath_TransitionWarnsOnceAndOnChange(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)
	rec := newReflectorPathLogCapture(t)

	gvr := schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "things"}
	// The GVR must have a registered informer or nothing is recorded at all
	// (RW-1) — this arm is about the transition logic, so establish that first.
	rememberReflectorPathGVR(gvr)

	recordReflectorPath(gvr, reflectorPathWatchList, false)
	recordReflectorPath(gvr, reflectorPathWatchList, false) // stable: no second line
	// A plain re-watch and a continue page must NOT rewrite the path: both
	// occur under either regime, and letting them flip it would produce a
	// transition WARN on ordinary watch recycling, making the signal unreadable.
	recordReflectorPath(gvr, reflectorPathWatchPlain, true)
	recordReflectorPath(gvr, reflectorPathListPage, true)
	if n := len(rec.records()); n != 1 {
		t.Fatalf("got %d WARN lines for a stable GVR, want exactly 1 (first observation). "+
			"A per-request line would flood the log the chart actually ships: %v", n, rec.records())
	}
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("a plain re-watch or a continue page rewrote the recorded path to %q", p)
	}

	// The flip that is worth waking someone for.
	recordReflectorPath(gvr, reflectorPathListEtcdDelegated, true)
	recs := rec.records()
	if len(recs) != 2 {
		t.Fatalf("got %d WARN lines after a watchlist→list flip, want 2: %v", len(recs), recs)
	}
	flip := recs[1]
	if flip.level != "WARN" {
		t.Fatalf("the transition line is %s, not WARN. The chart ships LOG_LEVEL=warn, so "+
			"anything below it is invisible exactly when it is needed", flip.level)
	}
	for _, want := range []string{"cache.reflector.path", gvr.String(), "list", "watchlist"} {
		if !strings.Contains(flip.text, want) {
			t.Fatalf("transition line %q does not name %q — it must carry the GVR and BOTH "+
				"sides of the flip or it cannot be acted on", flip.text, want)
		}
	}
	if n := ReflectorPathStatsSnapshot().PathTransitionsTotal; n != 2 {
		t.Fatalf("reflector_path_transitions_total = %d, want 2 (first observation + flip)", n)
	}
}

// TestReflectorPath_PassthroughLISTNeverAlarms is RW-1.
//
// In modePassthrough (CACHE_ENABLED=false — a supported diagnostic mode) there
// are NO reflectors in the process at all, yet listPassthrough issues a paged
// LIST per call on the same wrapped rest.Config. Ungated, the first one records
// a per-GVR path and emits `cache.reflector.path` — a WARN whose own hint says
// the apiserver stopped honouring SendInitialEvents, about a reflector that
// does not exist.
//
// That is the defect class this entire ship exists to remove, reproduced
// inside the instrument built to remove it. An inflated COUNTER is a
// measurement an operator can caveat; an alarm naming a cause that did not
// happen is a measurement that misleads whoever it wakes.
//
// RED before the fix: the WARN fires and the snapshot carries a row.
//
// THIS ARM CANNOT ATTRIBUTE ITS OWN GREEN, and that is worth stating because I
// measured it rather than assuming it. A passthrough LIST is excluded TWICE
// over — it carries no resourceVersion AND its GVR has no registered informer
// — so removing either guard alone leaves this arm passing. It pins the
// user-visible contract (modePassthrough must never alarm) and nothing more.
// Each guard therefore has its own discriminating arm:
// TestReflectorPath_FallthroughLISTDoesNotFlipARegisteredGVR for the
// resourceVersion rule, TestReflectorPath_TeardownIsNotUndoneByALateRequest for
// the registered-informer gate. Both were confirmed to fail alone.
func TestReflectorPath_PassthroughLISTNeverAlarms(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)
	rec := newReflectorPathLogCapture(t)

	// Exactly the wire shape listPassthrough produces: Limit set, NO
	// resourceVersion, no watch (watcher.go listPassthrough builds
	// metav1.ListOptions{Limit: listPageLimit, Continue: …}).
	req, err := http.NewRequest(http.MethodGet,
		"https://apiserver/apis/widgets.templates.krateo.io/v1beta1/pageheaders?limit=500", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	classifyReflectorRequest(req)

	if n := len(rec.records()); n != 0 {
		t.Fatalf("a passthrough LIST for a GVR with NO registered informer emitted %d "+
			"cache.reflector.path WARN(s): %v\nThere are no reflectors in modePassthrough, so "+
			"the line asserts a cause that cannot occur. Gate the map and the WARN on a "+
			"registered informer.", n, rec.records())
	}
	if p, ok := ReflectorPathsSnapshot()["widgets.templates.krateo.io/v1beta1, Resource=pageheaders"]; ok {
		t.Fatalf("the per-GVR map holds %q for a GVR with no registered informer — an operator "+
			"reading a reflector path for something that has no reflector", p)
	}
	if n := ReflectorPathStatsSnapshot().PathTransitionsTotal; n != 0 {
		t.Fatalf("reflector_path_transitions_total = %d for a non-reflector LIST, want 0", n)
	}

	// The COUNTERS still move — deliberately. The wire shape was real and the
	// honest-scope note says collection GETs are counted whoever issued them;
	// what must not happen is the alarm.
	got := ReflectorPathStatsSnapshot()
	if got.CollectionRequestsTotal != 1 || got.ListCacheEligibleTotal != 1 {
		t.Fatalf("the LIST must still be COUNTED (collection=%d cache_eligible=%d, want 1/1): "+
			"suppressing the counter too would hide real apiserver traffic, and the counters are "+
			"not what misleads", got.CollectionRequestsTotal, got.ListCacheEligibleTotal)
	}
}

// TestReflectorPath_FallthroughLISTDoesNotFlipARegisteredGVR is RW-1's second
// half. A registered-but-unservable GVR resolves through a fallthrough LIST on
// the same client, and that LIST carries no resourceVersion. Ungated, it would
// rewrite an established "watchlist" to "list", and the next real
// re-establishment would flip it back — a WARN and a transition tick per flip,
// each naming a reflector event that never occurred.
//
// The discriminator is on the wire and not a heuristic: r.list() ALWAYS sets a
// resourceVersion (relistResourceVersion returns "0" or the last synced RV),
// while listPassthrough leaves the field unset.
func TestReflectorPath_FallthroughLISTDoesNotFlipARegisteredGVR(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders"}
	rememberReflectorPathGVR(gvr)

	// A real watch-list establishment.
	recordReflectorPath(gvr, reflectorPathWatchList, false)
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("precondition: path = %q, want watchlist", p)
	}

	rec := newReflectorPathLogCapture(t)
	// Now a fallthrough LIST for the SAME, registered GVR: limit, no RV.
	recordReflectorPath(gvr, reflectorPathListCacheEligible, false)

	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("a fallthrough LIST (no resourceVersion) rewrote the established path to %q. "+
			"The reflector did not change path; a LIST our own resolver issued did", p)
	}
	if n := len(rec.records()); n != 0 {
		t.Fatalf("a fallthrough LIST emitted %d transition WARN(s): %v", n, rec.records())
	}

	// And the real thing still flips it, so the gate has not made the
	// instrument blind — the assertion that stops this being fixed by simply
	// never recording anything.
	recordReflectorPath(gvr, reflectorPathListEtcdDelegated, true)
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathList {
		t.Fatalf("a REAL relist (resourceVersion present) did not flip the path: %q", p)
	}
	if n := len(rec.records()); n != 1 {
		t.Fatalf("the real flip emitted %d WARN(s), want exactly 1: %v", n, rec.records())
	}
}

// TestReflectorPath_TeardownPrunesTheGVR is RW-3.
//
// deletePerGVRStateLocked is the single de-registration site, and its own
// comment says every map keyed by GVR is purged there so a future per-GVR map
// cannot be forgotten. This one was forgotten — which is #219's leak shape
// verbatim, and since pruneUnservedGVRs retires a composition version on every
// CRD upgrade, the snapshot would accumulate a permanent row per retired
// version. The harm is not memory: it is an operator reading a reflector path,
// mid-incident, for a GVR that no longer exists.
//
// RED before the fix: the row survives RemoveResourceType.
func TestReflectorPath_TeardownPrunesTheGVR(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	rw := storeStateWatcher(t, "1") // a real watcher with one registered GVR
	gvr := storeStateGVR

	recordReflectorPath(gvr, reflectorPathWatchList, false)
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("precondition: the GVR must be registered and recorded; got %q", p)
	}

	rw.RemoveResourceType(gvr)

	if p, ok := ReflectorPathsSnapshot()[gvr.String()]; ok {
		t.Fatalf("after teardown the per-GVR map still reports path %q for %s. Every per-GVR "+
			"map is purged in deletePerGVRStateLocked precisely so none can be forgotten; a "+
			"surviving row means an operator can read a reflector path for a GVR that no "+
			"longer has one", p, gvr)
	}

	// And a rebuild logs a FRESH first observation, which is the second half of
	// the value: after a schema relist, "which path did this GVR come back on?"
	// is exactly what needs confirming, and today that event is silent.
	rec := newReflectorPathLogCapture(t)
	rememberReflectorPathGVR(gvr)
	recordReflectorPath(gvr, reflectorPathWatchList, false)
	recs := rec.records()
	if len(recs) != 1 || !strings.Contains(recs[0].text, `"first_observation":true`) {
		t.Fatalf("a re-registered GVR must log a fresh first observation; got %v", recs)
	}
}

// TestReflectorPath_TeardownIsNotUndoneByALateRequest is what the
// registered-informer gate is actually load-bearing for, and it was found by
// removing the gate and discovering the passthrough arm above still passed.
//
// Pruning on teardown (RW-3) is not sufficient on its own: a reflector's
// request can be IN FLIGHT when its informer is torn down, and it carries a
// resourceVersion, so it passes the establishment rule. Without the
// registered-informer gate that late request re-creates the row the teardown
// just removed — resurrecting the exact stale entry, and logging a fresh
// "first observation" WARN for a GVR that no longer has a reflector at all.
//
// The two mechanisms are therefore not redundant and this arm is what proves
// it: the resourceVersion rule excludes OUR OWN non-reflector LISTs, and the
// registered gate excludes requests for GVRs that have no informer, including
// a real reflector's last request after teardown.
func TestReflectorPath_TeardownIsNotUndoneByALateRequest(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders"}
	rememberReflectorPathGVR(gvr)
	recordReflectorPath(gvr, reflectorPathWatchList, false)
	if p := ReflectorPathsSnapshot()[gvr.String()]; p != reflectorPathWatchList {
		t.Fatalf("precondition: path = %q, want watchlist", p)
	}

	forgetReflectorPath(gvr) // what deletePerGVRStateLocked does

	rec := newReflectorPathLogCapture(t)
	// A REAL reflector request for the torn-down GVR, still in flight: a relist
	// at a non-zero resourceVersion. It satisfies the establishment rule, so
	// only the registered gate can stop it.
	recordReflectorPath(gvr, reflectorPathListEtcdDelegated, true)

	if p, ok := ReflectorPathsSnapshot()[gvr.String()]; ok {
		t.Fatalf("a request in flight at teardown re-created the row (path %q) that "+
			"deletePerGVRStateLocked had just removed. The prune is then only as good as the "+
			"absence of concurrent traffic, and an operator can still read a reflector path "+
			"for a GVR with no reflector", p)
	}
	if n := len(rec.records()); n != 0 {
		t.Fatalf("the late request emitted %d WARN(s) for a torn-down GVR: %v", n, rec.records())
	}
}

// TestParseResourcePath_DoesNotAllocate is RW-2. The wrapper sits on the
// SHARED rest.Config, so every fallthrough GET-by-name on the serve path pays
// this scan. The earlier strings.Split allocated a []string before discovering
// the path was not a collection — on the single most common shape.
//
// Asserted as zero allocations rather than as a benchmark number, because the
// claim in the code ("counts and delegates — one atomic add and a
// non-allocating path scan") is a claim about allocation, not about speed.
func TestParseResourcePath_DoesNotAllocate(t *testing.T) {
	paths := []string{
		"/apis/widgets.templates.krateo.io/v1beta1/namespaces/krateo-system/pageheaders/portal-builder-page-header",
		"/apis/widgets.templates.krateo.io/v1beta1/pageheaders",
		"/api/v1/namespaces/krateo-system/configmaps",
		"/api/v1/namespaces/krateo-system",
		"/version",
	}
	if n := testing.AllocsPerRun(200, func() {
		for _, p := range paths {
			_, _, _ = parseResourcePath(p)
		}
	}); n != 0 {
		t.Fatalf("parseResourcePath allocates %.1f time(s) per run over %d paths, want 0 — the "+
			"hot path here is a named GET falling through to the apiserver, and it must pay "+
			"no garbage for an instrument it is not the subject of", n, len(paths))
	}
}

// TestReflectorPath_CountsOnlyCollectionGets is the correction that keeps the
// family from being two measurements added together. The wrapper sits on the
// SHARED rest.Config, which also backs discovery and every hot-path
// fallthrough GET-by-name, so a catch-all bucket would span "the reflector
// listed from the watch cache" and "a customer request fell through".
//
// It also pins the property that keeps C1 falsifiable: classification is on
// the PATH SHAPE, so a LIST carrying neither `limit` nor `watch` — which is
// exactly what a post-C1 relist at a non-zero resourceVersion looks like — is
// still counted. A query-token pre-scan would stop counting it, and
// list_etcd_delegated_total going to 0 would then be indistinguishable from
// "we stopped looking".
func TestReflectorPath_CountsOnlyCollectionGets(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		url        string
		counted    bool
		wantBucket string
	}{
		{name: "namespaced LIST", method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/namespaces/krateo-system/pageheaders?limit=500&resourceVersion=0",
			wantBucket: reflectorPathListCacheEligible},
		{name: "cluster-wide LIST at a non-zero RV with a limit", method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/pageheaders?limit=500&resourceVersion=100",
			wantBucket: reflectorPathListEtcdDelegated},
		{name: "post-C1 relist: no limit, non-zero RV, no query tokens to scan for",
			method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/pageheaders?resourceVersion=100",
			wantBucket: reflectorPathListCacheEligible},
		{name: "continue page", method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/pageheaders?limit=500&continue=abc",
			wantBucket: reflectorPathListPage},
		{name: "watch-list", method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/pageheaders?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan",
			wantBucket: reflectorPathWatchList},
		{name: "plain re-watch", method: "GET", counted: true,
			url:        "/apis/widgets.templates.krateo.io/v1beta1/pageheaders?watch=true&resourceVersion=100",
			wantBucket: reflectorPathWatchPlain},
		{name: "core-group LIST", method: "GET", counted: true,
			url:        "/api/v1/namespaces/krateo-system/configmaps?limit=500&resourceVersion=0",
			wantBucket: reflectorPathListCacheEligible},
		{name: "namespaces collection", method: "GET", counted: true,
			url:        "/api/v1/namespaces?limit=500",
			wantBucket: reflectorPathListCacheEligible},

		// NOT counted — the requests that would have made the family a mixed
		// number.
		{name: "fallthrough GET by name", method: "GET",
			url: "/apis/widgets.templates.krateo.io/v1beta1/namespaces/krateo-system/pageheaders/portal-builder-page-header"},
		{name: "GET one namespace by name", method: "GET", url: "/api/v1/namespaces/krateo-system"},
		{name: "subresource", method: "GET",
			url: "/apis/widgets.templates.krateo.io/v1beta1/namespaces/n/pageheaders/w/status"},
		{name: "discovery: group list", method: "GET", url: "/apis/widgets.templates.krateo.io/v1beta1"},
		{name: "discovery: root", method: "GET", url: "/apis"},
		{name: "non-resource path", method: "GET", url: "/version"},
		{name: "openapi", method: "GET", url: "/openapi/v2"},
		{name: "a POST to a collection is not a LIST", method: "POST",
			url: "/apis/widgets.templates.krateo.io/v1beta1/namespaces/n/pageheaders"},
		{name: "SubjectAccessReview", method: "POST",
			url: "/apis/authorization.k8s.io/v1/subjectaccessreviews"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetReflectorPathStatsForTest()
			req, err := http.NewRequest(tc.method, "https://apiserver"+tc.url, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			classifyReflectorRequest(req)

			got := ReflectorPathStatsSnapshot()
			if got.TransportRequestsTotal != 1 {
				t.Fatalf("transport denominator = %d, want 1 — EVERY delegated request counts "+
					"there, including the ones no bucket claims", got.TransportRequestsTotal)
			}
			wantCollection := uint64(0)
			if tc.counted {
				wantCollection = 1
			}
			if got.CollectionRequestsTotal != wantCollection {
				t.Fatalf("collection_requests_total = %d, want %d for %s %s",
					got.CollectionRequestsTotal, wantCollection, tc.method, tc.url)
			}
			if !tc.counted {
				return
			}
			buckets := map[string]uint64{
				reflectorPathWatchList:         got.WatchListEstablishedTotal,
				reflectorPathWatchPlain:        got.WatchPlainTotal,
				reflectorPathListPage:          got.ListPageTotal,
				reflectorPathListEtcdDelegated: got.ListEtcdDelegatedTotal,
				reflectorPathListCacheEligible: got.ListCacheEligibleTotal,
			}
			for bucket, n := range buckets {
				want := uint64(0)
				if bucket == tc.wantBucket {
					want = 1
				}
				if n != want {
					t.Fatalf("bucket %s = %d, want %d for %s (a request in two buckets, or in "+
						"the wrong one, makes every reading of this family ambiguous)",
						bucket, n, want, tc.url)
				}
			}
		})
	}
}

// TestReflectorPath_ClassifierIsRaceFree is the -race arm. The wrapper runs on
// every informer's reflector goroutine concurrently, and the per-GVR path map
// is shared mutable state written from all of them — converting a per-request
// classification into a shared map IS a concurrency change and needs a
// concurrent arm, not a content-equivalence check.
func TestReflectorPath_ClassifierIsRaceFree(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)

	const goroutines, perGoroutine = 8, 200
	// Register the GVRs the arm drives, or the RW-1 gate keeps every write out
	// of the map and the contention this test exists to exercise never happens.
	for g := 0; g < 3; g++ {
		rememberReflectorPathGVR(schema.GroupVersionResource{
			Group: fmt.Sprintf("g%d.example.io", g), Version: "v1", Resource: "things"})
	}
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			gvr := fmt.Sprintf("/apis/g%d.example.io/v1/things", g%3)
			for i := 0; i < perGoroutine; i++ {
				// Alternate the shape so the per-GVR path map is genuinely
				// contended (flip-flopping between watchlist and list).
				u := gvr + "?limit=500&resourceVersion=0"
				if i%2 == 0 {
					u = gvr + "?watch=true&sendInitialEvents=true"
				}
				req, _ := http.NewRequest(http.MethodGet, "https://apiserver"+u, nil)
				classifyReflectorRequest(req)
				_ = ReflectorPathsSnapshot()
				_ = ReflectorPathStatsByStat()
			}
		}(g)
	}
	wg.Wait()

	got := ReflectorPathStatsSnapshot()
	if want := uint64(goroutines * perGoroutine); got.TransportRequestsTotal != want {
		t.Fatalf("transport_requests_total = %d, want %d — a lost increment under contention",
			got.TransportRequestsTotal, want)
	}
	if got.CollectionRequestsTotal != got.WatchListEstablishedTotal+got.ListCacheEligibleTotal {
		t.Fatalf("collection_requests_total (%d) != the sum of its buckets (%d + %d)",
			got.CollectionRequestsTotal, got.WatchListEstablishedTotal, got.ListCacheEligibleTotal)
	}
}
