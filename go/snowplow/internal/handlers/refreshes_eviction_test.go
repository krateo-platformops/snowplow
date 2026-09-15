// refreshes_eviction_test.go — 1.12.6 item 7 (C10) wire-level falsifiers.
//
// Every arm here drives the REAL chain — middleware.RefreshAuth →
// handlers.Refreshes → validateSubscription (informer-only key derivation
// under the connection's identity) → cache.SubscribeRefresh → the handler's
// connection goroutine → the HTTP response body — and asserts the BYTES the
// browser would read (`event: refresh\ndata: <key>\n\n`), never a counter.
// Evictions enter through the two DELETE-semantics sites the design names:
// deps.go runEvictionBatch (an informer DELETE / objAbsent, via the OnDelete
// shim over OnObjectEvent) and deps.go EvictSelfGone (the refresher's
// definite self-404).
//
// This file compiles on main 10f4951 (no new exported symbols), so the RED
// transcripts are literal (reports/arms-red-main-*.log):
//
//	S1  — DELETE eviction of an armed key ⇒ the frame is on the wire.
//	S1b — the same for the self-404 route.
//	S9  — N=100 armed keys evicted in ONE tick ⇒ frames are PACED (≤ burst +
//	      rate in the first second) AND all N eventually arrive — the C10
//	      ship gate, through the real refreshSub + real connection goroutine
//	      (PM condition 3). The pending-set bound (condition 4) is S9b in
//	      refreshes_eviction_c12_test.go (it needs the new snapshot API).
//
// Hermetic: dynamicfake informer, RS256 static key, httptest server.

package handlers

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const evictTestNS = "krateo-system"

var panelGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}

// seedPanels is seedAuthTestWidget generalised to N panel CRs (S9 needs a
// hundred armed keys). Same RBAC grant (group devs: get/list panels), same
// informer wiring, same cleanup.
func seedPanels(t *testing.T, names []string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		crbGVR: "ClusterRoleBindingList",
		crGVR:  "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
		panelGVR: "PanelList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{panelGVR.Group}, Resources: []string{panelGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "panel-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "devs-bind", UID: types.UID("uid-devs")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
		},
	}
	for _, n := range names {
		seed = append(seed, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "widgets.templates.krateo.io/v1beta1",
			"kind":       "Panel",
			"metadata":   map[string]any{"name": n, "namespace": evictTestNS},
			"spec":       map[string]any{},
		}})
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer syncCancel()
	if err := rw.WaitForCacheSync(syncCtx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	_, _ = rw.EnsureResourceType(panelGVR)
	_ = rw.WaitForCacheSync(syncCtx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

// evictionWiring resets the hub + dep tracker and wires the process L1 store
// into the tracker (what ResolvedCache()'s once-init does in production).
// Returns the store. Cleanup deletes the keys the test put.
func evictionWiring(t *testing.T, keys *[]string) *cache.ResolvedCacheStore {
	t.Helper()
	cache.ResetRefreshBroadcasterForTest()
	cache.ResetDepsForTest()
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil — RESOLVED_CACHE_ENABLED not honoured")
	}
	cache.Deps().SetStore(store)
	t.Cleanup(func() {
		for _, k := range *keys {
			store.DeleteForTest(k)
		}
		cache.ResetDepsForTest()
		cache.ResetRefreshBroadcasterForTest()
	})
	return store
}

// expectedKey derives, OUTSIDE the handler but through the same single
// derivation body (dispatchers.deriveSubscriptionWithReason), the key the
// handler arms for a widgetContent coordinate under userA/devs.
func expectedKey(t *testing.T, name string) string {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}))
	ctx = cache.WithInformerOnlyReads(ctx)
	key, ok := dispatchers.DeriveSubscriptionKey(ctx, dispatchers.SubscriptionCoordinates{
		Class: cache.CacheEntryClassWidgetContent,
		Group: panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name, PerPage: 5, Page: 1,
	})
	if !ok || key == "" {
		t.Fatalf("expectedKey(%s): derivation failed", name)
	}
	return key
}

// putSelfEntry stores a widgetContent self-representation for the panel under
// key and records its self edge, so a DELETE of the panel evicts exactly it.
func putSelfEntry(store *cache.ResolvedCacheStore, key, name string) {
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"kind":"panel"}`), Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name,
	}})
	cache.Deps().Record(key, panelGVR, evictTestNS, name)
}

// subParamForNames builds ?sub= for the named panels (widgetContent coordinates).
func subParamForNames(t *testing.T, names []string) string {
	t.Helper()
	body := make([]map[string]any, 0, len(names))
	for _, n := range names {
		body = append(body, map[string]any{
			"class": "widgetContent", "group": panelGVR.Group, "version": panelGVR.Version,
			"resource": panelGVR.Resource, "namespace": evictTestNS, "name": n, "perPage": 5, "page": 1,
		})
	}
	raw, _ := json.Marshal(body)
	if len(raw) > refreshSubParamMaxBytes {
		t.Fatalf("subParamForNames: %d bytes exceeds the %d cap", len(raw), refreshSubParamMaxBytes)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// wireFrame is one `event: refresh` frame read off the response body.
type wireFrame struct {
	key string
	at  time.Time
}

// openArmedStream opens GET /refreshes armed for names as userA and returns
// the frame channel fed by a body reader goroutine. It waits until the hub
// reports every key armed (the handler's subscribe is asynchronous to the
// 200).
func openArmedStream(t *testing.T, base string, names []string, keys []string) (<-chan wireFrame, context.CancelFunc) {
	t.Helper()
	resp, cancel := openStream(t, base, "?sub="+subParamForNames(t, names), func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+mintToken(t, "userA"))
	})
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("stream: status=%d want 200", resp.StatusCode)
	}
	frames := make(chan wireFrame, 4096)
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		inRefresh := false
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "event: refresh":
				inRefresh = true
			case inRefresh && strings.HasPrefix(line, "data: "):
				frames <- wireFrame{key: strings.TrimPrefix(line, "data: "), at: time.Now()}
				inRefresh = false
			case line == "":
				inRefresh = false
			}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		armed := 0
		for _, k := range keys {
			if cache.HasRefreshSubscriber(k) {
				armed++
			}
		}
		if armed == len(keys) {
			return frames, cancel
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("stream: only %d/%d keys armed after 5s (validateSubscription skipped some)", armed, len(keys))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func awaitFrame(t *testing.T, frames <-chan wireFrame, d time.Duration) (wireFrame, bool) {
	t.Helper()
	select {
	case f := <-frames:
		return f, true
	case <-time.After(d):
		return wireFrame{}, false
	}
}

// --- S1 / S1b — the eviction frame is on the wire -----------------------------

func TestRefreshes_S1_DeleteEvictionWritesRefreshFrame(t *testing.T) {
	const name = "dashboard-piechart"
	seedPanels(t, []string{name})
	var keys []string
	store := evictionWiring(t, &keys)
	key := expectedKey(t, name)
	keys = append(keys, key)
	base := refreshServer(t)

	frames, cancel := openArmedStream(t, base, []string{name}, keys)
	defer cancel()
	putSelfEntry(store, key, name)

	// The informer-DELETE route: OnDelete is the documented shim over
	// OnObjectEvent(objAbsent) → runEvictionBatch.
	if n := cache.Deps().OnDelete(panelGVR, evictTestNS, name); n != 1 {
		t.Fatalf("S1 precondition: OnDelete evicted %d entries, want 1", n)
	}
	f, ok := awaitFrame(t, frames, 3*time.Second)
	if !ok {
		t.Fatalf("S1 RED: no `event: refresh` frame on the wire within 3s of a DELETE eviction of an armed key — " +
			"deps.go runEvictionBatch never publishes (loss mode L1)")
	}
	// Panic probe: the frame carries EXACTLY the evicted key (a wrong key
	// would be the L7 key-space mismatch).
	if f.key != key {
		t.Fatalf("S1: frame key %q != evicted key %q", f.key, key)
	}
	if _, extra := awaitFrame(t, frames, 200*time.Millisecond); extra {
		t.Fatalf("S1: a second frame arrived for a single eviction")
	}
}

func TestRefreshes_S1b_SelfGoneEvictionWritesRefreshFrame(t *testing.T) {
	const name = "dashboard-piechart"
	seedPanels(t, []string{name})
	var keys []string
	store := evictionWiring(t, &keys)
	key := expectedKey(t, name)
	keys = append(keys, key)
	base := refreshServer(t)

	frames, cancel := openArmedStream(t, base, []string{name}, keys)
	defer cancel()
	putSelfEntry(store, key, name)

	// The refresher self-404 route (1.12.5 #187).
	if !cache.Deps().EvictSelfGone(key) {
		t.Fatalf("S1b precondition: EvictSelfGone reported no eviction")
	}
	f, ok := awaitFrame(t, frames, 3*time.Second)
	if !ok {
		t.Fatalf("S1b RED: no `event: refresh` frame on the wire within 3s of a self-404 eviction of an armed key — " +
			"deps.go EvictSelfGone never publishes")
	}
	if f.key != key {
		t.Fatalf("S1b: frame key %q != evicted key %q", f.key, key)
	}
	// Idempotence probe: a second EvictSelfGone on the now-absent key is
	// not an eviction and must not publish.
	if cache.Deps().EvictSelfGone(key) {
		t.Fatalf("S1b: EvictSelfGone reported a second eviction")
	}
	if _, extra := awaitFrame(t, frames, 200*time.Millisecond); extra {
		t.Fatalf("S1b: a frame arrived for a non-eviction")
	}
}

// --- S9 — the burst bound, through the real handler -----------------------------

// s9N is the burst size. The design's 200 does not fit the handler's 16 KiB
// ?sub= cap (refreshSubParamMaxBytes; one widgetContent coordinate is 175 B
// on the wire, so one connection can arm at most ~93), so the wire arm uses
// 90 — still 9× the burst and 4.5× the per-second rate, so an unpaced build
// is unmistakable. The bucket is set to rate 20 / burst 10 for the arm (the
// production defaults 5/10 would make it a 20 s test); the bound asserted
// is the SAME formula, burst + rate in the first second.
const (
	s9N     = 90
	s9Rate  = 20
	s9Burst = 10
)

func s9Names() []string {
	names := make([]string, s9N)
	for i := range names {
		names[i] = fmt.Sprintf("s9-panel-%03d", i)
	}
	return names
}

// s9Burst drives the shared S9 setup: N panels seeded, one stream armed for
// all N, N entries put, N DELETE evictions in one tick. Returns the frame
// channel, the keys, and the tick time.
func s9BurstSetup(t *testing.T) (<-chan wireFrame, []string, time.Time, context.CancelFunc) {
	t.Helper()
	t.Setenv("REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND", fmt.Sprint(s9Rate))
	t.Setenv("REFRESH_EVICTION_PUBLISH_BURST", fmt.Sprint(s9Burst))
	names := s9Names()
	seedPanels(t, names)
	var keys []string
	store := evictionWiring(t, &keys)
	for _, n := range names {
		keys = append(keys, expectedKey(t, n))
	}
	base := refreshServer(t)
	frames, cancel := openArmedStream(t, base, names, keys)
	for i, n := range names {
		putSelfEntry(store, keys[i], n)
	}
	tick := time.Now()
	for _, n := range names {
		if got := cache.Deps().OnDelete(panelGVR, evictTestNS, n); got != 1 {
			cancel()
			t.Fatalf("S9 precondition: OnDelete(%s) evicted %d, want 1", n, got)
		}
	}
	return frames, keys, tick, cancel
}

func TestRefreshes_S9_BurstIsPacedAndComplete(t *testing.T) {
	frames, keys, tick, cancel := s9BurstSetup(t)
	defer cancel()

	want := map[string]struct{}{}
	for _, k := range keys {
		want[k] = struct{}{}
	}
	got := map[string]int{}
	var first time.Time
	var firstSecond int
	deadline := time.After(20 * time.Second)
	for len(got) < s9N {
		select {
		case f := <-frames:
			if first.IsZero() {
				first = f.at
			}
			if f.at.Sub(first) < time.Second {
				firstSecond++
			}
			got[f.key]++
		case <-deadline:
			t.Fatalf("S9 RED: %d/%d evicted keys reached the wire within 20s — the rest were DROPPED "+
				"(or, on main, never published)", len(got), s9N)
		}
	}
	// Half 1 — paced: the first second carries at most burst + rate frames
	// (+2 for timer jitter on the boundary). An unpaced publish puts all N
	// on the wire in one tick.
	if limit := s9Burst + s9Rate + 2; firstSecond > limit {
		t.Fatalf("S9 RED: %d frames in the first second after a %d-key eviction burst; the per-subscriber "+
			"bucket (burst %d + %d/s) allows <= %d — the eviction publish is UNPACED", firstSecond, s9N, s9Burst, s9Rate, limit)
	}
	// Half 2 — complete and exact: every key once, nothing foreign.
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Fatalf("S9: foreign key on the wire: %q", k)
		}
		if got[k] != 1 {
			t.Fatalf("S9: key %q delivered %d times, want 1", k, got[k])
		}
	}
	total := time.Since(tick)
	// Sanity on the pacing floor: N keys at burst B + R/s take at least
	// (N-B)/R seconds; an unpaced build finishes in milliseconds.
	if minTotal := time.Duration(float64(s9N-s9Burst)/float64(s9Rate)*float64(time.Second)) - 300*time.Millisecond; total < minTotal {
		t.Fatalf("S9 RED: all %d frames arrived in %v; a paced drain needs >= %v", s9N, total, minTotal)
	}
	t.Logf("S9: %d/%d frames; %d in the first second (limit %d); all delivered in %v", len(got), s9N, firstSecond, s9Burst+s9Rate+2, total)
}
