// refreshes_rotation_819_test.go — #819 part 2 wire-level arms: when the
// identity class of a tab's armed key moves, the tab gets exactly one
// `event: refresh` frame on the OLD key — the key it holds — so the SPA
// refetches, learns the new key from X-Snowplow-Refresh-Key and re-arms.
//
// The REAL chain: middleware.RefreshAuth → handlers.Refreshes →
// validateSubscription (key derived under the connection's identity, with its
// class tracked) → the connection goroutine waiting on
// cache.IdentityClassesMayHaveMoved → RefreshStream.Rotated → the bytes on the
// wire. RBAC moves through the real informer and the real #682 path.
//
//	R1  the bench run 10 cause: a ClusterRole ADD in the pre-build window, then
//	    the first bindings-index build bumps the #682 epoch → ONE frame on K.
//	    Re-deriving gives K' ≠ K, and a stream armed on K' (what the SPA opens
//	    after its refetch) receives K' publishes. A later unrelated publish
//	    sends no second frame (one per armed key per rotation). RED on main:
//	    no frame — the tab stays armed on a key nothing publishes again.
//	R2  a ClusterRoleBinding granting the tab's group → ONE frame on K.
//	R3  bounded: a rotation of a tab's whole armed set goes out through the
//	    per-connection eviction bucket — the first second carries at most
//	    burst + rate frames, and every key arrives exactly once (none dropped).
//	N1  negative: a binding for a group the identity is not in → a publish
//	    happens, the identity's class is unchanged → no frame.
//	N2  negative: an identity-free (widgetContent) armed key never rotates →
//	    no frame on the epoch bump.

package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	crb819GVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	cr819GVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
)

// widgetsKey819 derives the identity-bound (class widgets) key for a panel
// under userA/devs through the same derivation the handler arms with.
func widgetsKey819(t *testing.T, name string) string {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}}))
	ctx = cache.WithInformerOnlyReads(ctx)
	key, ok := dispatchers.DeriveSubscriptionKey(ctx, dispatchers.SubscriptionCoordinates{
		Class: "widgets",
		Group: panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name, PerPage: 5, Page: 1,
	})
	if !ok || key == "" {
		t.Fatalf("widgetsKey819(%s): derivation failed", name)
	}
	return key
}

func widgetsSub819(names ...string) string {
	body := make([]map[string]any, 0, len(names))
	for _, n := range names {
		body = append(body, map[string]any{
			"class": "widgets", "group": panelGVR.Group, "version": panelGVR.Version,
			"resource": panelGVR.Resource, "namespace": evictTestNS, "name": n, "perPage": 5, "page": 1,
		})
	}
	raw, _ := json.Marshal(body)
	return base64.StdEncoding.EncodeToString(raw)
}

// setup819 seeds the panels, resets the #682 state to "first publish done,
// index not built" and returns the fake client.
func setup819(t *testing.T, names ...string) *dynamicfake.FakeDynamicClient {
	t.Helper()
	dyn := seedPanelsClient(t, names)
	reset := func() {
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	}
	reset()
	t.Cleanup(reset)
	return dyn
}

func create819(t *testing.T, dyn *dynamicfake.FakeDynamicClient, gvr schema.GroupVersionResource, obj map[string]any) {
	t.Helper()
	if _, err := dyn.Resource(gvr).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// awaitPublished waits until a published snapshot satisfies pred (the real
// informer → rebuild → publish path, which also signals the class move).
func awaitPublished(t *testing.T, what string, pred func(*cache.RBACSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if s := cache.RBACSnapshotForTest(); s != nil && pred(s) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never reached a published snapshot", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func crb819(name, group string) map[string]any {
	return map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
		"metadata": map[string]any{"name": name, "uid": "uid-" + name},
		"subjects": []any{map[string]any{"kind": "Group", "name": group}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "panel-reader"},
	}
}

func hasCRB(name string) func(*cache.RBACSnapshot) bool {
	return func(s *cache.RBACSnapshot) bool {
		for _, b := range s.ClusterRoleBindings {
			if b != nil && b.Name == name {
				return true
			}
		}
		return false
	}
}

// expectOneFrame asserts exactly one frame, on key, arrives.
func expectOneFrame(t *testing.T, frames <-chan wireFrame, key, arm string) {
	t.Helper()
	f, ok := awaitFrame(t, frames, 3*time.Second)
	if !ok {
		t.Fatalf("%s RED: no `event: refresh` frame on the armed key after its identity class moved — "+
			"the tab stays armed on a key nothing publishes again (#819)", arm)
	}
	if f.key != key {
		t.Fatalf("%s: frame on %q, want the OLD armed key %q (the only key the tab can route)", arm, f.key, key)
	}
	if extra, more := awaitFrame(t, frames, 300*time.Millisecond); more {
		t.Fatalf("%s: a second frame (%q) for one rotation — at most one per armed key", arm, extra.key)
	}
}

func TestRefreshes819_R1_EpochBumpAnnouncesOldKey_NewKeyKeepsFlowing(t *testing.T) {
	const name = "rot-piechart"
	dyn := setup819(t, name)
	k := widgetsKey819(t, name)
	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, widgetsSub819(name), []string{k})
	defer cancel()

	// The window edit, through the real informer (#682 note), then the first
	// index build: the epoch moves every identity-bound class.
	create819(t, dyn, cr819GVR, map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "window-819"},
		"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}},
	})
	awaitPublished(t, "the window ClusterRole", func(s *cache.RBACSnapshot) bool { return s.ClusterRolesByName["window-819"] != nil })
	// The publish of the role itself signals too, but moves no class: drain
	// nothing here — any frame now would be the N1 failure mode.
	if f, got := awaitFrame(t, frames, 300*time.Millisecond); got {
		t.Fatalf("R1: frame %q before any class moved", f.key)
	}
	cache.BuildBindingsByGVRIndex(nil)
	if cache.RBACSubGenEpoch() != 1 {
		t.Fatalf("precondition: epoch=%d after the first build, want 1", cache.RBACSubGenEpoch())
	}

	expectOneFrame(t, frames, k, "R1")

	// The SPA's refetch: the key is now K'.
	k2 := widgetsKey819(t, name)
	if k2 == k {
		t.Fatal("R1: the key did not rotate with the epoch — the frame announced a live key")
	}
	// A later publish that moves nothing for this identity: no second frame.
	create819(t, dyn, crb819GVR, crb819("ops-819", "ops"))
	awaitPublished(t, "the ops binding", hasCRB("ops-819"))
	if f, more := awaitFrame(t, frames, 300*time.Millisecond); more {
		t.Fatalf("R1: frame %q after an unrelated publish — the rotation was announced twice", f.key)
	}

	// Re-armed under K' (the stream the SPA opens after the refetch), updates flow.
	frames2, cancel2 := openArmedStreamSub(t, base, widgetsSub819(name), []string{k2})
	defer cancel2()
	cache.PublishRefresh(k2)
	if f, ok := awaitFrame(t, frames2, 3*time.Second); !ok || f.key != k2 {
		t.Fatalf("R1: the stream armed on K' did not receive K' (got %q, ok=%v)", f.key, ok)
	}
}

func TestRefreshes819_R2_BindingForTheTabsGroupAnnouncesOldKey(t *testing.T) {
	const name = "rot-bar"
	dyn := setup819(t, name)
	cache.BuildBindingsByGVRIndex(nil) // index built, epoch quiet: a plain binding change
	k := widgetsKey819(t, name)
	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, widgetsSub819(name), []string{k})
	defer cancel()

	create819(t, dyn, crb819GVR, crb819("devs-extra-819", "devs"))
	awaitPublished(t, "the devs binding", hasCRB("devs-extra-819"))

	expectOneFrame(t, frames, k, "R2")
	if widgetsKey819(t, name) == k {
		t.Fatal("R2: the key did not rotate with the binding set")
	}
}

func TestRefreshes819_N1_UnrelatedBindingSendsNoFrame(t *testing.T) {
	const name = "rot-table"
	dyn := setup819(t, name)
	cache.BuildBindingsByGVRIndex(nil)
	k := widgetsKey819(t, name)
	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, widgetsSub819(name), []string{k})
	defer cancel()
	before := cache.RefreshBroadcasterStatsSnapshot().RotationAnnounced

	create819(t, dyn, crb819GVR, crb819("ops-only-819", "ops"))
	awaitPublished(t, "the ops binding", hasCRB("ops-only-819"))

	if f, got := awaitFrame(t, frames, 500*time.Millisecond); got {
		t.Fatalf("N1: frame %q although this identity's class did not move", f.key)
	}
	if widgetsKey819(t, name) != k {
		t.Fatal("N1 precondition: the key moved — the binding touched this identity")
	}
	if got := cache.RefreshBroadcasterStatsSnapshot().RotationAnnounced - before; got != 0 {
		t.Fatalf("N1: rotation_announced moved by %d with no rotation", got)
	}
}

func TestRefreshes819_N2_IdentityFreeKeyNeverRotates(t *testing.T) {
	const name = "rot-shell"
	dyn := setup819(t, name)
	k := expectedKey(t, name) // widgetContent: identity-free
	base := refreshServer(t)
	frames, cancel := openArmedStream(t, base, []string{name}, []string{k})
	defer cancel()

	create819(t, dyn, cr819GVR, map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "window-819-n2"},
		"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}},
	})
	awaitPublished(t, "the window ClusterRole", func(s *cache.RBACSnapshot) bool { return s.ClusterRolesByName["window-819-n2"] != nil })
	cache.BuildBindingsByGVRIndex(nil)
	if cache.RBACSubGenEpoch() != 1 {
		t.Fatalf("precondition: epoch=%d, want 1", cache.RBACSubGenEpoch())
	}
	if f, got := awaitFrame(t, frames, 500*time.Millisecond); got {
		t.Fatalf("N2: frame %q on an identity-free key — its key folds no class", f.key)
	}
	if expectedKey(t, name) != k {
		t.Fatal("N2 precondition: the identity-free key moved")
	}
}

func TestRefreshes819_R3_WholeArmedSetRotationIsPacedAndComplete(t *testing.T) {
	const n, rate, burst = 40, 20, 10
	t.Setenv("REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND", fmt.Sprint(rate))
	t.Setenv("REFRESH_EVICTION_PUBLISH_BURST", fmt.Sprint(burst))
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("rot-panel-%03d", i)
	}
	dyn := setup819(t, names...)
	want := map[string]bool{}
	keys := make([]string, 0, n)
	for _, nm := range names {
		k := widgetsKey819(t, nm)
		want[k] = true
		keys = append(keys, k)
	}
	base := refreshServer(t)
	frames, cancel := openArmedStreamSub(t, base, widgetsSub819(names...), keys)
	defer cancel()

	create819(t, dyn, cr819GVR, map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "window-819-r3"},
		"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}},
	})
	awaitPublished(t, "the window ClusterRole", func(s *cache.RBACSnapshot) bool { return s.ClusterRolesByName["window-819-r3"] != nil })
	tick := time.Now()
	cache.BuildBindingsByGVRIndex(nil) // the epoch moves every key of the tab at once

	got := map[string]int{}
	var first time.Time
	firstSecond := 0
	deadline := time.After(20 * time.Second)
	for len(got) < n {
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
			t.Fatalf("R3: %d/%d rotated keys announced within 20s — the rest were dropped", len(got), n)
		}
	}
	if limit := burst + rate + 2; firstSecond > limit {
		t.Fatalf("R3: %d frames in the first second of a %d-key rotation; the bucket allows <= %d — the rotation is UNPACED", firstSecond, n, limit)
	}
	for k, c := range got {
		if !want[k] || c != 1 {
			t.Fatalf("R3: key %q delivered %d times (armed=%v), want exactly once per armed key", k, c, want[k])
		}
	}
	if minTotal := time.Duration(float64(n-burst)/float64(rate)*float64(time.Second)) - 300*time.Millisecond; time.Since(tick) < minTotal {
		t.Fatalf("R3: all %d frames in %v; a paced drain needs >= %v", n, time.Since(tick), minTotal)
	}
	t.Logf("R3: %d/%d frames, %d in the first second, all in %v", len(got), n, firstSecond, time.Since(tick))
}
