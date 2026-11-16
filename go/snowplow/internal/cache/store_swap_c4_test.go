package cache

// store_swap_c4_test.go — #870 v5.1 C4, the K8 / K10 re-runs: an aborted or
// superseded copy's in-flight LIST is cancelled when the copy stops. The
// production streaming informer (watch-list on, client-go v0.36.5 defaults)
// talks to a stub apiserver that answers the LIVE copy's first request and then
// HOLDS every later request of the GVR — it never answers. The copy's Run must
// return (its done channel closes) without the held response ever being
// released. RED under X47 (context.TODO() in the streaming ListerWatcher: the
// held request outlives the copy, and so does its goroutine).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"context"
)

type c4Stub struct {
	target string // the GVR's collection path
	hold   chan struct{}
	first  atomic.Bool
	held   atomic.Int64
}

func (s *c4Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if strings.HasSuffix(r.URL.Path, s.target) && !s.first.CompareAndSwap(false, true) {
		// Every request after the live copy's first: never answered.
		s.held.Add(1)
		select {
		case <-s.hold:
		case <-r.Context().Done():
		}
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	apiVersion := "v1"
	if len(parts) >= 3 && parts[0] == "apis" {
		apiVersion = parts[1] + "/" + parts[2]
	}
	if q.Get("watch") == "true" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if q.Get("sendInitialEvents") == "true" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "BOOKMARK",
				"object": map[string]any{
					"apiVersion": apiVersion, "kind": "Thing",
					"metadata": map[string]any{
						"resourceVersion": "100",
						"annotations":     map[string]any{metav1.InitialEventsAnnotationKey: "true"},
					},
				},
			})
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": apiVersion, "kind": "ThingList",
		"metadata": map[string]any{"resourceVersion": "100"}, "items": []any{},
	})
}

func newC4Watcher(t *testing.T, gvr schema.GroupVersionResource) (*ResourceWatcher, *c4Stub) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	resetVerificationForArm(t)
	stub := &c4Stub{target: "/" + gvr.Resource, hold: make(chan struct{})}
	srv := httptest.NewServer(stub)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(stub.hold) }); srv.Close() })
	rc := &rest.Config{Host: srv.URL}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw, err := NewResourceWatcher(ctx, dyn)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	rw.SetRESTConfig(rc)
	// Stop the watcher BEFORE the stub releases its held requests (cleanups
	// are LIFO): what must end the copy is its own stop, never the response.
	t.Cleanup(func() { rw.Stop(); cancel() })
	prev := Global()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(prev) })
	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(30 * time.Second):
		t.Fatal("the live copy never synced")
	}
	if in := rw.liveInstance(gvr); in == nil || in.shape != shapeStreaming {
		t.Fatalf("precondition: the production streaming informer, got %+v", in)
	}
	return rw, stub
}

func waitHeld(t *testing.T, s *c4Stub, n int64) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); s.held.Load() < n; time.Sleep(2 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("the copy's request never reached the stub (held=%d)", s.held.Load())
		}
	}
}

func awaitDone(t *testing.T, c *storeInstance, what string) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(15 * time.Second):
		t.Fatalf("C4 RED (X47): %s — the copy's Run did not return while its request is held; the in-flight "+
			"request outlives the copy", what)
	}
}

func TestStoreSwap_C4_StoppedCopyCancelsItsHeldList(t *testing.T) {
	t.Run("K8_abort_by_teardown", func(t *testing.T) {
		gvr := schema.GroupVersionResource{Group: "c4a870.krateo.io", Version: "v1", Resource: "thingsa"}
		rw, stub := newC4Watcher(t, gvr)
		if !rw.requestStoreBuild(gvr, reasonSchema, nil) {
			t.Fatal("the build was refused")
		}
		c := rw.storeBuildForTest(gvr)
		waitHeld(t, stub, 1)
		rw.RemoveResourceType(gvr)
		awaitDone(t, c, "aborted by a teardown")
	})
	t.Run("K10_supersede", func(t *testing.T) {
		gvr := schema.GroupVersionResource{Group: "c4b870.krateo.io", Version: "v1", Resource: "thingsb"}
		rw, stub := newC4Watcher(t, gvr)
		if !rw.requestStoreBuild(gvr, reasonDivergence, []string{"ns/x"}) {
			t.Fatal("the build was refused")
		}
		first := rw.storeBuildForTest(gvr)
		waitHeld(t, stub, 1)
		if !rw.requestStoreBuild(gvr, reasonSchema, nil) { // supersedes the held copy
			t.Fatal("the schema request was refused")
		}
		awaitDone(t, first, "superseded")
		if rw.storeBuildForTest(gvr) == first {
			t.Fatal("the superseded copy is still the build's")
		}
	})
}
