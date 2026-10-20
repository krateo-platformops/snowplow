//go:build unit || integration

package cache_test

// issue796_refused_list_wait_test.go — #796: WaitForGVRSync stops waiting once
// the reflector's LATEST List/Watch error is a terminal refusal (NotFound /
// Forbidden), instead of running out its bound on a LIST that cannot heal by
// retry. A transient error keeps the bounded wait. Every arm drives the REAL
// reflector through a fake dynamic client (no FireWatchError), so the
// classification sees client-go's own wrapped LIST error.
//
//	W1  (403, 404) refused LIST: the first wait returns false in < 1 s, the
//	    next in < 50 ms. Mutation (classify nothing as terminal) → full bound.
//	W1t transient then healthy: a 500, then a successful relist inside the
//	    bound — the wait does NOT exit early and returns true. Mutation ("any
//	    error") → returns false at the first error.
//	W1r the latest error decides: a 404 then 500s — the wait does not exit
//	    early. Mutation (never delete from listRefused) → exits at once.
//	W2  healthy but slow LIST: the wait returns true when it lands.
//	W3  synced, then a terminal WATCH error: still true at once (the HasSynced
//	    latch), and IsServable still declines (conjunct 3).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func w796GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "w796things"}
}

// w796Watcher starts a real watcher whose fake apiserver answers w796GVR's
// LIST through reactor (the n-th call is passed its 1-based index; returning
// handled=false lets the fake serve its empty list), and registers the GVR.
func w796Watcher(t *testing.T, reactor func(n int64) (bool, runtime.Object, error)) (*cache.ResourceWatcher, *atomic.Int64) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := phase1ListKinds()
	kinds[w796GVR()] = "W796ThingList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(newTestScheme(), kinds)
	var calls atomic.Int64
	dyn.PrependReactor("list", w796GVR().Resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return reactor(calls.Add(1))
	})
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.EnsureResourceType(w796GVR())
	return rw, &calls
}

func w796Refuse(kind string) func(int64) (bool, runtime.Object, error) {
	gr := w796GVR().GroupResource()
	return func(int64) (bool, runtime.Object, error) {
		if kind == "403" {
			return true, nil, apierrors.NewForbidden(gr, "", nil)
		}
		return true, nil, apierrors.NewNotFound(gr, "")
	}
}

// W1 — a terminal refusal ends the wait instead of running out the bound.
func TestIssue796_W1_RefusedListEndsTheWait(t *testing.T) {
	for _, kind := range []string{"403", "404"} {
		t.Run(kind, func(t *testing.T) {
			rw, _ := w796Watcher(t, w796Refuse(kind))
			start := time.Now()
			synced := rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second)
			first := time.Since(start)
			if synced {
				t.Fatal("W1: a refused LIST read as synced")
			}
			if first >= time.Second {
				t.Fatalf("#796 W1 RED (%s): the wait ran %s on a LIST the reflector reported refused — want < 1 s", kind, first)
			}
			start = time.Now()
			rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second)
			if again := time.Since(start); again >= 50*time.Millisecond {
				t.Fatalf("#796 W1 RED (%s): the second wait ran %s — the refusal is already known, want < 50 ms", kind, again)
			}
			if !rw.ServabilitySnapshotFor(w796GVR()).ListRefused {
				t.Fatalf("W1 (%s): the snapshot does not report the refusal", kind)
			}
		})
	}
}

// W1t — a transient error keeps the bounded wait; the relist lands inside it.
func TestIssue796_W1t_TransientErrorKeepsTheWait(t *testing.T) {
	rw, calls := w796Watcher(t, func(n int64) (bool, runtime.Object, error) {
		if n == 1 {
			return true, nil, apierrors.NewInternalError(context.DeadlineExceeded)
		}
		return false, nil, nil // the retry succeeds (the fake's empty list)
	})
	synced := rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second)
	if !synced {
		t.Fatalf("#796 W1t RED: a transient first LIST error ended the wait (LIST calls %d) — the retry would have synced inside the bound", calls.Load())
	}
	if calls.Load() < 2 {
		t.Fatalf("PRECONDITION W1t: the first LIST error never happened (calls %d)", calls.Load())
	}
}

// W1r — the latest error decides: a 404 followed by 500s is not a refusal.
func TestIssue796_W1r_LatestErrorDecides(t *testing.T) {
	gr := w796GVR().GroupResource()
	rw, calls := w796Watcher(t, func(n int64) (bool, runtime.Object, error) {
		if n == 1 {
			return true, nil, apierrors.NewNotFound(gr, "")
		}
		return true, nil, apierrors.NewInternalError(context.DeadlineExceeded)
	})
	deadline := time.Now().Add(10 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("PRECONDITION W1r: the reflector never retried after the 404 (calls %d)", calls.Load())
	}
	// The handler runs right after the failing LIST returns; give it a beat.
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	rw.WaitForGVRSync(context.Background(), w796GVR(), 1500*time.Millisecond)
	if took := time.Since(start); took < 1400*time.Millisecond {
		t.Fatalf("#796 W1r RED: the wait ended after %s although the latest error was a 500 — a stale 404 decided it", took)
	}
}

// W2 — a healthy but slow initial LIST: the wait holds until it lands.
func TestIssue796_W2_SlowHealthyListIsWaitedFor(t *testing.T) {
	rw, _ := w796Watcher(t, func(int64) (bool, runtime.Object, error) {
		time.Sleep(800 * time.Millisecond)
		return false, nil, nil
	})
	if !rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second) {
		t.Fatal("#796 W2 RED: a healthy slow LIST did not read as synced inside the bound")
	}
}

// W6 — a refusal that healed (the CRD version appeared: a later LIST
// succeeds and the informer syncs) is cleared by the recovery hook, the same
// one that clears watchBroken.
func TestIssue796_W6_RecoveryClearsTheRefusal(t *testing.T) {
	gr := w796GVR().GroupResource()
	rw, _ := w796Watcher(t, func(n int64) (bool, runtime.Object, error) {
		if n == 1 {
			return true, nil, apierrors.NewNotFound(gr, "")
		}
		// A real apiserver list carries a resourceVersion; the recovery hook
		// keys on it advancing.
		l := &unstructured.UnstructuredList{Object: map[string]any{
			"apiVersion": w796GVR().GroupVersion().String(), "kind": "W796ThingList",
			"metadata": map[string]any{"resourceVersion": "796"},
		}}
		return true, l, nil
	})
	deadline := time.Now().Add(10 * time.Second)
	for !rw.ServabilitySnapshotFor(w796GVR()).HasSynced && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !rw.ServabilitySnapshotFor(w796GVR()).HasSynced {
		t.Fatal("PRECONDITION W6: the informer never synced after the 404")
	}
	if !rw.ServabilitySnapshotFor(w796GVR()).ListRefused {
		t.Fatal("PRECONDITION W6: the 404 was not recorded (nothing to clear)")
	}
	rw.RefreshDiscovery(context.Background())
	if rw.ServabilitySnapshotFor(w796GVR()).ListRefused {
		t.Fatal("#796 W6 RED: a healed refusal survived the recovery hook")
	}
}

// W3 — a synced informer that later reports a terminal WATCH error still reads
// as synced (the client-go latch); the caller's IsServable still declines.
func TestIssue796_W3_SyncedThenRefusedStaysSynced(t *testing.T) {
	rw, _ := w796Watcher(t, func(int64) (bool, runtime.Object, error) { return false, nil, nil })
	if !rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second) {
		t.Fatal("PRECONDITION W3: the informer never synced")
	}
	rw.InvokeWatchErrorHandlerForTest(w796GVR(), apierrors.NewNotFound(w796GVR().GroupResource(), ""))
	if !rw.ServabilitySnapshotFor(w796GVR()).ListRefused {
		t.Fatal("PRECONDITION W3: the terminal WATCH error was not recorded")
	}
	start := time.Now()
	if !rw.WaitForGVRSync(context.Background(), w796GVR(), 5*time.Second) {
		t.Fatal("#796 W3 RED: a synced informer read as unsynced after a later refusal")
	}
	if took := time.Since(start); took >= 50*time.Millisecond {
		t.Fatalf("W3: a synced informer waited %s", took)
	}
	if rw.IsServable(w796GVR()) {
		t.Fatal("W3: a watch-broken informer must not be servable (conjunct 3)")
	}
}
