// phase1_barrier_refused_1532_test.go — #1532: the Phase 1 sync barrier stops
// waiting on an informer whose LIST the apiserver refuses (NotFound /
// Forbidden: rw.listRefused, #796), as WaitForGVRSync does, instead of running
// out a pass (45 s by default) or the whole Phase 1 budget (the sandbox's 25 s:
// `sync_wait_incomplete informer_count=6 waited_ms=23736`).
//
// Every arm sets the pass grace and the quiescence window to an hour, so no
// timer of the barrier's own can end the wait: only the refusal signal can.
// Each exit is bounded by a literal (exitBound1532), not by anything derived
// from the code's constants; the arm's ctx is only the backstop that turns a
// hang into a failure.

package cache_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// exitBound1532 is how soon the barrier must return once nothing it should
// wait on is left: the fake apiserver answers a LIST in milliseconds and the
// barrier polls every 100 ms.
const exitBound1532 = 5 * time.Second

func refusedGVR1532() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
}

func slowGVR1532() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "slowthing"}
}

func listKinds1532() map[schema.GroupVersionResource]string {
	m := phase1ListKinds()
	m[refusedGVR1532()] = "PanelList"
	m[slowGVR1532()] = "SlowThingList"
	return m
}

// gatedList answers gvr's LIST with fail() while the gate is closed, then lets
// it through to the fake tracker (an empty list: the informer syncs). It never
// blocks, so it never holds the fake client's shared lock (see stuckGVR).
type gatedList struct {
	open atomic.Bool
	fail func() error
}

func (g *gatedList) reactor() k8stesting.ReactionFunc {
	return func(k8stesting.Action) (bool, runtime.Object, error) {
		if g.open.Load() {
			return false, nil, nil
		}
		return true, nil, g.fail()
	}
}

func notFound1532() error {
	return apierrors.NewNotFound(schema.GroupResource{Group: refusedGVR1532().Group, Resource: refusedGVR1532().Resource}, "")
}

func forbidden1532() error {
	return apierrors.NewForbidden(schema.GroupResource{Group: refusedGVR1532().Group, Resource: refusedGVR1532().Resource},
		"", context.Canceled)
}

func unavailable1532() error { return apierrors.NewServiceUnavailable("the LIST stalls") }

// syncBuf is a goroutine-safe log sink: informer goroutines log while the arm
// reads.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureInfo1532 makes the process default logger write INFO and above into a
// buffer until the arm ends (cache tests never run in parallel: t.Setenv).
func captureInfo1532(t *testing.T) *syncBuf {
	t.Helper()
	b := &syncBuf{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return b
}

// completeLine is the barrier's sync_wait_complete line, "" if none.
func completeLine(logs string) string {
	for _, l := range strings.Split(logs, "\n") {
		if strings.Contains(l, "msg=cache.phase1.sync_wait_complete") {
			return l
		}
	}
	return ""
}

func newWatcher1532(t *testing.T, reactors map[string]k8stesting.ReactionFunc) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("PHASE1_SYNC_PASS_GRACE_SECONDS", "3600")
	t.Setenv("PHASE1_SYNC_QUIESCENCE_SECONDS", "3600")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(newTestScheme(), listKinds1532())
	for resource, r := range reactors {
		dyn.PrependReactor("list", resource, r)
	}
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.RegisterMetaQuerySeeds(context.Background())
	return rw
}

// T1, T2 — a LIST answered NotFound (the sandbox's panels: a resource the
// apiserver does not serve inside a served group) or Forbidden (an RBAC gap)
// ends the barrier's wait for that informer: the barrier returns nil, well
// inside its ctx, with the refused GVR named, unsynced and not servable, and
// every other informer synced (mutation: a barrier waiting on HasSynced alone
// runs out the ctx; Forbidden not classified as refused runs out T2's ctx).
func TestPhase1Barrier1532_ARefusedListEndsTheWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func() error
	}{
		{"not found", notFound1532},
		{"forbidden", forbidden1532},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &gatedList{fail: tc.fail}
			rw := newWatcher1532(t, map[string]k8stesting.ReactionFunc{refusedGVR1532().Resource: g.reactor()})
			logs := captureInfo1532(t)
			rw.EnsureResourceType(refusedGVR1532())

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := time.Now()
			if err := rw.WaitAllInformersSynced(ctx); err != nil {
				t.Fatalf("the barrier ran out its ctx (%v after %v): it waited on an informer whose LIST is refused", err, time.Since(start))
			}
			if elapsed := time.Since(start); elapsed > exitBound1532 {
				t.Fatalf("the barrier returned after %v; want within %v of a refused LIST", elapsed, exitBound1532)
			}
			if rw.IsSynced(refusedGVR1532()) || rw.IsServable(refusedGVR1532()) {
				t.Fatalf("the refused GVR reads synced %v, servable %v; a refusal must never read as synced",
					rw.IsSynced(refusedGVR1532()), rw.IsServable(refusedGVR1532()))
			}
			for _, s := range cache.MetaQuerySeeds() {
				if !rw.IsSynced(s) {
					t.Fatalf("%v is not synced at the barrier's return: only the refused informer may be", s)
				}
			}
			line := completeLine(logs.String())
			if !strings.Contains(line, "refused_count=1") || !strings.Contains(line, refusedGVR1532().String()) {
				t.Fatalf("the complete line must name the refused GVR (refused_count=1, %s); got %q", refusedGVR1532(), line)
			}
		})
	}
}

// T3 — a TRANSIENT LIST failure (503) is not a refusal: the barrier keeps
// waiting while it fails, and returns once the LIST succeeds, the informer
// synced and nothing named refused (mutation: every watch error counted as a
// refusal returns while the LIST still fails).
func TestPhase1Barrier1532_ATransientFailureIsWaitedOn(t *testing.T) {
	g := &gatedList{fail: unavailable1532}
	rw := newWatcher1532(t, map[string]k8stesting.ReactionFunc{slowGVR1532().Resource: g.reactor()})
	logs := captureInfo1532(t)
	rw.EnsureResourceType(slowGVR1532())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rw.WaitAllInformersSynced(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("the barrier returned (%v) while the LIST still failed transiently", err)
	case <-time.After(2 * time.Second): // the reflector has failed and retried by now (backoff from 800 ms)
	}
	g.open.Store(true)
	opened := time.Now()
	if err := <-done; err != nil {
		t.Fatalf("the barrier did not return once the LIST succeeded: %v", err)
	}
	// the reflector's retry backoff (from 800 ms, doubling) decides when the
	// LIST is next tried: a literal generous enough for a few doublings
	if elapsed := time.Since(opened); elapsed > 4*exitBound1532 {
		t.Fatalf("the barrier returned %v after the LIST was served; want within %v", elapsed, 4*exitBound1532)
	}
	if !rw.IsSynced(slowGVR1532()) {
		t.Fatal("the barrier returned with the transiently failing informer unsynced")
	}
	if line := completeLine(logs.String()); !strings.Contains(line, "refused_count=0") {
		t.Fatalf("nothing was refused; the complete line says %q", line)
	}
}

// T4 — a refusal that heals: the barrier returns on the refusal with the GVR
// unsynced; once the resource is served, the reflector's retry lists it and
// the informer syncs (a refused informer is deferred, never dropped)
// (mutation: a refusal reported as synced reads IsSynced true at the return).
func TestPhase1Barrier1532_ARefusalThatHealsSyncsLater(t *testing.T) {
	g := &gatedList{fail: notFound1532}
	rw := newWatcher1532(t, map[string]k8stesting.ReactionFunc{refusedGVR1532().Resource: g.reactor()})
	rw.EnsureResourceType(refusedGVR1532())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	if err := rw.WaitAllInformersSynced(ctx); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if elapsed := time.Since(start); elapsed > exitBound1532 {
		t.Fatalf("the barrier returned after %v; want within %v of a refused LIST", elapsed, exitBound1532)
	}
	if rw.IsSynced(refusedGVR1532()) {
		t.Fatal("the refused GVR reads synced at the barrier's return")
	}
	if !rw.IsRegistered(refusedGVR1532()) {
		t.Fatal("the refused GVR was dropped; it must stay registered so it heals")
	}
	g.open.Store(true) // the CRD is served now
	deadline := time.Now().Add(45 * time.Second)
	for !rw.IsSynced(refusedGVR1532()) {
		if time.Now().After(deadline) {
			t.Fatal("the healed GVR did not sync within 45 s of being served")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// T5 — a refused informer registered DURING a pass is part of the barrier's
// set: the pass that ends with the set grown is not the exit; the next pass
// takes the refused informer in and the complete line names it (mutation: an
// exit that ignores the set's stability returns at the first pass, before the
// late informer, and names nothing).
func TestPhase1Barrier1532_ALateRefusedInformerIsInTheSet(t *testing.T) {
	slow := &gatedList{fail: unavailable1532}
	refused := &gatedList{fail: notFound1532}
	rw := newWatcher1532(t, map[string]k8stesting.ReactionFunc{
		slowGVR1532().Resource:    slow.reactor(),
		refusedGVR1532().Resource: refused.reactor(),
	})
	logs := captureInfo1532(t)
	rw.EnsureResourceType(slowGVR1532()) // holds the first pass open

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rw.WaitAllInformersSynced(ctx) }()
	time.Sleep(500 * time.Millisecond) // the first pass is waiting on the slow informer
	rw.EnsureResourceType(refusedGVR1532())
	time.Sleep(500 * time.Millisecond)
	slow.open.Store(true) // the first pass can end now, with the set grown
	opened := time.Now()
	if err := <-done; err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if elapsed := time.Since(opened); elapsed > 4*exitBound1532 {
		t.Fatalf("the barrier returned %v after the slow LIST was served; want within %v", elapsed, 4*exitBound1532)
	}
	line := completeLine(logs.String())
	if !strings.Contains(line, "refused_count=1") || !strings.Contains(line, refusedGVR1532().String()) {
		t.Fatalf("the late refused informer must be in the barrier's final set and named; complete line %q", line)
	}
	if strings.Contains(line, "passes=1 ") {
		t.Fatalf("the barrier ended at its first pass although the set grew during it: %q", line)
	}
}
