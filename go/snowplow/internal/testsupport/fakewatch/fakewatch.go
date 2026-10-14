// Package fakewatch closes the list/watch gap of client-go's fake dynamic
// client, for tests only (#660, #673, #686).
//
// THE GAP. A real apiserver watch starts from the LIST's resourceVersion and
// replays every change after it. The fake's ObjectTracker ignores
// resourceVersion: a watch sees only the events emitted AFTER it was
// registered. An informer's HasSynced (WaitForCacheSync) turns true once its
// initial LIST is processed, which can be BEFORE its Watch call reaches the
// tracker. A test that mutates an object in that window loses the event for
// good: the informer never observes the change, whatever the deadline. Under
// CPU contention the window widens, so the test flakes.
//
// THE BARRIER. Install wraps the fake's watch reaction so it counts a watch
// only after the tracker has REGISTERED it. Wait blocks until every named
// resource has an established watch. A harness calls Wait after
// WaitForCacheSync and before the test mutates anything, so no event can be
// lost. It never changes what the fake serves.
package fakewatch

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Barrier counts the established watches per resource (plural).
type Barrier struct {
	mu    sync.Mutex
	n     map[string]int
	ready chan struct{} // closed and replaced on every new watch
}

// Install puts the barrier in front of dyn's watch reaction. Call it right
// after creating the fake, before any informer starts.
func Install(dyn *dynamicfake.FakeDynamicClient) *Barrier {
	b := &Barrier{n: map[string]int{}, ready: make(chan struct{})}
	tracker := dyn.Tracker()
	dyn.PrependWatchReactor("*", func(a k8stesting.Action) (bool, watch.Interface, error) {
		gvr := a.GetResource()
		w, err := tracker.Watch(gvr, a.GetNamespace())
		if err != nil {
			return false, nil, err
		}
		// Registered with the tracker: from here on every event reaches it.
		b.mu.Lock()
		b.n[gvr.Resource]++
		close(b.ready)
		b.ready = make(chan struct{})
		b.mu.Unlock()
		return true, w, nil
	})
	return b
}

// Wait blocks until each resource has at least one established watch, or
// ctx is done.
func (b *Barrier) Wait(ctx context.Context, resources ...string) error {
	for {
		b.mu.Lock()
		var missing []string
		for _, r := range resources {
			if b.n[r] == 0 {
				missing = append(missing, r)
			}
		}
		ready := b.ready
		b.mu.Unlock()
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-ready:
		case <-ctx.Done():
			sort.Strings(missing)
			return fmt.Errorf("fakewatch: no established watch for %s: %w", strings.Join(missing, ", "), ctx.Err())
		}
	}
}

// WaitFor is Wait with a timeout.
func (b *Barrier) WaitFor(timeout time.Duration, resources ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return b.Wait(ctx, resources...)
}
