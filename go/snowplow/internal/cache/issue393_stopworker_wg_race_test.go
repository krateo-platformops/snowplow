// issue393_stopworker_wg_race_test.go — #393: the test-only depWatch.stopWorker
// shim Waits on workerWG while an informer event can still reach
// startWorker's lazy startOnce.Do → workerWG.Add(1). A positive Add from a
// zero counter that is not ordered before Wait is a sync.WaitGroup misuse
// (the -race report on PR #379 / #395: deps_watch.go Wait vs Add, reached
// from the AddFunc handler → submitDepEvent → startWorker).
//
// Both arms use an ISOLATED &depWatch{} (never the singleton) and drive the
// exact frame the CI stack shows: submitDepEvent, the last hop of every
// informer handler. A nil watcher makes the worker's probe return objUnknown,
// which only requeues with backoff — no tracker or store is touched.

package cache

import (
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func issue393Key() depEventKey {
	return depEventKey{
		gvr:       schema.GroupVersionResource{Group: "example.org", Version: "v1", Resource: "flexes"},
		namespace: "demo-system",
		name:      "flex-393",
	}
}

// issue393Reap stops whatever worker an arm left behind, so a RED run does
// not leak a goroutine into later tests. It deliberately bypasses stopWorker
// (the code under test).
func issue393Reap(w *depWatch) {
	if q := w.q(); q != nil {
		q.ShutDown()
	}
	w.workerWG.Wait()
}

// Deterministic arm: once stopWorker has returned, no later event may start a
// worker. On the pre-fix tree the first event after stop fires startOnce,
// builds a fresh queue and Adds to the WaitGroup the shim already Waited on —
// the event the probe test meant to LOSE is processed instead, and the
// goroutine outlives the stop.
func TestIssue393_StopWorker_NoWorkerStartsAfterStop(t *testing.T) {
	w := &depWatch{}
	t.Cleanup(func() { issue393Reap(w) })

	w.stopWorker()
	w.submitDepEvent(nil, issue393Key(), dmSourceWatch)

	if w.q() != nil {
		t.Fatalf("an event after stopWorker started a worker: the queue was built and workerWG.Add(1) " +
			"followed the shim's Wait (#393)")
	}
}

// Concurrent arm: an informer event delivered concurrently with stopWorker.
// Under -race the pre-fix tree reports the WaitGroup Add/Wait race when the
// event's startWorker lands just before the shim's Wait and before any queue
// mutex hand-off orders them; without -race it still fails whenever the
// event starts the worker after the stop (the deterministic arm's defect).
func TestIssue393_StopWorker_ConcurrentEventNeverAddsAfterWait(t *testing.T) {
	const iterations = 500
	startedAfterStop := 0
	for i := 0; i < iterations; i++ {
		w := &depWatch{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			w.submitDepEvent(nil, issue393Key(), dmSourceWatch)
		}()
		close(start)
		w.stopWorker()
		wg.Wait()

		// stopWorker has returned and the event has been submitted. Either the
		// worker started BEFORE the stop (queue built and shut down) or it never
		// started (no queue). A live, un-shut queue means a worker began after
		// the stop.
		if q := w.q(); q != nil && !q.ShuttingDown() {
			startedAfterStop++
		}
		issue393Reap(w)
	}
	if startedAfterStop > 0 {
		t.Fatalf("%d/%d iterations: the event started a worker after stopWorker returned (#393)",
			startedAfterStop, iterations)
	}
}

// The same Add-after-Wait shape on the second lazily-started worker in this
// package: crdDiscovery.startCRDDiscoveryWorker's startOnce.Do →
// workerWG.Add(1), reached from the CRD informer handlers through
// submitCRDLifecycleEvent, against the test-only stopCRDDiscoveryWorker's
// Wait. An out-of-range kind makes processEvent's defensive default the only
// effect (a WARN and the per-instance processed counter), so no global state
// is touched.
func issue393CRDDiscovery() *crdDiscovery {
	return &crdDiscovery{
		queue:  make(chan crdDiscoveryEvent, crdDiscoveryQueueDepth),
		stopCh: make(chan struct{}),
	}
}

const issue393UnknownCRDKind = crdLifecycleKind(99)

func TestIssue393_CRDDiscoveryStop_NoWorkerStartsAfterStop(t *testing.T) {
	c := issue393CRDDiscovery()
	c.stopCRDDiscoveryWorker()
	c.submitCRDLifecycleEvent(nil, issue393UnknownCRDKind)
	c.workerWG.Wait() // reap a late worker, if one started

	if got := c.eventsProcessed.Load(); got != 0 {
		t.Fatalf("an event after stopCRDDiscoveryWorker was processed (%d): a worker started and "+
			"workerWG.Add(1) followed the shim's Wait (#393)", got)
	}
}

// Concurrent arm for crdDiscovery: under -race the pre-fix tree can report
// the Add/Wait race. There is no non-race assertion here: a start that wins
// before the stop legitimately drains the event, so only -race discriminates
// (the deterministic arm above covers the start-after-stop defect).
func TestIssue393_CRDDiscoveryStop_ConcurrentEventNeverAddsAfterWait(t *testing.T) {
	const iterations = 500
	for i := 0; i < iterations; i++ {
		c := issue393CRDDiscovery()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c.submitCRDLifecycleEvent(nil, issue393UnknownCRDKind)
		}()
		close(start)
		c.stopCRDDiscoveryWorker()
		wg.Wait()
		c.workerWG.Wait()
	}
}
