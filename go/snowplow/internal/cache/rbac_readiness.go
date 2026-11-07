package cache

// rbac_readiness.go — #1231: a pod is not Ready until the RBAC snapshot the
// evaluator reads is complete.
//
// THE DEFECT. /readyz read Phase1Done alone. Phase 1 starts after main.go's 60 s
// WaitForCacheSync, which continues on timeout, and it sets Phase1Done on every
// return. So a pod whose RBAC LISTs took longer went Ready on a snapshot that an
// event-driven rebuild had published during the initial LIST (Synced: false),
// and refused users their bindings grant until all four informers synced, or
// for good if one never did.
//
// THE RULE. Readiness requires the initial RBAC publish to be done: the live
// snapshot is Synced (built after all four RBAC informers had finished their
// initial LIST) AND waitAndPublishInitialRBACSnapshot has returned, so the
// bindings-index build #819 owes before Ready, deferred to that publish when
// Phase 1 flipped first, has run too. Nothing else decides it. No timer
// and no cap: a sync that never completes keeps the pod NotReady, and says which
// informer it waits on (the readiness body, the expvar and gauge below, and the
// boot WARN in main.go). Liveness (/health) does not read this, so a slow sync
// never gets the pod killed.
//
// ONLY A WATCHER THAT RUNS THE RBAC INFORMERS has a snapshot to wait for, and
// the watcher says so itself (runsRBACInformers). With the cache off main.go
// still builds a watcher, a passthrough one with no informers at all, and a
// cache whose setup failed builds none. In both, requests are authorised by the
// apiserver, so readiness is Phase1Done's alone, as before.
//
// TWO WAYS TO WAIT, told apart in the readiness body: an RBAC informer still
// listing (it is named), or every informer synced and the initial publish not
// done yet (a rebuild that keeps panicking, #691, is the case that can last).

import (
	"sync/atomic"
	"time"
)

// rbacCompleteNanos is when the first Synced snapshot was published (unix
// nanos), 0 until then.
var rbacCompleteNanos atomic.Int64

// noteRBACSnapshotPublished records the first complete publish. Called with
// every snapshot the rebuild stores.
func noteRBACSnapshotPublished(s *RBACSnapshot) {
	if s != nil && s.Synced {
		rbacCompleteNanos.CompareAndSwap(0, time.Now().UnixNano())
	}
}

// RBACReadinessState is readiness's RBAC half.
type RBACReadinessState struct {
	// Complete: readiness may come as far as RBAC is concerned.
	Complete bool
	// Unsynced names the RBAC informers that have not finished their initial
	// LIST, by resource, in a fixed order. Empty when Complete.
	Unsynced []string
	// PublishPending: not Complete although every RBAC informer has synced,
	// so the initial publish has not completed yet.
	PublishPending bool
}

// runsRBACInformers reports whether rw runs the RBAC informers the in-process
// evaluator reads: every watcher but a passthrough one (CACHE_ENABLED=false),
// which runs no informers and leaves authorisation to the apiserver.
func (rw *ResourceWatcher) runsRBACInformers() bool {
	return rw != nil && rw.mode != modePassthrough
}

// RBACReadiness reports readiness's RBAC half (see the file comment). Reads only
// atomics and the informers' HasSynced: safe on every probe.
func RBACReadiness() RBACReadinessState {
	rw := Global()
	if !rw.runsRBACInformers() {
		return RBACReadinessState{Complete: true}
	}
	if s := LiveRBACSnapshot(); s != nil && s.Synced && rbacInitialPublishReturned(rw) {
		return RBACReadinessState{Complete: true}
	}
	unsynced := unsyncedRBACInformers(rw)
	return RBACReadinessState{Unsynced: unsynced, PublishPending: len(unsynced) == 0}
}

// unsyncedRBACInformers names the RBAC informers of rw that are missing or have
// not synced, in rbacTypedGVRs order.
func unsyncedRBACInformers(rw *ResourceWatcher) []string {
	rw.mu.RLock()
	type probe struct {
		name      string
		hasSynced func() bool
	}
	probes := make([]probe, 0, len(rbacTypedGVRs))
	for _, gvr := range rbacTypedGVRs {
		p := probe{name: gvr.Resource}
		if gi, ok := rw.informers[gvr]; ok {
			p.hasSynced = gi.Informer().HasSynced
		}
		probes = append(probes, p)
	}
	rw.mu.RUnlock()
	var out []string
	for _, p := range probes {
		if p.hasSynced == nil || !p.hasSynced() {
			out = append(out, p.name)
		}
	}
	return out
}

// UnsyncedRBACInformers names the RBAC informers of the process watcher that
// have not synced; nil for a watcher that runs none. For the boot WARN in
// main.go.
func UnsyncedRBACInformers() []string {
	rw := Global()
	if !rw.runsRBACInformers() {
		return nil
	}
	return unsyncedRBACInformers(rw)
}

// RBACSnapshotCompleteSnapshot mirrors the snowplow_rbac_snapshot_complete expvar
// for the OTLP gauges: complete is 1 while readiness's RBAC half holds, else 0;
// elapsedMs is process start to the first complete snapshot, -1 until there is
// one.
func RBACSnapshotCompleteSnapshot() (complete int64, elapsedMs int64) {
	if RBACReadiness().Complete {
		complete = 1
	}
	return complete, rbacCompleteElapsedMs()
}

// rbacCompleteElapsedMs is process start to the first complete snapshot, -1
// until there is one.
func rbacCompleteElapsedMs() int64 {
	if n := rbacCompleteNanos.Load(); n > 0 {
		return (n - prewarmProcessStartNanos) / int64(time.Millisecond)
	}
	return -1
}

// rbacSnapshotCompleteExpvar is the snowplow_rbac_snapshot_complete value.
func rbacSnapshotCompleteExpvar() any {
	st := RBACReadiness()
	complete := 0
	if st.Complete {
		complete = 1
	}
	unsynced := st.Unsynced
	if unsynced == nil {
		unsynced = []string{}
	}
	return map[string]any{
		"complete":                complete,
		"elapsed_ms":              rbacCompleteElapsedMs(),
		"unsynced_informers":      unsynced,
		"initial_publish_pending": st.PublishPending,
	}
}

// ResetRBACCompleteForTest clears the first-complete instant. Tests only.
func ResetRBACCompleteForTest() { rbacCompleteNanos.Store(0) }
