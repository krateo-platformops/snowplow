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
// WITHOUT A WATCHER there is no in-process evaluator and nothing to wait for:
// with the cache off, or a watcher that failed to build, requests are
// authorised by the apiserver, so readiness is Phase1Done's alone, as before.

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

// RBACReadiness reports whether readiness may come as far as RBAC is concerned:
// the live snapshot is complete, or there is no watcher (see the file comment).
// When it is not, unsynced names the RBAC informers that have not finished their
// initial LIST, by resource, in a fixed order. Reads only atomics and the
// informers' HasSynced: safe on every probe.
func RBACReadiness() (complete bool, unsynced []string) {
	rw := Global()
	if rw == nil {
		return true, nil
	}
	if s := LiveRBACSnapshot(); s != nil && s.Synced && rbacInitialPublishReturned(rw) {
		return true, nil
	}
	return false, unsyncedRBACInformers(rw)
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
// have not synced; nil without a watcher. For the boot WARN in main.go.
func UnsyncedRBACInformers() []string {
	rw := Global()
	if rw == nil {
		return nil
	}
	return unsyncedRBACInformers(rw)
}

// RBACSnapshotCompleteSnapshot mirrors the snowplow_rbac_snapshot_complete expvar
// for the OTLP gauges: complete is 1 while readiness's RBAC half holds, else 0;
// elapsedMs is process start to the first complete snapshot, -1 until there is
// one.
func RBACSnapshotCompleteSnapshot() (complete int64, elapsedMs int64) {
	if ok, _ := RBACReadiness(); ok {
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
	ok, unsynced := RBACReadiness()
	complete := 0
	if ok {
		complete = 1
	}
	if unsynced == nil {
		unsynced = []string{}
	}
	return map[string]any{
		"complete":           complete,
		"elapsed_ms":         rbacCompleteElapsedMs(),
		"unsynced_informers": unsynced,
	}
}

// ResetRBACCompleteForTest clears the first-complete instant. Tests only.
func ResetRBACCompleteForTest() { rbacCompleteNanos.Store(0) }
