// unserved_version_block.go — #807: a version the CRD no longer serves is torn
// down when its LIST is refused with 404, and is kept from re-registering until
// the CRD serves it again.
//
// THE MEASURED SHAPE (057, 1.12.50, 2026-10-20). All 9 informers stuck in
// watch_broken were composition.krateo.io versions the cluster no longer
// serves (installers v0-3-435/436, portals/portalagents/builderpublishes
// v1-8-75, frontends/frontendcrds v1-8-6, krateoobservabilities v0-1-22,
// meetupsites v0-1-0). Every one of the 26 cache.watch.broken lines on the pod
// was a 404. #219's prune (pruneUnservedGVRs) had run 50 times, so the
// retirement WAS seen; the versions came back because a resolver touched the
// retired path again (a RESTAction that hard-codes it, or a link built before
// the upgrade) and EnsureResourceType registered it afresh. Nothing removes a
// version registered AFTER the CRD event that retired it, because no further
// CRD event names it: its reflector re-LISTs a 404 forever (watch_errors_total
// 13,048 and 26,496 cache.reflector.path WARN lines on that pod).
//
// THE RULE, IN TWO HALVES.
//
//   - Retire. A version is torn down only when BOTH hold: its LIST/WATCH was
//     refused with 404, AND the CRD informer's latest event for that
//     group+plural lists a non-empty served set without this version. The 404
//     alone is not enough: an informer for a version the CRD has just started
//     to serve can 404 while the apiserver's discovery catches up, and an
//     informer that is merely unsynced must never be pruned. The CRD record
//     alone is not used either: it is the trigger's confirmation, not a
//     standing reason to refuse a registration nobody has tried.
//   - Block. A retired version is recorded in rw.unservedVersions and every
//     registration funnel (EnsureResourceType, EnsureResourceTypeMetadataOnly,
//     AddResourceType) refuses it, the way the #119 unserved-group gate does:
//     (false, an already-closed channel), so the read falls through to the
//     apiserver as the caller, which is what it was doing anyway. The block is
//     lifted by the first CRD ADD/UPDATE whose served set names the version
//     again — on EVERY such event, not only on a fingerprint change, so the
//     clear can never be skipped by the #218 no-op gate.
//
// #219's own prune records the same block, so a version a CRD upgrade retired
// is refused from the start instead of being re-registered once and pruned
// again on its first 404.
//
// WHERE IT RUNS. The 404 arrives on the reflector goroutine, which must not
// tear its own informer down. watchErrorHandlerFor hands the GVR to the
// CRD-lifecycle worker (submitListRefused) without blocking. The worker is the
// same single goroutine that applies CRD events, so the refusal check and the
// served-set record it reads are serialised against each other: a CRD UPDATE
// that re-serves the version is either applied before the check (the check
// declines) or after it (the UPDATE lifts the block).
package cache

import (
	"log/slog"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// crdServedKey identifies one CRD by the coordinates a GVR carries.
type crdServedKey struct{ group, resource string }

// recordServedVersions remembers the CRD's current served set and lifts the
// block on every version in it. Runs on the CRD-lifecycle worker for every ADD
// and UPDATE, before any fingerprint gate.
//
// An empty served set FORGETS the record instead of storing "serves nothing":
// crdServedGVRs returns nil both for a CRD that serves no version and for one
// whose versions subtree could not be read, and a refusal checked against an
// unreadable spec must decline, not prune — the same reason pruneUnservedGVRs
// declines on an empty set.
func (c *crdDiscovery) recordServedVersions(u *unstructured.Unstructured) {
	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
	if group == "" || plural == "" {
		return
	}
	key := crdServedKey{group: group, resource: plural}
	served := crdServedGVRs(u)
	if len(served) == 0 {
		c.servedVersions.Delete(key)
		return
	}
	set := make(map[string]struct{}, len(served))
	for _, g := range served {
		set[g.Version] = struct{}{}
	}
	c.servedVersions.Store(key, set)
	Global().unblockServedVersions(served)
}

// forgetServedVersions drops the CRD's served-set record on DELETE. A refusal
// for a kind whose CRD is gone then declines: tearing down a deleted CRD's
// versions is triggerCRDDelete's job, and it does not block them.
func (c *crdDiscovery) forgetServedVersions(u *unstructured.Unstructured) {
	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
	if group == "" || plural == "" {
		return
	}
	c.servedVersions.Delete(crdServedKey{group: group, resource: plural})
}

// submitListRefused hands a 404'd GVR to the CRD-lifecycle worker. Called from
// the reflector goroutine, so it never parks: on a full queue the refusal is
// dropped and counted, and the reflector's next retry reports it again.
func (c *crdDiscovery) submitListRefused(rw *ResourceWatcher, gvr schema.GroupVersionResource) {
	c.startCRDDiscoveryWorker()
	select {
	case c.queue <- crdDiscoveryEvent{kind: crdLifecycleListRefused, rw: rw, gvr: gvr}:
		c.eventsEnqueued.Add(1)
	default:
		c.listRefusedDropped.Add(1)
	}
}

// handleListRefused is the worker-side half: retire gvr only if the CRD's
// recorded served set exists and does not name its version.
func (c *crdDiscovery) handleListRefused(rw *ResourceWatcher, gvr schema.GroupVersionResource) {
	if rw == nil {
		return
	}
	c.listRefusedChecks.Add(1)
	v, known := c.servedVersions.Load(crdServedKey{group: gvr.Group, resource: gvr.Resource})
	if !known {
		// No CRD record: a built-in or aggregated kind, a CRD the informer has
		// not delivered yet, or no CRD informer at all. Nothing says the version
		// is retired.
		c.listRefusedCRDUnknown.Add(1)
		return
	}
	set, _ := v.(map[string]struct{})
	if _, served := set[gvr.Version]; served {
		// The CRD serves this version: the 404 is the apiserver catching up, or
		// an informer that has not synced yet. Never pruned.
		c.listRefusedStillServed.Add(1)
		return
	}
	if !rw.retireUnservedVersion(gvr, confirmRetractRefusedVersionPruned) {
		// Already torn down by an earlier refusal still queued behind this one.
		return
	}
	c.refusedVersionPruned.Add(1)
	slog.Info("cache.crd_discovery.refused_version_pruned",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("hint", "the LIST/WATCH was refused with 404 and the CRD no longer serves this version; "+
			"the informer is stopped and the version is refused registration until the CRD serves it again (#807)"),
	)
}

// retireUnservedVersion blocks gvr, then tears its informer down. The block is
// written FIRST so a registration racing the teardown cannot re-create the
// informer in between. Reports whether an informer was registered.
func (rw *ResourceWatcher) retireUnservedVersion(gvr schema.GroupVersionResource, reason string) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	rw.mu.Lock()
	if rw.unservedVersions == nil {
		rw.unservedVersions = map[schema.GroupVersionResource]struct{}{}
	}
	rw.unservedVersions[gvr] = struct{}{}
	_, registered := rw.informers[gvr]
	rw.mu.Unlock()
	rw.removeResourceTypeWithReason(gvr, reason)
	return registered
}

// unblockServedVersions lifts the block on every GVR the CRD serves again.
func (rw *ResourceWatcher) unblockServedVersions(served []schema.GroupVersionResource) {
	if rw == nil || len(served) == 0 {
		return
	}
	rw.mu.Lock()
	for _, g := range served {
		delete(rw.unservedVersions, g)
	}
	rw.mu.Unlock()
}

// unservedVersionBlockedLocked reports whether gvr is a retired version a
// registration funnel must refuse. Caller holds rw.mu (read or write).
func (rw *ResourceWatcher) unservedVersionBlockedLocked(gvr schema.GroupVersionResource) bool {
	_, blocked := rw.unservedVersions[gvr]
	return blocked
}

// refuseUnservedVersion counts one refused registration and returns the
// already-closed channel the funnel hands back, so a caller waiting on it never
// blocks and the read falls through to the apiserver.
func refuseUnservedVersion() chan struct{} {
	crdDiscoverySingleton().unservedRegistrationsRefused.Add(1)
	closed := make(chan struct{})
	close(closed)
	return closed
}

// IsUnservedVersionBlocked reports whether gvr is currently refused
// registration as a retired CRD version (#807).
func (rw *ResourceWatcher) IsUnservedVersionBlocked(gvr schema.GroupVersionResource) bool {
	if rw == nil {
		return false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	_, blocked := rw.unservedVersions[gvr]
	return blocked
}
