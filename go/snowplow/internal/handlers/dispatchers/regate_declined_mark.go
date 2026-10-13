// regate_declined_mark.go — #677 (2): the refresher takes a KNOWN-DECLINED mark
// when a re-resolve's only stage errors were correct per-identity re-gate
// denies, and re-checks the facts the mark holds under before every skip.
//
// The set, the Put / evict / GONE retire rules and the counters live in
// cache/regate_declined.go. This file owns the two refresher-side halves:
//
//   - TAKE (regateDeclinedMarkIfAllDenies): called from resolveAndPopulateL1's
//     stage-error decline. Marks ONLY when every stage error was a re-gate deny
//     (RegateDenySink == StageErrorSink, both > 0) AND the resolve touched no
//     external endpoint, AND every definition object is readable now.
//   - CONSULT (regateDeclinedSkip): called at the top of resolveAndPopulateL1,
//     the refresher's handler. Nothing else calls it — a /call, the #547 demand
//     revalidation, the cohort seed and the prewarm engine never consult it.
//
// Definitions are read from the shared informer indexer only
// (ResourceWatcher.GetObject on a servable GVR): an uid + resourceVersion fact
// check that serves nothing to anyone and never dials. A non-servable GVR reads
// as unreadable: never marks, and retires a held mark.

package dispatchers

import (
	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// readDefinitionVersion reads one definition object's uid + resourceVersion
// from the informer indexer. ok=false when it cannot be read from a servable
// informer right now.
func readDefinitionVersion(k cache.DepKey) (*unstructured.Unstructured, bool) {
	rw := cache.Global()
	if rw == nil || rw.IsPassthrough() || !rw.IsServable(k.GVR) {
		return nil, false
	}
	obj, ok := rw.GetObject(k.GVR, k.Namespace, k.Name)
	if !ok || obj == nil {
		return nil, false
	}
	return obj, true
}

// regateDeclinedDefinitions returns the definition facts for a refresher cell:
// the cell's own CR, and for a CR carrying spec.apiRef (a widget), the
// RESTAction it references. ok=false if any of them cannot be read now.
func regateDeclinedDefinitions(inputs *cache.ResolvedKeyInputs) ([]cache.DefinitionVersion, bool) {
	self := cache.DepKey{
		GVR:       schema.GroupVersionResource{Group: inputs.Group, Version: inputs.Version, Resource: inputs.Resource},
		Namespace: inputs.Namespace,
		Name:      inputs.Name,
	}
	if self.Name == "" || self.GVR.Resource == "" {
		return nil, false
	}
	obj, ok := readDefinitionVersion(self)
	if !ok {
		return nil, false
	}
	defs := []cache.DefinitionVersion{{Key: self, UID: string(obj.GetUID()), ResourceVersion: obj.GetResourceVersion()}}
	raName, _, _ := unstructured.NestedString(obj.Object, "spec", "apiRef", "name")
	raNS, _, _ := unstructured.NestedString(obj.Object, "spec", "apiRef", "namespace")
	if raName != "" {
		ra := cache.DepKey{GVR: restActionGVR, Namespace: raNS, Name: raName}
		raObj, ok := readDefinitionVersion(ra)
		if !ok {
			return nil, false
		}
		defs = append(defs, cache.DefinitionVersion{Key: ra, UID: string(raObj.GetUID()), ResourceVersion: raObj.GetResourceVersion()})
	}
	return defs, true
}

// regateDeclinedRBACMovedFn is retire rule (a). A seam ONLY so the mutation arm
// can remove it and prove it is load-bearing; production never reassigns it.
var regateDeclinedRBACMovedFn = func(m *cache.RegateDeclinedMark) bool {
	return cache.RBACGen() != m.RBACPublishSeq
}

// regateDeclinedMarkHolds re-checks every fact the mark was taken under.
func regateDeclinedMarkHolds(m *cache.RegateDeclinedMark) (bool, string) {
	if m == nil {
		return false, cache.RegateDeclinedRetiredNilMark
	}
	if regateDeclinedRBACMovedFn(m) {
		return false, cache.RegateDeclinedRetiredRBACPublished
	}
	for _, d := range m.Defs {
		obj, ok := readDefinitionVersion(d.Key)
		if !ok {
			return false, cache.RegateDeclinedRetiredDefinitionUnreadable
		}
		if string(obj.GetUID()) != d.UID || obj.GetResourceVersion() != d.ResourceVersion {
			return false, cache.RegateDeclinedRetiredDefinitionChanged
		}
	}
	return true, ""
}

// regateDeclinedSkip is the refresher consult: true iff key is marked and every
// fact behind the mark still holds. A mark that no longer holds is retired here
// and the cell is resolved.
func regateDeclinedSkip(key string) bool {
	m, ok := cache.LookupRegateDeclined(key)
	if !ok {
		return false
	}
	if holds, reason := regateDeclinedMarkHolds(m); !holds {
		cache.RetireRegateDeclined(key, reason)
		return false
	}
	cache.NoteRegateDeclinedSkip()
	return true
}

// regateDeclinedMarkIfAllDenies takes the mark at the stage-error decline.
// rbacGen0 is RBACGen() read BEFORE the resolve, so a publish that landed
// mid-resolve retires the mark at the first consult.
func regateDeclinedMarkIfAllDenies(key string, inputs *cache.ResolvedKeyInputs,
	rbacGen0 uint64, stageErrs, regateDenies, externalTouches int64) bool {
	if stageErrs <= 0 || regateDenies != stageErrs || externalTouches > 0 {
		return false
	}
	defs, ok := regateDeclinedDefinitions(inputs)
	if !ok {
		return false
	}
	cache.MarkRegateDeclined(key, &cache.RegateDeclinedMark{RBACPublishSeq: rbacGen0, Defs: defs})
	slog.Default().Debug("resolveAndPopulateL1: refresh declined only on per-identity re-gate denies; marked known-declined",
		slog.String("subsystem", "cache"),
		slog.String("key_hash", key),
		slog.String("handler", inputs.CacheEntryClass),
		slog.Int64("regate_denies", regateDenies),
		slog.String("effect", "the refresher skips this cell until RBAC is republished, its definition changes or is "+
			"deleted, or it is Put; the cell stays KNOWN-STALE and is revalidated on read under the requester (#677)"),
	)
	return true
}
