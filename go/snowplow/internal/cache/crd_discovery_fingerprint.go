// crd_discovery_fingerprint.go — #218: the discovery-identity fingerprint that
// gates the CRD-re-list warmth amplifier.
//
// triggerCRDDiscovery fired three global invalidations on EVERY CRD ADD/UPDATE,
// including the ~9.5min idle re-list (UpdateFunc Sync, ResyncPeriod:0):
//   :519 DiscoverGroupResourcesFresh  — global discovery memcache wipe + re-read
//   :534 invalidateSADiscovery        — SA CachedDiscoveryClient → RESTMapper wipe
//   :542 invalidateCRDSchemaMemo      — snowplow's compiled-schema validation memo
// Only :556 triggerCRDSchemaRelist was fingerprint-guarded. So an idle re-list
// re-paid a cluster-wide discovery + schema-recompile every 9.5min per CRD.
//
// TWO fingerprints gate the UPDATE path (the compare itself makes an ADD — no
// prior fp — and a real change fire, while an idle re-list trips neither):
//   - schema fingerprint (crdSchemaFingerprint, existing) gates :542.
//   - discovery-identity fingerprint (this file) gates :519 + :534, on the
//     FAIL-SAFE UNION (schemaChanged OR discoveryChanged): both wipe shared
//     caches that hold discovery AND OpenAPI, so fire on either — over-invalidate
//     on a rare real schema change, never toward staleness on an idle re-list.
//
// COVERAGE (correctness-critical, arm 3): the fields here are exactly the CRD
// spec fields the discovery surface + RESTMapper are built from. DiscoverGroup-
// ResourcesFresh re-reads the group's apiserver discovery, whose APIResourceList
// is determined by {group, names, scope, per-version served/storage}; the SA
// CachedDiscoveryClient/RESTMapper caches that same group->resource->version
// mapping (incl. the storage version and deprecated). A version served:true->
// false, or a names/scope change, changes discovery+mapper but NOT the schema
// fingerprint — so gating :519/:534 on the schema fp alone would UNDER-invalidate
// (stale discovery/mapper). This fingerprint closes that. A per-version status or
// scale subresource is INCLUDED by presence (#218 TL ruling): the apiserver
// publishes an enabled one as its own discovery resource ("<plural>/status",
// "<plural>/scale") and #282 routes /call to it, so toggling one changes the
// served surface; it never moves on an idle re-list, so inclusion costs zero
// amplification. Schema (openAPIV3Schema), spec.conversion, additionalPrinter-
// Columns and selectableFields are deliberately EXCLUDED — grep-confirmed read by
// NONE of the three gated caches' consumers (discovery memcache, SA RESTMapper,
// schema memo), and not discovery identity; including them would defeat the
// thrash guard. Printer-column churn is proven a no-op in arm D.

package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// crdDiscoveryFingerprint projects the served-GVR IDENTITY of a decoded CRD:
// spec.group, spec.names (whole map — plural/singular/kind/listKind/shortNames/
// categories), spec.scope, and per-version {name, served, storage, deprecated,
// status-subresource presence, scale-subresource presence}. Returns "" when the
// versions subtree is unreadable (caller treats "" as a fail-safe change →
// invalidate, never leave the discovery cache stale).
func crdDiscoveryFingerprint(u *unstructured.Unstructured) string {
	versions, found, err := unstructured.NestedSlice(u.Object, "spec", "versions")
	if err != nil || !found {
		return ""
	}
	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	names, _, _ := unstructured.NestedMap(u.Object, "spec", "names")
	scope, _, _ := unstructured.NestedString(u.Object, "spec", "scope")

	type vfp struct {
		Name       string `json:"name"`
		Served     bool   `json:"served"`
		Storage    bool   `json:"storage"`
		Deprecated bool   `json:"deprecated"`
		// #218 (TL ruling): the apiserver publishes an enabled per-version status
		// or scale subresource as its OWN discovery resource ("<plural>/status",
		// "<plural>/scale"), and #282 now routes /call to it. Toggling one changes
		// the served-GVR surface, so its PRESENCE is discovery identity. The scale
		// subresource's config (specReplicasPath, …) is NOT — "<plural>/scale" is
		// the same discovery resource regardless — so fingerprint presence only.
		StatusSubresource bool `json:"statusSubresource"`
		ScaleSubresource  bool `json:"scaleSubresource"`
	}
	proj := struct {
		Group    string         `json:"group"`
		Names    map[string]any `json:"names"`
		Scope    string         `json:"scope"`
		Versions []vfp          `json:"versions"`
	}{Group: group, Names: names, Scope: scope}

	for _, v := range versions {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(vm, "name")
		served, _, _ := unstructured.NestedBool(vm, "served")
		storage, _, _ := unstructured.NestedBool(vm, "storage")
		deprecated, _, _ := unstructured.NestedBool(vm, "deprecated")
		// Presence only — reading a nil map (no subresources) is safe in Go and
		// yields false for both, which is the correct "no subresource" identity.
		subs, _, _ := unstructured.NestedMap(vm, "subresources")
		_, hasStatus := subs["status"]
		_, hasScale := subs["scale"]
		proj.Versions = append(proj.Versions, vfp{
			Name: name, Served: served, Storage: storage, Deprecated: deprecated,
			StatusSubresource: hasStatus, ScaleSubresource: hasScale,
		})
	}

	b, mErr := json.Marshal(proj) // json.Marshal sorts map keys → canonical
	if mErr != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// schemaFingerprintChanged reports whether the schema fingerprint for name
// differs from the last stored one. LOAD-ONLY — it does NOT store, because
// triggerCRDSchemaRelist owns the schemaFingerprints store (so the gate and the
// relist decide against the SAME prior value). An unreadable fp ("") reads as
// CHANGED (fail-safe: invalidate the schema memo rather than leave it stale).
func (c *crdDiscovery) schemaFingerprintChanged(name, fp string) bool {
	if fp == "" {
		return true
	}
	prev, had := c.schemaFingerprints.Load(name)
	return !had || prev.(string) != fp
}

// discoveryFingerprintChangedAndStore reports whether the discovery-identity
// fingerprint for name changed, and stores the new one (this map has no other
// writer, unlike schemaFingerprints). An unreadable fp ("") reads as CHANGED and
// is NOT stored (leave any prior readable value so a transient unreadable event
// does not poison the next compare).
func (c *crdDiscovery) discoveryFingerprintChangedAndStore(name, fp string) bool {
	if fp == "" {
		return true
	}
	prev, had := c.discoveryFingerprints.Load(name)
	c.discoveryFingerprints.Store(name, fp)
	return !had || prev.(string) != fp
}

// dropCRDFingerprints removes the schema + discovery-identity fingerprints for a
// deleted CRD, so a later recreate is treated as a first observation and re-fires
// the invalidations. Called from triggerCRDDelete (#218) — without it, a
// delete+recreate reusing the identical spec would match the stale fingerprint
// and no-op, leaving the discovery cache / RESTMapper missing the recreated GVR.
func (c *crdDiscovery) dropCRDFingerprints(name string) {
	if name == "" {
		return
	}
	c.schemaFingerprints.Delete(name)
	c.discoveryFingerprints.Delete(name)
}

// ResetCRDDiscoveryFingerprintsForTest clears the discovery-identity fingerprint
// map. TEST-ONLY.
func (c *crdDiscovery) ResetCRDDiscoveryFingerprintsForTest() {
	c.discoveryFingerprints.Range(func(k, _ any) bool {
		c.discoveryFingerprints.Delete(k)
		return true
	})
}
