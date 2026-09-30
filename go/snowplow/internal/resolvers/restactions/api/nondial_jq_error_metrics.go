// nondial_jq_error_metrics.go — #341 observability for the two NON-dial
// evalJQ-swallow sites cleaned up as the #302 sibling: the cluster_list
// GVR-derivation probe (deriveTargetGVRForClusterList) and the templated
// endpointRef.name render (evalEndpointRef). Neither DIALS the rendered value —
// a jq error there fails SAFE (no collapse / a name-miss 404) — so pre-#341 the
// error was SILENTLY swallowed by evalJQ (masqueraded as a non-parsing path / a
// garbage name). #341 routes both through evalJQE and SURFACES the error:
// behaviour is byte-identical (still no collapse / still a miss), but a
// mis-authored RA is now observable via a bounded DEBUG line at the site AND
// this per-site counter, instead of invisible.
//
// This is CLEANLINESS/OBSERVABILITY, not a garbage-dial fix — the dial-class
// (payload/header) was #302.
//
// NOTE (#311-class): /debug/vars-only — there is no expvar→OTLP bridge, so these
// reasons are visible at /debug/vars, not ClickStack. OTLP wiring is out of
// #341 scope, tracked with the rest of the family.

package api

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// #341 per-site reason keys — the stable metric label values (also the DEBUG
// site tag). Two non-dial evalJQ sites, each its own owner:
//   - cluster_list_gvr_probe (deriveTargetGVRForClusterList) — a jq error in the
//     collapse-eligibility probe → no collapse (per-element fallback);
//   - endpoint_ref_name (evalEndpointRef) — a jq error in a templated
//     endpointRef.name → a downstream Secret miss (honest-error posture).
const (
	nondialClusterListGVRProbe = "cluster_list_gvr_probe"
	nondialEndpointRefName     = "endpoint_ref_name"
)

// nondialJQError counts jq eval errors surfaced at the two NON-dial sites, per
// site. Package-level atomics — safe to Add/Load without external locking.
var nondialJQError struct {
	clusterListGVRProbe atomic.Uint64
	endpointRefName     atomic.Uint64
}

// bumpNondialJQError increments the per-site counter. An unknown reason is a
// no-op (bounded, compile-time key set — no per-input cardinality).
func bumpNondialJQError(reason string) {
	switch reason {
	case nondialClusterListGVRProbe:
		nondialJQError.clusterListGVRProbe.Add(1)
	case nondialEndpointRefName:
		nondialJQError.endpointRefName.Add(1)
	}
}

// NondialJQErrorByReason returns the process-wide count for one site. Exported
// so the #341 falsifier can assert the error is COUNTED (surfaced), not just
// logged (feedback_measurement_use_expvar_not_log_tails).
func NondialJQErrorByReason(reason string) uint64 {
	switch reason {
	case nondialClusterListGVRProbe:
		return nondialJQError.clusterListGVRProbe.Load()
	case nondialEndpointRefName:
		return nondialJQError.endpointRefName.Load()
	}
	return 0
}

// NondialJQErrorTotal returns the process-wide total across both sites.
func NondialJQErrorTotal() uint64 {
	return nondialJQError.clusterListGVRProbe.Load() + nondialJQError.endpointRefName.Load()
}

var nondialJQErrorExpvarOnce sync.Once

func init() { registerNondialJQErrorExpvar() }

// registerNondialJQErrorExpvar publishes snowplow_nondial_jq_error_total to
// /debug/vars as a PER-SITE object {cluster_list_gvr_probe, endpoint_ref_name}.
// NOT gated on cache mode — both sites run on every RA resolve (cache on or
// off), so the counter must always be observable; it is therefore listed in the
// CFG-1 nonCacheInitPublishers exception (cfg1_structural_test.go). Idempotent
// (sync.Once).
func registerNondialJQErrorExpvar() {
	nondialJQErrorExpvarOnce.Do(func() {
		expvar.Publish("snowplow_nondial_jq_error_total", expvar.Func(func() any {
			return map[string]uint64{
				nondialClusterListGVRProbe: nondialJQError.clusterListGVRProbe.Load(),
				nondialEndpointRefName:     nondialJQError.endpointRefName.Load(),
			}
		}))
	})
}
