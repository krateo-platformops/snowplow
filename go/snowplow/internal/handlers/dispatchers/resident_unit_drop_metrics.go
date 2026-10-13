package dispatchers

import (
	"expvar"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// #647 — RESTAction units the resident enumeration (enumerateResident: the
// RBAC-shift reseed, the learned-class seed, the #258 rotated-target filter)
// DROPPED because no target GVR could be derived for them. A dropped unit is
// reseeded for nobody, so its cells for the rotated / newly learned identities
// stay cold until a customer read refills them.
//
// Reasons (closed set, raTargetMiss values):
//   - get_failed       — objects.Get refused or failed. This is the #647
//     signature: it read one per RESTAction per run (98 on 057) while the
//     lookup ran with no identity. Healthy: 0.
//   - decode_failed    — the object is not a RESTAction shape. Healthy: 0.
//   - no_static_target — the RESTAction declares no static userAccessFilter
//     {group, resource} (ResourcesFrom, or no filter). Expected and stable per
//     cluster: the boot seed seeds such an RA for no identity either.
//
// /debug/vars: snowplow_prewarm_resident_units_dropped_total (map reason → n);
// OTLP: snowplow_prewarm_resident_units_dropped_total{reason}, hand-wired in
// internal/metrics/metrics.go.

const (
	raTargetFound              = ""
	residentDropGetFailed      = "get_failed"
	residentDropDecodeFailed   = "decode_failed"
	residentDropNoStaticTarget = "no_static_target"
)

// residentDropReasons is the closed set, in emission order.
var residentDropReasons = []string{residentDropGetFailed, residentDropDecodeFailed, residentDropNoStaticTarget}

var residentUnitsDropped = map[string]*atomic.Uint64{
	residentDropGetFailed:      {},
	residentDropDecodeFailed:   {},
	residentDropNoStaticTarget: {},
}

// recordResidentUnitDropped counts one dropped RA unit. A reason outside the
// closed set is folded into get_failed rather than lost.
func recordResidentUnitDropped(reason string) {
	c, ok := residentUnitsDropped[reason]
	if !ok {
		c = residentUnitsDropped[residentDropGetFailed]
	}
	c.Add(1)
}

// residentUnitsDroppedSnapshot returns every reason of the closed set (zeros
// included).
func residentUnitsDroppedSnapshot() map[string]uint64 {
	out := make(map[string]uint64, len(residentDropReasons))
	for _, r := range residentDropReasons {
		out[r] = residentUnitsDropped[r].Load()
	}
	return out
}

// ResidentUnitDropCell is one {reason, count} row for the OTLP mirror.
type ResidentUnitDropCell struct {
	Reason string
	Count  uint64
}

// ResidentUnitsDroppedCells returns the closed set in emission order (zeros
// included) for internal/metrics.
func ResidentUnitsDroppedCells() []ResidentUnitDropCell {
	out := make([]ResidentUnitDropCell, 0, len(residentDropReasons))
	for _, r := range residentDropReasons {
		out = append(out, ResidentUnitDropCell{Reason: r, Count: residentUnitsDropped[r].Load()})
	}
	return out
}

// RecordResidentUnitDroppedForTest drives the production record path from
// another package's test (internal/metrics OTLP arm).
func RecordResidentUnitDroppedForTest(reason string) { recordResidentUnitDropped(reason) }

// CFG-1: the key is published only when the cache subsystem is on (no
// prewarm engine runs otherwise).
func init() {
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_prewarm_resident_units_dropped_total", expvar.Func(func() any {
		return residentUnitsDroppedSnapshot()
	}))
}
