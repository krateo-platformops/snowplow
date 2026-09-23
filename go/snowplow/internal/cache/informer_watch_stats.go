// informer_watch_stats.go — 1.12.7 observability. Two counters for failures
// the informer layer could not previously be asked about, derived onto all
// four surfaces (expvar, OTLP, the docs guard and the C7 parity arms) from
// struct tags.
//
// WHY A NEW FAMILY. The watcher/servable layer had no stats struct at all: it
// publishes `snowplow_informer_servable` from ServableCounts' five named
// return ints, and its per-GVR detail only reaches /debug/servable. Neither
// carries a counter, so both failures below were invisible to anything but a
// log tail. Registering one tagged family here is what makes them reachable
// from a dashboard and keeps them from drifting: a stat added to the struct is
// guarded on every surface from then on (see TaggedStatFamilies).
//
// WHAT EACH ONE ANSWERS, AND WHY A LOG WAS NOT ENOUGH.
//
//   - watch_errors_total. Our reflector error handlers REPLACE client-go's
//     default, and every one of them logs only the FIRST failure — per GVR for
//     the informer handler, and once per process for the secrets and
//     controller-health ones, which are sticky until restart. That is correct
//     for the log (a broken watch would otherwise flood it) but it means the
//     retry RATE is not observable anywhere: a watch failing once and a watch
//     failing every second for an hour produce the same single WARN. This
//     counts every invocation, so the rate is visible while the one-shot WARN
//     is untouched.
//
//   - confirm_retracted_total. A GVR that was CONFIRMED servable and is later
//     un-confirmed silently stops serving from the informer and falls through
//     to the apiserver. #217 took a day to characterise for exactly this
//     reason: the retraction left no trace, so the state had to be inferred
//     from its downstream effects. The grand total lives here; the per-reason
//     breakdown is a separate expvar map, because the C7 tag system has NO
//     label facility — a tag produces one scalar. See
//     ConfirmRetractedByReasonSnapshot.
package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// Process-scoped counters. Package-level rather than per-ResourceWatcher
// because the secrets and controller-health reflectors are singletons with no
// watcher, and an operator asking "are our watches erroring" wants one number
// for the process, not one per informer family.
var (
	watchErrorsTotal      atomic.Uint64
	confirmRetractedTotal atomic.Uint64

	// confirmRetractedByReason is the {reason} breakdown. A plain map under a
	// mutex, not a sync.Map: the label set is CLOSED and tiny (six values, all
	// compile-time constants below), and retraction is a rare control-plane
	// event, so there is no contention to design around.
	confirmRetractedMu       sync.Mutex
	confirmRetractedByReason = map[string]uint64{}
)

// Retraction reasons. Closed set, enumerated from the two sites that can
// retract a confirmation — every value here is emitted by exactly one caller,
// so a non-zero bucket names the code path rather than a category.
const (
	// The type is no longer served by the apiserver (conjunct 4 re-evaluated
	// with discovery available). Three callers reach it.
	confirmRetractDiscoveryRefresh = "discovery_refresh" // the periodic full pass
	confirmRetractScopedConfirm    = "scoped_confirm"    // one GVR, LIST-register / apistage prime
	confirmRetractWalkConfirm      = "walk_confirm"      // the walk-registered set

	// The informer itself was torn down, taking its confirmation with it.
	confirmRetractSchemaRelist       = "schema_relist"        // CRD schema widened; remove + re-ensure
	confirmRetractStaleVersionPruned = "stale_version_pruned" // #219: CRD stopped serving this version
	confirmRetractCRDDeleted         = "crd_deleted"          // the CRD itself was deleted
	confirmRetractUnspecified        = "unspecified"          // a bare RemoveResourceType (tests)
	// #237 B: the store was found to disagree with the apiserver and the GVR
	// was relisted to repair it. It shares the relist machinery with
	// schema_relist and MUST NOT share its bucket — a store repair folded into
	// schema_relist would read as a CRD schema widen, which is the
	// wrong-cause misattribution #237 exists to stop.
	confirmRetractStoreRepair = "store_repair"
)

// InformerWatchStats is the tagged family. Counters only — the servable GAUGES
// stay on snowplow_informer_servable, which is a different question (how many
// GVRs are servable right now) from these (how often did it break).
type InformerWatchStats struct {
	WatchErrorsTotal      uint64 `stat:"watch_errors_total" desc:"Reflector ListAndWatch errors across every informer family. Counts EVERY invocation; the WARN is one-shot."`
	ConfirmRetractedTotal uint64 `stat:"confirm_retracted_total" desc:"GVRs whose servability confirmation was retracted after having been granted. See the by-reason map for which path."`
}

// informerWatchStatsOverride is the test seam every C7 family carries, so the
// parity arms can drive distinct values through without touching production
// counters.
var informerWatchStatsOverride atomic.Pointer[InformerWatchStats]

// SetInformerWatchStatsForTest installs (or clears, with nil) a fixed snapshot.
func SetInformerWatchStatsForTest(s *InformerWatchStats) {
	informerWatchStatsOverride.Store(s)
}

// InformerWatchStatsSnapshot reads both counters coherently.
func InformerWatchStatsSnapshot() InformerWatchStats {
	if o := informerWatchStatsOverride.Load(); o != nil {
		return *o
	}
	return InformerWatchStats{
		WatchErrorsTotal:      watchErrorsTotal.Load(),
		ConfirmRetractedTotal: confirmRetractedTotal.Load(),
	}
}

// InformerWatchStatsByStat is the family's Values function.
func InformerWatchStatsByStat() map[string]any {
	return statsByTag(InformerWatchStatsSnapshot())
}

// recordWatchError counts one reflector error. Called from EVERY watch error
// handler, unconditionally, before whatever one-shot guard that handler uses
// to decide about logging. Takes no lock and is safe from a reflector
// goroutine holding rw.mu.
func recordWatchError() { watchErrorsTotal.Add(1) }

// recordConfirmRetracted counts one retraction of a granted confirmation.
//
// CALL IT ONLY WHEN THE GVR WAS ACTUALLY CONFIRMED. Both retraction sites
// delete unconditionally from the confirmed map, so calling this without a
// membership check would count no-ops — a GVR that was never confirmed being
// "un-confirmed" is not an event, and counting it would make the number climb
// on every ordinary teardown. Count the effect, never the invocation.
func recordConfirmRetracted(reason string) {
	confirmRetractedTotal.Add(1)
	confirmRetractedMu.Lock()
	confirmRetractedByReason[reason]++
	confirmRetractedMu.Unlock()
}

// ConfirmRetractedByReasonSnapshot returns a copy of the {reason} breakdown.
func ConfirmRetractedByReasonSnapshot() map[string]uint64 {
	confirmRetractedMu.Lock()
	defer confirmRetractedMu.Unlock()
	out := make(map[string]uint64, len(confirmRetractedByReason))
	for k, v := range confirmRetractedByReason {
		out[k] = v
	}
	return out
}

// ResetInformerWatchStatsForTest zeroes both counters and the breakdown.
func ResetInformerWatchStatsForTest() {
	watchErrorsTotal.Store(0)
	confirmRetractedTotal.Store(0)
	confirmRetractedMu.Lock()
	confirmRetractedByReason = map[string]uint64{}
	confirmRetractedMu.Unlock()
	informerWatchStatsOverride.Store(nil)
}

var informerWatchExpvarOnce sync.Once

// registerInformerWatchExpvar publishes the family plus the by-reason map.
// Guarded by sync.Once so init() and the test helper can both call it.
func registerInformerWatchExpvar() {
	informerWatchExpvarOnce.Do(func() {
		expvar.Publish("snowplow_informer_watch", expvar.Func(func() any {
			return InformerWatchStatsByStat()
		}))
		// The {reason} breakdown rides alongside rather than inside the tagged
		// family: a stat tag yields one scalar and the tag system has no label
		// facility, so a labelled counter has to be its own map. Same shape as
		// snowplow_phase1_harvest_forgotten_total.
		expvar.Publish("snowplow_informer_confirm_retracted_by_reason", expvar.Func(func() any {
			return ConfirmRetractedByReasonSnapshot()
		}))
	})
}

// RegisterInformerWatchExpvarForTest exposes the publish for tests.
func RegisterInformerWatchExpvarForTest() { registerInformerWatchExpvar() }

func init() {
	if Disabled() {
		return
	}
	registerInformerWatchExpvar()
}
