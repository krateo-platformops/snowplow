// rbac_publish_health.go — #691 (security hardening): make a STUCK RBAC
// snapshot rebuild visible.
//
// THE GAP. drainRBACRebuild recovers a panicking rebuild, re-marks dirty and
// logs it; since #687 the loop retries on a bounded backoff. A rebuild that
// panics DETERMINISTICALLY never republishes: every later revoke stays
// unpublished and in-process authorization keeps the revoked grant. The only
// signal was a log line, and snowplow_rbac_publish_seq simply stops moving —
// which is exactly what a QUIET cluster looks like.
//
// THE SIGNALS.
//   - snowplow_rbac_rebuild_panics_total: one per recovered rebuild panic.
//   - snowplow_rbac_unpublished_event_age_seconds: age of the OLDEST RBAC
//     event not yet reflected in a completed rebuild; 0 when everything seen
//     has been rebuilt. Stuck = > 0 and growing; quiet = 0.
//   - snowplow_rbac_last_successful_publish_age_seconds: time since the last
//     publish, for context only (it grows on a quiet cluster too).
//
// HOW THE OLDEST UNPUBLISHED EVENT IS TRACKED. scheduleRBACRebuild stamps
// rbacPendingSince (CAS 0 → now, so the OLDEST stamp wins) BEFORE it marks
// dirty. Each rebuild iteration, right after it clears dirty, MOVES that stamp
// into rbacInFlightSince (so an event arriving mid-rebuild starts a fresh
// pending stamp). A rebuild that completes clears the in-flight stamp; one that
// panics puts it back into pending (keeping the older of the two). The gauge is
// the age of the older of pending and in-flight.
package cache

import (
	"sync/atomic"
	"time"
)

var (
	rbacPendingSince        atomic.Int64 // unix nanos of the oldest event not yet taken by a rebuild; 0 = none
	rbacInFlightSince       atomic.Int64 // unix nanos of the stamp the running rebuild took; 0 = none
	rbacLastPublishUnixNano atomic.Int64 // unix nanos of the last successful publish; 0 = never
	rbacRebuildPanics       atomic.Uint64
)

// noteRBACEventPending stamps the oldest unpublished event (called by
// scheduleRBACRebuild before it marks dirty).
func noteRBACEventPending() {
	rbacPendingSince.CompareAndSwap(0, time.Now().UnixNano())
}

// takePendingForRebuild moves the pending stamp into the in-flight slot (called
// by the rebuild loop right after it clears dirty). Only the slot holder calls
// it, so the in-flight slot has a single writer.
func takePendingForRebuild() {
	v := rbacPendingSince.Load()
	rbacInFlightSince.Store(v)
	if v != 0 {
		rbacPendingSince.CompareAndSwap(v, 0)
	}
}

// rebuildCompleted clears the in-flight stamp: every event it covered is in the
// snapshot this rebuild produced.
func rebuildCompleted() { rbacInFlightSince.Store(0) }

// rebuildPanicked puts the in-flight stamp back into pending, keeping the older
// of the two, and counts the panic.
func rebuildPanicked() {
	rbacRebuildPanics.Add(1)
	v := rbacInFlightSince.Swap(0)
	if v == 0 {
		return
	}
	for {
		cur := rbacPendingSince.Load()
		if cur != 0 && cur <= v {
			return
		}
		if rbacPendingSince.CompareAndSwap(cur, v) {
			return
		}
	}
}

// notePublished records a successful publish (called where PublishSeq moves).
func notePublished() { rbacLastPublishUnixNano.Store(time.Now().UnixNano()) }

// RBACRebuildPanicsTotal returns the recovered rebuild panic count.
func RBACRebuildPanicsTotal() uint64 { return rbacRebuildPanics.Load() }

// RBACUnpublishedEventAge returns the age of the oldest RBAC event not yet
// reflected in a completed rebuild, or 0 when there is none.
func RBACUnpublishedEventAge() time.Duration {
	oldest := rbacPendingSince.Load()
	if f := rbacInFlightSince.Load(); f != 0 && (oldest == 0 || f < oldest) {
		oldest = f
	}
	if oldest == 0 {
		return 0
	}
	return time.Since(time.Unix(0, oldest))
}

// RBACLastPublishAge returns the time since the last successful publish, or 0
// when nothing has been published yet.
func RBACLastPublishAge() time.Duration {
	v := rbacLastPublishUnixNano.Load()
	if v == 0 {
		return 0
	}
	return time.Since(time.Unix(0, v))
}

// ResetRBACPublishHealthForTest zeroes the #691 state. Tests only.
func ResetRBACPublishHealthForTest() {
	rbacPendingSince.Store(0)
	rbacInFlightSince.Store(0)
	rbacRebuildPanics.Store(0)
}
