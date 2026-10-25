// incomplete_metrics.go — AUDIT-4 (krateo-platformops/installer#410): the
// count of audit records the emitter refused because they could not name their
// actor, action, method or target (missingField, audit.go).
//
// snowplow_audit_incomplete_total{actor,action,method,target} at /debug/vars.
// Every cell is published at startup, so a 0 is a reading. It is NOT a cache
// surface: audit runs in either cache mode, so the publisher is ungated on
// cache.Disabled() and declared as a non-cache publisher in the CFG-1 structural
// matrix (e2e/bench/cfg1_probe, nonCacheInitPublishers).
package audit

import "expvar"

// Incomplete-record reasons: the closed set of fields whose absence makes a
// record unanswerable, in the order missingField checks them.
const (
	IncompleteActor  = "actor"
	IncompleteAction = "action"
	IncompleteMethod = "method"
	IncompleteTarget = "target"
)

var incomplete = new(expvar.Map)

func init() {
	for _, k := range []string{IncompleteActor, IncompleteAction, IncompleteMethod, IncompleteTarget} {
		incomplete.Add(k, 0)
	}
	expvar.Publish("snowplow_audit_incomplete_total", incomplete)
}

// IncompleteCount returns how many records were refused for reason.
func IncompleteCount(reason string) int64 {
	if v, ok := incomplete.Get(reason).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}
