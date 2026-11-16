// incomplete_metrics.go — AUDIT-4 (krateo-platformops/installer#410, #1291):
// the count of incomplete audit records, by marker.
//
// The shared writer never drops a record: one that lacks a key snowplow's
// writer class must write is emitted marked krateo.audit.incomplete=<markers>
// (audit.go), and each marker is counted under the shared writer's own
// instrument, krateo.audit.records.incomplete{marker}, on the global
// MeterProvider: that is the OTLP side, exported whenever metrics are on.
//
// /debug/vars keeps its mirror under the name it has had since AUDIT-4,
// snowplow_audit_incomplete_total{<marker>}, now one cell per marker of the
// schema (audit.Markers()), each published at startup so a 0 is a reading.
// Until #1291 "actor" counted records REFUSED; nothing is refused now.
// It is read off the record itself: incompleteMirror sits between the shared
// writer and the LoggerProvider and adds one per marker the record names, so
// the expvar cell and the OTLP point count the same records by the same
// markers, with or without an OTLP metrics pipeline. It is NOT a cache
// surface: audit runs in either cache mode, so the publisher is ungated on
// cache.Disabled() and declared as a non-cache publisher in the CFG-1
// structural matrix (e2e/bench/cfg1_probe, nonCacheInitPublishers).
package audit

import (
	"context"
	"expvar"
	"strings"

	krateoaudit "github.com/krateo-platformops/audit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

// The markers of krateo.audit.incomplete snowplow's records can carry: the
// schema's (audit.Markers()), named for the emit sites and tests.
const (
	IncompleteActor      = "actor"
	IncompleteActorClass = "actor_class"
	IncompleteAction     = "action"
	IncompleteVerb       = "verb"
	IncompleteMethod     = "method"
	IncompletePreview    = "preview"
	IncompleteTarget     = "target"
)

// IncompleteVar is the /debug/vars name of the mirror. It keeps the name the
// count has had since AUDIT-4 (snowplow's expvar names are snowplow_*, which is
// how the CFG-1 structural matrix finds them); its cells are the shared
// writer's markers, and each counts what krateo.audit.records.incomplete
// {marker} counts on OTLP (TestAudit1291_M).
const IncompleteVar = "snowplow_audit_incomplete_total"

var incomplete = new(expvar.Map)

func init() {
	for _, m := range krateoaudit.Markers() {
		incomplete.Add(m, 0)
	}
	expvar.Publish("snowplow_audit_incomplete_total", incomplete)
}

// IncompleteCount returns how many records were emitted marked with marker.
func IncompleteCount(marker string) int64 {
	if v, ok := incomplete.Get(marker).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}

// incompleteMirror is the logger the shared writer emits to: it counts each
// marker of the record's krateo.audit.incomplete, then hands the record on
// unchanged.
type incompleteMirror struct {
	log.Logger
}

func (l incompleteMirror) Emit(ctx context.Context, r log.Record) {
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) != krateoaudit.KeyIncomplete {
			return true
		}
		for _, m := range strings.Split(kv.Value.AsString(), ",") {
			if m != "" {
				incomplete.Add(m, 1)
			}
		}
		return false
	})
	l.Logger.Emit(ctx, r)
}
