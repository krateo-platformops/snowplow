// rbac_subgen_expvar.go — #247 key-rotation vantage, /debug/vars half.
//
// WHY THIS FILE EXISTS AT ALL (read before assuming internal/metrics covers it):
// internal/metrics is the OTLP MIRROR. It declares OpenTelemetry observable
// instruments and exports them to the collector, and its whole pipeline is gated
// by OTEL_METRICS_ENABLED (default false). It publishes NOTHING to expvar —
// metrics.go's own header says the /debug/vars surface is UNTOUCHED. So
// `snowplow_rbac_publish_seq` reaches /debug/vars not from metrics.go:181 but
// from rbac_snapshot_expvar.go, an independent expvar.Publish. The two counters
// here follow that same two-surface shape: this file owns /debug/vars, and
// internal/metrics/metrics.go owns the OTLP mirror.
//
// WHAT THEY MEASURE (#247): the L1 cache key folds RBACSubGenForSubject
// (rbac_subgen.go), which sums the requesting identity's PER-SUBJECT counters.
// The only published RBAC counter before this file, snowplow_rbac_publish_seq,
// is GLOBAL — one bump per snapshot publish. It sits one link too early on the
// chain rbac_subgen.go documents:
//
//	snapshot publish → per-subject bump → key MINTED → resident excess
//
// Each link is necessary for the next and sufficient for none, so a publish
// count cannot say whether any key rotated, let alone whose. Using it as the
// rotation proxy is what the original 40.7%-residue analysis did.
//
// snowplow_rbac_subgen_bumps_total closes the second link: it counts every
// per-subject bump regardless of whose subject it was. Neither key closes the
// third — nothing here reports whether a rotated key was ever minted.
// snowplow_rbac_subgen_subjects_tracked gives the blast-radius denominator —
// 12,000 bumps over 3 subjects is a hot loop on one tenant; 12,000 bumps over
// 12,000 subjects is a fleet-wide rotation, and only the second one invalidates
// the per-subject design's bound.
//
// ZERO-READABILITY (load-bearing): both keys are published UNCONDITIONALLY from
// main.go's HTTP bootstrap, cache mode-agnostic, exactly like
// RegisterRBACSnapshotExpvar. expvar.Func is evaluated at scrape time, so the
// KEY is present in /debug/vars from the first scrape onward and a `0` reads as
// "no rotation occurred", never as "not instrumented". That distinction is the
// entire point of the counters — a key that can be absent would reintroduce the
// vantage hole they exist to close.
//
// Mirrors the sister-shape idiom at rbac_snapshot_expvar.go:44-58
// (expvar.Publish + expvar.Func + sync.Once).

package cache

import (
	"expvar"
	"sync"
)

// rbacSubGenExpvarOnce guards RegisterRBACSubGenExpvar so the registration body
// runs at most once per process. expvar.Publish panics on a duplicate key;
// sync.Once prevents that under repeated invocation from tests or an accidental
// dual call site.
var rbacSubGenExpvarOnce sync.Once

// RegisterRBACSubGenExpvar publishes the snowplow_rbac_subgen_* expvar keys.
// Idempotent under repeated invocation. Called from main.go's HTTP mux
// bootstrap next to RegisterRBACSnapshotExpvar; safe to call from tests via the
// public name.
//
// Both values are a single atomic.Uint64.Load behind an expvar.Func, so there
// is no per-scrape cost beyond the int boxing expvar performs at JSON-encode
// time, and no cost at all when nothing scrapes.
func RegisterRBACSubGenExpvar() {
	rbacSubGenExpvarOnce.Do(func() {
		expvar.Publish("snowplow_rbac_subgen_bumps_total", expvar.Func(func() any {
			return RBACSubGenBumpsTotal()
		}))
		expvar.Publish("snowplow_rbac_subgen_subjects_tracked", expvar.Func(func() any {
			return RBACSubGenSubjectsTracked()
		}))
	})
}
