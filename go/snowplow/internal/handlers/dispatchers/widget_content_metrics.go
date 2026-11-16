// widget_content_metrics.go — Ship 1.3 (lever 1) observability for the
// identity-free widget-content empty-shell guard.
//
// PURPOSE. populateWidgetContentL1 (widget_content.go) refuses to store a
// TRANSIENT-EMPTY POISON SHELL — an apiRef+resourcesRefsTemplate widget
// that resolved with empty status.resourcesRefs.items under the SA-maximal
// walk identity (a boot data-availability artifact, never a genuine zero).
// This counter surfaces how often the guard fired so the falsifier can
// confirm the boot walk stopped poisoning the compositions datagrid cell
// (a non-zero value during boot = the guard caught at least one poison
// shell; zero after warm-up = the cell is populated, not re-guarded).
//
// PRIOR ART — phase1_walk_metrics.go (snowplow_phase1_walk_zero_children_total):
// the SAME atomic.Uint64 + expvar.Func + sync.Once idiom, same CFG-1
// cache-off init gate (project_cache_off_is_transparent_fallback). Read-
// only, lazy at scrape time, zero per-/call cost.

package dispatchers

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// widgetContentSkippedEmptyShellTotal is the process-wide count of
// populateWidgetContentL1 Puts the lever-1 guard declined because the
// resolved widget was a transient-empty poison shell.
var widgetContentSkippedEmptyShellTotal atomic.Uint64

// widgetContentSkippedRBACSensitiveTotal is the process-wide count of
// populateWidgetContentL1 Puts the RBAC-sensitivity guard (task #69)
// declined because the widget is an apiRef-driven render-template widget
// that renders from status.widgetData (not narrowed by the serve-gate) and
// is therefore routed to the per-cohort `widgets` L1 instead of the
// shared, identity-free content cell. A non-zero value confirms the
// classified piechart/table/datagrid widgets are NOT poisoning the shared
// cell — they self-route to the RBAC-correct per-cohort path.
var widgetContentSkippedRBACSensitiveTotal atomic.Uint64

// bumpWidgetContentSkippedEmptyShell increments the guard counter. Called
// by populateWidgetContentL1 when shouldSkipEmptyWidgetShell fires.
func bumpWidgetContentSkippedEmptyShell() {
	widgetContentSkippedEmptyShellTotal.Add(1)
}

// bumpWidgetContentSkippedRBACSensitive increments the RBAC-sensitivity
// guard counter. Called by populateWidgetContentL1 when
// isRBACSensitiveApiRefWidget fires.
func bumpWidgetContentSkippedRBACSensitive() {
	widgetContentSkippedRBACSensitiveTotal.Add(1)
}

// widgetSkippedUndeclaredExtrasPutTotal is the process-wide count of per-cohort
// `widgets` Puts the F6 self-quarantine guard declined (arch-ruled 2026-07-13).
// A request carrying request-extras the widget did NOT declare in spec.keyExtras
// would, post-F6, key onto the SAME shared cohort cell as a clean request (the
// undeclared extras drop from the key) — so its extras-influenced body must NOT
// write the cohort cell (it would be served to a co-cohort user who did not send
// those extras). The request still gets its own correct per-request 200; it just
// declines the Put. A non-zero value = at least one polluting request was
// quarantined; the declared corpus + chrome widgets never trip it.
var widgetSkippedUndeclaredExtrasPutTotal atomic.Uint64

// bumpWidgetSkippedUndeclaredExtrasPut increments the F6 quarantine counter.
// Called by the widgets dispatcher when requestExtrasFullyDeclared is false at
// the genuine per-cohort Put gate.
func bumpWidgetSkippedUndeclaredExtrasPut() {
	widgetSkippedUndeclaredExtrasPutTotal.Add(1)
}

// widgetUndeclaredExtrasExemptTotal counts requests that carried undeclared,
// non-identity extras and were NOT quarantined because the widget has no
// extras-reading spec surface (#777 b′, widgets.WidgetReadsNoRequestExtras).
// Its share next to widgetSkippedUndeclaredExtrasPutTotal is the fraction of
// would-be F6 declines the exemption turned into Puts.
var widgetUndeclaredExtrasExemptTotal atomic.Uint64

func bumpWidgetUndeclaredExtrasExempt() {
	widgetUndeclaredExtrasExemptTotal.Add(1)
}

// WidgetUndeclaredExtrasExemptTotal is the typed accessor the expvar closure
// and the OTLP instrument both read.
func WidgetUndeclaredExtrasExemptTotal() uint64 { return widgetUndeclaredExtrasExemptTotal.Load() }

// AddWidgetUndeclaredExtrasExemptForTest moves the exemption counter so the
// OTLP wiring can be asserted from the metrics package. Production MUST NOT use it.
func AddWidgetUndeclaredExtrasExemptForTest(n uint64) { widgetUndeclaredExtrasExemptTotal.Add(n) }

// widgetContentMetricsOnce guards expvar.Publish against the double-
// publish panic (mirrors phase1WalkMetricsOnce).
var widgetContentMetricsOnce sync.Once

func init() {
	// CFG-1 mirror: under CACHE_ENABLED=false the content layer does not
	// run and this gauge MUST NOT be registered.
	if cache.Disabled() {
		return
	}
	registerWidgetContentMetrics()
}

// registerWidgetContentMetrics publishes the lever-1 guard counter.
// Guarded by widgetContentMetricsOnce so it is safe from init() and a
// test helper.
func registerWidgetContentMetrics() {
	widgetContentMetricsOnce.Do(func() {
		expvar.Publish("snowplow_widget_content_skipped_empty_shell_total", expvar.Func(func() any {
			return widgetContentSkippedEmptyShellTotal.Load()
		}))
		expvar.Publish("snowplow_widget_content_skipped_rbac_sensitive_total", expvar.Func(func() any {
			return widgetContentSkippedRBACSensitiveTotal.Load()
		}))
		expvar.Publish("snowplow_widget_skipped_undeclared_extras_put_total", expvar.Func(func() any {
			return widgetSkippedUndeclaredExtrasPutTotal.Load()
		}))
		expvar.Publish("snowplow_widget_undeclared_extras_exempt_total", expvar.Func(func() any {
			return WidgetUndeclaredExtrasExemptTotal()
		}))
	})
}

// RegisterWidgetContentMetricsForTest forces registration under tests that
// flip CACHE_ENABLED=true via t.Setenv after init() already ran with the
// var unset. Idempotent. Production callers MUST NOT use this.
func RegisterWidgetContentMetricsForTest() {
	registerWidgetContentMetrics()
}
