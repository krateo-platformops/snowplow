// readiness_backstop_metrics.go — F5 (#131, Diego-ruled): backstop-Ready is an
// EXPLICIT FAILURE SIGNAL, never silent.
//
// THE CONTRACT (#130 acceptance / #131 ruling). Under the prewarm-complete
// readiness gate (shape A, #87) /readyz is meant to flip 200 only once every
// cohort's first-nav widgets are seeded (the firstNav latch fires). But the C2
// backstop (§F.0) flips Ready-DEGRADED regardless on PHASE1_TIMEOUT /
// pipGlobalTimeout expiry or a seed error — so a pod that CANNOT warm in budget
// still joins the Service with COLD cells. Per Diego's #130 ruling that is a
// FAILED boot: the pod serves, but its first customer /call per affected cohort
// falls back to a cold per-user resolve (the exact miss #130 set out to close).
// It MUST be loud: an ERROR-level structured log + a process expvar counter, so
// an operator/alert sees "this boot degraded" instead of a silent green /readyz.
//
// SCOPE (tiny, per the ruling). This is instrumentation ONLY — it changes NO
// readiness behaviour (the backstop still fires; MarkPhase1Done is untouched).
// It fires EXACTLY on the backstop path (engineSeed returns via bootErr /
// pctx.Done() without the firstNav latch having fired, OR pipSeed returns an
// error), NEVER on the happy path (firstNav fired → clean Ready). #402: EXACTLY
// ONCE per readiness release — the Step 7.6 seed block records it from
// recordPhase1SeedExit only (phase1BackstopReason), never also from engineSeed.
//
// PRIOR ART: the expvar shape mirrors l1_lookup_metrics.go (sync.Once-guarded
// expvar.Publish of an atomic counter via expvar.Func) — the established
// process-counter idiom in this package.
package dispatchers

import (
	"expvar"
	"log/slog"
	"sync"
	"time"
)

// readinessBackstopFired is the process-wide count of boots whose /readyz flipped
// Ready via the C2 backstop rather than the firstNav-complete happy path — i.e.
// FAILED-but-serving boots (#131). Published as snowplow_readyz_backstop_fired so
// an alert can fire on any non-zero value. A healthy boot leaves it 0.
var readinessBackstopFired expvar.Int

// readinessBackstopMetricsOnce guards the expvar.Publish (panics on duplicate
// key) — same pattern as l1LookupMetricsOnce.
var readinessBackstopMetricsOnce sync.Once

func init() { registerReadinessBackstopMetrics() }

// registerReadinessBackstopMetrics publishes the single expvar key. Idempotent
// (sync.Once), so a test helper can call it after init without a double-publish.
func registerReadinessBackstopMetrics() {
	readinessBackstopMetricsOnce.Do(func() {
		expvar.Publish("snowplow_readyz_backstop_fired", &readinessBackstopFired)
	})
}

// recordReadinessBackstop is the SINGLE F5 alerting site. Called ONLY when
// readiness flipped via the backstop (a FAILED boot per #130). It bumps the
// process counter and emits ONE ERROR-level structured log carrying the reason
// (which backstop arm fired), the elapsed seed time, and the count of nav-widget
// units still unseeded at the flip (-1 when that count is not measurable at the
// call site — the timeout arm cannot read the latch's internal remaining, so it
// reports the "did the firstNav latch fire" boolean via reason instead).
//
// reason values (#402: one per release, the most specific cause):
//   - "phase1_timeout"     — the PHASE1_TIMEOUT parent ctx ended BEFORE the
//     firstNav latch fired: nav widgets not all seeded.
//   - "pip_global_timeout" — the seed's own pipGlobalTimeout budget ended first.
//   - "canceled"           — the seed ctx was canceled (shutdown mid-boot).
//   - "boot_error"         — the seed returned an error with its ctx still live
//     (the boot scope finished with an error before the latch could fire).
//   - "seed_panic"         — the seed panicked; the recover + C2 backstop flipped.
//   - "no_sa_endpoint" / "no_dyn_client" — Phase1Warmup could not start the walk
//     (#401); readiness released Ready-DEGRADED with nothing prewarmed
//     (outcome boot_aborted). Recorded once by releasePhase1BootAborted.
func recordReadinessBackstop(reason string, elapsed time.Duration, unseededRemaining int) {
	readinessBackstopFired.Add(1)
	slog.Error("readyz.backstop.fired",
		slog.String("subsystem", "cache"),
		slog.String("reason", reason),
		slog.Int64("elapsed_ms", elapsed.Milliseconds()),
		slog.Int("unseeded_remaining", unseededRemaining),
		slog.String("effect", "/readyz flipped Ready-DEGRADED via the C2 backstop, NOT the "+
			"firstNav-complete path — this boot FAILED to warm every cohort's first-nav L1 in "+
			"budget (#130 acceptance). The pod serves, but the first /call per affected cohort "+
			"falls back to a cold per-user resolve. Investigate seed budget / cluster health; "+
			"snowplow_readyz_backstop_fired is now non-zero (alert)."),
	)
}
