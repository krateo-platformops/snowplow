package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

// readyzInfo is the JSON body returned by /readyz.
type readyzInfo struct {
	// Status is "ready" on 200, "warming" on 503.
	Status string `json:"status"`
	// Phase1Done mirrors cache.IsPhase1Done() — the Tag B startup
	// informer-warmup signal.
	Phase1Done bool `json:"phase1Done"`
	// Reason (#397, warming only) is the phase-1 stage the pod is in, from a
	// closed code-defined set (e.g. "informer_sync", "boot_seed_in_progress").
	Reason string `json:"reason,omitempty"`
	// ElapsedS (#397, warming only) is the whole seconds since process start.
	ElapsedS *int64 `json:"elapsed_s,omitempty"`
	// Outcome (#397, ready only) is how readiness was released: "latch",
	// "deadline", "boot_error", "boot_aborted", "seed_panic", "seed_returned" or
	// "none-configured" — the prewarm.phase1.readiness_exit outcome. Omitted
	// when no exit was recorded.
	Outcome string `json:"outcome,omitempty"`
}

// ReadyCheck is the 0.30.102 Tag B probe-gated readiness endpoint.
//
// It returns 200 iff cache.IsPhase1Done() is true — the Phase 1
// SA-credentialed resolution walk has finished AND every registered
// informer (including the CRD-watch-spawned composition informers
// present at boot) has reached HasSynced. Until then it returns 503 so
// the Kubernetes readinessProbe / startupProbe withhold traffic from a
// pod whose navigated informers are still cold.
//
// Behavior-neutral under cache-off: when the cache subsystem is OFF
// (CACHE_ENABLED unset/false), prewarm is implicit-off (#57 — prewarm is
// implicit-on-cache), so the startup sequence calls cache.MarkPhase1Done()
// immediately — there is nothing to warm — so /readyz returns 200 from
// the first probe and the gate is a no-op. The informer pivot is likewise
// implicit-on-cache (#57); the cold-vs-warm bench toggles them all via the
// single CACHE_ENABLED gate.
//
// /readyz is distinct from /health: /health is liveness-only (process
// alive — a still-warming pod is alive and must NOT be restarted).
// /readyz is readiness-only (safe to receive traffic). The chart wires
// the livenessProbe to /health and the readinessProbe + startupProbe to
// /readyz.
//
// @Summary Readiness Endpoint
// @Description Returns 200 once the Tag B startup informer-warmup (Phase 1) has completed; 503 while warming.
// @ID readyz
// @Produce  json
// @Success 200 {object} readyzInfo
// @Failure 503 {object} readyzInfo
// @Router /readyz [get]
func ReadyCheck() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		done := cache.IsPhase1Done()

		body := readyzInfo{Phase1Done: done}
		code := http.StatusServiceUnavailable
		body.Status = "warming"
		if done {
			code = http.StatusOK
			body.Status = "ready"
			body.Outcome = dispatchers.Phase1ReadinessExitOutcome()
		} else {
			body.Reason = dispatchers.Phase1WarmingReason()
			elapsed := int64(dispatchers.Phase1SinceProcessStart().Seconds())
			body.ElapsedS = &elapsed
		}

		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(code)
		_ = json.NewEncoder(wri).Encode(body)
	}
}
