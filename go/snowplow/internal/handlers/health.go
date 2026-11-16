package handlers

import (
	"net/http"
)

// aliveBody is the static, pre-encoded body returned by HealthCheck.
// Defined once at package init so each probe is a single Write of a
// shared byte slice — zero allocation in the hot path.
var aliveBody = []byte(`{"status":"alive"}`)

// HealthCheck is the Ship 0.5.5 liveness-only probe handler.
//
// It returns 200 + a static 17-byte JSON body unconditionally as long
// as the HTTP server goroutine is scheduled enough to call Write. There
// is NO call to rest.InClusterConfig (which re-reads the service-account
// token + ca.crt from disk per call), NO namespace lookup, and NO
// allocation per probe. This is the process-alive signal; the
// safe-to-receive-traffic signal lives at /readyz which gates on
// cache.IsPhase1Done() and the RBAC snapshot (#1231). Liveness reads neither, so
// a slow RBAC sync never gets the pod restarted.
//
// The serviceName / build / nsgetter parameters are kept in the
// signature for API stability with prior callers — they are intentionally
// unused. A future build-info endpoint can re-instate them at a
// dedicated path (e.g. /debug/build-info) without disturbing the
// liveness contract.
//
// @Summary Liveness Endpoint
// @Description Liveness-only — returns 200 as long as the HTTP server is up.
// @ID health
// @Produce  json
// @Success 200 {object} serviceInfo
// @Router /health [get]
func HealthCheck(_ string, _ string, _ func() (string, error)) http.HandlerFunc {
	return func(wri http.ResponseWriter, _ *http.Request) {
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_, _ = wri.Write(aliveBody)
	}
}

// serviceInfo is preserved for swagger/back-compat with the prior
// /health payload shape. The active HealthCheck handler no longer
// populates it; a future build-info endpoint may.
type serviceInfo struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Build     string `json:"build"`
}
