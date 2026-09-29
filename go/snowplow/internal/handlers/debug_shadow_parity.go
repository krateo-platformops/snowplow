package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// shadowParityBody is the JSON body both shadow-parity routes return: the one
// boolean that is the whole of the toggle's observable state. No per-identity
// data, no cache bodies — the toggle gates a dark measurement, nothing keyed.
type shadowParityBody struct {
	Enabled bool `json:"enabled"`
}

// DebugShadowParityGet is the read half of the v7 Step-2 shadow-parity toggle
// surface (#272).
//
// GET /debug/shadow-parity → 200 {"enabled": <rbac.ShadowParityEnabled()>}
//
// PURE READ. One atomic Load, no mutation, no request body, no per-identity
// data. It works cache-on AND cache-off: the toggle is a process-local flag in
// package rbac with no cache dependency, so this reports its state regardless of
// the cache subsystem flag (unlike /debug/store, it has nothing to read from the
// informer store).
//
// PROCESS-LOCAL (per-pod). The toggle it reports is a process-local atomic with
// no env/config backing (rbac/shadow_hook.go): each pod carries its own state
// and a restart resets it to default-off. A measurement that toggles this MUST
// therefore pin to a single pod — a multi-pod Deployment would report and flip
// whichever replica the request happened to land on (#272 acceptance C4).
//
// GATE. Mounted behind the SAME authn-only /debug gate as its siblings
// (debug_routes.go, middleware.RefreshAuth). That gate is authn-only: any valid
// Krateo JWT can read — and, via the POST sibling, flip — this toggle. That is a
// deliberate, accepted blast radius: the shadow-parity hook is read-only and
// panic-isolated and changes no verdict, no served byte and no cache key
// (rbac/shadow_hook.go), so flipping it perturbs no served traffic — it only
// turns the dark parity MEASUREMENT on or off.
//
// @Summary Read the v7 shadow-parity toggle
// @Description Read-only report of the process-local, default-off shadow-parity observability toggle. Pure atomic read; mutates nothing; works cache-on and cache-off. The toggle is per-pod, so a measurement that flips it must pin to one pod.
// @ID debug-shadow-parity-get
// @Produce json
// @Success 200 {object} shadowParityBody
// @Router /debug/shadow-parity [get]
func DebugShadowParityGet() http.HandlerFunc {
	return func(wri http.ResponseWriter, _ *http.Request) {
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(shadowParityBody{Enabled: rbac.ShadowParityEnabled()})
	}
}

// DebugShadowParitySet is the write half of the shadow-parity toggle surface
// (#272).
//
// POST /debug/shadow-parity?enabled=true|false → 200 {"enabled": <live state>}
//
// The `enabled` query parameter is parsed with strconv.ParseBool. An ABSENT or
// UNPARSEABLE value is a 400, never a silent guess — the same "a diagnostic that
// answers about a different thing than the one asked for is worse than an error"
// convention debug_store.go's parseGVRParam applies. There is no request body.
//
// HONEST COMMITTED-STATE REPORT. After the write, the handler reads the toggle
// BACK and returns the LIVE state, not an echo of the input, so the response is
// what the process actually committed. (The two agree in the normal case; the
// read-back is the contract, not an optimisation.)
//
// PROCESS-LOCAL and AUTHN-ONLY: see DebugShadowParityGet — the same per-pod
// scoping (C4) and the same accepted authn-only blast radius apply. Flipping the
// toggle changes no served verdict/byte/key; it only enables or disables the
// dark parity measurement, whose on-cluster arm runs in a cache-ON window (the
// parity counters are cache-only expvars) (#272 acceptance C5).
//
// @Summary Flip the v7 shadow-parity toggle
// @Description Turn the process-local, default-off shadow-parity observability toggle on or off. `enabled` is parsed with strconv.ParseBool; absent or unparseable is a 400. Returns the committed live state read back after the write, not an echo. Per-pod; changes no served verdict/byte/key.
// @ID debug-shadow-parity-set
// @Produce json
// @Param enabled query string true "true|false (strconv.ParseBool)"
// @Success 200 {object} shadowParityBody
// @Failure 400 {string} string "missing or unparseable enabled"
// @Router /debug/shadow-parity [post]
func DebugShadowParitySet() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		v, err := strconv.ParseBool(req.URL.Query().Get("enabled"))
		if err != nil {
			http.Error(wri, "enabled must be a boolean (true|false)", http.StatusBadRequest)
			return
		}
		rbac.SetShadowParityEnabled(v)
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(shadowParityBody{Enabled: rbac.ShadowParityEnabled()})
	}
}
