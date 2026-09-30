package api

// endpoints_tls_233.go — #233. The CAPABILITY delegation in httpClientForEndpoint
// (endpoints_tls.go: caPEMBytes could not parse an endpoint-owned CA) was
// previously SILENT: the operator saw only plumbing's x509:unknown-authority with
// nothing saying snowplow read the bundle and could not parse it. This makes that
// delegation visible and bounded. It does NOT touch the POLICY delegation (the
// declarative "not our shape" site) — the two are deliberately distinct.

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/endpoints"
)

// unparseableCADelegationTotal is the monotonic, UNCAPPED count of unparseable-CA
// delegations (the endpoint resolves per stage per /call). It is the OTLP
// DETECTOR (snowplow_unparseable_ca_delegations_total): non-zero = a broken CA
// bundle, which is alertable. Deliberately INDEPENDENT of the one-shot WARN
// below, so the rate is never bounded even when the log is suppressed at its cap
// (#233 pm-freshness C1 — the log/counter split is what makes the cap safe).
var unparseableCADelegationTotal atomic.Uint64

// UnparseableCADelegationTotal exposes the counter for the OTLP callback in
// internal/metrics. Import direction is one-way (metrics -> api); api does not
// import internal/metrics, mirroring the cache.* accessors.
func UnparseableCADelegationTotal() uint64 { return unparseableCADelegationTotal.Load() }

// unparseableCAWarnCap bounds the distinct (ServerURL, CA-fingerprint) set that
// suppresses duplicate WARNs. A structural cap, not an env knob; on overflow the
// set is dropped so a NEW bad CA still warns — the cap fails toward VISIBILITY,
// never toward silence (#233 pm-freshness C1). The counter above is never bounded.
const unparseableCAWarnCap = 256

var (
	unparseableCAMu     sync.Mutex
	unparseableCAWarned = map[string]struct{}{}
)

// warnUnparseableCADelegation bumps the uncapped detector counter and emits a
// bounded one-shot WARN keyed by (ServerURL, sha256(CAData)). The fingerprint key
// re-warns on a CHANGED-but-still-bad CA (failed-fix visibility) while suppressing
// the SAME (flood). It logs ServerURL + the sha256 FINGERPRINT (the same hex it
// keys on) + the x509 consequence, NEVER the raw CA bytes: the endpoints that
// reach this branch are owned-CA apiserver dials (endpointNeedsOwnedCAClient) —
// ServerURL is the endpoint URL and CertificateAuthorityData the public cluster
// CA, neither a tenant credential; the token is the secret and is never logged.
func warnUnparseableCADelegation(ep *endpoints.Endpoint) {
	unparseableCADelegationTotal.Add(1) // uncapped rate — the detector, always

	sum := sha256.Sum256([]byte(ep.CertificateAuthorityData))
	fp := hex.EncodeToString(sum[:])
	key := ep.ServerURL + "\x00" + fp

	unparseableCAMu.Lock()
	_, seen := unparseableCAWarned[key]
	if !seen {
		if len(unparseableCAWarned) >= unparseableCAWarnCap {
			// Overflow: drop the set so a NEW bad CA still warns (fail toward
			// visibility). Worst case is re-warning a previously-seen bad CA — the
			// safe direction. The counter above is untouched.
			unparseableCAWarned = map[string]struct{}{}
		}
		unparseableCAWarned[key] = struct{}{}
	}
	unparseableCAMu.Unlock()

	if seen {
		return
	}
	slog.Warn("endpoints_tls.unparseable_ca_delegated",
		slog.String("subsystem", "cache"),
		slog.String("server_url", ep.ServerURL),
		slog.String("ca_sha256", fp),
		slog.String("effect", "CertificateAuthorityData is present and this is the endpoint-owned-CA shape, "+
			"but it is UNPARSEABLE (neither raw PEM nor (double-)base64 PEM) — delegated to plumbing; if "+
			"plumbing cannot resolve it the dial fails x509:unknown-authority. Fix the CA bundle."))
}

// resetUnparseableCAStateForTest clears the #233 counter + one-shot set.
func resetUnparseableCAStateForTest() {
	unparseableCADelegationTotal.Store(0)
	unparseableCAMu.Lock()
	unparseableCAWarned = map[string]struct{}{}
	unparseableCAMu.Unlock()
}

// RecordUnparseableCADelegationForTest bumps the #233 counter — support for the
// OTLP-callback registration test in internal/metrics (mirrors the ForTest
// recorders those tests use for other counters, e.g. dispatchers.RecordL1LookupForTest).
func RecordUnparseableCADelegationForTest() { unparseableCADelegationTotal.Add(1) }

// ResetUnparseableCADelegationForTest zeroes the #233 counter + one-shot set,
// exported for the internal/metrics OTLP-registration test.
func ResetUnparseableCADelegationForTest() { resetUnparseableCAStateForTest() }
