// self_host.go — #57 (C-b): the configured SELF-LOOPBACK host the
// bearer-append's self-loopback arm compares a resolved endpoint against.
//
// A prewarm seed (or any caller) whose api-step is a literal `/call?...`
// loopback with the named `snowplow-endpoint` EndpointRef resolves to
// snowplow's OWN JWT-gated `/call` — so the seed's authn bearer MUST be
// appended even though a named EndpointRef is present (the original gate
// suppresses it for named endpoints, assuming they carry their own auth —
// true for a genuine external endpoint, false for the self-loopback). The
// self-host is the discriminator.
//
// HARD (C-b): the comparison is EXACT (scheme, port) field-equality against the
// PARSED URL_SELF plus a HOST match that is EITHER exact-string OR — when BOTH
// hosts are in-cluster Kubernetes Service DNS names of the SAME (name,
// namespace) — svc short/FQDN equivalence (#57 fix B, host-form-agnostic). It
// is NEVER strings.Contains / suffix / prefix. An external endpoint whose host
// merely CONTAINS "snowplow" (snowplow-foo.example.com) must NOT match → the
// bearer stays off it. No resource/name/path literal — a uniform host predicate
// (feedback_no_special_cases).
//
// #57 FIX B (host-form normalization) — WHY: URL_SELF is snowplow-chart-owned
// (the FQDN form `snowplow.krateo-system.svc.cluster.local:8081`) while the
// snowplow-endpoint the composition-resources RAs loop back through is
// PORTAL-chart-owned (the short form `snowplow.krateo-system.svc:8081`). The two
// charts are mutually blind, so the exact-string predicate never fired for the
// short form → those loopbacks got no seed bearer → 401. Normalizing the
// canonical in-cluster Service DNS forms of the SAME (name, namespace) makes
// snowplow recognize its own service regardless of which form the endpoint uses.
// Mirrors the existing endpoints.go internal-host normalization precedent
// (isInternal → ServerURL rewritten to https://kubernetes.default.svc). The
// leak guard is PRESERVED: any host that is not the canonical svc-DNS shape of
// the SAME name+ns falls back to EXACT string equality.
package api

import (
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// selfHostKey is the (scheme, host, port) tuple parsed once from URL_SELF.
// host is url.URL.Hostname() (no port); port is url.URL.Port() (the explicit
// port string, "" when absent). Compared field-by-field — exact equality.
type selfHostKey struct {
	scheme string
	host   string
	port   string
}

// selfHost holds the configured self-loopback host, parsed once at startup.
// atomic.Pointer so SetSelfHost (startup / tests) and the read in
// parsedHostEqualsSelf are race-free without a mutex. nil = unconfigured →
// the self-loopback arm never fires (pre-#57 behaviour; the bearer-append
// gate is then exactly the original EndpointRef==nil||ExportJWT).
var selfHost atomic.Pointer[selfHostKey]

// SetSelfHost parses rawURL (e.g. http://snowplow.krateo-system.svc.cluster.local:8081)
// and installs it as the self-loopback host. An empty or unparseable rawURL
// CLEARS it (self-loopback arm disabled). Wired once at startup from main.go
// (URL_SELF env) and by tests. Returns true when a non-empty host was set.
func SetSelfHost(rawURL string) bool {
	if rawURL == "" {
		selfHost.Store(nil)
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		selfHost.Store(nil)
		return false
	}
	selfHost.Store(&selfHostKey{scheme: u.Scheme, host: u.Hostname(), port: u.Port()})
	return true
}

// svcCanonicalKey reports whether host is an in-cluster Kubernetes Service DNS
// name and, if so, returns its canonical (name, namespace) identity. The
// recognized shapes are EXACTLY (labels split on '.'):
//   - name.namespace                       (2 labels)
//   - name.namespace.svc                   (3 labels, trailing "svc")
//   - name.namespace.svc.cluster.local     (5 labels, trailing "svc.cluster.local")
//
// Any other shape (an external FQDN, a subdomain-of-self leak attempt, a
// trailing-garbage prefix attempt, a bare hostname) returns ok=false, so the
// caller falls back to exact-string host equality — the leak guard.
//
// This is DELIBERATELY NOT a suffix/prefix/Contains match:
//   - evil.snowplow.krateo-system.svc.cluster.local → labels[0]="evil" (name),
//     [1]="snowplow" (ns), trailing {"krateo-system","svc","cluster","local"}
//     is NOT an allowed trailing set → ok=false → exact-string fallback → no match.
//   - snowplow.krateo-system.svc.cluster.local.evil.com → trailing set includes
//     "evil"/"com" → not allowed → ok=false → exact-string fallback → no match.
//   - snowplow-foo.example.com → trailing {"com"} → not allowed → ok=false.
//
// Only the SAME (name, namespace) with a canonical svc trailing set collapses.
func svcCanonicalKey(host string) (name, namespace string, ok bool) {
	labels := strings.Split(host, ".")
	switch len(labels) {
	case 2: // name.namespace
	case 3: // name.namespace.svc
		if labels[2] != "svc" {
			return "", "", false
		}
	case 5: // name.namespace.svc.cluster.local
		if labels[2] != "svc" || labels[3] != "cluster" || labels[4] != "local" {
			return "", "", false
		}
	default:
		return "", "", false
	}
	if labels[0] == "" || labels[1] == "" {
		return "", "", false
	}
	return labels[0], labels[1], true
}

// hostsMatch reports whether candidate host a equals self host b — either by
// EXACT string equality, or (#57 fix B) when BOTH are canonical in-cluster
// Service DNS names of the SAME (name, namespace). Exact-string is the leak-guard
// fallback for anything that is not the recognized svc-DNS shape.
func hostsMatch(a, b string) bool {
	if a == b {
		return true
	}
	an, ans, aok := svcCanonicalKey(a)
	bn, bns, bok := svcCanonicalKey(b)
	return aok && bok && an == bn && ans == bns
}

// parsedHostEqualsSelf reports whether rawURL matches the configured self-host:
// EXACT scheme+port equality, plus a host match that is exact-string OR svc
// short/FQDN equivalence of the SAME (name, namespace) — #57 fix B. False when
// self-host is unconfigured, rawURL is unparseable, or on any scheme/port
// mismatch or a host that is neither the exact string nor the same in-cluster
// Service (a substring/suffix near-miss like snowplow-foo.example.com returns
// false — the JWT-leak guard).
func parsedHostEqualsSelf(rawURL string) bool {
	sh := selfHost.Load()
	if sh == nil || rawURL == "" {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme == sh.scheme && u.Port() == sh.port && hostsMatch(u.Hostname(), sh.host)
}

// bearerAppendForStage is the #57 (C-b) user-bearer-append DECISION for a
// non-UAF stage, extracted as a pure function so the gate wiring is unit-
// testable without the resolveRun/runStage harness (C-c arm-1 light coverage;
// the on-cluster post-deploy gate is the full functional proof). Callers gate
// it under `!uafActive && accessToken != ""` (resolve.go) — this only decides
// the WHICH-endpoint half.
//
// True (append the bearer) when EITHER:
//   - the original gate: no named EndpointRef, or ExportJWT==true (a named
//     endpoint is assumed to carry its own auth); OR
//   - the #57 self-loopback arm (ADDITIVE): the resolved endpoint host EXACTLY
//     equals the configured self-host (parsedHostEqualsSelf — exact
//     scheme+host+port, never a substring near-miss), i.e. the step loops back
//     at snowplow's OWN JWT-gated /call and needs the seed bearer despite its
//     named snowplow-endpoint ref.
//
// False for a genuine EXTERNAL named endpoint (incl. a host that merely
// CONTAINS "snowplow") — the bearer stays off it (the JWT-leak guard).
func bearerAppendForStage(apiCall *templates.API, ep endpoints.Endpoint, isSA bool) bool {
	if apiCall == nil {
		return false
	}
	// #271: never append the user's Krateo authn JWT to a dial of snowplow's own
	// SA endpoint (the apiserver). The apiserver accepts only k8s tokens; a Krateo
	// RS256 JWT on this dial suppresses the SA BearerTokenFile token (client-go's
	// bearer round-tripper won't overwrite an existing Authorization header,
	// transport/round_trippers.go) → the SAR reaches the apiserver bearing a
	// non-k8s JWT → 401, and the JWT leaks to the apiserver. Keyed on PROVENANCE
	// (isSA), never shape — a token-auth clientconfig is shape-identical to the SA
	// endpoint (the #267/#268 discipline). NOT redundant with the caller's
	// !uafActive gate: the #271 bug is a NON-UAF nil-ref stage that resolves to the
	// SA endpoint via an internal (prewarm) driver.
	if isSA {
		return false
	}
	return apiCall.EndpointRef == nil ||
		ptr.Deref(apiCall.ExportJWT, false) ||
		parsedHostEqualsSelf(ep.ServerURL)
}
