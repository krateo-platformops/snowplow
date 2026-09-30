// self_host_test.go — #57 C-b/C-c falsifier for the exact self-host predicate
// + the bearer-append decision.
//
// HARD (C-b): parsedHostEqualsSelf is EXACT scheme+host+port equality — NEVER
// strings.Contains / suffix / prefix. C-c arm-2: a genuine EXTERNAL endpoint
// whose host CONTAINS "snowplow" (snowplow-foo.example.com) must NOT match, so
// the bearer is never appended to it (the JWT-leak guard). This test is the
// structural proof of that — a substring matcher would FAIL the near-miss case.
// C-c arm-1 (light): TestBearerAppendForStage_SelfLoopbackArm proves the
// append-DECISION wiring (self-loopback→append, external→no-append) without the
// heavy resolveRun harness; the full functional proof is the on-cluster gate.
//
// C-d (shared-shell narrow re-narrow) — DISCHARGED by #58's merged arm, NOT
// duplicated here. The #57 seed populates the SAME identity-free apistage
// content cells the request path serves; C-d's claim is "a narrow requester
// Get-hitting an SA-maximal shared cell is re-narrowed to its own scope at
// serve time, never over-served the seed's broader view." That is EXACTLY what
// TestFalsifier58_NoUAF_NarrowTenantGetsOnlyOwnNamespaces (shipped in 1.5.7,
// apistage_list_overserve_falsifier_test.go) asserts: an SA-maximal cell served
// under a narrow identity returns ONLY the granted namespaces. The #58
// UAF-aware serve gate IS the C-d mechanism — so #57's seed-populated cells
// inherit the same gated-serve re-narrowing. No separate #57 test needed.

package api

import (
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func TestParsedHostEqualsSelf_ExactMatchOnly(t *testing.T) {
	const self = "http://snowplow.krateo-system.svc.cluster.local:8081"
	if !SetSelfHost(self) {
		t.Fatalf("SetSelfHost(%q) should succeed", self)
	}
	t.Cleanup(func() { SetSelfHost("") })

	cases := []struct {
		name   string
		rawURL string
		want   bool
		why    string
	}{
		{"exact self", self, true, "the configured self-loopback host"},
		{"exact self, trailing path differs", "http://snowplow.krateo-system.svc.cluster.local:8081/call?x=1", true,
			"path/query are NOT part of the (scheme,host,port) key — a /call on the self host still matches"},
		{"C-c near-miss: external host CONTAINS snowplow", "http://snowplow-foo.example.com:8081", false,
			"substring 'snowplow' must NOT match — exact host equality, the JWT-leak guard"},
		{"near-miss: subdomain of self", "http://evil.snowplow.krateo-system.svc.cluster.local:8081", false,
			"a host with the self host as a SUFFIX must NOT match"},
		{"near-miss: self as prefix", "http://snowplow.krateo-system.svc.cluster.local.evil.com:8081", false,
			"a host with the self host as a PREFIX must NOT match"},
		{"wrong port", "http://snowplow.krateo-system.svc.cluster.local:9999", false,
			"port mismatch → not self (exact port equality)"},
		{"wrong scheme", "https://snowplow.krateo-system.svc.cluster.local:8081", false,
			"scheme mismatch → not self"},
		{"genuine external", "https://api.github.com/repos/foo/bar", false, "an unrelated external endpoint"},
		{"empty", "", false, "empty URL never matches"},
		{"unparseable", "://::not a url", false, "unparseable URL never matches"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parsedHostEqualsSelf(tc.rawURL); got != tc.want {
				t.Fatalf("parsedHostEqualsSelf(%q) = %v, want %v — %s", tc.rawURL, got, tc.want, tc.why)
			}
		})
	}
}

// TestParsedHostEqualsSelf_SvcShortFQDNEquivalence is the #57 fix-B falsifier:
// the self-loopback arm MUST fire when the resolved endpoint host is the SHORT
// in-cluster Service form of the SAME service the FQDN URL_SELF names (and vice
// versa) — because URL_SELF is snowplow-chart-owned (FQDN) while the
// snowplow-endpoint the composition-resources RAs loop back through is
// portal-chart-owned (short svc form). Pre-fix (exact-string only) the RED arms
// below returned false → those loopbacks 401'd. The GUARD arms prove fix B did
// NOT weaken the leak guard: near-misses + different name/ns stay OFF.
//
// RED-proven: with the exact-string-only predicate, the "self FQDN vs candidate
// short" and "self short vs candidate FQDN" arms FAIL (want true, got false) —
// they discriminate fix B from the status quo. The GUARD arms pass either way;
// a suffix/Contains mis-implementation of B would FAIL them.
func TestParsedHostEqualsSelf_SvcShortFQDNEquivalence(t *testing.T) {
	const (
		fqdn  = "http://snowplow.krateo-system.svc.cluster.local:8081"
		short = "http://snowplow.krateo-system.svc:8081"
		twoL  = "http://snowplow.krateo-system:8081" // name.namespace (2-label short form)
	)

	// selfFQDN: URL_SELF is the FQDN (the chart default) — a short-form or
	// 2-label candidate of the SAME svc MUST match (the #57 fix-B positive).
	t.Run("self=FQDN", func(t *testing.T) {
		if !SetSelfHost(fqdn) {
			t.Fatalf("SetSelfHost(%q) failed", fqdn)
		}
		t.Cleanup(func() { SetSelfHost("") })
		cases := []struct {
			name   string
			rawURL string
			want   bool
			why    string
		}{
			{"RED: short svc form of same svc → MATCH", short, true,
				"the portal endpoint uses the short svc form; fix B must recognize it as self"},
			{"RED: 2-label name.ns of same svc → MATCH", twoL, true,
				"name.namespace is a valid in-cluster short form of the same service"},
			{"FQDN self → MATCH", fqdn, true, "exact-string still matches"},
			// GUARD arms — must stay OFF (leak guard preserved):
			{"GUARD: external contains snowplow → NO", "http://snowplow-foo.example.com:8081", false,
				"substring 'snowplow' is not a canonical svc form → exact-string fallback → no match"},
			{"GUARD: self as subdomain suffix → NO", "http://evil.snowplow.krateo-system.svc.cluster.local:8081", false,
				"labels[0]=evil,[1]=snowplow → different (name,ns) AND non-canonical trailing → no match"},
			{"GUARD: self as prefix + garbage → NO", "http://snowplow.krateo-system.svc.cluster.local.evil.com:8081", false,
				"trailing labels include evil/com → not an allowed svc trailing set → exact-string fallback → no match"},
			{"GUARD: different svc name, same ns → NO", "http://other.krateo-system.svc:8081", false,
				"name mismatch → not the same service"},
			{"GUARD: same name, different ns → NO", "http://snowplow.other-ns.svc:8081", false,
				"namespace mismatch → not the same service"},
			{"GUARD: wrong port on short form → NO", "http://snowplow.krateo-system.svc:9999", false,
				"port is still exact-equality — svc equivalence never relaxes scheme/port"},
			{"GUARD: wrong scheme on short form → NO", "https://snowplow.krateo-system.svc:8081", false,
				"scheme is still exact-equality"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if got := parsedHostEqualsSelf(tc.rawURL); got != tc.want {
					t.Fatalf("parsedHostEqualsSelf(%q) = %v, want %v — %s", tc.rawURL, got, tc.want, tc.why)
				}
			})
		}
	})

	// selfShort: URL_SELF is the SHORT form — the FQDN candidate of the same svc
	// MUST match (symmetry; a cluster could configure URL_SELF either way).
	t.Run("self=short", func(t *testing.T) {
		if !SetSelfHost(short) {
			t.Fatalf("SetSelfHost(%q) failed", short)
		}
		t.Cleanup(func() { SetSelfHost("") })
		if !parsedHostEqualsSelf(fqdn) {
			t.Fatalf("RED: self=short, candidate=FQDN of same svc must MATCH (symmetry)")
		}
		if !parsedHostEqualsSelf(twoL) {
			t.Fatalf("RED: self=short, candidate=name.ns of same svc must MATCH")
		}
		if parsedHostEqualsSelf("http://snowplow-foo.example.com:8081") {
			t.Fatalf("GUARD: external near-miss must NOT match even when self is short-form")
		}
	})
}

// TestParsedHostEqualsSelf_UnconfiguredNeverMatches: with no self-host set
// (URL_SELF empty), the self-loopback arm is disabled — the predicate is
// always false, so the bearer-append falls back to the pre-#57 gate exactly.
func TestParsedHostEqualsSelf_UnconfiguredNeverMatches(t *testing.T) {
	SetSelfHost("") // clear
	t.Cleanup(func() { SetSelfHost("") })
	if parsedHostEqualsSelf("http://snowplow.krateo-system.svc.cluster.local:8081") {
		t.Fatalf("unconfigured self-host must never match (self-loopback arm disabled)")
	}
}

// TestBearerAppendForStage_SelfLoopbackArm is the #57 C-c arm-1 LIGHT wiring
// proof (the positive "bearer IS appended on a self-loopback step" half) —
// the append-DECISION in isolation, without the heavy resolveRun/runStage
// harness. The full functional proof (the authenticated loopback actually
// warms composition-resources) is the on-cluster post-deploy gate. ARM-2 (the
// leak-guard near-miss) is in TestParsedHostEqualsSelf_ExactMatchOnly.
func TestBearerAppendForStage_SelfLoopbackArm(t *testing.T) {
	const self = "http://snowplow.krateo-system.svc.cluster.local:8081"
	if !SetSelfHost(self) {
		t.Fatalf("SetSelfHost failed")
	}
	t.Cleanup(func() { SetSelfHost("") })

	namedRef := &templates.Reference{Name: "snowplow-endpoint", Namespace: "krateo-system"}
	selfEP := endpoints.Endpoint{ServerURL: self}
	extEP := endpoints.Endpoint{ServerURL: "https://snowplow-foo.example.com:8081"} // CONTAINS "snowplow", not self

	cases := []struct {
		name    string
		apiCall *templates.API
		ep      endpoints.Endpoint
		isSA    bool
		want    bool
		why     string
	}{
		{"self-loopback w/ named ref → APPEND", &templates.API{EndpointRef: namedRef}, selfEP, false, true,
			"the #57 arm: a named-endpoint step that resolves to the self-host needs the seed bearer"},
		{"external near-miss host w/ named ref → NO append", &templates.API{EndpointRef: namedRef}, extEP, false, false,
			"the leak guard: a genuine external endpoint (host merely contains 'snowplow') must NOT get the bearer"},
		{"#292 no EndpointRef (isSA=false) → NO append (dropped disjunct)", &templates.API{}, extEP, false, false,
			"#292: the bare EndpointRef==nil disjunct is dropped — a nil ref is an apiserver-credential (SA or " +
				"clientconfig) dial that never carries the user Krateo JWT (leak/suppress); ExportJWT + self-loopback " +
				"remain the only append triggers"},
		{"named ref + ExportJWT → APPEND (original gate)", &templates.API{EndpointRef: namedRef, ExportJWT: ptr.To(true)}, extEP, false, true,
			"the original gate: exportJWT:true forces the bearer even on a named external endpoint"},
		{"named external ref, no exportJWT, not self → NO append", &templates.API{EndpointRef: namedRef}, endpoints.Endpoint{ServerURL: "https://api.github.com"}, false, false,
			"a genuine external named endpoint carries its own auth — no user bearer"},
		{"nil apiCall → NO append (defensive)", nil, selfEP, false, false, "nil guard"},
		{"#271 SA dial + nil ref → NO append (isSA gate)", &templates.API{}, selfEP, true, false,
			"#271: isSA overrides the nil-ref append trigger — an apiserver SA dial never carries the user Krateo JWT"},
		{"#271 SA dial + ExportJWT → NO append (isSA gate)", &templates.API{ExportJWT: ptr.To(true)}, selfEP, true, false,
			"#271: isSA overrides ExportJWT:true — provenance beats the export flag on an SA dial"},
		{"#271 SA dial + self-loopback → NO append (isSA gate)", &templates.API{EndpointRef: namedRef}, selfEP, true, false,
			"#271: isSA overrides the #57 self-loopback trigger — an SA dial never carries the user JWT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bearerAppendForStage(tc.apiCall, tc.ep, tc.isSA); got != tc.want {
				t.Fatalf("bearerAppendForStage = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// TestSetSelfHost_PortDefaulting: a URL with no explicit port has port "" —
// it matches only another no-explicit-port URL of the same scheme+host (we do
// NOT infer 80/443; exact string equality of url.Port()). Documents the
// contract so the chart's URL_SELF must carry the explicit :8081.
func TestSetSelfHost_PortDefaulting(t *testing.T) {
	if !SetSelfHost("http://snowplow.krateo-system.svc:8081") {
		t.Fatalf("set should succeed")
	}
	t.Cleanup(func() { SetSelfHost("") })
	// No-port variant does NOT match the :8081 self (Port() "" != "8081").
	if parsedHostEqualsSelf("http://snowplow.krateo-system.svc") {
		t.Fatalf("a no-port URL must not match the :8081 self-host (exact port equality)")
	}
}
