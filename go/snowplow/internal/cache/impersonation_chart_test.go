package cache

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// impersonation_chart_test.go — #574 (chart half): the RBAC grant the code in
// impersonation.go is inert without, and the empty-list trap that grant carries.
//
// WHY THIS FILE EXISTS. The Go half of #574 shipped in 1.12.42 and did nothing
// in production for three days, because the ClusterRole never granted
// `impersonate`. The startup probe answered false, the refresher kept the SA
// transport, and krateo-057 logged 9,433 re-gate denials in six hours. No Go
// test could have caught that: the defect was entirely in the chart. These arms
// read the shipped chart, exactly as internal/tracing/chart_sampler_test.go does
// for the sampler values after the same class of miss.
//
// THE TRAP THE SECOND ARM GUARDS. In Kubernetes RBAC an absent or empty
// `resourceNames` means EVERY resource of that type. So a groups rule rendered
// unconditionally from an empty allowlist does not grant nothing — it grants
// impersonation of every group on the cluster, including one bound to
// cluster-admin. The template must therefore omit the rule entirely when the
// list is empty, which is what the `with` blocks do. Someone "simplifying" those
// blocks away would silently turn the safe default into a cluster-admin path,
// and `helm lint` / `helm template` would both still pass.
//
// WHAT THESE ARMS DO AND DO NOT PROVE. They read the template as text, so they
// prove the guard is PRESENT, not that Helm renders it correctly. The render
// itself is verified in the PR body against both an empty and a populated
// values file; this is the regression guard, not that verification.

func chartPath(t *testing.T, parts ...string) string {
	t.Helper()
	// go test runs with cwd = the package dir: go/snowplow/internal/cache.
	base := []string{"..", "..", "..", "..", "helm", "snowplow"}
	return filepath.Join(append(base, parts...)...)
}

// TestImpersonation_ChartGrantsUsers — the grant that makes the feature live at
// all. Without it the startup probe answers false and everything in
// impersonation.go is dead code in production.
func TestImpersonation_ChartGrantsUsers(t *testing.T) {
	path := chartPath(t, "templates", "clusterrole.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	src := string(raw)

	if !strings.Contains(src, `verbs: ["impersonate"]`) {
		t.Fatalf("#574: the ClusterRole grants no `impersonate` verb at all.\n"+
			"The Go half (impersonation.go, the refresher's impersonating dial, the startup probe) "+
			"is INERT without it: the probe answers false, the refresher keeps snowplow's SA "+
			"transport, and every per-user-cohort background refresh stays re-gated. That is not a "+
			"hypothetical — it is what 1.12.42 shipped, and it cost 9,433 refused refreshes in 6h "+
			"on krateo-057.\nchart: %s", path)
	}

	// The users rule must be UNCONDITIONAL. It is the one the probe asks for, so
	// hiding it behind a value would make the feature opt-in and reintroduce the
	// silently-inert state this file exists to prevent.
	usersRule := regexp.MustCompile(`(?m)^- apiGroups: \[""\]\n  resources: \["users"\]\n  verbs: \["impersonate"\]`)
	if !usersRule.MatchString(src) {
		t.Fatalf("#574: the unconditional `impersonate users` rule is missing or was made "+
			"conditional.\nIt must stay unconditional: impersonating a user does not inherit that "+
			"user's groups (the apiserver uses exactly the groups supplied), so it cannot escalate, "+
			"and 1000 users cannot be enumerated. It is also the exact permission the startup probe "+
			"SelfSubjectAccessReviews for.\nchart: %s", path)
	}
}

// TestImpersonation_EscalatingGrantsAreAllowlistedAndDefaultEmpty — the trap.
func TestImpersonation_EscalatingGrantsAreAllowlistedAndDefaultEmpty(t *testing.T) {
	tplPath := chartPath(t, "templates", "clusterrole.yaml")
	raw, err := os.ReadFile(tplPath)
	if err != nil {
		t.Fatalf("read %s: %v", tplPath, err)
	}
	src := string(raw)

	// Each escalating resource must appear ONLY inside a `with` block over its
	// values list, and must carry resourceNames. `with` is what skips the rule
	// for an empty list; without it, empty renders as "every one of them".
	for _, c := range []struct{ resource, value string }{
		{"groups", "impersonateGroups"},
		{"serviceaccounts", "impersonateServiceAccounts"},
	} {
		marker := `resources: ["` + c.resource + `"]`
		idx := strings.Index(src, marker)
		if idx < 0 {
			continue // not granted at all — strictly safe, nothing to guard
		}
		guard := `{{- with .Values.rbac.` + c.value + ` }}`
		gIdx := strings.LastIndex(src[:idx], guard)
		if gIdx < 0 {
			t.Fatalf("#574 SECURITY: the `%s` impersonate rule is not guarded by %s.\n"+
				"An empty resourceNames in Kubernetes RBAC means EVERY resource of that type, so an "+
				"unguarded rule rendered from the empty default would let snowplow impersonate every "+
				"%s on the cluster — including one bound to cluster-admin. The `with` block is what "+
				"omits the rule entirely when the list is empty.\nchart: %s", c.resource, guard, c.resource, tplPath)
		}
		// resourceNames must sit between the guard and the verbs, i.e. in this rule.
		seg := src[gIdx:]
		if end := strings.Index(seg, "{{- end }}"); end > 0 {
			seg = seg[:end]
		}
		if !strings.Contains(seg, "resourceNames:") {
			t.Fatalf("#574 SECURITY: the `%s` impersonate rule carries no resourceNames.\n"+
				"Without it the rule grants impersonation of every %s on the cluster regardless of "+
				"the allowlist.\nchart: %s", c.resource, c.resource, tplPath)
		}
	}

	// And the shipped defaults must be empty, so a fresh install cannot
	// impersonate anything that carries rights directly.
	valsPath := chartPath(t, "values.yaml")
	vraw, err := os.ReadFile(valsPath)
	if err != nil {
		t.Fatalf("read %s: %v", valsPath, err)
	}
	var values struct {
		RBAC struct {
			ImpersonateGroups          []string `json:"impersonateGroups"`
			ImpersonateServiceAccounts []string `json:"impersonateServiceAccounts"`
		} `json:"rbac"`
	}
	if err := yaml.Unmarshal(vraw, &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	if n := len(values.RBAC.ImpersonateGroups); n != 0 {
		t.Fatalf("#574 SECURITY: chart ships rbac.impersonateGroups with %d entries, want 0.\n"+
			"A group carries its rights directly, so the shipped default must grant none and let the "+
			"operator opt in per cluster. Unlisted cohorts are simply not background-refreshed — they "+
			"expire at TTL and refill on read, the accepted cost in the #574 ruling.\nchart: %s", n, valsPath)
	}
	if n := len(values.RBAC.ImpersonateServiceAccounts); n != 0 {
		t.Fatalf("#574 SECURITY: chart ships rbac.impersonateServiceAccounts with %d entries, want 0.\n"+
			"A ServiceAccount carries its rights directly, exactly as a group does.\nchart: %s", n, valsPath)
	}
}
