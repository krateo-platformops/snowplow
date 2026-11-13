package cache

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// issue1334_public_identity_chart_test.go — #1334: the public identity is OFF
// by default and ONE chart value turns on both the switch the binary reads
// (PUBLIC_IDENTITY_ENABLED in the ConfigMap) and the only grant it needs:
// impersonate on users [krateo:public] and groups [system:unauthenticated], by
// resourceNames. Off, neither renders.
//
// Rendered by the real `helm template` (helm is required: SNOWPLOW_REQUIRE_HELM=1
// makes its absence a failure instead of a skip). The server-side dry-run of the
// same rendered ClusterRole is chartcrd's kind arm
// (TestIssue1334_Kind_PublicIdentityChartAdmitted).

func i1334RenderChart(t *testing.T, template string, args ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("SNOWPLOW_REQUIRE_HELM") == "1" {
			t.Fatal("helm not on PATH and SNOWPLOW_REQUIRE_HELM=1")
		}
		t.Skip("helm not on PATH — NOT RUN: the #1334 chart arms need helm")
	}
	dst := filepath.Join(t.TempDir(), "snowplow")
	if out, err := exec.Command("cp", "-R", chartPath(t), dst).CombinedOutput(); err != nil {
		t.Fatalf("copy chart: %v: %s", err, out)
	}
	chartYAML := filepath.Join(dst, "Chart.yaml")
	raw, err := os.ReadFile(chartYAML)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("CHART_VERSION"), []byte("0.0.0"))
	raw = bytes.ReplaceAll(raw, []byte("APP_VERSION"), []byte("0.0.0"))
	if err := os.WriteFile(chartYAML, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := append([]string{"template", "t", dst, "--namespace", "krateo-system", "--show-only", template}, args...)
	out, err := exec.Command(helm, cmd...).CombinedOutput()
	return string(out), err
}

func i1334ClusterRole(t *testing.T, args ...string) *rbacv1.ClusterRole {
	t.Helper()
	out, err := i1334RenderChart(t, "templates/clusterrole.yaml", args...)
	if err != nil {
		t.Fatalf("helm template clusterrole %v: %v\n%s", args, err, out)
	}
	var cr rbacv1.ClusterRole
	if err := yaml.Unmarshal([]byte(out), &cr); err != nil {
		t.Fatalf("rendered ClusterRole does not parse: %v\n%s", err, out)
	}
	return &cr
}

func i1334ConfigMapValue(t *testing.T, args ...string) (string, bool) {
	t.Helper()
	out, err := i1334RenderChart(t, "templates/configmap.yaml", args...)
	if err != nil {
		t.Fatalf("helm template configmap %v: %v\n%s", args, err, out)
	}
	var cm corev1.ConfigMap
	if err := yaml.Unmarshal([]byte(out), &cm); err != nil {
		t.Fatalf("rendered ConfigMap does not parse: %v\n%s", err, out)
	}
	v, ok := cm.Data["PUBLIC_IDENTITY_ENABLED"]
	return v, ok
}

// i1334PublicRules returns every impersonate rule that names a public-identity
// principal, or that could reach one (no resourceNames on users is the broad
// #574 users rule, which is reported separately by the caller).
func i1334PublicRules(cr *rbacv1.ClusterRole) (users, groups []rbacv1.PolicyRule) {
	for _, r := range cr.Rules {
		if !slices.Contains(r.Verbs, "impersonate") {
			continue
		}
		for _, res := range r.Resources {
			switch {
			case res == "users" && slices.Contains(r.ResourceNames, "krateo:public"):
				users = append(users, r)
			case res == "groups" && (len(r.ResourceNames) == 0 || slices.Contains(r.ResourceNames, "system:unauthenticated")):
				groups = append(groups, r)
			}
		}
	}
	return users, groups
}

// OFF (the shipped default, and an explicit false): no grant for the public
// identity, and the binary is told it is off.
func TestIssue1334_ChartPublicIdentityOff_RendersNoGrantAndSwitchOff(t *testing.T) {
	for _, args := range [][]string{nil, {"--set", "publicIdentity.enabled=false"}} {
		users, groups := i1334PublicRules(i1334ClusterRole(t, args...))
		if len(users) != 0 || len(groups) != 0 {
			t.Fatalf("publicIdentity off (%v) rendered a public-identity impersonate grant: users=%+v groups=%+v", args, users, groups)
		}
		v, ok := i1334ConfigMapValue(t, args...)
		if !ok || v != "false" {
			t.Fatalf("publicIdentity off (%v): ConfigMap PUBLIC_IDENTITY_ENABLED = %q (present=%v), want \"false\"", args, v, ok)
		}
	}
}

// ON: exactly the two rules, each restricted by resourceNames to exactly the
// one principal, and the switch on. Nothing else in the ClusterRole changes.
func TestIssue1334_ChartPublicIdentityOn_RendersExactlyTheRestrictedGrant(t *testing.T) {
	off := i1334ClusterRole(t)
	on := i1334ClusterRole(t, "--set", "publicIdentity.enabled=true")
	users, groups := i1334PublicRules(on)
	check := func(what string, rules []rbacv1.PolicyRule, resource, name string) {
		t.Helper()
		if len(rules) != 1 {
			t.Fatalf("publicIdentity on: want exactly one impersonate %s rule naming %s, got %+v", resource, name, rules)
		}
		r := rules[0]
		if !slices.Equal(r.APIGroups, []string{""}) || !slices.Equal(r.Resources, []string{resource}) ||
			!slices.Equal(r.ResourceNames, []string{name}) || !slices.Equal(r.Verbs, []string{"impersonate"}) {
			t.Fatalf("publicIdentity on: the %s rule must be apiGroups [\"\"], resources [%s], resourceNames [%s], verbs [impersonate]; got %+v",
				what, resource, name, r)
		}
	}
	check("users", users, "users", "krateo:public")
	check("groups", groups, "groups", "system:unauthenticated")
	if got, want := len(on.Rules), len(off.Rules)+2; got != want {
		t.Fatalf("publicIdentity on rendered %d rules, want the %d of the default plus exactly 2", got, len(off.Rules))
	}
	if !slices.EqualFunc(on.Rules[:len(off.Rules)], off.Rules, func(a, b rbacv1.PolicyRule) bool {
		return slices.Equal(a.Verbs, b.Verbs) && slices.Equal(a.Resources, b.Resources) &&
			slices.Equal(a.ResourceNames, b.ResourceNames) && slices.Equal(a.APIGroups, b.APIGroups)
	}) {
		t.Fatalf("publicIdentity on changed the default rules:\n on  %+v\n off %+v", on.Rules, off.Rules)
	}
	if v, ok := i1334ConfigMapValue(t, "--set", "publicIdentity.enabled=true"); !ok || v != "true" {
		t.Fatalf("publicIdentity on: ConfigMap PUBLIC_IDENTITY_ENABLED = %q (present=%v), want \"true\"", v, ok)
	}
}

// The switch cannot be set apart from its grant: env.PUBLIC_IDENTITY_ENABLED
// is refused, and a non-boolean enabled is refused by the schema.
func TestIssue1334_ChartPublicIdentity_SwitchOnlyThroughTheValue(t *testing.T) {
	out, err := i1334RenderChart(t, "templates/configmap.yaml", "--set-string", "env.PUBLIC_IDENTITY_ENABLED=true")
	if err == nil || !strings.Contains(out, "set publicIdentity.enabled") {
		t.Fatalf("env.PUBLIC_IDENTITY_ENABLED rendered (err=%v); it must be refused so the switch never ships without its grant\n%s", err, out)
	}
	for _, v := range []string{`"true"`, `1`, `{}`} {
		out, err := i1334RenderChart(t, "templates/clusterrole.yaml", "--set-json", "publicIdentity.enabled="+v)
		if err == nil {
			t.Fatalf("publicIdentity.enabled=%s rendered; the schema must accept only a boolean\n%s", v, out)
		}
		assertSchemaRejectedValue(t, "publicIdentity.enabled="+v, out)
	}
	out, err = i1334RenderChart(t, "templates/clusterrole.yaml", "--set", "publicIdentity.other=true")
	if err == nil {
		t.Fatalf("publicIdentity.other rendered; publicIdentity takes only enabled\n%s", out)
	}
	assertSchemaRejectedValue(t, "publicIdentity.other", out)
}

// The refresher allowlist still refuses system:unauthenticated (#631): the
// public identity's grant does not loosen it.
func TestIssue1334_ChartImpersonateGroupsStillRefusesUnauthenticated(t *testing.T) {
	out, err := i1334RenderChart(t, "templates/clusterrole.yaml",
		"--set", "publicIdentity.enabled=true", "--set-json", `rbac.impersonateGroups=["system:unauthenticated"]`)
	if err == nil {
		t.Fatalf("rbac.impersonateGroups=[system:unauthenticated] rendered with publicIdentity on\n%s", out)
	}
	assertSchemaRejectedValue(t, "rbac.impersonateGroups=[system:unauthenticated]", out)
}
